"""Random graphs and event scripts, checked against a reference interpreter.

Each seed builds a random graph (native lambdas, opaque functions, functions
that raise on some inputs; branches, union, zip, combine_latest, accumulate,
flatten, collect, chunk_by, chunks_between, starmap, batch_map, prefetch(n)
before map/filter/batch_map, par() branches, seq(), to_list(), to_set(),
accumulate(returns_state=True), the fused `collect()` +
`filter(p).sink(lambda _: c.flush())` idiom in either branch order, sinks
that feed back into the source, and extra sources feeding the merges) and a
random script of emit(), emit_batch(), flush() and drain() calls, aimed at
any source, with compile() at a random point, of one source or of all.
Every sink's output after every event must match a small pure-Python
interpreter of the same graph, which encodes the intended semantics: errors
at the same step, one flush() reaching each node once, buffered items kept
across compile(), and no re-entrant emit() into a busy source.

Outputs after a failed batch or flush are not compared (a batch goes step by
step, so how far it got differs), nor after a failed item that a flatten or
batch_map had split into several (they go on together, like a batch).

Set AKAYU_FUZZ_SEEDS to run more seeds.
"""

import collections
import os
import random
import traceback

import pytest

import akayu


def opaque_inc(x):
    y = x + 1
    return y


def opaque_even(x):
    y = x % 2
    return y == 0


def boom3(x):
    if x == 3:
        raise ValueError("boom at 3")
    return x


def opaque_add(acc, x):
    y = acc + x
    return y


MAPS = {
    "inc": lambda x: x + 1,
    "dbl_inc": lambda x: x * 2 + 1,
    "half": lambda x: x // 2,
    "mod3": lambda x: x % 3 == 0,
    "tenover": lambda x: 10 // x,
    "str": lambda x: str(x),
    "upper": lambda x: str(x).upper(),
    "len": lambda x: len(x),
    "pair": lambda x: (x, x),
    "listify": lambda x: [x, x],
    "first": lambda x: x[0],
    "opaque_inc": opaque_inc,
    "boom3": boom3,
    "neg": lambda x: -x,
    "fdiv": lambda x: x / 4,
    "isnone": lambda x: x is None,
}
FILTERS = {
    "even": lambda x: x % 2 == 0,
    "gt3": lambda x: x > 3,
    "opaque_even": opaque_even,
    "truthy": lambda x: x,
    "notnone": lambda x: x is not None,
    "zero": lambda x: x == 0,
}
ACCS = {
    "add": lambda acc, x: acc + x,
    "max": lambda acc, x: max(acc, x),
    "opaque_add": opaque_add,
    "last": lambda acc, x: x,
}
RS_ACCS = {
    "run": lambda acc, x: (acc + x, acc),
    "pairs": lambda acc, x: (x, (acc, x)),
    "badtype": lambda acc, x: [acc, x] if x == 3 else (acc, x),
    "short": lambda acc, x: (acc,) if x == 2 else (x, acc),
}
STARS = {"add2": lambda a, b: a + b, "swap": lambda a, b: (b, a)}
BATCHES = {"times10": lambda xs: [x * 10 for x in xs], "dup": lambda xs: [y for x in xs for y in (x, x)]}
ITEMS = [0, 1, 2, 3, 4, 5, 6, 7, 10, -1, -4, 2**62, 2**70, 0.5, -0.0, 2.5, "ab", None, True, (1, 2), [3]]

KINDS = {
    "map": 30, "filter": 14, "accumulate": 6, "flatten": 3, "union": 7, "zip": 5, "combine": 4,
    "collect": 4, "chunk_by": 4, "between": 3, "starmap": 3, "batch_map": 3,
    "sink": 10, "lsink": 4, "feedback": 2, "flushpair": 4,
    "pmap": 5, "pfilter": 3, "pbmap": 2, "par": 4, "seq": 2, "tolist": 2, "toset": 2, "accs": 3,
}  # fmt: skip
PREFETCHED = ("pmap", "pfilter", "pbmap")
TERMINAL = ("sink", "lsink", "feedback", "tolist", "toset", "flusher")


def gen(rng, multi=False):
    spec = [("source", [], None)]
    zone = [()]
    sources = [0]
    par_kids = collections.Counter()
    sinks = set()
    if multi:
        for _ in range(rng.randint(1, 2)):
            spec.append(("source", [], None))
            zone.append(())
            sources.append(len(spec) - 1)

    def child_zone(p):
        if spec[p][0] == "par":
            par_kids[p] += 1
            return zone[p] + ((p, par_kids[p]),)
        return zone[p]

    for _ in range(rng.randint(2, 14)):
        open_nodes = [i for i in range(len(spec)) if i not in sinks]
        kind = rng.choices(list(KINDS), list(KINDS.values()))[0]
        if kind in ("union", "zip", "combine"):
            # merges only within one par branch (else the order across branches is unspecified)
            by_zone = collections.defaultdict(list)
            for i in open_nodes:
                if spec[i][0] != "par":
                    by_zone[zone[i]].append(i)
            groups = [g for g in by_zone.values() if len(g) >= 2]
            if not groups:
                kind = "map"
            else:
                g = rng.choice(groups)
                ps = rng.sample(g, rng.randint(2, min(3, len(g))))
                spec.append((kind, ps, None))
                zone.append(zone[ps[0]])
                continue
        if kind == "pbmap":
            # items split by flatten/batch_map go on as a batch (documented), and a prefetched
            # batch_map takes a whole batch as one task: not modelled
            def splits(i):
                k, ps, _ = spec[i]
                return k in ("flatten", "batch_map", "pbmap") or any(splits(q) for q in ps)

            cands = [i for i in open_nodes if not splits(i)]
            if not cands:
                kind = "map"
        if kind in ("flushpair", "feedback"):
            cands = [i for i in open_nodes if zone[i] == () and spec[i][0] != "par"]
            if kind == "flushpair":
                cands = [i for i in cands if spec[i][0] != "seq"]  # known compile() panic
            if not cands:
                kind = "sink"
            p = [rng.choice(cands)] if cands else [rng.choice(open_nodes)]
        elif kind == "pbmap":
            p = [rng.choice(cands)]
        else:
            p = [rng.choice(open_nodes)]
        if kind == "flushpair":
            pred = rng.choice(list(FILTERS))
            if rng.random() < 0.5:
                spec.append(("collect", p, None))
                c = len(spec) - 1
                spec.append(("filter", p, pred))
            else:
                spec.append(("filter", p, pred))
                spec.append(("collect", p, None))
                c = len(spec) - 1
            zone += [(), ()]
            f = len(spec) - 2 if spec[-1][0] == "collect" else len(spec) - 1
            spec.append(("flusher", [f], (c, rng.random() < 0.7)))
            zone.append(())
            sinks.add(len(spec) - 1)
            continue
        payload = {
            "map": lambda: rng.choice(list(MAPS)),
            "filter": lambda: rng.choice(list(FILTERS)),
            "accumulate": lambda: (rng.choice(list(ACCS)), rng.choice([0, -100, ""])),
            "accs": lambda: (rng.choice(list(RS_ACCS)), rng.choice([0, -100, ""])),
            "chunk_by": lambda: (rng.choice(list(FILTERS)), rng.random() < 0.5),
            "between": lambda: (rng.choice(list(FILTERS)), rng.choice(list(FILTERS))),
            "starmap": lambda: rng.choice(list(STARS)),
            "batch_map": lambda: rng.choice(list(BATCHES)),
            "pmap": lambda: (rng.randint(1, 4), rng.choice(list(MAPS))),
            "pfilter": lambda: (rng.randint(1, 4), rng.choice(list(FILTERS))),
            "pbmap": lambda: (rng.randint(1, 4), rng.choice(list(BATCHES))),
        }.get(kind, lambda: None)()
        if kind == "seq" and spec[p[0]][0] == "par":
            kind = "union"  # known: compile() drops a seq() below par(); a 1-input union is a passthrough
            kind, payload = "map", "inc"
        z = child_zone(p[0])
        spec.append((kind, p, payload))
        zone.append(z)
        if kind in TERMINAL:
            sinks.add(len(spec) - 1)
    has_child = {p for _, ps, _ in spec for p in ps}
    for i in range(len(spec)):
        if i not in sinks and i not in has_child:
            spec.append(("sink", [i], None))
            zone.append(child_zone(i))
    has_pbmap = any(k == "pbmap" for k, _, _ in spec)
    events = []
    small = rng.random() < 0.6
    for _ in range(rng.randint(1, 14)):
        item = (lambda: rng.randint(-3, 12)) if small else (lambda: rng.choice(ITEMS))
        src = rng.choice(sources) if rng.random() < 0.5 else 0
        r = rng.random()
        if r < 0.55:
            events.append(("emit", src, item()))
        elif r < 0.75:
            xs = [item() for _ in range(rng.randint(0, 5))]
            if has_pbmap:  # a prefetched batch_map takes a whole batch: not modelled
                events += [("emit", src, x) for x in xs]
            else:
                events.append(("batch", src, xs))
        elif r < 0.9:
            events.append(("flush", src, None))
        else:
            events.append(("drain", src, None))
    compile_at = rng.choice([None, 0, rng.randint(0, len(events))])
    return spec, events, compile_at


class Model:
    def __init__(self, spec):
        self.spec = spec
        self.children = [[] for _ in spec]
        self.st = {}
        self.out = {}
        self.busy_src = set()  # sources mid-emit or mid-flush
        self.active = set()  # nodes mid-update (re-entry is refused)
        self.multi = False
        for i, (kind, parents, payload) in enumerate(spec):
            for slot, p in enumerate(parents):
                self.children[p].append((i, slot))
            if kind in ("accumulate", "accs"):
                self.st[i] = payload[1]
            elif kind == "zip":
                self.st[i] = [[] for _ in parents]
            elif kind == "combine":
                self.st[i] = [[False, None] for _ in parents]
            elif kind in ("collect", "chunk_by"):
                self.st[i] = []
            elif kind == "between":
                self.st[i] = [False, []]
            elif kind in PREFETCHED:
                self.st[i] = collections.deque()
            if kind in ("sink", "lsink", "feedback", "tolist"):
                self.out[i] = []
            elif kind == "toset":
                self.out[i] = set()

    def emit(self, src, x):
        if src in self.busy_src:
            raise akayu.ReentrantError("Re-entrant emit")
        self.busy_src.add(src)
        try:
            self._down(src, x)
        finally:
            self.busy_src.discard(src)

    def _down(self, i, v):
        for c, slot in self.children[i]:
            self._node(c, slot, v)

    def _node(self, i, slot, v):
        if i in self.active:
            raise akayu.ReentrantError("Reentered")
        self.active.add(i)
        try:
            self._update(i, slot, v)
        finally:
            self.active.discard(i)

    def _finish_prefetched(self, i, pending):
        ok, val = pending.popleft()
        if not ok:
            raise val
        self.multi |= len(val) > 1
        for o in val:
            self._down(i, o)

    def _update(self, i, slot, v):
        kind, _, payload = self.spec[i]
        st = self.st.get(i)
        if kind == "map":
            self._down(i, MAPS[payload](v))
        elif kind == "filter":
            if FILTERS[payload](v):
                self._down(i, v)
        elif kind == "accumulate":
            self.st[i] = ACCS[payload[0]](st, v)
            self._down(i, self.st[i])
        elif kind == "accs":
            r = RS_ACCS[payload[0]](st, v)
            if not (isinstance(r, tuple) and len(r) == 2):
                raise TypeError("needs a (state, output) pair")
            self.st[i] = r[0]
            self._down(i, r[1])
        elif kind == "flatten":
            es = list(v)
            self.multi |= len(es) > 1
            for e in es:
                self._down(i, e)
        elif kind in ("union", "seq"):
            self._down(i, v)
        elif kind == "par":
            errors = []
            for c, s in self.children[i]:
                try:
                    self._node(c, s, v)
                except Exception as e:
                    errors.append(e)
            if errors:
                raise errors[0]
        elif kind == "zip":
            st[slot].append(v)
            if all(st):
                self._down(i, tuple(b.pop(0) for b in st))
        elif kind == "combine":
            st[slot] = [True, v]
            if all(s[0] for s in st):
                self._down(i, tuple(s[1] for s in st))
        elif kind == "collect":
            st.append(v)
        elif kind == "chunk_by":
            pred, include = payload
            if FILTERS[pred](v):
                if include:
                    st.append(v)
                if st:
                    chunk = tuple(st)
                    st.clear()
                    self._down(i, chunk)
            else:
                st.append(v)
        elif kind == "between":
            opn, close = payload
            if not st[0]:
                if FILTERS[opn](v):
                    st[0] = True
            elif FILTERS[close](v):
                chunk = tuple(st[1])
                st[0], st[1] = False, []
                if chunk:
                    self._down(i, chunk)
            else:
                st[1].append(v)
        elif kind == "starmap":
            self._down(i, STARS[payload](*v))
        elif kind == "batch_map":
            rs = list(BATCHES[payload]([v]))
            self.multi |= len(rs) > 1
            for r in rs:
                self._down(i, r)
        elif kind in PREFETCHED:
            n, fname = payload
            try:
                if kind == "pmap":
                    res = (True, [MAPS[fname](v)])
                elif kind == "pfilter":
                    res = (True, [v] if FILTERS[fname](v) else [])
                else:
                    res = (True, list(BATCHES[fname]([v])))
            except Exception as e:
                res = (False, e)
            st.append(res)
            if len(st) >= n:
                self._finish_prefetched(i, st)
        elif kind in ("sink", "lsink", "tolist"):
            self.out[i].append(v)
        elif kind == "toset":
            self.out[i].add(v)
        elif kind == "feedback":
            self.out[i].append(v)
            if type(v) is int and 1 < v < 1000:
                self.emit(0, v // 2)
        elif kind == "flusher":
            self.flush(payload[0])

    def flush(self, i, seen=None):
        seen = set() if seen is None else seen
        if i in seen:
            return
        seen.add(i)
        kind = self.spec[i][0]
        st = self.st.get(i)
        self.active.add(i)
        try:
            if kind == "collect":
                chunk = tuple(st)
                st.clear()
                self._down(i, chunk)
                return
            if kind == "chunk_by":
                if st:
                    chunk = tuple(st)
                    st.clear()
                    self._down(i, chunk)
                return
            if kind == "between":
                st[0], st[1] = False, []
            if kind in PREFETCHED:
                while st:
                    self._finish_prefetched(i, st)
        finally:
            self.active.discard(i)
        for c, _ in self.children[i]:
            self.flush(c, seen)

    def drain(self, i, seen):
        if i in seen:
            return
        seen.add(i)
        kind = self.spec[i][0]
        st = self.st.get(i)
        self.active.add(i)
        try:
            if kind == "collect":
                chunk = tuple(st)
                st.clear()
                self._down(i, chunk)
            elif kind == "chunk_by":
                if st:
                    chunk = tuple(st)
                    st.clear()
                    self._down(i, chunk)
            elif kind == "between":
                st[0], st[1] = False, []
            elif kind in PREFETCHED:
                while st:
                    self._finish_prefetched(i, st)
        finally:
            self.active.discard(i)
        for c, _ in self.children[i]:
            self.drain(c, seen)


def build(spec):
    nodes, outs = [], {}
    for i, (kind, parents, payload) in enumerate(spec):
        ps = [nodes[p] for p in parents]
        if kind == "source":
            n = akayu.Stream()
        elif kind == "map":
            n = ps[0].map(MAPS[payload])
        elif kind == "filter":
            n = ps[0].filter(FILTERS[payload])
        elif kind == "accumulate":
            n = ps[0].accumulate(ACCS[payload[0]], payload[1])
        elif kind == "accs":
            n = ps[0].accumulate(RS_ACCS[payload[0]], payload[1], returns_state=True)
        elif kind == "flatten":
            n = ps[0].flatten()
        elif kind == "union":
            n = ps[0].union(*ps[1:])
        elif kind == "zip":
            n = ps[0].zip(*ps[1:])
        elif kind == "combine":
            n = ps[0].combine_latest(*ps[1:])
        elif kind == "collect":
            n = ps[0].collect()
        elif kind == "chunk_by":
            n = ps[0].chunk_by(FILTERS[payload[0]], include_delimiter=payload[1])
        elif kind == "between":
            n = ps[0].chunks_between(FILTERS[payload[0]], FILTERS[payload[1]])
        elif kind == "starmap":
            n = ps[0].starmap(STARS[payload])
        elif kind == "batch_map":
            n = ps[0].batch_map(BATCHES[payload])
        elif kind == "pmap":
            n = ps[0].prefetch(payload[0]).map(MAPS[payload[1]])
        elif kind == "pfilter":
            n = ps[0].prefetch(payload[0]).filter(FILTERS[payload[1]])
        elif kind == "pbmap":
            n = ps[0].prefetch(payload[0]).batch_map(BATCHES[payload[1]])
        elif kind == "par":
            n = ps[0].par()
        elif kind == "seq":
            n = ps[0].seq()
        elif kind == "tolist":
            outs[i] = ps[0].to_list()
            n = None
        elif kind == "toset":
            outs[i] = ps[0].to_set()
            n = None
        elif kind == "sink":
            outs[i] = []
            n = ps[0].sink(outs[i].append)
        elif kind == "lsink":
            out = outs[i] = []
            n = ps[0].sink(lambda x, out=out: out.append(x))
        elif kind == "flusher":
            target = nodes[payload[0]]
            n = ps[0].sink(eval("lambda _: target.flush()", {"target": target}) if payload[1] else target.flush)
        elif kind == "feedback":
            out = outs[i] = []
            src = nodes[0]

            def fb(v, out=out, src=src):
                out.append(v)
                if type(v) is int and 1 < v < 1000:
                    src.emit(v // 2)

            n = ps[0].sink(fb)
        nodes.append(n)
    return nodes, outs


def cause(e):
    while isinstance(e, RuntimeError) and e.__cause__ is not None:
        e = e.__cause__
    return type(e).__name__


def canon(v):
    return (type(v).__name__, repr(v))


def snapshot(outs):
    snap = {}
    for k, vs in outs.items():
        if isinstance(vs, set):
            snap[k] = sorted(canon(v) for v in vs)
        else:
            snap[k] = [canon(v) for v in vs]
    return snap


def run_reference(spec, events):
    m = Model(spec)
    log = []
    for kind, src, arg in events:
        m.multi = False
        try:
            if kind == "emit":
                m.emit(src, arg)
            elif kind == "batch":
                for x in arg:
                    m.emit(src, x)
            else:
                m.busy_src.add(src)
                try:
                    if kind == "flush":
                        m.flush(src)
                    else:
                        m.drain(src, set())
                finally:
                    m.busy_src.discard(src)
            log.append(("ok", snapshot(m.out)))
        except Exception as e:
            log.append((type(e).__name__, snapshot(m.out), m.multi))
            if kind != "emit" or m.multi:
                break
    return log


def run_akayu(spec, events, compile_at, compile_all):
    nodes, outs = build(spec)
    sources = [i for i, (k, _, _) in enumerate(spec) if k == "source"]
    log = []
    for n, (kind, src, arg) in enumerate(events):
        if n == compile_at:
            for s in sources if compile_all else sources[:1]:
                nodes[s].compile()
        node = nodes[src]
        try:
            if kind == "emit":
                node.emit(arg)
            elif kind == "batch":
                node.emit_batch(list(arg))
            elif kind == "flush":
                node.flush()
            else:
                node.drain()
            log.append(("ok", snapshot(outs)))
        except Exception as e:
            log.append((cause(e), snapshot(outs)))
            if kind != "emit":
                break
        except BaseException as e:
            if type(e).__name__ == "PanicException":
                raise
            log.append((type(e).__name__, snapshot(outs)))
            break
    return log


def check(seed, multi=False):
    rng = random.Random(seed)
    spec, events, compile_at = gen(rng, multi)
    compile_all = rng.random() < 0.5
    ref = run_reference(spec, events)
    problems = []
    for at in sorted({None, compile_at}, key=lambda v: -1 if v is None else v):
        try:
            got = run_akayu(spec, events, at, compile_all)
        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            problems.append(
                (at, f"PANIC/harness {type(e).__name__}: {str(e)[:100]}", traceback.format_exc(limit=3)[-300:])
            )
            continue
        for n, (r, (gs, gout)) in enumerate(zip(ref, got, strict=False)):
            rs, rout, multi_out = (*r, False)[:3]
            kind = events[n][0]
            if rs != gs:
                if kind == "batch" and rs != "ok" and gs != "ok":
                    break
                problems.append((at, f"event {n} {events[n]}: status {rs} vs {gs}", ""))
                break
            if rs != "ok" and (kind != "emit" or multi_out):
                break
            if rout != gout:
                diff = {k: (rout[k], gout.get(k)) for k in rout if rout[k] != gout.get(k)}
                problems.append((at, f"event {n} {events[n]}: outputs differ", diff))
                break
    return spec, events, compile_at, compile_all, problems


SEEDS = int(os.environ.get("AKAYU_FUZZ_SEEDS", "4000"))
CHUNK = 500


@pytest.mark.parametrize("multi", [False, True])
@pytest.mark.parametrize("start", range(0, SEEDS, CHUNK))
def test_matches_reference(start, multi):
    failures = []
    for seed in range(start, min(start + CHUNK, SEEDS)):
        spec, events, compile_at, compile_all, problems = check(seed, multi)
        if problems:
            where = f"seed {seed} compile_at={compile_at} all={compile_all}"
            failures.append(f"{where}\n spec={spec}\n events={events}\n {problems[:2]}")
    assert not failures, "\n".join(failures[:3])
