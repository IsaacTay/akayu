"""Random graphs and event scripts, checked against a reference interpreter.

Each seed builds a random graph (native lambdas, opaque functions, functions
that raise on some inputs; branches, union, zip, combine_latest, accumulate,
flatten, collect, chunk_by, chunks_between, starmap, batch_map, the fused
`collect()` + `filter(p).sink(lambda _: c.flush())` idiom in either branch
order, and sinks that feed back into the source) and a random script of
emit(), emit_batch() and flush() calls, with compile() at a random point.
Every sink's output after every event must match a small pure-Python
interpreter of the same graph, which encodes the intended semantics: errors
at the same step, one flush() reaching each node once, buffered items kept
across compile(), and no re-entrant emit() into a busy source.

Outputs after a failed batch or flush are not compared (a batch goes step by
step, so how far it got differs), nor after a failed item that a flatten or
batch_map had split into several (they go on together, like a batch).

Set AKAYU_FUZZ_SEEDS to run more seeds.
"""

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
STARS = {"add2": lambda a, b: a + b, "swap": lambda a, b: (b, a)}
BATCHES = {"times10": lambda xs: [x * 10 for x in xs], "dup": lambda xs: [y for x in xs for y in (x, x)]}
ITEMS = [0, 1, 2, 3, 4, 5, 6, 7, 10, -1, -4, 2**62, 2**70, 0.5, -0.0, 2.5, "ab", None, True, (1, 2), [3]]

KINDS = {
    "map": 30, "filter": 14, "accumulate": 6, "flatten": 3, "union": 7, "zip": 5, "combine": 4,
    "collect": 4, "chunk_by": 4, "between": 3, "starmap": 3, "batch_map": 3,
    "sink": 10, "lsink": 4, "feedback": 2, "flushpair": 4,
}  # fmt: skip


def gen(rng):
    spec = [("source", [], None)]
    sinks = set()
    for _ in range(rng.randint(2, 12)):
        open_nodes = [i for i in range(len(spec)) if i not in sinks]
        kind = rng.choices(list(KINDS), list(KINDS.values()))[0]
        if kind in ("union", "zip", "combine"):
            if len(open_nodes) < 2:
                kind = "map"
            else:
                spec.append((kind, rng.sample(open_nodes, rng.randint(2, min(3, len(open_nodes)))), None))
                continue
        p = [rng.choice(open_nodes)]
        if kind == "flushpair":
            pred = rng.choice(list(FILTERS))
            if rng.random() < 0.5:  # collect branch first
                spec.append(("collect", p, None))
                c = len(spec) - 1
                spec.append(("filter", p, pred))
            else:
                spec.append(("filter", p, pred))
                spec.append(("collect", p, None))
                c = len(spec) - 1
            f = len(spec) - 2 if spec[-1][0] == "collect" else len(spec) - 1
            spec.append(("flusher", [f], (c, rng.random() < 0.7)))
            sinks.add(len(spec) - 1)
            continue
        payload = {
            "map": lambda: rng.choice(list(MAPS)),
            "filter": lambda: rng.choice(list(FILTERS)),
            "accumulate": lambda: (rng.choice(list(ACCS)), rng.choice([0, -100, ""])),
            "chunk_by": lambda: (rng.choice(list(FILTERS)), rng.random() < 0.5),
            "between": lambda: (rng.choice(list(FILTERS)), rng.choice(list(FILTERS))),
            "starmap": lambda: rng.choice(list(STARS)),
            "batch_map": lambda: rng.choice(list(BATCHES)),
        }.get(kind, lambda: None)()
        spec.append((kind, p, payload))
        if kind in ("sink", "lsink", "feedback"):
            sinks.add(len(spec) - 1)
    has_child = {p for _, ps, _ in spec for p in ps}
    for i in range(len(spec)):
        if i not in sinks and i not in has_child:
            spec.append(("sink", [i], None))
    events = []
    small = rng.random() < 0.6
    for _ in range(rng.randint(1, 12)):
        item = (lambda: rng.randint(-3, 12)) if small else (lambda: rng.choice(ITEMS))
        r = rng.random()
        if r < 0.6:
            events.append(("emit", item()))
        elif r < 0.8:
            events.append(("batch", [item() for _ in range(rng.randint(0, 5))]))
        else:
            events.append(("flush", None))
    compile_at = rng.choice([None, 0, rng.randint(0, len(events))])
    return spec, events, compile_at


class Model:
    def __init__(self, spec):
        self.spec = spec
        self.children = [[] for _ in spec]
        self.st = {}
        self.out = {}
        self.emitting = False
        self.flushing = False
        self.multi = False  # this event went through a step that output several items
        for i, (kind, parents, payload) in enumerate(spec):
            for slot, p in enumerate(parents):
                self.children[p].append((i, slot))
            if kind == "accumulate":
                self.st[i] = payload[1]
            elif kind == "zip":
                self.st[i] = [[] for _ in parents]
            elif kind == "combine":
                self.st[i] = [[False, None] for _ in parents]
            elif kind in ("collect", "chunk_by"):
                self.st[i] = []
            elif kind == "between":
                self.st[i] = [False, []]
            if kind in ("sink", "lsink", "feedback"):
                self.out[i] = []

    def emit(self, x):
        if self.emitting or self.flushing:
            # akayu rejects re-entrant emit() into a node that is mid-emit
            raise RuntimeError("Re-entrant emit")
        self.emitting = True
        try:
            self._down(0, x)
        finally:
            self.emitting = False

    def _down(self, i, v):
        for c, slot in self.children[i]:
            self._node(c, slot, v)

    def _node(self, i, slot, v):
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
        elif kind == "flatten":
            es = list(v)
            self.multi |= len(es) > 1
            for e in es:
                self._down(i, e)
        elif kind == "union":
            self._down(i, v)
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
        elif kind in ("sink", "lsink"):
            self.out[i].append(v)
        elif kind == "feedback":
            self.out[i].append(v)
            if type(v) is int and 1 < v < 1000:
                self.emit(v // 2)
        elif kind == "flusher":
            self.flush(payload[0])

    def flush(self, i=0, seen=None):
        seen = set() if seen is None else seen
        if i in seen:
            return
        seen.add(i)
        kind = self.spec[i][0]
        st = self.st.get(i)
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
        for c, _ in self.children[i]:
            self.flush(c, seen)


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
        elif kind == "sink":
            outs[i] = []
            n = ps[0].sink(outs[i].append)
        elif kind == "lsink":
            out = outs[i] = []
            n = ps[0].sink(lambda x, out=out: out.append(x))
        elif kind == "flusher":
            out = outs[i] = []
            target = nodes[payload[0]]

            def flusher(v, out=out, target=target):
                out.append(v)
                target.flush()

            # the sink(c.flush) idiom compile() fuses; record what reached it in a separate branch
            # The lambda spelling is the one compile() fuses into a chunk_by.
            n = ps[0].sink(eval("lambda _: target.flush()", {"target": target}) if payload[1] else target.flush)
            nodes.append(n)
            outs.pop(i)
            continue
        elif kind == "feedback":
            out = outs[i] = []
            src = nodes[0]

            def fb(v, out=out, src=src):
                out.append(v)
                if type(v) is int and 1 < v < 1000:
                    src.emit(v // 2)

            n = ps[0].sink(fb)
        nodes.append(n)
    return nodes[0], outs


UNWRAPPED = set()


def cause(e):
    """The innermost step error under akayu's "failed at node" wrappers."""
    if not (isinstance(e, RuntimeError) and e.__cause__ is not None):
        UNWRAPPED.add(type(e).__name__)
    while isinstance(e, RuntimeError) and e.__cause__ is not None:
        e = e.__cause__
    return type(e).__name__


def canon(v):
    return (type(v).__name__, repr(v))


def snapshot(outs):
    return {k: [canon(v) for v in vs] for k, vs in outs.items()}


def run_reference(spec, events):
    """Per event: ('ok' | error name, outputs after it). Stops after an erroring batch."""
    m = Model(spec)
    log = []
    for kind, arg in events:
        m.multi = False
        try:
            if kind == "emit":
                m.emit(arg)
            elif kind == "batch":
                for x in arg:
                    m.emit(x)
            else:
                m.flushing = True  # the source is busy: feedback into it is refused
                try:
                    m.flush()
                finally:
                    m.flushing = False
            log.append(("ok", snapshot(m.out)))
        except Exception as e:
            log.append((type(e).__name__, snapshot(m.out), m.multi))
            if kind != "emit" or m.multi:
                break
    return log


def run_akayu(spec, events, compile_at):
    src, outs = build(spec)
    log = []
    for n, (kind, arg) in enumerate(events):
        if n == compile_at:
            src.compile()
        try:
            if kind == "emit":
                src.emit(arg)
            elif kind == "batch":
                src.emit_batch(list(arg))
            else:
                src.flush()
            log.append(("ok", snapshot(outs)))
        except Exception as e:
            log.append((cause(e), snapshot(outs)))
            if kind != "emit":
                break
    return log


def check(seed):
    rng = random.Random(seed)
    spec, events, compile_at = gen(rng)
    ref = run_reference(spec, events)
    problems = []
    for at in sorted({None, compile_at}, key=lambda v: -1 if v is None else v):
        try:
            got = run_akayu(spec, events, at)
        except Exception:
            problems.append((at, "harness exception", traceback.format_exc(limit=4)))
            continue
        for n, (r, (gs, gout)) in enumerate(zip(ref, got, strict=False)):
            rs, rout, multi = (*r, False)[:3]
            kind = events[n][0]
            if rs != gs:
                # a batch may surface a different item's error first (it goes step by step)
                if kind == "batch" and rs != "ok" and gs != "ok":
                    break
                problems.append((at, f"event {n} {events[n]}: status {rs} vs {gs}", ""))
                break
            if rs != "ok" and (kind != "emit" or multi):
                break  # outputs after a failed batch/flush, or below a multi-output step, are unspecified
            if rout != gout:
                diff = {k: (rout[k], gout.get(k)) for k in rout if rout[k] != gout.get(k)}
                problems.append((at, f"event {n} {events[n]}: outputs differ", diff))
                break
    return spec, events, compile_at, problems


SEEDS = int(os.environ.get("AKAYU_FUZZ_SEEDS", "4000"))
CHUNK = 500


@pytest.mark.parametrize("start", range(0, SEEDS, CHUNK))
def test_matches_reference(start):
    failures = []
    for seed in range(start, min(start + CHUNK, SEEDS)):
        spec, events, compile_at, problems = check(seed)
        if problems:
            failures.append(f"seed {seed} compile_at={compile_at}\n spec={spec}\n events={events}\n {problems[:2]}")
    assert not failures, "\n".join(failures[:3])
