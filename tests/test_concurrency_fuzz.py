"""Concurrency fuzzer: random graphs driven by several threads at once, some
emitting (emit and emit_batch) and some flushing, with callbacks that let
other threads in mid-item. Every sink must end up with exactly the multiset
of items its path predicts, with no errors and no hangs, uncompiled and
compiled.

Graphs are built from steps whose result doesn't depend on thread timing:
map, filter, flatten, prefetch, union, par branches, collect and an
accumulate that counts.

AKAYU_CONCURRENCY_FUZZ_SEEDS sets the number of seeds (default 40).
"""

import collections
import os
import random
import sys
import threading
import time

import pytest

import akayu

SEEDS = int(os.environ.get("AKAYU_CONCURRENCY_FUZZ_SEEDS", "40"))
CHUNK = 10


def let_others_in(x):
    time.sleep(0)
    return x


def shifted(f, c):
    return lambda item: [(y[0], y[1], y[2] + c) for y in f(item)]


def kept(f, m):
    return lambda item: [y for y in f(item) if y[1] % m != 0]


def doubled(f):
    return lambda item: [z for y in f(item) for z in (y, (y[0], y[1], y[2] * 10))]


def negated(f):
    return lambda item: [(y[0], y[1], -y[2]) for y in f(item)]


def tagged(f, tag):
    return lambda item: [(tag, *y) for y in f(item)]


def merged(parts):
    return lambda item: [y for f, sources in parts if item[0] in sources for y in f(item)]


class Graph:
    """A random graph, with the items each sink should receive: `predict`
    maps a source item (source index, id, value) to that path's outputs."""

    def __init__(self, rng, compiled):
        self.rng = rng
        self.sources = [akayu.Stream() for _ in range(rng.randint(1, 3))]
        self.sinks = []  # (out, predict, sources reaching it)
        self.counters = []  # (out, predict, sources): accumulate counting items
        self.collects = []
        tails = [self.chain(s, lambda item: [item], {i}) for i, s in enumerate(self.sources)]
        if len(tails) > 1 and rng.random() < 0.7:
            k = rng.randint(2, len(tails))
            picked, tails = tails[:k], tails[k:]
            union = akayu.union(*(node for node, _, _ in picked))
            reach = set().union(*(sources for _, _, sources in picked))
            tails.append(self.chain(union, merged([(f, s) for _, f, s in picked]), reach))
        for tail in tails:
            self.terminal(*tail)
        if compiled:
            for s in self.sources:
                try:
                    s.compile()
                except RuntimeError:  # compiled already, through a shared node
                    pass

    def chain(self, node, predict, sources):
        rng = self.rng
        for _ in range(rng.randint(0, 3)):
            k = rng.randrange(6)
            if k == 0:
                c = rng.randint(1, 9)
                node, predict = node.map(lambda x, c=c: (x[0], x[1], x[2] + c)), shifted(predict, c)
            elif k == 1:
                m = rng.randint(2, 3)
                node, predict = node.filter(lambda x, m=m: x[1] % m != 0), kept(predict, m)
            elif k == 2:
                node = node.map(lambda x: [x, (x[0], x[1], x[2] * 10)]).flatten()
                predict = doubled(predict)
            elif k == 3:
                node = node.map(let_others_in)
            elif k == 4:
                node = node.prefetch(rng.randint(1, 4)).map(lambda x: (x[0], x[1], -x[2]))
                predict = negated(predict)
            else:
                node = node.map(lambda x: x)
        return node, predict, sources

    def terminal(self, node, predict, sources):
        k = self.rng.randrange(4)
        if k == 0:
            out = []
            node.sink(out.append)
            self.sinks.append((out, predict, sources))
        elif k == 1:
            out = []
            collected = node.collect()
            collected.sink(out.extend)
            self.collects.append(collected)
            self.sinks.append((out, predict, sources))
        elif k == 2:
            out = []
            node.accumulate(lambda n, x: n + 1, 0).sink(out.append)
            self.counters.append((out, predict, sources))
        else:
            branches = node.par()
            for tag in ("a", "b"):
                out = []
                branches.map(lambda x, tag=tag: (tag, *x)).sink(out.append)
                self.sinks.append((out, tagged(predict, tag), sources))


def problems(seed, compiled):
    rng = random.Random(seed)
    g = Graph(rng, compiled)
    errors, emitted, lock = [], collections.Counter(), threading.Lock()
    done = threading.Event()
    per_thread = rng.randint(50, 300)

    def emitter(t):
        r = random.Random(seed * 100 + t)
        i = 0
        while i < per_thread:
            si = r.randrange(len(g.sources))
            try:
                if r.random() < 0.3:
                    n = min(r.randint(1, 8), per_thread - i)
                    items = [(si, t * 10000 + i + j, i + j) for j in range(n)]
                    g.sources[si].emit_batch(items)
                else:
                    items = [(si, t * 10000 + i, i)]
                    g.sources[si].emit(items[0])
                with lock:
                    emitted.update(items)
                i += len(items)
            except Exception as e:
                errors.append(f"emit: {e!r}")
                return

    def flusher():
        r = random.Random(seed * 7)
        while not done.is_set():
            try:
                r.choice(g.collects + g.sources).flush()
            except Exception as e:
                errors.append(f"flush: {e!r}")
                return
            time.sleep(r.random() * 0.002)

    emitters = [threading.Thread(target=emitter, args=(t,)) for t in range(rng.randint(2, 5))]
    flushers = [threading.Thread(target=flusher) for _ in range(rng.randint(0, 2))]
    for t in emitters + flushers:
        t.start()
    for t in emitters:
        t.join(60)
    done.set()
    for t in flushers:
        t.join(60)
    if any(t.is_alive() for t in emitters + flushers):
        return ["hung"]
    for node in g.sources + g.collects:
        node.flush()
    found = errors[:3]
    for out, predict, sources in g.sinks:
        want = collections.Counter(
            y for item, n in emitted.items() if item[0] in sources for _ in range(n) for y in predict(item)
        )
        if collections.Counter(out) != want:
            found.append(f"a sink got {len(out)} items, expected {sum(want.values())}")
    for out, predict, sources in g.counters:
        want = sum(len(predict(item)) * n for item, n in emitted.items() if item[0] in sources)
        if len(out) != want or (out and out[-1] != want):
            found.append(f"a counter reached {out[-1] if out else 0} in {len(out)} steps, expected {want}")
    return found


@pytest.fixture(autouse=True)
def frequent_thread_switches():
    old = sys.getswitchinterval()
    sys.setswitchinterval(1e-5)
    yield
    sys.setswitchinterval(old)


@pytest.mark.parametrize("compiled", [False, True])
@pytest.mark.parametrize("start", range(0, SEEDS, CHUNK))
def test_concurrent_emits_and_flushes_deliver_every_item(start, compiled):
    found = {seed: p for seed in range(start, min(start + CHUNK, SEEDS)) if (p := problems(seed, compiled))}
    assert not found, found
