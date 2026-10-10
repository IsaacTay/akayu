"""Stream graphs must be garbage-collectable, and GC must be safe mid-emit.

Every node holds strong references to its parents and its downstreams, so any
graph with two or more nodes is a reference cycle that only Python's cyclic
GC can free.
"""

import contextlib
import functools
import gc
import operator
import weakref

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import akayu


def _sink(_x):
    return None


# --- Builders -----------------------------------------------------------------
# Each takes a `probe` callable, embeds it in a graph, drives some data through
# and returns the source. The caller drops everything and checks `probe` dies.


def _map(probe):
    s = akayu.Stream()
    s.map(probe).sink(_sink)
    s.emit(1)
    return s


def _filter(probe):
    s = akayu.Stream()
    s.filter(probe).sink(_sink)
    s.emit(1)
    return s


def _sink_only(probe):
    s = akayu.Stream()
    s.sink(probe)
    s.emit(1)
    return s


def _compiled_chain(probe):
    s = akayu.Stream()
    s.map(probe).map(lambda x: x + 1).filter(lambda x: x > 0).sink(_sink)
    s.compile()
    s.emit_batch([1, 2, 3])
    return s


def _unemitted(probe):
    s = akayu.Stream()
    s.map(probe).sink(_sink)
    return s


def _collect(probe):
    s = akayu.Stream()
    s.map(probe).collect().sink(_sink)
    s.emit(1)  # leave an item buffered
    return s


def _accumulate(probe):
    s = akayu.Stream()
    s.accumulate(lambda acc, x: probe(acc + x), start=0).sink(_sink)
    s.emit_batch([1, 2])
    return s


def _flatten(probe):
    s = akayu.Stream()
    s.map(lambda x: [probe(x)]).flatten().sink(_sink)
    s.emit(1)
    return s


def _batch_map(probe):
    s = akayu.Stream()
    s.batch_map(lambda xs: [probe(x) for x in xs]).sink(_sink)
    s.emit_batch([1, 2])
    return s


def _chunk_by(probe):
    s = akayu.Stream()
    s.chunk_by(lambda x: probe(x) == 0).sink(_sink)
    s.emit_batch([1, 0, 2])  # leave an item buffered
    return s


def _chunks_between(probe):
    s = akayu.Stream()
    s.chunks_between(lambda x: probe(x) == 0, lambda x: x == 9).sink(_sink)
    s.emit_batch([0, 1])  # leave a chunk open
    return s


def _zip(probe):
    a, b = akayu.Stream(), akayu.Stream()
    akayu.zip(a.map(probe), b).sink(_sink)
    a.emit(1)  # leave an item waiting for its partner
    return a


def _union(probe):
    a, b = akayu.Stream(), akayu.Stream()
    akayu.union(a.map(probe), b).sink(_sink)
    a.emit(1)
    return a


def _combine_latest(probe):
    a, b = akayu.Stream(), akayu.Stream()
    akayu.combine_latest(a.map(probe), b).sink(_sink)
    a.emit(1)
    b.emit(2)
    return a


def _par(probe):
    s = akayu.Stream()
    p = s.par()
    akayu.union(p.map(probe), p.map(lambda x: x)).sink(_sink)
    s.emit_batch([1, 2, 3])
    return s


def _prefetch_map(probe):
    s = akayu.Stream()
    s.prefetch(2).map(probe).sink(_sink)
    s.emit_batch([1, 2, 3])
    s.flush()
    return s


def _prefetch_filter(probe):
    s = akayu.Stream()
    s.prefetch(2).filter(probe).sink(_sink)
    s.emit_batch([1, 2, 3])
    s.flush()
    return s


def _flush_target(probe):
    # collect() + filter(pred).sink(collect.flush) is fused into ChunkBy
    s = akayu.Stream()
    c = s.map(probe).collect()
    c.sink(_sink)
    s.filter(lambda x: x == 0).sink(c.flush)
    s.compile()
    s.emit_batch([1, 0, 2])
    return s


def _emit_target(probe):
    s, t = akayu.Stream(), akayu.Stream()
    s.map(probe).sink(t.emit)
    t.sink(_sink)
    s.compile()
    s.emit(1)
    return s


def _multistream(probe):
    s = akayu.Stream()
    ms = akayu.MultiStream({"a": s.map(probe), "b": s.map(lambda x: x)})
    ms.chunks_between(lambda x: x == 0, lambda x: x == 9).to_list()
    s.emit(1)
    return s


class _SubStream(akayu.Stream):
    pass


def _subclass(probe):
    s = _SubStream()
    s.child = s.map(probe)  # instance __dict__ referencing the graph
    s.child.sink(_sink)
    s.emit(1)
    return s


BUILDERS = [
    _map, _filter, _sink_only, _compiled_chain, _unemitted, _collect,
    _accumulate, _flatten, _batch_map, _chunk_by, _chunks_between, _zip,
    _union, _combine_latest, _par, _prefetch_map, _prefetch_filter,
    _flush_target, _emit_target, _multistream, _subclass,
]


def _assert_freed(build):
    def probe(x):
        return x

    probe_ref = weakref.ref(probe)
    source_ref = weakref.ref(build(probe))
    del probe
    gc.collect()
    assert source_ref() is None, "source Stream was not freed"
    assert probe_ref() is None, "function held by the graph was not freed"


@pytest.mark.parametrize("build", BUILDERS, ids=[b.__name__.lstrip("_") for b in BUILDERS])
def test_graph_is_freed(build):
    _assert_freed(build)


def test_many_graphs_are_freed():
    refs = []
    for i in range(2000):
        s = akayu.Stream()
        s.map(lambda x, i=i: x + i).filter(lambda x: x > 0).sink(_sink)
        if i % 2:
            s.compile()
        s.emit(i)
        refs.append(weakref.ref(s))
    del s
    gc.collect()
    assert sum(r() is not None for r in refs) == 0


@pytest.mark.xfail(strict=True, reason="buffered data is not traversed, so cycles through it are not collected")
def test_cycle_through_buffered_data_is_not_collected():
    def build(probe):
        s = akayu.Stream()
        s.map(probe).collect().sink(_sink)
        s.emit(s)  # the buffered item references the graph itself
        return s

    _assert_freed(build)


# --- GC while data is flowing --------------------------------------------------


@contextlib.contextmanager
def _aggressive_gc():
    old = gc.get_threshold()
    gc.set_threshold(1, 1, 1)
    try:
        yield
    finally:
        gc.set_threshold(*old)


def _collecting(x):
    gc.collect()
    return x


def _stress_pipelines():
    """(name, build) pairs; build(step) returns (source, results) with `step`
    called on every item, so a GC can run in the middle of propagation."""

    def chain(step):
        s, out = akayu.Stream(), []
        s.map(step).map(lambda x: x * 2).filter(lambda x: x % 3).sink(out.append)
        return s, out

    def accumulate(step):
        s, out = akayu.Stream(), []
        s.map(step).accumulate(lambda acc, x: acc + x, start=0).sink(out.append)
        return s, out

    def chunks(step):
        s, out = akayu.Stream(), []
        s.map(step).chunk_by(lambda x: x % 4 == 0).sink(out.append)
        s.map(step).chunks_between(lambda x: x % 5 == 0, lambda x: x % 5 == 3).sink(out.append)
        return s, out

    def zipped(step):
        s, out = akayu.Stream(), []
        akayu.zip(s.map(step), s.map(lambda x: -x)).sink(out.append)
        return s, out

    def parallel(step):
        s, out = akayu.Stream(), []
        p = s.par()
        akayu.union(p.map(step), p.map(lambda x: x + 1000)).sink(out.append)
        return s, out

    def prefetched(step):
        s, out = akayu.Stream(), []
        s.prefetch(3).map(step).sink(out.append)
        return s, out

    return [("chain", chain), ("accumulate", accumulate), ("chunks", chunks),
            ("zip", zipped), ("par", parallel), ("prefetch", prefetched)]


STRESS = _stress_pipelines()


def _run(build, step, *, compiled, batched):
    s, out = build(step)
    if compiled:
        s.compile()
    items = list(range(60))
    if batched:
        s.emit_batch(items)
    else:
        for x in items:
            s.emit(x)
    s.flush()
    return out


@pytest.mark.parametrize("batched", [False, True], ids=["emit", "emit_batch"])
@pytest.mark.parametrize("compiled", [False, True], ids=["eager", "compiled"])
@pytest.mark.parametrize("build", [b for _, b in STRESS], ids=[n for n, _ in STRESS])
def test_gc_during_propagation(build, compiled, batched):
    # par() branches run concurrently, so their interleaving is not defined
    norm = sorted if build.__name__ == "parallel" else list

    expected = norm(_run(build, lambda x: x, compiled=compiled, batched=batched))
    assert norm(_run(build, _collecting, compiled=compiled, batched=batched)) == expected
    with _aggressive_gc():
        assert norm(_run(build, lambda x: x, compiled=compiled, batched=batched)) == expected


def test_graph_dropped_by_its_own_sink():
    """A sink that drops the last outside reference and collects mid-emit."""
    holder = {}
    out = []

    def drop_and_collect(x):
        holder.clear()
        gc.collect()
        out.append(x)

    s = akayu.Stream()
    s.map(lambda x: x + 1).sink(drop_and_collect)
    holder["s"] = s
    ref = weakref.ref(s)
    del s
    holder["s"].emit(1)
    assert out == [2]
    gc.collect()
    assert ref() is None


# --- Analysis cache must not be fooled by reused ids ---------------------------


def test_analysis_cache_survives_id_reuse():
    """A new function at a freed function's address must be analysed afresh."""

    def run(func):
        s, out = akayu.Stream(), []
        s.map(func).sink(out.append)
        s.compile()  # triggers bytecode analysis of func
        s.emit(3)
        return out

    old = eval("lambda x: x + 1")
    assert run(old) == [4]
    old_id = id(old)
    del old
    gc.collect()

    new = None
    for _ in range(100_000):
        candidate = eval("lambda x: x * 10")
        if id(candidate) == old_id:
            new = candidate
            break
        del candidate
    if new is None:
        pytest.skip("the freed address was never reused")
    assert run(new) == [30]


# --- Fuzz: random graphs give the same results under GC pressure, then die -----

_OPS = st.sampled_from(["map", "filter", "accumulate", "flatten", "chunk_by", "chunks_between", "branch"])


def _build_random(ops, step, out):
    s = akayu.Stream()
    node = s.map(step)
    for op in ops:
        if op == "map":
            node = node.map(lambda x: x * 3 + 1)
        elif op == "filter":
            node = node.filter(lambda x: x % 7 != 0)
        elif op == "accumulate":
            node = node.accumulate(lambda acc, x: (acc + x) % 1000, start=0)
        elif op == "flatten":
            node = node.map(lambda x: [x, x + 1]).flatten()
        elif op == "chunk_by":
            node = node.chunk_by(lambda x: x % 5 == 0).map(sum)
        elif op == "chunks_between":
            node = node.chunks_between(lambda x: x % 4 == 0, lambda x: x % 4 == 2).map(len)
        elif op == "branch":
            node = akayu.union(node.map(lambda x: x + 1), node.filter(lambda x: x % 2 == 0))
    node.sink(out.append)
    return s


@settings(max_examples=60)
@given(
    ops=st.lists(_OPS, max_size=6),
    data=st.lists(st.integers(0, 100), max_size=40),
    compiled=st.booleans(),
    batched=st.booleans(),
)
def test_random_graphs_under_gc(ops, data, compiled, batched):
    def drive(step):
        out = []
        s = _build_random(ops, step, out)
        if compiled:
            s.compile()
        if batched:
            s.emit_batch(data)
        else:
            for x in data:
                s.emit(x)
        s.flush()
        return out, weakref.ref(s)

    expected, _ = drive(lambda x: x)
    got, ref = drive(_collecting)
    assert got == expected
    gc.collect()
    assert ref() is None


def test_partial_and_method_callables_are_freed():
    """Non-function callables held by nodes are released too."""

    class Scaler:
        def scale(self, x):
            return x * 2

    scaler = Scaler()
    scaler_ref = weakref.ref(scaler)
    s = akayu.Stream()
    s.map(scaler.scale).map(functools.partial(operator.add, 1)).sink(_sink)
    s.compile()
    s.emit(1)
    source_ref = weakref.ref(s)
    del s, scaler
    gc.collect()
    assert source_ref() is None
    assert scaler_ref() is None


def test_par_node_locks_are_released():
    """Per-node locks for par()/prefetch() convergence used to be keyed by
    id(node) and never removed: one RLock leaked per node, forever."""
    from akayu._helpers import _node_locks

    def run_par_graph():
        s, out = akayu.Stream(), []
        p = s.par()
        akayu.union(p.map(lambda x: x), p.map(lambda x: -x)).sink(out.append)
        s.emit_batch([1, 2, 3])

    run_par_graph()
    gc.collect()
    baseline = len(_node_locks)
    for _ in range(50):
        run_par_graph()
    gc.collect()
    assert len(_node_locks) == baseline
