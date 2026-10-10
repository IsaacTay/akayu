"""The per-node lock behind emit(), emit_batch(), flush() and shared par()/union() children.

It used to be a Python threading.RLock per node plus a weak-keyed table of
Python locks for par()/union() edges; it is now a reentrant Rust lock that
waits with the GIL released. These tests pin the behaviour that has to
survive that: serialization, waiting without blocking other Python threads,
release on errors, and re-entry raising instead of deadlocking.
"""

import threading
import time

import pytest

import akayu

JOIN_TIMEOUT = 30


def _start(target, *args):
    t = threading.Thread(target=target, args=args, daemon=True)
    t.start()
    return t


def _join(*threads):
    for t in threads:
        t.join(JOIN_TIMEOUT)
        assert not t.is_alive(), "thread deadlocked"


@pytest.mark.parametrize("compiled", [False, True])
def test_contended_emit_waits_with_the_gil_released(compiled):
    """While one thread is inside emit(), a second emit() on the same source
    waits for it, and other Python threads keep running meanwhile."""
    entered, release = threading.Event(), threading.Event()
    order = []

    def slow_sink(x):
        order.append(("start", x))
        if x == "first":
            entered.set()
            assert release.wait(JOIN_TIMEOUT)
        order.append(("end", x))

    s = akayu.Stream()
    s.sink(slow_sink)
    if compiled:
        s.compile()

    first = _start(s.emit, "first")
    assert entered.wait(JOIN_TIMEOUT)
    second = _start(s.emit, "second")

    # The waiting emitter must not hold the GIL: this thread keeps running.
    ticks = 0
    deadline = time.monotonic() + 0.2
    while time.monotonic() < deadline:
        ticks += 1
    assert ticks > 1000
    assert ("start", "second") not in order  # still waiting for the lock

    release.set()
    _join(first, second)
    assert order == [("start", "first"), ("end", "first"), ("start", "second"), ("end", "second")]


@pytest.mark.parametrize("batch", [False, True])
def test_lock_is_released_when_the_pipeline_raises(batch):
    s = akayu.Stream()
    out = []
    s.map(lambda x: 1 // x).sink(out.append)
    emit = (lambda x: s.emit_batch([x])) if batch else s.emit
    with pytest.raises(RuntimeError, match="Stream operation failed"):
        emit(0)
    t = _start(emit, 1)  # another thread must get the lock
    _join(t)
    assert out == [1]


def _reentrant(exc):
    """The re-entrancy error arrives as the cause of the sink's error.
    Re-entering a node that's mid-update fails PyO3's borrow check first."""
    while exc is not None:
        if "Re-entrant" in str(exc) or "Already borrowed" in str(exc):
            return True
        exc = exc.__cause__
    return False


def test_reentrant_emit_from_a_sink_raises():
    s = akayu.Stream()
    s.sink(lambda x: s.emit(x + 1) if x < 3 else None)
    with pytest.raises(RuntimeError) as info:
        s.emit(0)
    assert _reentrant(info.value)
    s.emit(5)  # the lock was released


def test_reentrant_flush_from_a_sink_raises():
    s = akayu.Stream()
    buf = s.collect()
    buf.sink(lambda _: buf.flush())
    s.emit(1)
    with pytest.raises(RuntimeError) as info:
        buf.flush()
    assert _reentrant(info.value)
    s.emit(2)
    errors = []

    def flush_elsewhere():  # released: another thread gets in (and fails the same way)
        try:
            buf.flush()
        except RuntimeError as e:
            errors.append(e)

    _join(_start(flush_elsewhere))
    assert len(errors) == 1
    assert _reentrant(errors[0])


@pytest.mark.parametrize("compiled", [False, True])
def test_union_from_two_threads_loses_no_updates(compiled):
    """union() inputs fed from different threads meet at a shared stateful
    child; the edge locks serialize it."""
    a, b = akayu.Stream(), akayu.Stream()
    totals = []
    akayu.union(a, b).accumulate(lambda acc, x: acc + x, 0).sink(totals.append)
    if compiled:
        a.compile()

    def feed(src):
        for _ in range(2000):
            src.emit(1)

    _join(_start(feed, a), _start(feed, b))
    assert totals[-1] == 4000
    assert totals == list(range(1, 4001))


def test_par_branches_converge_without_lost_updates():
    s = akayu.Stream()
    p = s.par()
    totals = []
    merged = akayu.union(p.map(lambda x: x), p.map(lambda x: x))
    merged.accumulate(lambda acc, x: acc + x, 0).sink(totals.append)
    for _ in range(200):
        s.emit(1)
    assert totals[-1] == 400


def test_many_threads_on_one_source():
    s = akayu.Stream()
    seen = []
    s.map(lambda x: x).sink(seen.append)
    threads = [_start(lambda k=k: [s.emit((k, i)) for i in range(500)]) for k in range(8)]
    _join(*threads)
    assert len(seen) == 4000
    for k in range(8):  # each thread's items stay in its own order
        assert [i for (kk, i) in seen if kk == k] == list(range(500))
