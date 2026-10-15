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
from support import run_script

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


def test_a_cycle_through_a_merge_lock_held_by_a_third_thread_raises():
    """Three threads, each holding one lock and waiting for the next: T3 is
    inside a union (its lock) and emits into d, T1 holds d and emits into c,
    T2 holds c and emits into the union's other input. The waits used to be
    checked only through sources, and a merge's lock held by a third thread
    ended the search: all three waited forever. The callbacks form a loop
    across threads; an emit into a source raises, as a re-entrant emit, and
    the rest go on. (Which one raises decides whether the union's other item
    later meets the same loop again: one or two errors, never a hang.)"""
    code, last = run_script("""
        import threading, time, akayu
        a, z, c, d = (akayu.Stream() for _ in range(4))
        got, errors = [], []
        def later(target, value):
            def callback(x):
                time.sleep(0.2)  # every thread holds its first lock by now
                target.emit(value)
                got.append(x)
            return callback
        akayu.union(a, z).sink(later(d, "from the union"))
        d.sink(lambda x: x == "first" and later(c, "from d")(x))
        c.sink(lambda x: x == "first" and later(z, "from c")(x))
        def run(source):
            try:
                source.emit("first")
            except RuntimeError as e:
                errors.append(e)
        threads = [threading.Thread(target=run, args=(s,), daemon=True) for s in (a, d, c)]
        for t in threads:
            t.start()
            time.sleep(0.05)
        for t in threads:
            t.join(20)
        hung = any(t.is_alive() for t in threads)
        kinds = {type(e.__cause__ or e).__name__ for e in errors}
        print(hung, sorted(kinds), 1 <= len(errors) <= 2)
    """)
    assert (code, last) == (0, "False ['ReentrantError'] True")


def test_a_prefetched_call_may_emit_into_a_source_merged_with_its_own(compiled):
    """compile() gave a graph without par() one lock, so a prefetched call
    on `a` emitting into `b` (merged with `a`) waited for the lock its own
    caller held while waiting for it: compiled, it hung. Graphs with
    prefetch() keep their locks, as with par()."""
    code, last = run_script(f"""
        import time, akayu
        a, b = akayu.Stream(), akayu.Stream()
        out = []
        akayu.union(a, b).sink(out.append)
        def f(x):
            time.sleep(0.02)  # a pool worker claims the task meanwhile
            b.emit(x * 10)
            return x
        a.prefetch(2).map(f).sink(out.append)
        if {compiled}:
            a.compile(); b.compile()
        for i in range(1, 5):
            a.emit(i)
        a.flush()
        print(sorted(out))
    """)
    assert (code, last) == (0, "[1, 1, 2, 2, 3, 3, 4, 4, 10, 20, 30, 40]")
