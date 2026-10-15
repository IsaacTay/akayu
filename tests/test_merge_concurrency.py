"""Merges fed from several threads at once.

Streams merged with union, zip or combine_latest can be emitted from
different threads, and the merged node and everything after it is shared.
Only akayu.union() used to lock that: with a.union(b), zip or combine_latest,
a second thread raised "Already borrowed" (eager) or silently lost updates
(compiled). Every merge now locks its shared edges, and compile() replaces
those per-item locks with one lock for the whole graph when that is provably
enough (no par() in it).
"""

import threading

import pytest

import akayu

N = 5000


@pytest.fixture(autouse=True)
def _fast_switching(fast_switching):
    """Every test here runs with threads switching every microsecond."""


def _slow_add(acc, x):
    # A few bytecodes, so the GIL can switch threads mid-call.
    for _ in range(3):
        pass
    return acc + x


def _feed_concurrently(*sources):
    errors = []

    def feed(src):
        try:
            for _ in range(N):
                src.emit(1)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=feed, args=(s,)) for s in sources]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
        assert not t.is_alive(), "deadlocked"
    assert not errors, errors


MERGES = {
    "method union": lambda a, b: a.union(b),
    "module union": lambda a, b: akayu.union(a, b),
    "zip": lambda a, b: akayu.zip(a, b).map(sum),
    "method zip": lambda a, b: a.zip(b).map(sum),
    "combine_latest": lambda a, b: akayu.combine_latest(a, b).map(lambda t: 1),
}


@pytest.mark.parametrize("merge", MERGES)
def test_concurrent_inputs_lose_nothing(merge, compiled):
    a, b = akayu.Stream(), akayu.Stream()
    totals = []
    MERGES[merge](a, b).accumulate(_slow_add, 0).sink(totals.append)
    if compiled:
        a.compile()
        b.compile()
    _feed_concurrently(a, b)
    expected = {"zip": N * 2, "method zip": N * 2}.get(merge, 2 * N)
    if merge == "combine_latest":
        expected = len(totals)  # one output per input once both have a value
        assert totals == list(range(1, expected + 1))
    else:
        assert totals[-1] == expected
        assert totals == list(range(totals[0], expected + 1, totals[0]))


def test_branch_added_after_compile_to_an_uncompiled_input():
    """compile(a) shares one lock with b (an input it reaches through the
    merge); a branch added to b afterwards is still serialized with a."""
    a, b = akayu.Stream(), akayu.Stream()
    merged_totals, branch_totals = [], []
    a.union(b).accumulate(_slow_add, 0).sink(merged_totals.append)
    a.compile()
    b.accumulate(_slow_add, 0).sink(branch_totals.append)
    _feed_concurrently(a, b)
    assert merged_totals[-1] == 2 * N
    assert branch_totals[-1] == N


def test_graphs_with_par_keep_their_locks():
    s = akayu.Stream()
    p = s.par()
    totals = []
    akayu.union(p.map(lambda x: x), p.map(lambda x: x)).accumulate(_slow_add, 0).sink(totals.append)
    s.compile()
    for _ in range(200):
        s.emit(1)
    assert totals[-1] == 400


def test_concurrent_emits_through_an_emit_target_sink():
    """sink(target.emit) is optimised to emit into `target` directly; it now
    takes target's lock like target.emit() does."""
    target = akayu.Stream()
    totals = []
    target.accumulate(_slow_add, 0).sink(totals.append)
    a, b = akayu.Stream(), akayu.Stream()
    a.sink(target.emit)
    b.sink(target.emit)
    a.compile()
    b.compile()
    _feed_concurrently(a, b)
    assert totals[-1] == 2 * N
