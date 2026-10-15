"""emit_batch() below a branch point.

Below a fan-out, a batch normally goes through the branches item by item:
item 1 through every branch, then item 2, ... When compile() proves the
branches independent (only native steps, each appending to its own list),
each branch takes the whole batch instead, on the faster per-step path. The
results must be the same as emitting item by item; when the proof fails the
item-by-item order must be kept.
"""

from hypothesis import given, settings
from hypothesis import strategies as st

import akayu


def emit_all(src, data, batch):
    if batch:
        src.emit_batch(list(data))
    else:
        for x in data:
            src.emit(x)


def partition(data, batch, compiled=True):
    s = akayu.Stream()
    zeros, ones, doubled, totals = [], [], [], []
    s.filter(lambda x: x % 3 == 0).sink(zeros.append)
    s.filter(lambda x: x % 3 == 1).map(lambda x: x * 10).sink(ones.append)
    branch = s.map(lambda x: x * 2)
    branch.sink(doubled.append)  # a branch point inside a branch
    branch.accumulate(lambda acc, x: acc + x, 0).sink(totals.append)
    if compiled:
        s.compile()
    emit_all(s, data, batch)
    return zeros, ones, doubled, totals


@settings(max_examples=100)
@given(data=st.lists(st.integers(-1000, 1000), max_size=40))
def test_independent_branches_match_item_by_item(data):
    assert partition(data, batch=True) == partition(data, batch=False)
    assert partition(data, batch=True) == partition(data, batch=False, compiled=False)


def test_branches_sharing_a_list_keep_the_item_order():
    """Two branches append to one list: the batch must interleave them per
    item, exactly as emit() does."""
    for batch in (False, True):
        s, out = akayu.Stream(), []
        s.map(lambda x: x * 10).sink(out.append)
        s.map(lambda x: x * 100).sink(out.append)
        s.compile()
        emit_all(s, [1, 2, 3], batch)
        assert out == [10, 100, 20, 200, 30, 300]


def test_python_code_in_a_branch_keeps_the_item_order():
    log = []
    for batch in (False, True):
        log.clear()
        s, out = akayu.Stream(), []
        s.map(lambda x: x + 1).sink(out.append)
        s.sink(lambda x, out=out: log.append(("python", x, len(out))))
        s.compile()
        emit_all(s, [1, 2], batch)
        # Item 1 went through the first branch before the second saw it.
        assert log == [("python", 1, 1), ("python", 2, 2)]


def test_branches_that_merge_keep_the_item_order():
    for batch in (False, True):
        s, out = akayu.Stream(), []
        akayu.union(s.map(lambda x: x * 10), s.map(lambda x: x * 100)).sink(out.append)
        s.compile()
        emit_all(s, [1, 2], batch)
        assert out == [10, 100, 20, 200]


def test_branches_that_change_their_items_keep_the_item_order():
    """`x.pop()` changes the item, so the branches aren't independent: with
    the same list emitted twice, the second branch must see each pop of the
    first one in emit() order."""
    results = []
    for batch in (False, True):
        s, popped, sizes = akayu.Stream(), [], []
        s.map(lambda x: x.pop()).sink(popped.append)
        s.map(lambda x: len(x)).sink(sizes.append)
        s.compile()
        shared = [1, 2, 3, 4]
        emit_all(s, [shared, shared], batch)
        results.append((popped, sizes))
    assert results[0] == results[1] == ([4, 3], [3, 2])
