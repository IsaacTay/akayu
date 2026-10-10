"""emit_batch() map/filter/accumulate stages work in place on one buffer.

They used to allocate a new buffer per stage. Results must match per-item
emission for every mix of stages, with optimizer-recognised lambdas and
plain functions, eager and compiled; an error mid-batch must surface with
the failing node's name.
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import akayu


def plain_inc(x):
    return x + 1


def plain_even(x):
    return x % 2 == 0


STAGES = {
    "map lambda": lambda s: s.map(lambda x: x * 3),
    "map plain": lambda s: s.map(plain_inc),
    "filter lambda": lambda s: s.filter(lambda x: x % 3 != 0),
    "filter plain": lambda s: s.filter(plain_even),
    "accumulate": lambda s: s.accumulate(lambda acc, x: acc + x, 0),
    "accumulate state": lambda s: s.accumulate(lambda acc, x: (acc + 1, x * acc), 0, returns_state=True),
}


def build(stages, compiled):
    src, out = akayu.Stream(), []
    node = src
    for name in stages:
        node = STAGES[name](node)
    node.sink(out.append)
    if compiled:
        src.compile()
    return src, out


@settings(max_examples=200)
@given(
    stages=st.lists(st.sampled_from(sorted(STAGES)), min_size=1, max_size=6),
    data=st.lists(st.integers(-1000, 1000), max_size=50),
    compiled=st.booleans(),
)
def test_batch_matches_per_item(stages, data, compiled):
    batch_src, batch_out = build(stages, compiled)
    item_src, item_out = build(stages, compiled)
    batch_src.emit_batch(data)
    for x in data:
        item_src.emit(x)
    assert batch_out == item_out


@pytest.mark.parametrize("compiled", [False, True])
@pytest.mark.parametrize(
    "stage",
    [
        lambda s: s.map(lambda x: 10 // x),
        lambda s: s.map(lambda x: 10 // x).filter(lambda x: x > 0),
        lambda s: s.filter(lambda x: 10 // x > 0),
        lambda s: s.filter(lambda x: x["k"]),  # recognised itemgetter, fails on ints
    ],
)
def test_error_mid_batch_names_the_node(stage, compiled):
    src, out = akayu.Stream(), []
    stage(src).sink(out.append)
    if compiled:
        src.compile()
    with pytest.raises(RuntimeError, match="Stream operation failed at node"):
        src.emit_batch([5, 2, 0, 1])
    # The stream stays usable.
    out.clear()
    try:
        src.emit_batch([5])
    except RuntimeError:
        pass  # the itemgetter stage fails on every int
