"""flush() warns only when the user flushes a node where it can do nothing.

It used to warn for every non-sink leaf a flush propagated into, which after
compile() included every fused chain ending in a sink, under the name ''.
"""

import warnings

import pytest

import akayu


def _sink(_x):
    return None


def _chain_to_sink(s):
    s.map(lambda x: x + 1).map(lambda x: x * 2).filter(lambda x: x > 0).sink(_sink)


def _chain_to_collect(s):
    s.map(lambda x: x + 1).map(lambda x: x * 2).collect().sink(_sink)


def _non_sink_leaf(s):
    s.map(lambda x: x + 1)


def _fan_out(s):
    m = s.map(lambda x: x + 1)
    m.filter(lambda x: x % 2).sink(_sink)
    m.map(lambda x: -x).collect().sink(_sink)


def _fused_chunk_by(s):
    c = s.map(lambda x: x).collect()
    c.sink(_sink)
    s.filter(lambda x: x == 0).sink(c.flush)


def _chunks_between(s):
    s.chunks_between(lambda x: x == 0, lambda x: x == 9).map(len).sink(_sink)


def _prefetch(s):
    s.prefetch(2).map(lambda x: x).sink(_sink)


GRAPHS = [_chain_to_sink, _chain_to_collect, _non_sink_leaf, _fan_out, _fused_chunk_by, _chunks_between, _prefetch]


@pytest.mark.parametrize("compiled", [False, True], ids=["eager", "compiled"])
@pytest.mark.parametrize("build", GRAPHS, ids=[g.__name__.lstrip("_") for g in GRAPHS])
@pytest.mark.parametrize("method", ["flush", "drain"])
def test_source_flush_does_not_warn(build, compiled, method):
    s = akayu.Stream()
    build(s)
    if compiled:
        s.compile()
    s.emit_batch([1, 0, 2, 9, 3])
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        getattr(s, method)()


@pytest.mark.parametrize("compiled", [False, True], ids=["eager", "compiled"])
def test_flushing_a_leaf_directly_still_warns(compiled):
    s = akayu.Stream()
    leaf = s.map(lambda x: x, stream_name="double").map(lambda x: x, stream_name="inc")
    if compiled:
        s.compile()
    with pytest.warns(UserWarning, match="no buffer and no downstreams"):
        leaf.flush()


def test_fused_chain_head_is_named_after_its_steps():
    """A fused chain head used to lose its name ('')."""
    s = akayu.Stream()
    head = s.map(lambda x: x, stream_name="double")
    head.map(lambda x: x, stream_name="inc")
    s.compile()
    with pytest.warns(UserWarning, match=r"flush\(\) called on 'double -> inc'"):
        head.flush()


def test_fused_chain_ending_in_sink_counts_as_sink():
    s = akayu.Stream()
    head = s.map(lambda x: x)
    head.map(lambda x: x).sink(_sink)
    s.compile()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        head.flush()
