"""flush()/drain() on any node must behave the same compiled or not,
including whether it warns.

compile() fuses linear runs into a chain whose members used to lose their
downstreams, so flushing a member silently dropped everything buffered below.
"""

import warnings

from hypothesis import given, settings
from hypothesis import strategies as st

import akayu

_OPS = {
    "map": lambda n: n.map(lambda x: x * 3 + 1),
    "filter": lambda n: n.filter(lambda x: x % 4 != 0),
    "accumulate": lambda n: n.accumulate(lambda acc, x: acc + x, start=0),
    "flatten": lambda n: n.map(lambda x: [x, -x]).flatten(),
    "starmap": lambda n: n.map(lambda x: (x, 1)).starmap(lambda a, b: a + b),
}


def _run(ops, data, flush_index, method, compiled):
    out = []
    s = akayu.Stream()
    nodes = [s]
    for op in ops:
        nodes.append(_OPS[op](nodes[-1]))
    nodes.append(nodes[-1].collect())
    nodes[-1].sink(out.append)
    if compiled:
        s.compile()
    s.emit_batch(data)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        getattr(nodes[flush_index % len(nodes)], method)()
    warned = any("no buffer and no downstreams" in str(w.message) for w in caught)
    return out, warned


@settings(max_examples=150)
@given(
    ops=st.lists(st.sampled_from(sorted(_OPS)), min_size=1, max_size=6),
    data=st.lists(st.integers(-20, 20), max_size=15),
    flush_index=st.integers(0, 20),
    method=st.sampled_from(["flush", "drain"]),
)
def test_flush_any_node_same_when_compiled(ops, data, flush_index, method):
    eager = _run(ops, data, flush_index, method, compiled=False)
    assert _run(ops, data, flush_index, method, compiled=True) == eager


def test_flushing_a_fused_chain_member_reaches_the_collect():
    """Regression: compiled used to give [] here."""

    def run(compiled):
        s, out = akayu.Stream(), []
        a = s.map(lambda x: x)
        b = a.map(lambda x: x * 10)
        b.collect().sink(out.append)
        if compiled:
            s.compile()
        s.emit(1)
        s.emit(2)
        b.flush()
        return out

    assert run(compiled=False) == [(10, 20)]
    assert run(compiled=True) == [(10, 20)]
