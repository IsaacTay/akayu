"""Per-node memory must not creep back up.

A node used to cost ~565 bytes: a Python RLock each, a heap allocation per
parent and per downstream edge, and a 64-byte inline NodeLogic sized by its
rarest variants. These bounds leave some headroom over today's ~370 bytes.
"""

import gc
import sys

import psutil

import akayu


def test_inline_node_size():
    # The Rust struct plus the Python object header, GC header and weakref slot.
    assert sys.getsizeof(akayu.Stream()) <= 288


def _rss_per_node(compile_graphs):
    proc = psutil.Process()
    f = lambda x: x  # noqa: E731
    keep = []
    gc.collect()
    before = proc.memory_info().rss
    for _ in range(20_000):
        s = akayu.Stream()
        s.map(f).filter(f).sink(f)
        if compile_graphs:
            s.compile()
        keep.append(s)
    gc.collect()
    return (proc.memory_info().rss - before) / (len(keep) * 4)


def test_memory_per_node():
    assert _rss_per_node(compile_graphs=False) < 450


def test_memory_per_compiled_node():
    assert _rss_per_node(compile_graphs=True) < 450


def test_compile_frees_fused_chain_members():
    """compile() moves a chain's steps into its first node. The other
    members are only kept by references the user holds; the rest are freed."""
    import weakref

    s, out = akayu.Stream(), []
    head = s.map(lambda x: x + 1)
    middle = head.filter(lambda x: x % 2 == 0)
    tail = middle.map(lambda x: x * 10)
    buffered = tail.collect()  # not chainable: the chain ends at `tail`
    buffered.sink(out.append)
    member_refs = [weakref.ref(middle), weakref.ref(tail)]
    del middle, tail
    s.compile()
    gc.collect()
    assert [r() for r in member_refs] == [None, None]
    for x in range(4):
        s.emit(x)
    buffered.flush()
    assert out == [(20, 40)]
    assert head is not None


def test_a_held_member_still_flushes_after_compile():
    s, out = akayu.Stream(), []
    member = s.map(lambda x: x).map(lambda x: x)
    member.collect().sink(out.append)
    s.compile()
    gc.collect()
    s.emit(1)
    member.flush()
    assert out == [(1,)]
