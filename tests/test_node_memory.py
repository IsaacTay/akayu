"""Per-node memory must not creep back up.

A node used to cost ~565 bytes: a Python RLock each, a heap allocation per
parent and per downstream edge, and a 64-byte inline NodeLogic sized by its
rarest variants. Today: ~310 bytes uncompiled (~345 in a dev build; a lambda
node keeps its native op), ~300 compiled (fused members are freed; op trees
and numeric runs are shared between graphs). The bounds leave a little
headroom: a per-graph copy of a numeric run, for one, cost ~110 bytes per
compiled node.
"""

import gc
import sys

import pytest
from support import run_script

import akayu


def test_inline_node_size():
    # The Rust struct plus the Python object header, GC header and weakref slot.
    assert sys.getsizeof(akayu.Stream()) <= 256


def _rss_per_node(compile_graphs):
    """In a fresh interpreter: this one reuses what earlier tests freed, which
    made compiled nodes look like ~170 bytes after the uncompiled test ran."""
    code, last = run_script(f"""
        import gc
        import akayu, psutil
        proc = psutil.Process()
        f = lambda x: x * 2 + 1  # compile() runs this natively
        keep = []
        gc.collect()
        before = proc.memory_info().rss
        for _ in range(20_000):
            s = akayu.Stream()
            s.map(f).filter(f).sink(f)
            if {compile_graphs}:
                s.compile()
            keep.append(s)
        gc.collect()
        print((proc.memory_info().rss - before) / (len(keep) * 4))
    """)
    assert code == 0, last
    return float(last)


# Other platforms' allocators pad and reuse memory differently; these bounds
# were measured with glibc's.
GLIBC = pytest.mark.skipif(sys.platform != "linux", reason="bounds measured with glibc's malloc")


@GLIBC
def test_memory_per_node():
    assert _rss_per_node(compile_graphs=False) < 380


@GLIBC
def test_memory_per_compiled_node():
    assert _rss_per_node(compile_graphs=True) < 330


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
