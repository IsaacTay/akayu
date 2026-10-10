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
