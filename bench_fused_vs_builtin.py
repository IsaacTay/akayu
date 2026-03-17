"""Benchmark: compiled (with builtin skip-fusion) vs uncompiled vs old-fused behavior.

Now that builtins skip fusion, compile() should preserve the fast-paths.
This verifies compiled and uncompiled give similar performance for builtins.
"""
import time
from akayu import Stream

N = 10_000
WARMUP = 3
RUNS = 7

noop = lambda x: None

def bench(label, setup_fn):
    """Run a benchmark, return median wall time in ms."""
    times = []
    for i in range(WARMUP + RUNS):
        s, data = setup_fn()
        t0 = time.perf_counter()
        s.emit_batch(data)
        t1 = time.perf_counter()
        if i >= WARMUP:
            times.append((t1 - t0) * 1000)
    times.sort()
    median = times[len(times) // 2]
    print(f"  {label:55s}  {median:8.3f}ms")
    return median

mixed_data = [i if i % 2 == 0 else str(i) for i in range(N)]
list_data = [[i] * (i % 5) for i in range(N)]
int_data = list(range(N))
str_data = [str(i) for i in range(N)]

# === filter(isinstance, int) + sink ===
print("=== filter(isinstance, int).sink(noop) ===")

def setup_compiled():
    s = Stream(asynchronous=False)
    s.filter(isinstance, int).sink(noop)
    s.compile()
    return s, mixed_data

def setup_uncompiled():
    s = Stream(asynchronous=False)
    s.filter(isinstance, int).sink(noop)
    return s, mixed_data

t_compiled = bench("compiled (builtin skips fusion)", setup_compiled)
t_uncompiled = bench("uncompiled (builtin fast-path)", setup_uncompiled)
ratio = max(t_compiled, t_uncompiled) / min(t_compiled, t_uncompiled)
faster = "COMPILED" if t_compiled < t_uncompiled else "UNCOMPILED"
print(f"  {faster} faster by {ratio:.2f}x (should be ~equal now)\n")

# === map(len) + sink ===
print("=== map(len).sink(noop) ===")

def setup_len_compiled():
    s = Stream(asynchronous=False)
    s.map(len).sink(noop)
    s.compile()
    return s, list_data

def setup_len_uncompiled():
    s = Stream(asynchronous=False)
    s.map(len).sink(noop)
    return s, list_data

t_compiled = bench("compiled (builtin skips fusion)", setup_len_compiled)
t_uncompiled = bench("uncompiled (builtin fast-path)", setup_len_uncompiled)
ratio = max(t_compiled, t_uncompiled) / min(t_compiled, t_uncompiled)
faster = "COMPILED" if t_compiled < t_uncompiled else "UNCOMPILED"
print(f"  {faster} faster by {ratio:.2f}x (should be ~equal now)\n")

# === filter(isinstance).map(len).sink(noop) — chain with multiple builtins ===
print("=== filter(isinstance, list).map(len).sink(noop) ===")

chain_data = [[i] * (i % 5) if i % 2 == 0 else i for i in range(N)]

def setup_chain_compiled():
    s = Stream(asynchronous=False)
    s.filter(isinstance, list).map(len).sink(noop)
    s.compile()
    return s, chain_data

def setup_chain_uncompiled():
    s = Stream(asynchronous=False)
    s.filter(isinstance, list).map(len).sink(noop)
    return s, chain_data

t_compiled = bench("compiled (builtins skip fusion)", setup_chain_compiled)
t_uncompiled = bench("uncompiled (builtin fast-paths)", setup_chain_uncompiled)
ratio = max(t_compiled, t_uncompiled) / min(t_compiled, t_uncompiled)
faster = "COMPILED" if t_compiled < t_uncompiled else "UNCOMPILED"
print(f"  {faster} faster by {ratio:.2f}x (should be ~equal now)\n")

# === mixed: filter(isinstance).map(custom).sink(noop) — one builtin, one not ===
print("=== filter(isinstance, list).map(lambda x: x[0]).sink(noop) ===")

def setup_mixed_compiled():
    s = Stream(asynchronous=False)
    s.filter(isinstance, list).map(lambda x: len(x) + 1).sink(noop)
    s.compile()
    return s, chain_data

def setup_mixed_uncompiled():
    s = Stream(asynchronous=False)
    s.filter(isinstance, list).map(lambda x: len(x) + 1).sink(noop)
    return s, chain_data

t_compiled = bench("compiled (isinstance stays, custom unfused)", setup_mixed_compiled)
t_uncompiled = bench("uncompiled", setup_mixed_uncompiled)
ratio = max(t_compiled, t_uncompiled) / min(t_compiled, t_uncompiled)
faster = "COMPILED" if t_compiled < t_uncompiled else "UNCOMPILED"
print(f"  {faster} faster by {ratio:.2f}x\n")

# === pure custom: map(custom).sink(custom) — should still fuse ===
print("=== map(lambda).sink(lambda) — no builtins, should still fuse ===")

def setup_custom_compiled():
    s = Stream(asynchronous=False)
    s.map(lambda x: x * 2).sink(noop)
    s.compile()
    return s, int_data

def setup_custom_uncompiled():
    s = Stream(asynchronous=False)
    s.map(lambda x: x * 2).sink(noop)
    return s, int_data

t_compiled = bench("compiled (fused MapSink)", setup_custom_compiled)
t_uncompiled = bench("uncompiled (separate map + sink)", setup_custom_uncompiled)
ratio = max(t_compiled, t_uncompiled) / min(t_compiled, t_uncompiled)
faster = "COMPILED" if t_compiled < t_uncompiled else "UNCOMPILED"
print(f"  {faster} faster by {ratio:.2f}x (fusion should still help here)\n")
