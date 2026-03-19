"""Profiling script for py-spy: exercises key akayu hot paths."""
import akayu

N = 2_000_000

# --- Scenario 1: Compiled emit (the main fast path) ---
def scenario_compiled_emit():
    s = akayu.Stream(asynchronous=False)
    count = 0
    def inc(x):
        nonlocal count
        count += 1
    s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).map(lambda x: x * 2).sink(inc)
    s.compile()
    for i in range(N):
        s.emit(i)
    return count

# --- Scenario 2: Compiled emit_batch ---
def scenario_compiled_batch():
    s = akayu.Stream(asynchronous=False)
    count = 0
    def inc(x):
        nonlocal count
        count += 1
    s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).map(lambda x: x * 2).sink(inc)
    s.compile()
    batch = list(range(10_000))
    for _ in range(N // 10_000):
        s.emit_batch(batch)
    return count

# --- Scenario 3: Uncompiled emit (no optimizations) ---
def scenario_uncompiled_emit():
    s = akayu.Stream(asynchronous=False)
    count = 0
    def inc(x):
        nonlocal count
        count += 1
    s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).map(lambda x: x * 2).sink(inc)
    # no compile
    for i in range(N):
        s.emit(i)
    return count

# --- Scenario 4: Deep chain compiled ---
def scenario_deep_chain():
    s = akayu.Stream(asynchronous=False)
    node = s
    for _ in range(10):
        node = node.map(lambda x: x + 1)
    count = 0
    def inc(x):
        nonlocal count
        count += 1
    node.sink(inc)
    s.compile()
    for i in range(N):
        s.emit(i)
    return count

# --- Scenario 5: Builtin fast-paths compiled ---
def scenario_builtins():
    s = akayu.Stream(asynchronous=False)
    count = 0
    def inc(x):
        nonlocal count
        count += 1
    s.map(len).filter(isinstance, int).map(str).sink(inc)
    s.compile()
    for i in range(N):
        s.emit("x" * (i % 10))
    return count

# --- Scenario 6: Fan-out (branching) ---
def scenario_fanout():
    s = akayu.Stream(asynchronous=False)
    counts = [0] * 5
    for j in range(5):
        idx = j
        def make_inc(i):
            def inc(x):
                nonlocal counts
                counts[i] += 1
            return inc
        s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).sink(make_inc(idx))
    s.compile()
    for i in range(N):
        s.emit(i)
    return sum(counts)

# --- Scenario 7: Expansion (flatten) ---
def scenario_flatten():
    s = akayu.Stream(asynchronous=False)
    count = 0
    def inc(x):
        nonlocal count
        count += 1
    s.map(lambda x: [x] * 10).flatten().map(lambda x: x + 1).sink(inc)
    s.compile()
    batch = list(range(10_000))
    for _ in range(N // 100_000):
        s.emit_batch(batch)
    return count

print("=== Profiling akayu hot paths ===")
print(f"N = {N:,}")

import time
for name, fn in [
    ("compiled_emit", scenario_compiled_emit),
    ("compiled_batch", scenario_compiled_batch),
    ("uncompiled_emit", scenario_uncompiled_emit),
    ("deep_chain", scenario_deep_chain),
    ("builtins", scenario_builtins),
    ("fanout", scenario_fanout),
    ("flatten", scenario_flatten),
]:
    t0 = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - t0
    throughput = N / elapsed / 1e6
    print(f"  {name:20s}: {elapsed:6.2f}s  ({throughput:.2f}M items/s)  result={result}")

print("Done.")
