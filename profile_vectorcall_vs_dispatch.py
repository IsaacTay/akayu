"""Isolate: is the cost in vectorcall or in Rust dispatch?

Compare:
1. Builtin fast-paths (skip vectorcall entirely, pure Rust/C-API)
2. Python lambdas (vectorcall through CPython)
3. Minimal identity lambda (cheapest possible vectorcall)

All use the same topology shape: map -> sink, compiled.
"""
import time
import operator
import akayu

N = 5_000_000
REPS = 3

def bench(label, setup_fn):
    times = []
    for _ in range(REPS):
        s, batch = setup_fn()
        t0 = time.perf_counter()
        for _ in range(N // len(batch)):
            s.emit_batch(batch)
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
    best = min(times)
    throughput = N / best / 1e6
    print(f"  {label:40s}: {best:5.3f}s  ({throughput:.2f}M items/s)")

# --- 1. Builtin bool (pure C-API, no vectorcall) ---
def setup_builtin_bool():
    s = akayu.Stream(asynchronous=False)
    s.map(bool).sink(lambda x: None)
    s.compile()
    return s, list(range(10_000))

# --- 2. Builtin len (pure C-API, no vectorcall) ---
def setup_builtin_len():
    s = akayu.Stream(asynchronous=False)
    s.map(len).sink(lambda x: None)
    s.compile()
    return s, ["hello"] * 10_000

# --- 3. Lambda identity (cheapest vectorcall) ---
def setup_lambda_identity():
    s = akayu.Stream(asynchronous=False)
    s.map(lambda x: x).sink(lambda x: None)
    s.compile()
    return s, list(range(10_000))

# --- 4. Lambda arithmetic (typical vectorcall) ---
def setup_lambda_arith():
    s = akayu.Stream(asynchronous=False)
    s.map(lambda x: x + 1).sink(lambda x: None)
    s.compile()
    return s, list(range(10_000))

# --- 5. Builtin filter (is_not None, pure C-API pointer compare) ---
def setup_builtin_filter():
    s = akayu.Stream(asynchronous=False)
    s.filter(operator.is_not, None).sink(lambda x: None)
    s.compile()
    return s, list(range(10_000))

# --- 6. Lambda filter (vectorcall) ---
def setup_lambda_filter():
    s = akayu.Stream(asynchronous=False)
    s.filter(lambda x: x is not None).sink(lambda x: None)
    s.compile()
    return s, list(range(10_000))

# --- 7. No-op sink only (just dispatch + propagation overhead) ---
def setup_sink_only():
    s = akayu.Stream(asynchronous=False)
    s.sink(lambda x: None)
    s.compile()
    return s, list(range(10_000))

# --- 8. Builtin print sink (skips vectorcall) ---
import io, os
def setup_builtin_print_sink():
    s = akayu.Stream(asynchronous=False)
    s.sink(print)
    s.compile()
    return s, list(range(10_000))

print(f"=== vectorcall vs dispatch overhead ({N:,} items, best of {REPS}) ===\n")

print("Map -> Sink (compiled, emit_batch):")
bench("builtin bool (C-API, no vectorcall)", setup_builtin_bool)
bench("builtin len  (C-API, no vectorcall)", setup_builtin_len)
bench("lambda identity (cheapest vectorcall)", setup_lambda_identity)
bench("lambda x+1 (typical vectorcall)", setup_lambda_arith)

print("\nFilter -> Sink (compiled, emit_batch):")
bench("builtin is_not None (pointer cmp)", setup_builtin_filter)
bench("lambda x is not None (vectorcall)", setup_lambda_filter)

print("\nSink only (compiled, emit_batch):")
bench("lambda sink (vectorcall)", setup_sink_only)
