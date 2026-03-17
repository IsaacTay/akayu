"""Benchmark builtin fast-paths vs regular Python function dispatch.

Tests WITHOUT compile() so the builtin_op fast-path is actually active
(compile fusion clears builtin_op, making both paths equivalent).
"""
import time
from akayu import Stream

N_BATCH = 10_000
N_SINGLE = 10_000
WARMUP = 3
RUNS = 5

noop = lambda x: None

def bench(label, setup_fn, emit_fn, n_items):
    """Run a benchmark, return median wall time in ms."""
    times = []
    for i in range(WARMUP + RUNS):
        s, data = setup_fn()
        t0 = time.perf_counter()
        emit_fn(s, data)
        t1 = time.perf_counter()
        if i >= WARMUP:
            times.append((t1 - t0) * 1000)
    times.sort()
    median = times[len(times) // 2]
    print(f"  {label:45s}  {median:8.2f}ms  ({n_items} items)")
    return median

def make_batch_data():
    return list(range(N_BATCH))

def make_str_data():
    return [str(i) for i in range(N_BATCH)]

def emit_batch(s, data):
    s.emit_batch(data)

def emit_single(s, data):
    for x in data:
        s.emit(x)

# --- isinstance filter (batch) ---
print("=== filter(isinstance, int) — batch path ===")

def setup_isinstance_builtin():
    s = Stream(asynchronous=False)
    s.filter(isinstance, int).sink(noop)
    return s, make_batch_data()

def setup_isinstance_lambda():
    s = Stream(asynchronous=False)
    s.filter(lambda x: isinstance(x, int)).sink(noop)
    return s, make_batch_data()

t_builtin = bench("builtin: filter(isinstance, int)", setup_isinstance_builtin, emit_batch, N_BATCH)
t_lambda = bench("lambda:  filter(lambda x: isinstance(x,int))", setup_isinstance_lambda, emit_batch, N_BATCH)
print(f"  Speedup: {t_lambda / t_builtin:.2f}x\n")

# --- isinstance filter (single item) ---
print("=== filter(isinstance, int) — single item path ===")
t_builtin = bench("builtin: filter(isinstance, int)", setup_isinstance_builtin, emit_single, N_SINGLE)
t_lambda = bench("lambda:  filter(lambda x: isinstance(x,int))", setup_isinstance_lambda, emit_single, N_SINGLE)
print(f"  Speedup: {t_lambda / t_builtin:.2f}x\n")

# --- map(len) ---
print("=== map(len) — batch path ===")

def make_list_data():
    return [[i] * (i % 5) for i in range(N_BATCH)]

def setup_len_builtin():
    s = Stream(asynchronous=False)
    s.map(len).sink(noop)
    return s, make_list_data()

def setup_len_lambda():
    s = Stream(asynchronous=False)
    s.map(lambda x: len(x)).sink(noop)
    return s, make_list_data()

t_builtin = bench("builtin: map(len)", setup_len_builtin, emit_batch, N_BATCH)
t_lambda = bench("lambda:  map(lambda x: len(x))", setup_len_lambda, emit_batch, N_BATCH)
print(f"  Speedup: {t_lambda / t_builtin:.2f}x\n")

# --- map(str) ---
print("=== map(str) — batch path ===")

def setup_str_builtin():
    s = Stream(asynchronous=False)
    s.map(str).sink(noop)
    return s, make_batch_data()

def setup_str_lambda():
    s = Stream(asynchronous=False)
    s.map(lambda x: str(x)).sink(noop)
    return s, make_batch_data()

t_builtin = bench("builtin: map(str)", setup_str_builtin, emit_batch, N_BATCH)
t_lambda = bench("lambda:  map(lambda x: str(x))", setup_str_lambda, emit_batch, N_BATCH)
print(f"  Speedup: {t_lambda / t_builtin:.2f}x\n")

# --- map(int) ---
print("=== map(int) — batch path ===")

def setup_int_builtin():
    s = Stream(asynchronous=False)
    s.map(int).sink(noop)
    return s, make_str_data()

def setup_int_lambda():
    s = Stream(asynchronous=False)
    s.map(lambda x: int(x)).sink(noop)
    return s, make_str_data()

t_builtin = bench("builtin: map(int)", setup_int_builtin, emit_batch, N_BATCH)
t_lambda = bench("lambda:  map(lambda x: int(x))", setup_int_lambda, emit_batch, N_BATCH)
print(f"  Speedup: {t_lambda / t_builtin:.2f}x\n")

# --- map(float) ---
print("=== map(float) — batch path ===")

def setup_float_builtin():
    s = Stream(asynchronous=False)
    s.map(float).sink(noop)
    return s, make_batch_data()

def setup_float_lambda():
    s = Stream(asynchronous=False)
    s.map(lambda x: float(x)).sink(noop)
    return s, make_batch_data()

t_builtin = bench("builtin: map(float)", setup_float_builtin, emit_batch, N_BATCH)
t_lambda = bench("lambda:  map(lambda x: float(x))", setup_float_lambda, emit_batch, N_BATCH)
print(f"  Speedup: {t_lambda / t_builtin:.2f}x\n")
