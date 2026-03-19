"""Microbenchmark: isolate per-step dispatch overhead for cheap builtins.

Tests isinstance and filter pipelines where C-API work is minimal,
so the gap between akayu and Python is purely dispatch/framework overhead.
"""

import time
import akayu

N = 500_000


def bench(name, fn, rounds=7):
    fn()  # warmup
    times = []
    for _ in range(rounds):
        start = time.perf_counter()
        fn()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    times.sort()
    median_s = times[len(times) // 2]
    ns_per_item = median_s / N * 1e9
    return ns_per_item


# ── isinstance filter pipeline ──

def make_isinstance_batch(steps):
    """filter(lambda x: isinstance(x, int)) × steps → BuiltinOp::IsInstance"""
    def fn():
        s = akayu.Stream(asynchronous=False)
        count = 0
        def inc(x):
            nonlocal count
            count += 1
        node = s
        for _ in range(steps):
            node = node.filter(lambda x: isinstance(x, int))
        node.sink(inc)
        s.compile()
        s.emit_batch(list(range(N)))
        assert count == N
    return fn


def make_isinstance_emit(steps):
    def fn():
        s = akayu.Stream(asynchronous=False)
        count = 0
        def inc(x):
            nonlocal count
            count += 1
        node = s
        for _ in range(steps):
            node = node.filter(lambda x: isinstance(x, int))
        node.sink(inc)
        s.compile()
        for i in range(N):
            s.emit(i)
        assert count == N
    return fn


def make_isinstance_python(steps):
    def fn():
        count = 0
        for i in range(N):
            x = i
            keep = True
            for _ in range(steps):
                if not isinstance(x, int):
                    keep = False
                    break
            if keep:
                count += 1
        assert count == N
    return fn


# ── bool map pipeline (cheapest possible map) ──

def make_bool_batch(steps):
    """map(bool) × steps → BuiltinOp::Bool (PyObject_IsTrue + singleton)"""
    def fn():
        s = akayu.Stream(asynchronous=False)
        count = 0
        def inc(x):
            nonlocal count
            count += 1
        node = s
        for _ in range(steps):
            node = node.map(bool)
        node.sink(inc)
        s.compile()
        s.emit_batch(list(range(N)))
        assert count == N
    return fn


def make_bool_emit(steps):
    def fn():
        s = akayu.Stream(asynchronous=False)
        count = 0
        def inc(x):
            nonlocal count
            count += 1
        node = s
        for _ in range(steps):
            node = node.map(bool)
        node.sink(inc)
        s.compile()
        for i in range(N):
            s.emit(i)
        assert count == N
    return fn


def make_bool_python(steps):
    def fn():
        count = 0
        for i in range(N):
            x = i
            for _ in range(steps):
                x = bool(x)
            count += 1
        assert count == N
    return fn


# ── type map pipeline (cheapest possible: just read ob_type) ──

def make_type_batch(steps):
    def fn():
        s = akayu.Stream(asynchronous=False)
        count = 0
        def inc(x):
            nonlocal count
            count += 1
        node = s
        for _ in range(steps):
            node = node.map(type)
        node.sink(inc)
        s.compile()
        s.emit_batch(list(range(N)))
        assert count == N
    return fn


def make_type_emit(steps):
    def fn():
        s = akayu.Stream(asynchronous=False)
        count = 0
        def inc(x):
            nonlocal count
            count += 1
        node = s
        for _ in range(steps):
            node = node.map(type)
        node.sink(inc)
        s.compile()
        for i in range(N):
            s.emit(i)
        assert count == N
    return fn


def make_type_python(steps):
    def fn():
        count = 0
        for i in range(N):
            x = i
            for _ in range(steps):
                x = type(x)
            count += 1
        assert count == N
    return fn


# ── x+1 for comparison ──

def make_add_batch(steps):
    def fn():
        s = akayu.Stream(asynchronous=False)
        count = 0
        def inc(x):
            nonlocal count
            count += 1
        node = s
        for _ in range(steps):
            node = node.map(lambda x: x + 1)
        node.sink(inc)
        s.compile()
        s.emit_batch(list(range(N)))
        assert count == N
    return fn


def make_add_python(steps):
    def fn():
        count = 0
        for i in range(N):
            x = i
            for _ in range(steps):
                x = x + 1
            count += 1
        assert count == N
    return fn


# ── Run ──

if __name__ == "__main__":
    step_counts = [1, 5, 10, 20]

    pipelines = [
        ("isinstance filter", make_isinstance_batch, make_isinstance_emit, make_isinstance_python),
        ("bool map",          make_bool_batch,       make_bool_emit,       make_bool_python),
        ("type map",          make_type_batch,        make_type_emit,       make_type_python),
        ("x+1 map",           make_add_batch,         None,                 make_add_python),
    ]

    print(f"\n{'='*70}")
    print(f"  Cheap builtin pipeline overhead  ({N:,} items, median of 7 runs)")
    print(f"{'='*70}")

    all_results = {}

    for name, make_batch, make_emit, make_python in pipelines:
        print(f"\n  ── {name} ──")
        print(f"  {'steps':>5s}  {'batch':>10s}  {'emit':>10s}  {'python':>10s}  {'batch/py':>10s}")

        for steps in step_counts:
            batch_ns = bench(f"{name} batch {steps}", make_batch(steps))
            emit_ns = bench(f"{name} emit {steps}", make_emit(steps)) if make_emit else None
            py_ns = bench(f"{name} python {steps}", make_python(steps))
            ratio = batch_ns / py_ns

            all_results[(name, "batch", steps)] = batch_ns
            all_results[(name, "python", steps)] = py_ns
            if emit_ns is not None:
                all_results[(name, "emit", steps)] = emit_ns

            emit_str = f"{emit_ns:>8.1f}ns" if emit_ns else "       n/a"
            print(f"  {steps:>5d}  {batch_ns:>8.1f}ns  {emit_str}  {py_ns:>8.1f}ns  {ratio:>9.2f}x")

    # ── Per-step marginal costs ──
    print(f"\n{'='*70}")
    print(f"  Per-step marginal cost (1→20 avg, ns/item/step)")
    print(f"{'='*70}")
    print(f"  {'pipeline':<20s}  {'batch':>8s}  {'emit':>8s}  {'python':>8s}  {'gap':>8s}")

    for name, _, make_emit, _ in pipelines:
        batch_d = (all_results[(name, "batch", 20)] - all_results[(name, "batch", 1)]) / 19
        py_d = (all_results[(name, "python", 20)] - all_results[(name, "python", 1)]) / 19
        gap = batch_d - py_d
        if make_emit and (name, "emit", 1) in all_results:
            emit_d = (all_results[(name, "emit", 20)] - all_results[(name, "emit", 1)]) / 19
            print(f"  {name:<20s}  {batch_d:>7.1f}   {emit_d:>7.1f}   {py_d:>7.1f}   {gap:>+7.1f}")
        else:
            print(f"  {name:<20s}  {batch_d:>7.1f}   {'n/a':>7s}   {py_d:>7.1f}   {gap:>+7.1f}")

    # ── Fixed overhead (1-step baseline) ──
    print(f"\n{'='*70}")
    print(f"  Fixed overhead at 1 step (ns/item)")
    print(f"{'='*70}")
    print(f"  {'pipeline':<20s}  {'batch':>8s}  {'python':>8s}  {'overhead':>10s}")
    for name, _, _, _ in pipelines:
        b1 = all_results[(name, "batch", 1)]
        p1 = all_results[(name, "python", 1)]
        print(f"  {name:<20s}  {b1:>7.1f}   {p1:>7.1f}   {b1 - p1:>+9.1f}")
