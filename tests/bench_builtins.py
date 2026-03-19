"""Benchmark each BuiltinOp variant vs equivalent pure Python.

Uses emit_batch with 10 steps to isolate per-step cost.
Reports akayu ns/step vs python ns/step and ratio.
"""

import time
import akayu
from operator import itemgetter, attrgetter

N = 500_000
STEPS = 10


def bench(fn, rounds=7):
    fn()  # warmup
    times = []
    for _ in range(rounds):
        start = time.perf_counter()
        fn()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    times.sort()
    return times[len(times) // 2]


def per_step_ns(make_fn):
    """Run with 1 step and STEPS steps, return marginal ns/item/step."""
    t1 = bench(make_fn(1))
    t10 = bench(make_fn(STEPS))
    return (t10 - t1) / (STEPS - 1) / N * 1e9


# ── Builders: each returns a (akayu_builder, python_builder) pair ──

def make_akayu(step_fn):
    """Generic akayu batch builder."""
    def builder(steps):
        def fn():
            s = akayu.Stream(asynchronous=False)
            count = 0
            def inc(x):
                nonlocal count
                count += 1
            node = s
            for _ in range(steps):
                node = step_fn(node)
            node.sink(inc)
            s.compile()
            s.emit_batch(list(range(N)))
        return fn
    return builder


# ── Test cases ──

cases = []

# --- Arithmetic (specialized in CPython) ---

cases.append(("x + 1 (Add)",
    make_akayu(lambda n: n.map(lambda x: x + 1)),
    lambda steps: (lambda: [None for _ in [None] if not (
        exec_loop(steps, lambda x: x + 1)
    )])
))

def make_python_arith(op_fn):
    def builder(steps):
        def fn():
            for i in range(N):
                x = i
                for _ in range(steps):
                    x = op_fn(x)
        return fn
    return builder

cases_data = [
    # (name, akayu_step, python_op)
    # Arithmetic
    ("x + 1",       lambda n: n.map(lambda x: x + 1),     lambda x: x + 1),
    ("x - 1",       lambda n: n.map(lambda x: x - 1),     lambda x: x - 1),
    ("x * 2",       lambda n: n.map(lambda x: x * 2),     lambda x: x * 2),
    ("x // 2",      lambda n: n.map(lambda x: x // 2),    lambda x: x // 2),
    ("x % 3",       lambda n: n.map(lambda x: x % 3),     lambda x: x % 3),

    # Comparisons
    ("x > 100",     lambda n: n.map(lambda x: x > 100),   lambda x: x > 100),
    ("x == 0",      lambda n: n.map(lambda x: x == 0),    lambda x: x == 0),
    ("x != 0",      lambda n: n.map(lambda x: x != 0),    lambda x: x != 0),

    # Type conversions
    ("bool(x)",     lambda n: n.map(bool),                 bool),
    ("int(x)",      lambda n: n.map(int),                  int),
    ("float(x)",    lambda n: n.map(float),                float),
    ("str(x)",      lambda n: n.map(str),                  str),
    ("type(x)",     lambda n: n.map(type),                 type),

    # Identity checks
    ("x is None",   lambda n: n.map(lambda x: x is None), lambda x: x is None),

    # isinstance
    ("isinstance(x,int)", lambda n: n.map(lambda x: isinstance(x, int)), lambda x: isinstance(x, int)),

    # len (needs a sequence input)
    ("len(x)",      lambda n: n.map(len),                  len),
]

# For len, we need list inputs
cases_special_input = [
    ("len(x)",      lambda n: n.map(len),   len,   [list(range(5)) for _ in range(N)]),
]

# itemgetter / attrgetter need dict/object inputs — skip for now, test separately

# --- Filters (isinstance as filter predicate) ---
filter_cases = [
    ("filter isinstance",
     lambda n: n.filter(lambda x: isinstance(x, int)),
     lambda x: isinstance(x, int)),
    ("filter x>100",
     lambda n: n.filter(lambda x: x > 100),
     lambda x: x > 100),
    ("filter x%2==0",
     lambda n: n.filter(lambda x: x % 2 == 0),
     lambda x: x % 2 == 0),
]


if __name__ == "__main__":
    print(f"\n{'='*72}")
    print(f"  BuiltinOp vs Python per-step cost  ({N:,} items, {STEPS} steps)")
    print(f"{'='*72}")

    # --- Map operations with int inputs ---
    print(f"\n  {'operation':<22s}  {'akayu':>8s}  {'python':>8s}  {'ratio':>8s}  {'verdict'}")
    print(f"  {'-'*22}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*10}")

    int_inputs = list(range(N))

    for name, akayu_step, py_op in cases_data:
        if name == "len(x)":
            continue  # handled separately

        # akayu
        def make_akayu_fn(steps, step=akayu_step):
            def fn():
                s = akayu.Stream(asynchronous=False)
                count = 0
                def inc(x):
                    nonlocal count
                    count += 1
                node = s
                for _ in range(steps):
                    node = step(node)
                node.sink(inc)
                s.compile()
                s.emit_batch(int_inputs)
            return fn

        # python
        def make_python_fn(steps, op=py_op):
            def fn():
                for i in range(N):
                    x = i
                    for _ in range(steps):
                        x = op(x)
            return fn

        t1_a = bench(make_akayu_fn(1))
        t10_a = bench(make_akayu_fn(STEPS))
        a_ns = (t10_a - t1_a) / (STEPS - 1) / N * 1e9

        t1_p = bench(make_python_fn(1))
        t10_p = bench(make_python_fn(STEPS))
        p_ns = (t10_p - t1_p) / (STEPS - 1) / N * 1e9

        ratio = a_ns / p_ns if p_ns > 0 else float('inf')
        verdict = "FASTER" if ratio < 0.9 else ("SLOWER" if ratio > 1.1 else "~SAME")
        print(f"  {name:<22s}  {a_ns:>7.1f}   {p_ns:>7.1f}   {ratio:>7.2f}x  {verdict}")

    # --- len with list inputs (can't chain, so just use 1-step absolute cost) ---
    print()
    name = "len(x)"
    list_inputs = [list(range(5)) for _ in range(N)]

    def make_len_akayu_1():
        def fn():
            s = akayu.Stream(asynchronous=False)
            count = 0
            def inc(x):
                nonlocal count
                count += 1
            node = s.map(len)
            node.sink(inc)
            s.compile()
            s.emit_batch(list_inputs)
        return fn

    def make_len_python_1():
        def fn():
            for lst in list_inputs:
                x = len(lst)
        return fn

    a_ns = bench(make_len_akayu_1()) / N * 1e9
    p_ns = bench(make_len_python_1()) / N * 1e9
    ratio = a_ns / p_ns if p_ns > 0 else float('inf')
    verdict = "FASTER" if ratio < 0.9 else ("SLOWER" if ratio > 1.1 else "~SAME")
    print(f"  {name:<22s}  {a_ns:>7.1f}*  {p_ns:>7.1f}*  {ratio:>7.2f}x  {verdict}  (*absolute, not per-step)")

    # --- Filter operations ---
    print(f"\n  {'filter operation':<22s}  {'akayu':>8s}  {'python':>8s}  {'ratio':>8s}  {'verdict'}")
    print(f"  {'-'*22}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*10}")

    for name, akayu_step, py_pred in filter_cases:
        def make_filter_akayu(steps, step=akayu_step):
            def fn():
                s = akayu.Stream(asynchronous=False)
                count = 0
                def inc(x):
                    nonlocal count
                    count += 1
                node = s
                for _ in range(steps):
                    node = step(node)
                node.sink(inc)
                s.compile()
                s.emit_batch(int_inputs)
            return fn

        def make_filter_python(steps, pred=py_pred):
            def fn():
                count = 0
                for i in range(N):
                    x = i
                    keep = True
                    for _ in range(steps):
                        if not pred(x):
                            keep = False
                            break
                    if keep:
                        count += 1
            return fn

        t1_a = bench(make_filter_akayu(1))
        t10_a = bench(make_filter_akayu(STEPS))
        a_ns = (t10_a - t1_a) / (STEPS - 1) / N * 1e9

        t1_p = bench(make_filter_python(1))
        t10_p = bench(make_filter_python(STEPS))
        p_ns = (t10_p - t1_p) / (STEPS - 1) / N * 1e9

        ratio = a_ns / p_ns if p_ns > 0 else float('inf')
        verdict = "FASTER" if ratio < 0.9 else ("SLOWER" if ratio > 1.1 else "~SAME")
        print(f"  {name:<22s}  {a_ns:>7.1f}   {p_ns:>7.1f}   {ratio:>7.2f}x  {verdict}")

    # --- itemgetter with dict inputs ---
    print(f"\n  {'dict operation':<22s}  {'akayu':>8s}  {'python':>8s}  {'ratio':>8s}  {'verdict'}")
    print(f"  {'-'*22}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*10}")

    dict_inputs = [{"a": i, "b": i * 2, "c": "hello"} for i in range(N)]

    for name, akayu_step_fn, py_op in [
        ("d['a'] (itemgetter)", lambda n: n.map(itemgetter("a")),  lambda x: x["a"]),
        ("d['a'] (lambda)",     lambda n: n.map(lambda x: x["a"]), lambda x: x["a"]),
    ]:
        def make_dict_akayu_1(step=akayu_step_fn):
            def fn():
                s = akayu.Stream(asynchronous=False)
                count = 0
                def inc(x):
                    nonlocal count
                    count += 1
                node = step(s)
                node.sink(inc)
                s.compile()
                s.emit_batch(dict_inputs)
            return fn

        def make_dict_python_1(op=py_op):
            def fn():
                for d in dict_inputs:
                    x = op(d)
            return fn

        a_ns = bench(make_dict_akayu_1()) / N * 1e9
        p_ns = bench(make_dict_python_1()) / N * 1e9

        ratio = a_ns / p_ns if p_ns > 0 else float('inf')
        verdict = "FASTER" if ratio < 0.9 else ("SLOWER" if ratio > 1.1 else "~SAME")
        print(f"  {name:<22s}  {a_ns:>7.1f}*  {p_ns:>7.1f}*  {ratio:>7.2f}x  {verdict}  (*absolute)")
