"""Benchmark pure propagation overhead: no-op nodes to isolate borrow_mut cost.

Compares compiled (raw ptr) vs uncompiled (borrow_mut) propagation.
Uses identity-like operations so per-node work is near zero.
"""

import time

import akayu

N = 200_000


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


def compare(name, fns, n=N):
    """Compare multiple variants. fns is list of (label, make_fn) tuples."""
    results = []
    for label, make_fn in fns:
        t = bench(make_fn())
        ns = t / n * 1e9
        results.append((label, ns))

    base_ns = results[0][1]
    print(f"  {name}")
    for label, ns in results:
        ratio = ns / base_ns if base_ns > 0 else float('inf')
        print(f"    {label:<40s}  {ns:>7.1f} ns/item  ({ratio:.2f}x)")
    print()


if __name__ == "__main__":
    int_inputs = list(range(N))

    print(f"\n{'='*80}")
    print(f"  Propagation overhead isolation  ({N:,} items)")
    print(f"{'='*80}\n")

    # ── Fan-out: compiled vs uncompiled, identity sinks ──
    print("  ── Fan-out with no-op sinks (identity) ──\n")

    for fan in [2, 5, 10, 20]:
        def _compiled(fan=fan):
            def make():
                s = akayu.Stream(asynchronous=False)
                for _ in range(fan):
                    s.sink(lambda x: None)
                s.compile()
                s.emit_batch(int_inputs)
            return make

        def _uncompiled(fan=fan):
            def make():
                s = akayu.Stream(asynchronous=False)
                for _ in range(fan):
                    s.sink(lambda x: None)
                # no compile
                for i in int_inputs:
                    s.emit(i)
            return make

        def _python(fan=fan):
            def make():
                def noop(x): pass
                for i in range(N):
                    for _ in range(fan):
                        noop(i)
            return make

        compare(f"fan-out ×{fan} (no-op sinks)", [
            ("compiled (raw ptr)", _compiled),
            ("uncompiled (borrow_mut)", _uncompiled),
            ("python", _python),
        ])

    # ── Fan-out with builtin bool map + sink ──
    print("  ── Fan-out with bool map → sink ──\n")

    for fan in [2, 5, 10]:
        def _compiled_bool(fan=fan):
            def make():
                s = akayu.Stream(asynchronous=False)
                for _ in range(fan):
                    s.map(bool).sink(lambda x: None)
                s.compile()
                s.emit_batch(int_inputs)
            return make

        def _uncompiled_bool(fan=fan):
            def make():
                s = akayu.Stream(asynchronous=False)
                for _ in range(fan):
                    s.map(bool).sink(lambda x: None)
                for i in int_inputs:
                    s.emit(i)
            return make

        def _python_bool(fan=fan):
            def make():
                def noop(x): pass
                for i in range(N):
                    for _ in range(fan):
                        noop(bool(i))
            return make

        compare(f"fan-out ×{fan} (bool→sink per branch)", [
            ("compiled (raw ptr)", _compiled_bool),
            ("uncompiled (borrow_mut)", _uncompiled_bool),
            ("python", _python_bool),
        ])

    # ── Linear chain: compiled vs uncompiled ──
    print("  ── Linear chain (no-op identity maps) ──\n")

    for depth in [2, 5, 10, 20]:
        def _compiled_chain(depth=depth):
            def make():
                s = akayu.Stream(asynchronous=False)
                node = s
                for _ in range(depth):
                    node = node.map(bool)
                node.sink(lambda x: None)
                s.compile()
                s.emit_batch(int_inputs)
            return make

        def _uncompiled_chain(depth=depth):
            def make():
                s = akayu.Stream(asynchronous=False)
                node = s
                for _ in range(depth):
                    node = node.map(bool)
                node.sink(lambda x: None)
                for i in int_inputs:
                    s.emit(i)
            return make

        def _python_chain(depth=depth):
            def make():
                def noop(x): pass
                for i in range(N):
                    x = i
                    for _ in range(depth):
                        x = bool(x)
                    noop(x)
            return make

        compare(f"chain ×{depth} (bool maps)", [
            ("compiled (fused + raw ptr)", _compiled_chain),
            ("uncompiled (borrow_mut)", _uncompiled_chain),
            ("python", _python_chain),
        ])

    # ── Diamond: compiled vs uncompiled ──
    print("  ── Diamond (fan-out → union → sink) ──\n")

    for fan in [2, 5, 10]:
        def _compiled_diamond(fan=fan):
            def make():
                s = akayu.Stream(asynchronous=False)
                branches = [s.map(bool) for _ in range(fan)]
                node = branches[0]
                for b in branches[1:]:
                    node = node.union(b)
                node.sink(lambda x: None)
                s.compile()
                s.emit_batch(int_inputs)
            return make

        def _uncompiled_diamond(fan=fan):
            def make():
                s = akayu.Stream(asynchronous=False)
                branches = [s.map(bool) for _ in range(fan)]
                node = branches[0]
                for b in branches[1:]:
                    node = node.union(b)
                node.sink(lambda x: None)
                for i in int_inputs:
                    s.emit(i)
            return make

        def _python_diamond(fan=fan):
            def make():
                def noop(x): pass
                for i in range(N):
                    for _ in range(fan):
                        noop(bool(i))
            return make

        compare(f"diamond ×{fan} (bool→union→sink)", [
            ("compiled (raw ptr)", _compiled_diamond),
            ("uncompiled (borrow_mut)", _uncompiled_diamond),
            ("python", _python_diamond),
        ])
