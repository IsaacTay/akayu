"""Benchmark branchy/wide topologies where graph overhead dominates.

Tests scenarios where Python has simple inline if/else but akayu must
propagate through multiple nodes with borrow_mut/update/propagate per branch.
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


def compare(name, make_akayu, make_python, n=N):
    a_s = bench(make_akayu())
    p_s = bench(make_python())
    a_ns = a_s / n * 1e9
    p_ns = p_s / n * 1e9
    ratio = a_ns / p_ns if p_ns > 0 else float('inf')
    verdict = "FASTER" if ratio < 0.9 else ("SLOWER" if ratio > 1.1 else "~SAME")
    print(f"  {name:<45s}  {a_ns:>7.1f}   {p_ns:>7.1f}   {ratio:>6.2f}x  {verdict}")
    return ratio


if __name__ == "__main__":
    int_inputs = list(range(N))

    print(f"\n{'='*80}")
    print(f"  Branchy topology overhead  ({N:,} items, absolute ns/item)")
    print(f"{'='*80}")
    print(f"  {'operation':<45s}  {'akayu':>7s}   {'python':>7s}   {'ratio':>6s}  verdict")
    print(f"  {'-'*45}  {'-'*7}   {'-'*7}   {'-'*6}  {'-'*7}")

    # ── Fan-out: source → N branches, each with sink ──
    print("\n  ── Fan-out (source → N sinks, each with identity map) ──")

    for fan in [2, 5, 10, 20]:
        def _akayu(fan=fan):
            def make():
                s = akayu.Stream()
                counts = [0]
                def inc(x):
                    counts[0] += 1
                for _ in range(fan):
                    s.map(lambda x: x).sink(inc)
                s.compile()
                s.emit_batch(int_inputs)
                assert counts[0] == N * fan
            return make

        def _python(fan=fan):
            def make():
                count = 0
                def noop(x):
                    return x
                for i in range(N):
                    for _ in range(fan):
                        noop(i)
                        count += 1
                assert count == N * fan
            return make

        compare(f"fan-out ×{fan} (map→sink per branch)", _akayu, _python)

    # ── Fan-out with builtin maps ──
    print("\n  ── Fan-out with builtin maps (no lambda call) ──")

    for fan in [2, 5, 10, 20]:
        def _akayu_builtin(fan=fan):
            def make():
                s = akayu.Stream()
                counts = [0]
                def inc(x):
                    counts[0] += 1
                for _ in range(fan):
                    s.map(bool).sink(inc)
                s.compile()
                s.emit_batch(int_inputs)
            return make

        def _python_builtin(fan=fan):
            def make():
                count = 0
                for i in range(N):
                    for _ in range(fan):
                        bool(i)
                        count += 1
            return make

        compare(f"fan-out ×{fan} (bool→sink per branch)", _akayu_builtin, _python_builtin)

    # ── Diamond: source → N maps → union → sink ──
    print("\n  ── Diamond (source → N maps → union → sink) ──")

    for fan in [2, 5, 10, 20]:
        def _akayu_diamond(fan=fan):
            def make():
                s = akayu.Stream()
                count = 0
                def inc(x):
                    nonlocal count
                    count += 1
                branches = [s.map(lambda x, k=k: x + k) for k in range(fan)]
                node = branches[0]
                for b in branches[1:]:
                    node = node.union(b)
                node.sink(inc)
                s.compile()
                s.emit_batch(int_inputs)
                assert count == N * fan
            return make

        def _python_diamond(fan=fan):
            def make():
                count = 0
                for i in range(N):
                    for k in range(fan):
                        i + k
                        count += 1
                assert count == N * fan
            return make

        compare(f"diamond ×{fan} (map→union→sink)", _akayu_diamond, _python_diamond)

    # ── Deep chain of single-step branches (worst case for propagate) ──
    # source → map → map → map → ... → sink
    # but each map has its own node (no chain fusion, use non-compilable lambdas)
    print("\n  ── Uncompiled deep chain (no fusion, closure lambdas) ──")

    for depth in [2, 5, 10, 20]:
        def _akayu_deep(depth=depth):
            def make():
                s = akayu.Stream()
                count = 0
                def inc(x):
                    nonlocal count
                    count += 1
                node = s
                for k in range(depth):
                    offset = k  # closure prevents builtin detection
                    node = node.map(lambda x, o=offset: x + o)
                node.sink(inc)
                # NOTE: no compile() — so no chain fusion, each map is separate node
                for i in range(N):
                    s.emit(i)
                assert count == N
            return make

        def _python_deep(depth=depth):
            def make():
                count = 0
                for i in range(N):
                    x = i
                    for k in range(depth):
                        x = x + k
                    count += 1
                assert count == N
            return make

        compare(f"uncompiled ×{depth} maps (per-node overhead)", _akayu_deep, _python_deep)

    # ── Compiled deep chain for comparison ──
    print("\n  ── Compiled deep chain (fused, for comparison) ──")

    for depth in [2, 5, 10, 20]:
        def _akayu_compiled(depth=depth):
            def make():
                s = akayu.Stream()
                count = 0
                def inc(x):
                    nonlocal count
                    count += 1
                node = s
                for _ in range(depth):
                    node = node.map(lambda x: x + 1)
                node.sink(inc)
                s.compile()
                s.emit_batch(int_inputs)
                assert count == N
            return make

        def _python_compiled(depth=depth):
            def make():
                count = 0
                for i in range(N):
                    x = i
                    for _ in range(depth):
                        x = x + 1
                    count += 1
                assert count == N
            return make

        compare(f"compiled ×{depth} maps (fused chain)", _akayu_compiled, _python_compiled)

    # ── Wide fan-out → filter → sink (many branches, most filtered) ──
    print("\n  ── Fan-out → filter → sink (wide sparse) ──")

    for fan in [5, 10, 20]:
        def _akayu_filter_fan(fan=fan):
            def make():
                s = akayu.Stream()
                counts = [0]
                def inc(x):
                    counts[0] += 1
                for k in range(fan):
                    s.filter(lambda x, k=k: x % fan == k).map(lambda x: x * 2).sink(inc)
                s.compile()
                s.emit_batch(int_inputs)
            return make

        def _python_filter_fan(fan=fan):
            def make():
                count = 0
                for i in range(N):
                    for k in range(fan):
                        if i % fan == k:
                            i * 2
                            count += 1
            return make

        compare(f"fan ×{fan} (filter→map→sink per branch)", _akayu_filter_fan, _python_filter_fan)

    # ── Worst case: many tiny nodes, no fusion ──
    print("\n  ── Pathological: uncompiled fan-out ──")

    for fan in [2, 5, 10]:
        def _akayu_unfused_fan(fan=fan):
            def make():
                s = akayu.Stream()
                counts = [0]
                def inc(x):
                    counts[0] += 1
                for k in range(fan):
                    offset = k
                    s.map(lambda x, o=offset: x + o).sink(inc)
                # no compile!
                for i in range(N):
                    s.emit(i)
            return make

        def _python_unfused_fan(fan=fan):
            def make():
                count = 0
                for i in range(N):
                    for k in range(fan):
                        i + k
                        count += 1
            return make

        compare(f"uncompiled fan ×{fan} (per-emit per-node)", _akayu_unfused_fan, _python_unfused_fan)
