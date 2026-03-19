"""
Example 03: Compilation and Optimization

This example demonstrates:
- What compile() does and why it matters
- How compilation optimizes the stream graph (chain fusion)
- Performance differences between compiled and uncompiled streams

When you call compile(), akayu performs topology optimizations that
fuse consecutive operations together, reducing overhead significantly.
"""

import time
import akayu


def main():
    print("=" * 60)
    print("AKAYU EXAMPLE 03: Compilation")
    print("=" * 60)

    # ---------------------------------------------------------
    # SECTION: What compile() Does
    # ---------------------------------------------------------
    # compile() freezes the stream graph and applies optimizations.
    # The main optimization is "chain fusion" - consecutive map
    # operations are combined into a single operation.
    print("\n--- Understanding compile() ---")

    # Before compilation: multiple nodes in the graph
    source = akayu.Stream()
    mapped = source.map(lambda x: x * 2)
    filtered = mapped.filter(lambda x: x > 5)
    sink_node = filtered.sink(lambda x: None)

    print(f"Stream compiled before optimize: {source.compiled}")

    # Call compile to freeze and optimize
    source.compile()

    print(f"Stream compiled after optimize: {source.compiled}")

    # ---------------------------------------------------------
    # SECTION: Chain Fusion
    # ---------------------------------------------------------
    # When you have consecutive maps like: map(f).map(g)
    # Compilation fuses them into a single map that does f(g(x))
    print("\n--- Chain fusion explained ---")

    # This creates 3 nodes: map(double), map(inc), sink
    results_uncompiled = []
    source1 = akayu.Stream()
    source1.map(lambda x: x * 2).map(lambda x: x + 1).sink(results_uncompiled.append)

    # This creates 1 fused node after compilation
    results_compiled = []
    source2 = akayu.Stream()
    source2.map(lambda x: x * 2).map(lambda x: x + 1).sink(results_compiled.append)
    source2.compile()

    # Both produce the same output
    source1.emit_batch([1, 2, 3])
    source2.emit_batch([1, 2, 3])

    print(f"Uncompiled results: {results_uncompiled}")
    print(f"Compiled results: {results_compiled}")
    print("Both produce identical output, but compiled version is faster")

    # ---------------------------------------------------------
    # SECTION: Performance Comparison
    # ---------------------------------------------------------
    # Time a pipeline with and without compilation
    print("\n--- Performance comparison ---")

    iterations = 100000

    # Uncompiled pipeline
    results_unc = []
    source_unc = akayu.Stream()
    source_unc.map(lambda x: x * 2).map(lambda x: x + 1).map(lambda x: x - 3).sink(results_unc.append)

    start = time.perf_counter()
    for i in range(iterations):
        source_unc.emit(i)
    unc_time = time.perf_counter() - start

    # Compiled pipeline (same structure)
    results_comp = []
    source_comp = akayu.Stream()
    source_comp.map(lambda x: x * 2).map(lambda x: x + 1).map(lambda x: x - 3).sink(results_comp.append)
    source_comp.compile()

    start = time.perf_counter()
    for i in range(iterations):
        source_comp.emit(i)
    comp_time = time.perf_counter() - start

    print(f"Iterations: {iterations:,}")
    print(f"Uncompiled time: {unc_time:.3f}s")
    print(f"Compiled time:   {comp_time:.3f}s")
    print(f"Speedup: {unc_time / comp_time:.2f}x")

    # ---------------------------------------------------------
    # SECTION: Verify Correctness
    # ---------------------------------------------------------
    # Ensure compiled version still produces correct output
    print("\n--- Verifying correctness after compilation ---")

    results_a = []
    source_a = akayu.Stream()
    source_a.map(lambda x: x ** 2).filter(lambda x: x < 50).sink(results_a.append)
    source_a.compile()

    results_b = []
    source_b = akayu.Stream()
    source_b.map(lambda x: x ** 2).filter(lambda x: x < 50).sink(results_b.append)
    # Don't compile

    test_data = list(range(10))

    source_a.emit_batch(test_data)
    source_b.emit_batch(test_data)

    print(f"Compiled output:    {results_a}")
    print(f"Uncompiled output:   {results_b}")
    print(f"Outputs match: {results_a == results_b}")

    # ---------------------------------------------------------
    # SECTION: Important Notes About compile()
    # ---------------------------------------------------------
    print("\n--- Important compile() notes ---")
    print("1. Once compiled, you cannot add more nodes to the graph")
    print("2. Call compile() AFTER building your pipeline, BEFORE emitting")
    print("3. Compilation enables chain fusion for consecutive maps/filters")
    print("4. The compiled graph is immutable - no further modifications")

    print("\n" + "=" * 60)
    print("EXAMPLE 03 COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()