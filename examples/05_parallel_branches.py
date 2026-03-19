"""
Example 05: Parallel Branches

This example demonstrates:
- par() for creating parallel execution branches
- How branches process data concurrently
- union() to merge parallel branches back together
- seq() for explicit sequential sections

par() is key for I/O-bound operations where branches can overlap
their waiting time, significantly improving throughput.
"""

import time
import akayu
from akayu import Stream, union


def main():
    print("=" * 60)
    print("AKAYU EXAMPLE 05: Parallel Branches")
    print("=" * 60)

    # ---------------------------------------------------------
    # SECTION: Basic Parallel Branching
    # ---------------------------------------------------------
    # par() creates a branch point where all downstream operations
    # run in parallel threads
    print("\n--- Basic parallel branching ---")

    branch_a = []
    branch_b = []

    source = akayu.Stream()
    p = source.par()  # Create parallel branch point

    # Both branches run in parallel
    p.map(lambda x: f"A: {x}").sink(branch_a.append)
    p.map(lambda x: f"B: {x}").sink(branch_b.append)

    source.emit_batch(["hello", "world"])

    print(f"Branch A received: {branch_a}")
    print(f"Branch B received: {branch_b}")

    # ---------------------------------------------------------
    # SECTION: Timing Comparison: Sequential vs Parallel
    # ---------------------------------------------------------
    # Demonstrate that parallel branches complete faster for I/O operations
    print("\n--- Sequential vs Parallel timing ---")

    def slow_operation(x, branch_name, delay=0.05):
        """Simulates an I/O bound operation (like API call, file read)"""
        time.sleep(delay)
        return f"{branch_name}: {x}"

    # Sequential version
    seq_results = []
    source_seq = akayu.Stream()
    source_seq.map(lambda x: slow_operation(x, "seq")).sink(seq_results.append)
    source_seq.map(lambda x: slow_operation(x, "seq")).sink(seq_results.append)

    start = time.perf_counter()
    source_seq.emit("data")
    seq_time = time.perf_counter() - start

    # Parallel version
    par_results_a = []
    par_results_b = []
    source_par = akayu.Stream()
    p = source_par.par()
    p.map(lambda x: slow_operation(x, "par_A")).sink(par_results_a.append)
    p.map(lambda x: slow_operation(x, "par_B")).sink(par_results_b.append)

    start = time.perf_counter()
    source_par.emit("data")
    par_time = time.perf_counter() - start

    print(f"Sequential time: {seq_time:.3f}s (branches run one after another)")
    print(f"Parallel time:   {par_time:.3f}s (branches run simultaneously)")
    print(f"Speedup: {seq_time / par_time:.2f}x")

    # ---------------------------------------------------------
    # SECTION: Merging with union()
    # ---------------------------------------------------------
    # union() combines multiple streams into one, emitting items
    # as they arrive from any source
    print("\n--- Using union() to merge branches ---")

    merged_results = []

    source = akayu.Stream()
    p = source.par()

    # Branch 1: numbers doubled
    branch1 = p.map(lambda x: x * 2)

    # Branch 2: numbers offset by 100
    branch2 = p.map(lambda x: x + 100)

    # Merge both branches back into one stream
    merged = union(branch1, branch2)
    merged.sink(merged_results.append)

    source.emit_batch([1, 2, 3])

    print(f"Input: [1, 2, 3]")
    print(f"Branch 1 (*2): [2, 4, 6]")
    print(f"Branch 2 (+100): [101, 102, 103]")
    print(f"Merged output (order may vary): {sorted(merged_results)}")

    # ---------------------------------------------------------
    # SECTION: Sequential After Parallel with seq()
    # ---------------------------------------------------------
    # seq() explicitly marks a sequential execution point after
    # parallel branches
    print("\n--- Using seq() after parallel branches ---")

    seq_results = []

    source = akayu.Stream()
    p = source.par()

    branch_a = p.map(lambda x: x * 2)
    branch_b = p.map(lambda x: x + 10)

    merged = union(branch_a, branch_b)
    sequential_point = merged.seq()  # Back to sequential execution

    # These operations run sequentially (one after another)
    sequential_point.map(lambda x: f"final_{x}").sink(seq_results.append)

    source.emit(5)

    print(f"Input: 5")
    print(f"Branch A: 10, Branch B: 15")
    print(f"Merged and sequentialized: {seq_results}")

    # ---------------------------------------------------------
    # SECTION: Parallel Branches with Different Operations
    # ---------------------------------------------------------
    # Each branch can have completely different transformations
    print("\n--- Different operations per branch ---")

    transformed_data = {
        "doubled": [],
        "tripled": [],
        "negative": []
    }

    source = akayu.Stream()
    p = source.par()

    # Branch 1: double
    p.map(lambda x: x * 2).sink(lambda x: transformed_data["doubled"].append(x))

    # Branch 2: triple
    p.map(lambda x: x * 3).sink(lambda x: transformed_data["tripled"].append(x))

    # Branch 3: negate
    p.map(lambda x: -x).sink(lambda x: transformed_data["negative"].append(x))

    source.emit_batch([10, 20, 30])

    print(f"Input: [10, 20, 30]")
    print(f"Doubled: {transformed_data['doubled']}")
    print(f"Tripled: {transformed_data['tripled']}")
    print(f"Negative: {transformed_data['negative']}")

    # ---------------------------------------------------------
    # SECTION: Real-world Pattern: Multi-source Processing
    # ---------------------------------------------------------
    # Common pattern: fetch from multiple sources in parallel, merge results
    print("\n--- Real-world pattern: parallel data fetching ---")

    def mock_fetch(url, delay=0.03):
        """Simulates fetching data from different sources"""
        time.sleep(delay)
        return f"data_from_{url}"

    fetch_results = []

    source = akayu.Stream()
    p = source.par()

    # Simulate fetching from different services
    branch1 = p.map(lambda url: mock_fetch(url, 0.05))
    branch2 = p.map(lambda url: mock_fetch(url, 0.03))
    branch3 = p.map(lambda url: mock_fetch(url, 0.04))

    merged = union(branch1, branch2, branch3)
    merged.sink(fetch_results.append)

    start = time.perf_counter()
    source.emit("api.example.com")
    total_time = time.perf_counter() - start

    print(f"Fetched from 3 sources in parallel: {fetch_results}")
    print(f"Total time: {total_time:.3f}s (would be ~0.12s if sequential)")

    print("\n" + "=" * 60)
    print("EXAMPLE 05 COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()