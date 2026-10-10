"""
Example 02: Method Chaining

This example demonstrates:
- Building complex pipelines with method chaining
- Branching a stream to multiple paths
- How data flows through chained operations

Method chaining is at the heart of akayu's API. Each operation returns
a new Stream node that you can continue to transform.
"""

import akayu


def main():
    print("=" * 60)
    print("AKAYU EXAMPLE 02: Method Chaining")
    print("=" * 60)

    # ---------------------------------------------------------
    # SECTION: Basic Chaining
    # ---------------------------------------------------------
    # You can chain operations in a single expression
    print("\n--- Basic method chaining ---")

    result = []

    source = akayu.Stream()
    source.map(lambda x: x * 2).map(lambda x: x + 1).filter(lambda x: x > 5).sink(result.append)

    source.emit_batch([1, 2, 3, 4, 5])

    print(f"Input: [1, 2, 3, 4, 5]")
    print(f"Operations: *2, +1, filter(>5)")
    print(f"Output: {result}")

    # ---------------------------------------------------------
    # SECTION: Branching with par()
    # ---------------------------------------------------------
    # Use par() to create parallel branches. Each branch processes
    # the same data independently.
    print("\n--- Branching with par() ---")

    branch_a = []
    branch_b = []

    source = akayu.Stream()
    parallel = source.par()

    parallel.map(lambda x: x * 2).sink(branch_a.append)
    parallel.map(lambda x: x + 100).sink(branch_b.append)

    source.emit_batch([1, 2, 3])

    print(f"Input: [1, 2, 3]")
    print(f"Branch A (*2): {branch_a}")
    print(f"Branch B (+100): {branch_b}")

    # ---------------------------------------------------------
    # SECTION: Complex Pipeline with Multiple Transformations
    # ---------------------------------------------------------
    # A more realistic example: process log entries
    print("\n--- Processing log-like data ---")

    logs = [
        "INFO: User logged in",
        "ERROR: Connection failed",
        "INFO: Request processed",
        "WARN: Slow response",
        "ERROR: Timeout occurred",
        "DEBUG: Cache hit",
    ]

    processed_logs = []

    source = akayu.Stream()

    # Extract level, convert to uppercase, filter errors
    (
        source.map(lambda log: log.split(":")[0])  # Extract level
        .map(lambda level: level.strip().upper())  # Normalize
        .filter(lambda level: level == "ERROR")  # Keep only errors
        .sink(processed_logs.append)
    )

    for log in logs:
        source.emit(log)

    print(f"Input: {logs}")
    print(f"Error logs extracted: {processed_logs}")

    # ---------------------------------------------------------
    # SECTION: Multiple Operations in One Chain
    # ---------------------------------------------------------
    # Chain map, filter, map, sink - demonstrating full pipeline
    print("\n--- Full pipeline: map -> filter -> map -> sink ---")

    numbers = list(range(1, 11))

    final_result = []

    source = akayu.Stream()
    (
        source.map(lambda x: x * 3)  # Multiply by 3
        .filter(lambda x: x % 2 == 0)  # Keep even
        .map(lambda x: f"Value: {x}")  # Format as string
        .sink(final_result.append)
    )

    source.emit_batch(numbers)

    print(f"Input: {numbers}")
    print(f"Operations: *3, keep even, format")
    print(f"Output: {final_result}")

    # ---------------------------------------------------------
    # SECTION: Using starmap for Tuple Unpacking
    # ---------------------------------------------------------
    # starmap() unpacks each element as arguments to the function
    print("\n--- Using starmap() for tuple unpacking ---")

    coords = [(1, 2), (3, 4), (5, 6)]
    distances = []

    source = akayu.Stream()
    source.starmap(lambda x, y: (x**2 + y**2) ** 0.5).sink(distances.append)

    source.emit_batch(coords)

    print(f"Input (x, y): {coords}")
    print(f"Euclidean distance from origin: {[round(d, 2) for d in distances]}")

    # ---------------------------------------------------------
    # SECTION: Accumulate for Running State
    # ---------------------------------------------------------
    # accumulate() keeps state between items - great for running totals
    print("\n--- Using accumulate() for running totals ---")

    running_sum = []

    source = akayu.Stream()
    source.accumulate(lambda state, x: state + x, start=0).sink(running_sum.append)

    source.emit_batch([10, 5, 3, 7, 2])

    print(f"Input: [10, 5, 3, 7, 2]")
    print(f"Running sum: {running_sum}")

    print("\n" + "=" * 60)
    print("EXAMPLE 02 COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
