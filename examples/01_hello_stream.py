"""
Example 01: Hello Stream

This example demonstrates the basics of akayu:
- Creating a Stream
- Using map() to transform data
- Using filter() to select data
- Using sink() for terminal operations
- Emitting values into the stream

akayu is a Rust-powered stream processing library that lets you build
data pipelines with high performance and low overhead.
"""

import akayu


def main():
    print("=" * 60)
    print("AKAYU EXAMPLE 01: Hello Stream")
    print("=" * 60)

    # ---------------------------------------------------------
    # SECTION: Basic Stream Creation
    # ---------------------------------------------------------
    # A Stream is the core building block. Think of it as a pipeline
    # that data flows through. You create a source stream and then
    # attach transformation nodes to it.
    print("\n--- Creating a simple stream pipeline ---")

    results = []

    source = akayu.Stream()
    source.map(lambda x: x * 2).sink(results.append)

    for i in range(5):
        source.emit(i)

    print(f"Double each number 0-4: {results}")

    # ---------------------------------------------------------
    # SECTION: Using Filter
    # ---------------------------------------------------------
    # filter() only allows items through that match a condition
    print("\n--- Filtering even numbers ---")

    evens = []

    source2 = akayu.Stream()
    source2.filter(lambda x: x % 2 == 0).sink(evens.append)

    for i in range(6):
        source2.emit(i)

    print(f"Even numbers 0-5: {evens}")

    # ---------------------------------------------------------
    # SECTION: Chaining Operations
    # ---------------------------------------------------------
    # You can chain multiple operations together. Data flows from
    # left to right through each transformation.
    print("\n--- Chaining map and filter ---")

    processed = []

    source3 = akayu.Stream()
    # First double, then filter for values > 5
    source3.map(lambda x: x * 2).filter(lambda x: x > 5).sink(processed.append)

    for i in range(6):
        source3.emit(i)

    print(f"Doubled values > 5: {processed}")

    # ---------------------------------------------------------
    # SECTION: Using sink() for Side Effects
    # ---------------------------------------------------------
    # sink() is a terminal operation that calls a function for each
    # item but doesn't transform the data. Great for printing, saving, etc.
    print("\n--- Using sink() for printing ---")

    source4 = akayu.Stream()
    source4.map(lambda x: f"Item: {x}").sink(print)

    for i in range(3):
        source4.emit(i)

    # ---------------------------------------------------------
    # SECTION: Multiple Sinks (Branching)
    # ---------------------------------------------------------
    # A stream can have multiple sinks - each item flows to all of them
    print("\n--- Multiple sinks (broadcasting) ---")

    sink_a = []
    sink_b = []

    source5 = akayu.Stream()
    source5.map(lambda x: x + 1).sink(sink_a.append)
    source5.map(lambda x: x * 10).sink(sink_b.append)

    source5.emit(5)

    print(f"Sink A received: {sink_a}")
    print(f"Sink B received: {sink_b}")

    # ---------------------------------------------------------
    # SECTION: emit_batch() for Multiple Values
    # ---------------------------------------------------------
    # emit_batch() is more efficient than calling emit() multiple times
    print("\n--- Using emit_batch() for bulk data ---")

    batch_results = []

    source6 = akayu.Stream()
    source6.map(lambda x: x ** 2).sink(batch_results.append)

    source6.emit_batch([1, 2, 3, 4, 5])

    print(f"Squares 1-5: {batch_results}")

    print("\n" + "=" * 60)
    print("EXAMPLE 01 COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()