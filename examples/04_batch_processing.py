"""
Example 04: Batch Processing

This example demonstrates:
- emit_batch() for efficient bulk data emission
- batch_map() for vectorized operations on entire batches
- flatten() for handling nested data

Batch operations are significantly more efficient than processing
items one at a time, especially for vectorized operations like
NumPy or pandas transformations.
"""

import time
import akayu


def main():
    print("=" * 60)
    print("AKAYU EXAMPLE 04: Batch Processing")
    print("=" * 60)

    # ---------------------------------------------------------
    # SECTION: emit_batch() vs Multiple emit()
    # ---------------------------------------------------------
    # emit_batch() is more efficient than calling emit() in a loop
    print("\n--- emit_batch() for bulk emission ---")

    results = []

    source = akayu.Stream()
    source.map(lambda x: x * 2).sink(results.append)

    # Emit a batch of 1000 numbers
    source.emit_batch(list(range(1000)))

    print(f"Emitted 1000 items via emit_batch")
    print(f"First 10 results: {results[:10]}")
    print(f"Last 10 results: {results[-10:]}")

    # ---------------------------------------------------------
    # SECTION: batch_map() for Vectorized Operations
    # ---------------------------------------------------------
    # batch_map() processes the entire batch at once with one function call.
    # This is perfect for NumPy operations that work on arrays.
    print("\n--- batch_map() for vectorized operations ---")

    try:
        import numpy as np

        source = akayu.Stream()
        source.batch_map(lambda batch: np.sqrt(batch).tolist()).sink(print)

        source.emit_batch([1, 4, 9, 16, 25])

        print("\nNote: batch_map calls your function once per batch,")
        print("not once per item. Great for NumPy/pandas operations!")

    except ImportError:
        print("NumPy not available - showing basic batch_map instead")

        source = akayu.Stream()
        source.batch_map(lambda batch: [x * 2 for x in batch]).sink(print)

        source.emit_batch([1, 2, 3, 4, 5])

    # ---------------------------------------------------------
    # SECTION: Performance: batch_map vs map
    # ---------------------------------------------------------
    print("\n--- batch_map vs map performance ---")

    data = list(range(10000))

    # Using map (processes one at a time)
    results_map = []
    source_map = akayu.Stream()
    source_map.map(lambda x: x ** 0.5).sink(results_map.append)

    start = time.perf_counter()
    source_map.emit_batch(data)
    map_time = time.perf_counter() - start

    # Using batch_map (processes entire batch at once)
    results_batch = []
    source_batch = akayu.Stream()
    source_batch.batch_map(lambda batch: [x ** 0.5 for x in batch]).sink(results_batch.append)

    start = time.perf_counter()
    source_batch.emit_batch(data)
    batch_time = time.perf_counter() - start

    print(f"map() time:     {map_time:.4f}s")
    print(f"batch_map() time: {batch_time:.4f}s")
    print(f"Results match: {results_map == results_batch}")

    # ---------------------------------------------------------
    # SECTION: flatten() for Nested Data
    # ---------------------------------------------------------
    # flatten() takes each emitted item (which is an iterable) and
    # emits its elements individually
    print("\n--- flatten() for nested data ---")

    nested_data = [
        [1, 2, 3],
        [4, 5],
        [],
        [6, 7, 8, 9]
    ]

    flat_results = []

    source = akayu.Stream()
    source.flatten().sink(flat_results.append)

    source.emit_batch(nested_data)

    print(f"Input: {nested_data}")
    print(f"Flattened: {flat_results}")

    # ---------------------------------------------------------
    # SECTION: Combining batch_map and flatten
    # ---------------------------------------------------------
    # batch_map can return multiple items per batch, flatten emits them
    print("\n--- Combining batch_map and flatten ---")

    pairs = []

    source = akayu.Stream()
    # Split each number into two items (x, x*10)
    source.batch_map(lambda batch: [(x, x * 10) for x in batch]).flatten().sink(pairs.append)

    source.emit_batch([1, 2, 3])

    print(f"Input: [1, 2, 3]")
    print(f"Output (pairs): {pairs}")

    # ---------------------------------------------------------
    # SECTION: Real-world Batch Processing Pattern
    # ---------------------------------------------------------
    # A common pattern: batch process rows, transform, flatten to individual records
    print("\n--- Real-world batch pattern ---")

    # Simulate database rows being processed in batches
    batches = [
        {"user_id": 1, "name": "Alice", "scores": [85, 92, 78]},
        {"user_id": 2, "name": "Bob", "scores": [90, 88, 95]},
        {"user_id": 3, "name": "Charlie", "scores": [72, 81, 88]},
    ]

    student_records = []

    source = akayu.Stream()

    def process_batch(batch):
        records = []
        for student in batch:
            for score in student["scores"]:
                records.append({
                    "student": student["name"],
                    "score": score
                })
        return records

    source.batch_map(process_batch).flatten().sink(student_records.append)

    source.emit_batch(batches)

    print(f"Processed {len(batches)} student batches")
    print(f"Output records: {student_records}")

    print("\n" + "=" * 60)
    print("EXAMPLE 04 COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()