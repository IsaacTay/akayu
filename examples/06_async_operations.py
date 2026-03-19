"""
Example 06: Async Operations

This example demonstrates:
- Using async functions with map() and filter()
- How emit() returns a coroutine that must be awaited
- Mixing sync and async operations in the same pipeline
- Error handling in async streams

akayu has built-in support for async/await. When you use async
functions, emit() returns a coroutine that you await.
"""

import asyncio
import akayu


async def main():
    print("=" * 60)
    print("AKAYU EXAMPLE 06: Async Operations")
    print("=" * 60)

    # ---------------------------------------------------------
    # SECTION: Basic Async Map
    # ---------------------------------------------------------
    # When using async functions, emit returns a coroutine
    print("\n--- Basic async map ---")

    results = []

    async def async_double(x):
        await asyncio.sleep(0.001)  # Simulate async I/O
        return x * 2

    source = akayu.Stream()
    source.map(async_double).sink(results.append)

    await source.emit(10)
    await source.emit(20)

    print(f"Results: {results}")

    # ---------------------------------------------------------
    # SECTION: Async Filter
    # ---------------------------------------------------------
    # Async predicates work the same way
    print("\n--- Async filter ---")

    filtered_results = []

    async def async_gt_5(x):
        await asyncio.sleep(0.001)
        return x > 5

    source = akayu.Stream()
    source.filter(async_gt_5).sink(filtered_results.append)

    await source.emit(3)
    await source.emit(8)
    await source.emit(10)
    await source.emit(4)

    print(f"Input: [3, 8, 10, 4]")
    print(f"Filtered (> 5): {filtered_results}")

    # ---------------------------------------------------------
    # SECTION: Mixed Sync and Async
    # ---------------------------------------------------------
    # You can mix sync and async operations in the same pipeline
    print("\n--- Mixed sync and async operations ---")

    mixed_results = []

    source = akayu.Stream()

    # Sync operation first
    s1 = source.map(lambda x: x + 1)

    # Then async operation
    async def async_multiply(x):
        await asyncio.sleep(0.001)
        return x * 2

    s2 = s1.map(async_multiply)

    # Sync again
    s3 = s2.map(lambda x: x + 100)

    s3.sink(mixed_results.append)

    await source.emit(10)  # 10 -> 11 -> 22 -> 122

    print(f"Input: 10")
    print(f"Operations: +1 (sync), *2 (async), +100 (sync)")
    print(f"Output: {mixed_results[0]}")

    # ---------------------------------------------------------
    # SECTION: Batch Async with emit_batch
    # ---------------------------------------------------------
    # emit_batch also works with async pipelines
    print("\n--- Async with emit_batch ---")

    batch_results = []

    async def async_square(x):
        await asyncio.sleep(0.001)
        return x ** 2

    source = akayu.Stream()
    source.map(async_square).sink(batch_results.append)

    await source.emit_batch([1, 2, 3, 4, 5])

    print(f"Input: [1, 2, 3, 4, 5]")
    print(f"Squared: {batch_results}")

    # ---------------------------------------------------------
    # SECTION: Error Handling in Async
    # ---------------------------------------------------------
    # Errors in async functions propagate through the stream
    print("\n--- Error handling in async streams ---")

    source = akayu.Stream()

    async def might_fail(x):
        await asyncio.sleep(0.001)
        if x == 3:
            raise ValueError("3 is not allowed!")
        return x * 2

    source.map(might_fail).sink(lambda x: print(f"Got: {x}"))

    for i in [1, 2, 3, 4]:
        try:
            await source.emit(i)
        except ValueError as e:
            print(f"Error at emit({i}): {e}")

    # ---------------------------------------------------------
    # SECTION: Awaiting Multiple Emits
    # ---------------------------------------------------------
    # You can await multiple emits in sequence
    print("\n--- Awaiting multiple emits ---")

    collected = []
    source = akayu.Stream()
    source.map(lambda x: x ** 2).sink(collected.append)

    # Emit multiple values and wait for all to process
    for i in range(5):
        await source.emit(i)

    print(f"Squares 0-4: {collected}")

    # ---------------------------------------------------------
    # SECTION: gather() for Concurrent Emits
    # ---------------------------------------------------------
    # You can gather multiple emit coroutines for concurrent processing
    print("\n--- Concurrent emits with gather ---")

    collected = []
    source = akayu.Stream()
    source.map(lambda x: x * 10).sink(collected.append)

    # Create coroutines but don't await yet
    coroutines = [source.emit(i) for i in range(5)]

    # Gather all at once
    if coroutines:
        await asyncio.gather(*coroutines)

    print(f"Emitted 5 values concurrently: {collected}")

    print("\n" + "=" * 60)
    print("EXAMPLE 06 COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())