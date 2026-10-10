"""
Example 06: Concurrent I/O

This example demonstrates:
- prefetch(n) to overlap I/O-bound work (network, disk) within one branch
- That prefetch preserves output order
- flush() to drain items still in flight
- prefetch with filter()
- How errors raised in worker threads surface

akayu does not use asyncio. Concurrency is thread-based: prefetch(n) runs up
to n calls of the next operation at once in a shared thread pool, while par()
(see example 05) runs whole branches concurrently.
"""

import time

import akayu


def fetch_price(item_id):
    """Pretend to call a slow web API."""
    time.sleep(0.03)
    return {"id": item_id, "price": item_id * 1.5}


def main():
    print("=" * 60)
    print("AKAYU EXAMPLE 06: Concurrent I/O")
    print("=" * 60)

    item_ids = list(range(12))

    # ---------------------------------------------------------
    # SECTION: Sequential Baseline
    # ---------------------------------------------------------
    # A plain map() waits for each call before starting the next
    print("\n--- Sequential map (one request at a time) ---")

    source = akayu.Stream()
    sequential = []
    source.map(fetch_price).sink(sequential.append)

    start = time.perf_counter()
    source.emit_batch(item_ids)
    sequential_ms = (time.perf_counter() - start) * 1000
    print(f"Fetched {len(sequential)} prices in {sequential_ms:.0f} ms")

    # ---------------------------------------------------------
    # SECTION: prefetch() for Concurrent Requests
    # ---------------------------------------------------------
    # prefetch(n) keeps up to n calls in flight at once
    print("\n--- prefetch(6): up to 6 requests in flight ---")

    source = akayu.Stream()
    concurrent = []
    source.prefetch(6).map(fetch_price).sink(concurrent.append)

    start = time.perf_counter()
    source.emit_batch(item_ids)
    # Results are released in input order as they complete; flush() waits for
    # the requests still in flight and emits the rest.
    source.flush()
    concurrent_ms = (time.perf_counter() - start) * 1000

    print(f"Fetched {len(concurrent)} prices in {concurrent_ms:.0f} ms")
    print(f"Speedup: {sequential_ms / concurrent_ms:.1f}x")
    print(f"Same results in the same order: {concurrent == sequential}")

    # ---------------------------------------------------------
    # SECTION: prefetch() with filter()
    # ---------------------------------------------------------
    # prefetch also works with filter, e.g. checking items against a service
    print("\n--- prefetch(4).filter(): concurrent checks ---")

    def is_in_stock(item_id):
        time.sleep(0.03)  # Pretend to ask an inventory service
        return item_id % 3 != 0

    source = akayu.Stream()
    in_stock = []
    source.prefetch(4).filter(is_in_stock).sink(in_stock.append)

    source.emit_batch(item_ids)
    source.flush()
    print(f"In stock: {in_stock}")

    # ---------------------------------------------------------
    # SECTION: Errors from Worker Threads
    # ---------------------------------------------------------
    # An exception in a worker thread is raised from emit/flush like any
    # other akayu error: a RuntimeError with the original as __cause__
    print("\n--- Errors from worker threads ---")

    def flaky_fetch(item_id):
        time.sleep(0.01)
        if item_id == 3:
            raise ConnectionError(f"timeout fetching item {item_id}")
        return item_id

    source = akayu.Stream()
    received = []
    source.prefetch(4).map(flaky_fetch).sink(received.append)

    try:
        source.emit_batch(list(range(6)))
        source.flush()
    except RuntimeError as e:
        print(f"Caught: {type(e.__cause__).__name__}: {e.__cause__}")
    print(f"Items delivered before the failure: {received}")

    # ---------------------------------------------------------
    # Summary
    # ---------------------------------------------------------
    print("\n" + "=" * 60)
    print("KEY TAKEAWAYS:")
    print("1. prefetch(n) overlaps I/O-bound calls within a single branch")
    print("2. Output order matches input order")
    print("3. Call flush() to drain items still in flight")
    print("4. Worker errors surface as RuntimeError with __cause__")
    print("5. Use par() (example 05) to run whole branches concurrently")
    print("=" * 60)


if __name__ == "__main__":
    main()
