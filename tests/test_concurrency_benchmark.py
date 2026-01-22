import pytest
import akayu
import streamz
import asyncio
import time

DELAY = 0.05
BRANCHES = 100
ITEMS = 10


# Async Benchmark (Existing)
async def benchmark_concurrency(source_cls):
    s = source_cls()
    completed = 0

    async def slow_op(x):
        await asyncio.sleep(DELAY)
        return x

    def sink_fn(x):
        nonlocal completed
        completed += 1

    for _ in range(BRANCHES):
        s.map(slow_op).sink(sink_fn)

    start = time.time()
    for i in range(ITEMS):
        await s.emit(i)
    end = time.time()

    duration = end - start
    expected_ops = ITEMS * BRANCHES
    assert completed == expected_ops
    return duration


@pytest.mark.asyncio
async def test_benchmark_concurrency_streamz():
    # Attempt to fix streamz async behavior
    # Streamz map needs to know it's async?
    # Usually it auto-detects.
    duration = await benchmark_concurrency(lambda: streamz.Stream(asynchronous=True))
    print(f"\nStreamz (Async) Duration: {duration:.4f}s")


@pytest.mark.asyncio
async def test_benchmark_concurrency_akayu():
    duration = await benchmark_concurrency(akayu.Stream)
    print(f"\nRStreamz (Async) Duration: {duration:.4f}s")


# Sync Benchmark
def benchmark_sync(source_cls):
    s = source_cls()
    completed = 0

    # Pure sync operation, purely CPU/Overhead bound
    def fast_op(x):
        return x + 1

    def sink_fn(x):
        nonlocal completed
        completed += 1

    # 100 branches
    for _ in range(BRANCHES):
        s.map(fast_op).sink(sink_fn)

    start = time.time()
    # 1000 items
    SYNC_ITEMS = 1000
    for i in range(SYNC_ITEMS):
        s.emit(i)
    end = time.time()

    duration = end - start
    expected_ops = SYNC_ITEMS * BRANCHES
    assert completed == expected_ops
    return duration


def test_benchmark_sync_streamz(benchmark):
    # We use pytest-benchmark for more accurate stats if possible,
    # but here we just run it once like the async one for comparison print.
    duration = benchmark_sync(streamz.Stream)
    print(f"\nStreamz (Sync) Duration: {duration:.4f}s")


def test_benchmark_sync_akayu(benchmark):
    duration = benchmark_sync(akayu.Stream)
    print(f"\nRStreamz (Sync) Duration: {duration:.4f}s")
