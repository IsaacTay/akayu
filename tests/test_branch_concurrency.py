import rstreamz
import asyncio
import pytest
import time


@pytest.mark.asyncio
async def test_concurrent_branches():
    s = rstreamz.Stream()
    res1 = []
    res2 = []

    async def slow_fn_1(x):
        await asyncio.sleep(0.1)
        return x * 2

    async def slow_fn_2(x):
        await asyncio.sleep(0.1)
        return x * 3

    # Branching
    s.map(slow_fn_1).sink(res1.append)
    s.map(slow_fn_2).sink(res2.append)

    start = time.time()

    # Emit creates two coroutines, one for each branch.
    # rstreamz should gather them.
    # If sequential: 0.1s + 0.1s = 0.2s
    # If concurrent: max(0.1, 0.1) = 0.1s
    await s.emit(10)

    end = time.time()
    duration = end - start

    print(f"Concurrent branches duration: {duration:.3f}s")

    assert res1 == [20]
    assert res2 == [30]

    # Allow small overhead, but strictly less than sequential sum
    assert duration < 0.18


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(test_concurrent_branches())
