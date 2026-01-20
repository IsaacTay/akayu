import rstreamz
import asyncio
import pytest


@pytest.mark.asyncio
async def test_async_map():
    s = rstreamz.Stream()
    res = []

    async def slow_double(x):
        await asyncio.sleep(0.001)
        return x * 2

    s.map(slow_double).sink(res.append)

    await s.emit(10)
    await s.emit(20)

    assert res == [20, 40]


@pytest.mark.asyncio
async def test_async_filter():
    s = rstreamz.Stream()
    res = []

    async def async_pred(x):
        await asyncio.sleep(0.001)
        return x > 5

    s.filter(async_pred).sink(res.append)

    await s.emit(1)
    await s.emit(10)

    assert res == [10]


@pytest.mark.asyncio
async def test_mixed_sync_async():
    s = rstreamz.Stream()
    res = []

    # Sync map
    s1 = s.map(lambda x: x + 1)

    # Async map
    async def async_double(x):
        await asyncio.sleep(0.001)
        return x * 2

    s2 = s1.map(async_double)

    s2.sink(res.append)

    await s.emit(10)  # 10 -> 11 -> 22

    assert res == [22]
