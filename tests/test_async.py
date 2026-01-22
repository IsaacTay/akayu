import akayu
import asyncio
import pytest


@pytest.mark.asyncio
async def test_async_map():
    s = akayu.Stream()
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
    s = akayu.Stream()
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
    s = akayu.Stream()
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


@pytest.mark.asyncio
async def test_async_map_error():
    """Test error propagation in async map."""
    s = akayu.Stream()

    async def failing_async(x):
        await asyncio.sleep(0.001)
        raise ValueError("async boom")

    s.map(failing_async).sink(lambda x: None)
    # Async errors propagate directly as ValueError
    with pytest.raises(ValueError, match="async boom"):
        await s.emit(1)


@pytest.mark.asyncio
async def test_async_filter_error():
    """Test error in async filter predicate."""
    s = akayu.Stream()

    async def failing_pred(x):
        raise ValueError("filter boom")

    s.filter(failing_pred).sink(lambda x: None)
    # Async errors propagate directly as ValueError
    with pytest.raises(ValueError, match="filter boom"):
        await s.emit(1)
