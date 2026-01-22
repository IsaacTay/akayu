import asyncio
from rstreamz import Stream

async def my_coro():
    return 42

def test_async_false_skips_await():
    """
    Verify that asynchronous=False explicitly disables the awaitable check,
    causing coroutines to be passed through as-is.
    """
    source = Stream(asynchronous=False)
    results = []
    source.sink(results.append)
    
    coro = my_coro()
    
    # This should NOT be awaited because we said async=False
    # It should be passed through as the coroutine object
    source.emit(coro)
    
    assert len(results) == 1
    assert asyncio.iscoroutine(results[0])
    
    # Cleanup to avoid "coroutine was never awaited" warning
    asyncio.run(results[0])

def test_async_default_awaits():
    """
    Verify that default behavior (asynchronous=None) DOES check and await.
    """
    source = Stream()
    results = []
    source.sink(results.append)
    
    coro = my_coro()
    
    # This SHOULD be awaited because default is auto-detect (and we disabled the optimization that locks it to sync)
    # Actually, default is: asynchronous=None, skip_async_check=False.
    # propagate() checks !skip_async_check (True).
    # checks is_awaitable(coro) -> True.
    # calls _process_async -> returns a Future/Task (which is awaitable).
    
    ret = source.emit(coro)
    
    # rstreamz returns a coroutine/future when async processing happens
    assert asyncio.iscoroutine(ret) or asyncio.isfuture(ret)
    
    asyncio.run(ret)
    
    assert results == [42]
