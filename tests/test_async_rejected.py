"""akayu never awaits coroutines, so async callables are rejected up front.

Before, `s.map(async_fn)` silently emitted un-awaited coroutine objects.
"""

import functools

import pytest

import akayu


async def async_fn(x):
    return x


async def async_gen_fn(x):
    yield x


class Service:
    async def fetch(self, x):
        return x


def _ops(fn):
    """Every operation that takes a callable, applied to `fn`."""
    return {
        "map": lambda s: s.map(fn),
        "starmap": lambda s: s.starmap(fn),
        "batch_map": lambda s: s.batch_map(fn),
        "filter": lambda s: s.filter(fn),
        "chunk_by": lambda s: s.chunk_by(fn),
        "chunks_between(open)": lambda s: s.chunks_between(fn, lambda x: True),
        "chunks_between(close)": lambda s: s.chunks_between(lambda x: True, fn),
        "sink": lambda s: s.sink(fn),
        "accumulate": lambda s: s.accumulate(fn, start=0),
        "prefetch().map": lambda s: s.prefetch(2).map(fn),
        "prefetch().filter": lambda s: s.prefetch(2).filter(fn),
        "multistream.map": lambda s: akayu.MultiStream([s, s]).map(fn),
    }


ASYNC_CALLABLES = {
    "coroutine function": async_fn,
    "async generator function": async_gen_fn,
    "partial of async": functools.partial(async_fn),
    "bound async method": Service().fetch,
}


@pytest.mark.parametrize("kind", ASYNC_CALLABLES)
@pytest.mark.parametrize("op", _ops(None))
def test_async_callable_rejected(op, kind):
    build = _ops(ASYNC_CALLABLES[kind])[op]
    with pytest.raises(TypeError, match="does not run coroutines"):
        build(akayu.Stream())


def test_error_names_the_operation_and_function():
    with pytest.raises(TypeError, match=r"^map\(\) got async function 'async_fn'"):
        akayu.Stream().map(async_fn)


class Callable:
    def __call__(self, x):
        return x


SYNC_CALLABLES = {
    "lambda": lambda x: x,
    "builtin": abs,
    "partial": functools.partial(max, 0),
    "callable object": Callable(),
}


@pytest.mark.parametrize("kind", SYNC_CALLABLES)
def test_sync_callables_accepted(kind):
    s, out = akayu.Stream(), []
    s.map(SYNC_CALLABLES[kind]).sink(out.append)
    s.emit(3)
    assert out == [3]


def test_coroutine_objects_as_data_pass_through():
    """Only async *functions* are rejected; a coroutine is just data."""
    coro = async_fn(1)
    s, out = akayu.Stream(), []
    s.map(lambda x: x).sink(out.append)
    s.emit(coro)
    assert out == [coro]
    coro.close()


def test_emit_flush_drain_return_none():
    s = akayu.Stream()
    s.map(lambda x: x).collect().sink(lambda x: None)
    assert s.emit(1) is None
    assert s.emit_batch([2, 3]) is None
    assert s.flush() is None
    assert s.drain() is None
