import rstreamz
import pytest


def test_map_args_kwargs():
    s = rstreamz.Stream()
    res = []

    def multiplier(x, factor, offset=0):
        return (x * factor) + offset

    # streamz style: map(func, *args, **kwargs)
    # expect call: multiplier(x, 2, offset=1)
    s.map(multiplier, 2, offset=1).sink(res.append)

    s.emit(10)

    # 10 * 2 + 1 = 21
    assert res == [21]


def test_filter_args_kwargs():
    s = rstreamz.Stream()
    res = []

    # Check if x is instance of type t
    # filter(isinstance, int) -> isinstance(x, int)
    s.filter(isinstance, int).sink(res.append)

    s.emit(1)
    s.emit("string")
    s.emit(2)

    assert res == [1, 2]


def test_sink_args_kwargs():
    s = rstreamz.Stream()
    res = []

    def my_sink(x, prefix=""):
        res.append(f"{prefix}{x}")

    s.sink(my_sink, prefix="val: ")

    s.emit(10)

    assert res == ["val: 10"]
