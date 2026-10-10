"""map/filter/sink/batch_map(func, *args, **kwargs) bind the extras natively.

They used to run a Python wrapper per item, `func(x, *args, **kwargs)`,
which with keyword arguments built a new dict per call (4x the cost of the
map). Results, errors and garbage collection must be exactly as before.
"""

import gc
import weakref

import pytest

import akayu


def scale(x, factor=1, offset=0):
    return x * factor + offset


EXTRAS = {
    "positional": ((3,), {}),
    "keyword": ((), {"factor": 3}),
    "both": ((3,), {"offset": 1}),
    "two keywords": ((), {"offset": 1, "factor": 2}),
}


def run(build, items, mode):
    s, out = akayu.Stream(), []
    build(s).sink(out.append)
    if mode == "compiled":
        s.compile()
    if mode == "batch":
        s.emit_batch(items)
    else:
        for x in items:
            s.emit(x)
    return out


@pytest.mark.parametrize("mode", ["eager", "compiled", "batch"])
@pytest.mark.parametrize("extra", EXTRAS)
def test_map_with_extras(extra, mode):
    args, kwargs = EXTRAS[extra]
    out = run(lambda s: s.map(scale, *args, **kwargs), [1, 2, 3], mode)
    assert out == [scale(x, *args, **kwargs) for x in [1, 2, 3]]


@pytest.mark.parametrize("mode", ["eager", "compiled", "batch"])
def test_filter_with_extras(mode):
    def above(x, limit, inclusive=False):
        return x >= limit if inclusive else x > limit

    assert run(lambda s: s.filter(above, 2, inclusive=True), [1, 2, 3], mode) == [2, 3]
    assert run(lambda s: s.filter(isinstance, int), [1, "a", 2.0, 3], mode) == [1, 3]
    assert run(lambda s: s.filter(isinstance, (int, str)), [1, "a", 2.0], mode) == [1, "a"]


@pytest.mark.parametrize("mode", ["eager", "compiled", "batch"])
def test_sink_and_batch_map_with_extras(mode):
    seen = []

    def record(x, tag, *, sep=":"):
        seen.append(f"{tag}{sep}{x}")

    s = akayu.Stream()
    s.batch_map(lambda xs, k: [x * k for x in xs], 10).sink(record, "v", sep="=")
    if mode == "compiled":
        s.compile()
    if mode == "batch":
        s.emit_batch([1, 2])
    else:
        s.emit(1)
        s.emit(2)
    assert seen == ["v=10", "v=20"]


def test_errors_name_the_node_and_keep_the_cause():
    s = akayu.Stream()
    s.map(scale, factor="x", offset=1).sink(lambda _: None)
    with pytest.raises(RuntimeError, match=r"map\(scale\)") as info:
        s.emit(2)
    assert isinstance(info.value.__cause__, TypeError)


def test_bound_arguments_are_garbage_collected():
    """A cycle through a bound argument (here, the stream itself) is freed."""

    def push(x, target):
        target.emit(x)

    s = akayu.Stream()
    s.sink(push, s)
    ref = weakref.ref(s)
    del s
    gc.collect()
    assert ref() is None
