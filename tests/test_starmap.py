from collections import namedtuple

import pytest

import akayu


def test_starmap():
    s = akayu.Stream()
    res = []

    def adder(x, y, offset=0):
        return x + y + offset

    s.starmap(adder, offset=1).sink(res.append)

    # starmap expects iterables
    s.emit((1, 2))  # 1 + 2 + 1 = 4
    s.emit([3, 4])  # 3 + 4 + 1 = 8

    assert res == [4, 8]


def test_starmap_batch():
    """Test starmap with emit_batch."""
    s = akayu.Stream()
    L = []
    s.starmap(lambda a, b: a + b).sink(L.append)
    s.emit_batch([(1, 2), (3, 4), (5, 6)])
    assert L == [3, 7, 11]


Point = namedtuple("Point", "x y")


def _gen(*vals):
    return (v for v in vals)


ITEMS = {
    "tuple": lambda: (1, 2),
    "namedtuple": lambda: Point(1, 2),
    "list": lambda: [1, 2],
    "generator": lambda: _gen(1, 2),
    "dict": lambda: {1: "a", 2: "b"},  # *d passes the keys
}


@pytest.mark.parametrize("mode", ["eager", "compiled", "batch"])
@pytest.mark.parametrize("kind", ITEMS)
def test_starmap_unpacks_like_star_call(kind, mode):
    """starmap(f) without extra arguments calls f(*item) natively (it used to
    go through a Python wrapper per item); results must match *item."""

    def pair(a, b):
        return (a, b)

    s, out = akayu.Stream(), []
    s.starmap(pair).sink(out.append)
    if mode == "compiled":
        s.compile()
    expected = pair(*ITEMS[kind]())
    if mode == "batch":
        s.emit_batch([ITEMS[kind]()])
    else:
        s.emit(ITEMS[kind]())
    assert out == [expected]


@pytest.mark.parametrize("item", [5, (1, 2, 3)])
def test_starmap_errors_name_the_node(item):
    s = akayu.Stream()
    s.starmap(lambda a, b: a + b).sink(lambda _: None)
    with pytest.raises(RuntimeError, match=r"starmap\(<lambda>\)") as info:
        s.emit(item)
    assert isinstance(info.value.__cause__, TypeError)


def test_starmap_in_a_fused_chain():
    s, out = akayu.Stream(), []
    s.map(lambda x: (x, x * 10)).starmap(lambda a, b: a + b).filter(lambda v: v % 2 == 0).sink(out.append)
    s.compile()
    s.emit_batch([1, 2, 3, 4])
    assert out == [22, 44]
