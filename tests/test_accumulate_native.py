"""compile() runs common accumulate functions natively.

`acc + x`, `acc + 1`, `max(acc, x)`, `min(acc, x)` (and the other binary
operators) become native operations instead of a Python call per item.
Compiled results must match eager ones, which call the function, exactly:
same values, same winners on ties, same errors.
"""

from decimal import Decimal

import pytest
from akayu._helpers import _analyze_accumulate
from hypothesis import given, settings
from hypothesis import strategies as st

import akayu

step = 2


def max(a, b):  # shadows the builtin on purpose
    return a if a > b else b  # noqa: FURB136 - not the builtin


def run(func, start, items, compiled, batch=False):
    s, out = akayu.Stream(), []
    s.accumulate(func, start).sink(out.append)
    if compiled:
        s.compile()
    if batch:
        s.emit_batch(list(items))
    else:
        for x in items:
            s.emit(x)
    return out


FUNCS = {
    "sum": lambda acc, x: acc + x,
    "count": lambda acc, _: acc + 1,
    "product": lambda acc, x: acc * x,
    "minus": lambda acc, x: acc - x,
    "step": lambda acc, x: acc + step,
    "min": lambda acc, x: min(acc, x),
}


@pytest.mark.parametrize(
    ("func", "expected"),
    [
        (lambda acc, x: acc + x, ("acc_binop", "+")),
        (lambda a, b: a * b, ("acc_binop", "*")),
        (lambda acc, _: acc + 1, ("acc_binop_const", "+", 1)),
        (lambda acc, x: acc + step, ("acc_binop_const", "+", 2)),
        (lambda acc, x: min(acc, x), ("acc_min",)),
        (lambda acc, x: x + acc, None),  # order matters for + on str/list
        (lambda acc, x: min(x, acc), None),  # ties pick a different winner
        (lambda acc, x: max(acc, x), None),  # this module's max, not the builtin
        (lambda acc, x: acc.union(x), None),
        (lambda acc, x, y=1: acc + x, None),
    ],
)
def test_recognised_patterns(func, expected):
    assert _analyze_accumulate(func) == expected


# Defined outside this module, so `max` is the builtin (this module shadows it).
exec("def plain_max(acc, x):\n    return max(acc, x)\n", (ns := {}))
plain_max = ns["plain_max"]


def test_builtin_max_is_recognised():
    assert _analyze_accumulate(plain_max) == ("acc_max",)


SEQUENCES = {
    "ints": (0, [3, 1, 4, 1, 5, 9, 2, 6]),
    "floats": (0.5, [1.5, -2.0, 3.25]),
    "strings": ("", ["a", "b", "c"]),
    "lists": ([], [[1], [2, 3], []]),
    "decimals": (Decimal("1.1"), [Decimal("2.2"), Decimal("-0.3")]),
}


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("kind", SEQUENCES)
@pytest.mark.parametrize("name", ["sum", "count", "product"])
def test_compiled_matches_eager(name, kind, batch):
    func = FUNCS[name]
    start, items = SEQUENCES[kind]
    try:
        expected = run(func, start, items, compiled=False)
    except RuntimeError as e:
        with pytest.raises(RuntimeError) as info:
            run(func, start, items, compiled=True, batch=batch)
        assert type(info.value.__cause__) is type(e.__cause__)
        return
    assert run(func, start, items, compiled=True, batch=batch) == expected


def test_max_min_keep_the_first_of_equal_items():
    """max(acc, x) keeps acc unless x is strictly greater: with 1 and 1.0
    the winner's type shows which one was kept."""
    items = [1.0, 1, True, 2, 2.0]
    for func in (plain_max, lambda acc, x: min(acc, x)):
        eager = run(func, 1, items, compiled=False)
        compiled = run(func, 1, items, compiled=True)
        assert compiled == eager
        assert [type(v) for v in compiled] == [type(v) for v in eager]


def test_errors_match():
    with pytest.raises(RuntimeError, match=r"accumulate") as info:
        run(lambda acc, x: acc + x, 0, [1, "a"], compiled=True)
    assert isinstance(info.value.__cause__, TypeError)


@settings(max_examples=100)
@given(items=st.lists(st.integers(-100, 100), max_size=30), batch=st.booleans())
def test_fuzz_against_eager(items, batch):
    for func in (FUNCS["sum"], FUNCS["minus"], FUNCS["min"], plain_max):
        assert run(func, 0, items, compiled=True, batch=batch) == run(func, 0, items, compiled=False)
