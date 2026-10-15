"""Native operations without compile().

An uncompiled map or filter node runs its lambda natively when the lambda's
analysis depends only on its code (`x + 1`, `x["k"]`, `x.strip()`): there is
no closure, default or global value whose later change the node would miss.
Everything else is still called: lambdas that read such values (late binding
must hold), `def` functions (their body lines would stop running, which
coverage and breakpoints would notice), and a function whose code was
replaced (IPython's autoreload does that).
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from support import calls_during

import akayu

LAMBDAS = [
    lambda x: x + 1,
    lambda x: x * 2 + 1,
    lambda x: x % 3 == 0,
    lambda x: x // 2 - 1,
    lambda x: x["k"],
    lambda x: x.get("k", 0),
    lambda x: x.strip(),
    lambda x: x.upper().startswith("A"),
    lambda x: not x,
    lambda x: x is None,
    lambda x: len(x) > 1 if False else x,  # not analysable: always called
]

items = st.one_of(
    st.integers(-(2**70), 2**70),
    st.floats(allow_nan=False),
    st.text(alphabet="aA ", max_size=4),
    st.dictionaries(st.just("k"), st.integers(0, 3), max_size=1),
    st.none(),
    st.booleans(),
)


def python(func, x):
    try:
        return ("value", func(x))
    except Exception as e:
        return ("error", type(e), str(e))


def eager(func, x, op, batch):
    s, out = akayu.Stream(), []
    getattr(s, op)(func).sink(out.append)
    try:
        if batch:
            s.emit_batch([x])
        else:
            s.emit(x)
    except RuntimeError as e:
        return ("error", type(e.__cause__), str(e.__cause__))
    if op == "filter":
        return ("value", bool(out))
    (value,) = out
    return ("value", value)


@settings(max_examples=400, deadline=None)
@given(func=st.sampled_from(LAMBDAS), x=items, op=st.sampled_from(["map", "filter"]), batch=st.booleans())
def test_matches_python(func, x, op, batch):
    expected = python(func, x)
    if op == "filter" and expected[0] == "value":
        try:
            expected = ("value", bool(expected[1]))
        except Exception as e:
            expected = ("error", type(e), str(e))
    got = eager(func, x, op, batch)
    if expected[0] == "value" and got[0] == "value":
        assert (type(got[1]), repr(got[1])) == (type(expected[1]), repr(expected[1])), (x, expected, got)
    else:
        assert got == expected, (x, expected, got)


def run(func, items, op="map"):
    s, out = akayu.Stream(), []
    getattr(s, op)(func).sink(out.append)
    for x in items:
        s.emit(x)
    return out


def test_code_only_lambdas_run_natively():
    inc = lambda x: x + 1  # noqa: E731
    assert calls_during(lambda: run(inc, [1, 2, 3]), inc) == 0


def inc_def(x):
    return x + 1


def test_def_functions_are_still_called():
    """Coverage and breakpoints see a def function's body run."""
    assert calls_during(lambda: run(inc_def, [1, 2, 3]), inc_def) == 3


def test_closures_and_globals_keep_late_binding():
    k = 1
    s, out = akayu.Stream(), []
    s.map(lambda x: x + k).sink(out.append)
    s.emit(1)
    k = 10
    s.emit(1)
    assert out == [2, 11]


def test_globals_keep_late_binding():
    ns = {"limit": 3}
    exec("below = lambda x: x < limit", ns)
    s, out = akayu.Stream(), []
    s.filter(ns["below"]).sink(out.append)
    s.emit(2)
    ns["limit"] = 1
    s.emit(2)
    assert out == [2]


@pytest.mark.parametrize("batch", [False, True])
def test_replaced_code_runs(batch):
    """IPython's autoreload swaps a function's code in place."""
    f = lambda x: x + 1  # noqa: E731
    s, out = akayu.Stream(), []
    s.map(f).sink(out.append)
    s.emit(1)
    f.__code__ = (lambda x: x * 10).__code__
    s.emit_batch([1]) if batch else s.emit(1)
    f.__code__ = (lambda x: x + 1).__code__  # a new code object, same text
    s.emit(1)
    assert out == [2, 10, 2]


def test_compile_after_eager_emits():
    f = lambda x: x * 3  # noqa: E731
    s, out = akayu.Stream(), []
    s.map(f).filter(lambda x: x % 2 == 0).sink(out.append)
    s.emit_batch([1, 2, 3, 4])
    f.__code__ = (lambda x: x * 5).__code__
    s.compile()
    s.emit_batch([1, 2, 3, 4])
    assert out == [6, 12, 10, 20]
