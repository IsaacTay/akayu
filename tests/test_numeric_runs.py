"""compile()'s unboxed arithmetic on ints and floats.

Native steps like `x * 2 + 1` or `x % 3 == 0` with numeric constants run on
machine ints and floats after compile(): an item is unboxed once, consecutive
steps of a fused chain work on it unboxed, and the result is boxed once. The
results must be exactly Python's: the same values and types (int, float,
bool), the same items filtered out, and the same errors at the same step.
Anything the fast path can't do identically (bools, big ints, overflow,
division by zero, float // and %, int-float mixes beyond 2**53, NaN) must
fall back to the ordinary steps, also mid-batch.
"""

import math

import pytest
from akayu._helpers import _analyze_lambda
from hypothesis import given, settings
from hypothesis import strategies as st

import akayu

ARITH = ["+", "-", "*", "/", "//", "%"]
COMPARE = ["==", "!=", "<", ">", "<=", ">="]
BIG_INTS = [2**53, 2**53 + 1, -(2**53) - 1, 2**62, 2**63 - 1, -(2**63), 2**63, -(2**64)]
FLOATS = [0.0, -0.0, math.nan, math.inf, -math.inf, 0.5, -1.5, 1e308, 2.0**53, 2.0**53 + 2]

numbers = st.one_of(
    st.integers(-50, 50),
    st.integers(),
    st.sampled_from(BIG_INTS),
    st.floats(),
    st.sampled_from(FLOATS),
    st.booleans(),
)
constants = st.one_of(
    st.integers(-7, 7),
    st.sampled_from(BIG_INTS),
    st.floats(-8, 8),
    st.sampled_from(FLOATS),
)
steps = st.lists(
    st.one_of(
        st.tuples(st.just("map"), st.sampled_from(ARITH), constants),
        st.tuples(st.just("map"), st.sampled_from(COMPARE), constants),  # yields a bool
        st.tuples(st.just("filter"), st.sampled_from(COMPARE), constants),
        st.tuples(st.just("filter"), st.sampled_from(ARITH), constants),  # truthiness
        # Two operations in one function: a single native step.
        st.tuples(st.just("map2"), st.sampled_from(ARITH), constants, st.sampled_from(ARITH), constants),
    ),
    min_size=1,
    max_size=6,
)


def make_step(i, spec):
    kind = spec[0]
    if kind == "map2":
        _, op1, a, op2, b = spec
        f = eval(f"lambda x, a=a, b=b: (x {op1} a) {op2} b", {"a": a, "b": b})
        kind = "map"
    else:
        _, op, c = spec
        f = eval(f"lambda x, c=c: x {op} c", {"c": c})
    # Names the step in errors: "... at node 'map(step2)' ...".
    f.__name__ = f.__qualname__ = f"step{i}"
    assert _analyze_lambda(f) is not None  # a native step, as intended
    return kind, f


def reference(chain, x):
    """("value", v), ("filtered",) or ("error", step index, exception type)."""
    for i, (kind, f) in enumerate(chain):
        try:
            r = f(x)
        except Exception as e:
            return ("error", i, type(e))
        if kind == "filter":
            if not r:
                return ("filtered",)
        else:
            x = r
    return ("value", x)


def build(chain, compiled):
    s, out = akayu.Stream(), []
    node = s
    for kind, f in chain:
        node = node.map(f) if kind == "map" else node.filter(f)
    node.sink(out.append)
    if compiled:
        s.compile()
    return s, out


def canon(v):
    """Exact identity of a number: type and repr (NaN, -0.0, 2 vs 2.0)."""
    return type(v), repr(v)


def per_item(s, out, x):
    out.clear()
    try:
        s.emit(x)
    except RuntimeError as e:
        step = next(i for i in range(10) if f"(step{i})'" in str(e))
        return ("error", step, type(e.__cause__))
    if not out:
        return ("filtered",)
    (v,) = out
    return ("value", v)


def same(a, b):
    if a[0] == "value" and b[0] == "value":
        return canon(a[1]) == canon(b[1])
    return a == b


@settings(max_examples=400, deadline=None)
@given(specs=steps, items=st.lists(numbers, max_size=12))
def test_matches_python(specs, items):
    chain = [make_step(i, spec) for i, spec in enumerate(specs)]
    expected = [reference(chain, x) for x in items]
    for compiled in (False, True):
        s, out = build(chain, compiled)
        got = [per_item(s, out, x) for x in items]
        for x, e, g in zip(items, expected, got, strict=True):
            assert same(e, g), (compiled, x, specs, e, g)

    # Batches: the same survivors, or an error a step really raises. (Which
    # item's error comes first may differ: a batch goes step by step.)
    s, out = build(chain, compiled=True)
    errors = {e[2] for e in expected if e[0] == "error"}
    try:
        s.emit_batch(list(items))
    except RuntimeError as e:
        assert type(e.__cause__) in errors, (specs, items, e)
    else:
        assert not errors, (specs, items)
        values = [e[1] for e in expected if e[0] == "value"]
        assert [canon(v) for v in out] == [canon(v) for v in values], (specs, items)


def run_both(build_chain, items, batch):
    results = []
    for compiled in (False, True):
        s, out = akayu.Stream(), []
        build_chain(s).sink(out.append)
        if compiled:
            s.compile()
        if batch:
            s.emit_batch(list(items))
        else:
            for x in items:
                s.emit(x)
        results.append([canon(v) for v in out])
    return results


@pytest.mark.parametrize("batch", [False, True])
def test_fallback_mid_batch(batch):
    """Items the fast path can't take go the ordinary way; the others
    don't change type or value because of them."""

    f, keep, g = (lambda x: x * 3 + 1), (lambda x: x % 2 == 0), (lambda x: x // 2 - 1)
    items = [1, 3, True, 2**70, 5, 2**62, 7.0, -9, False, 11]
    eager, compiled = run_both(lambda s: s.map(f).filter(keep).map(g), items, batch)
    assert compiled == eager == [canon(g(f(x))) for x in items if keep(f(x))]


@pytest.mark.parametrize("batch", [False, True])
def test_overflow_falls_back_to_big_ints(batch):
    def chain(s):
        return s.map(lambda x: x * 4 + 1).map(lambda x: x * 4)

    items = [2**61, -(2**61), 2**60 + 3, (2**63 - 1) // 16]
    eager, compiled = run_both(chain, items, batch)
    assert compiled == eager == [(int, repr((x * 4 + 1) * 4)) for x in items]


@pytest.mark.parametrize(
    ("func", "items"),
    [
        (lambda x: x * 2 // 3, [7, -7, 8, -8, 0, 7.5, -7.5]),
        (lambda x: x * 1 % -3, [7, -7, 8, -8, 0, 7.5, -7.5]),
        (lambda x: x * 1 / 3, [1, -1, 2**53, 2**53 + 1, 2**60, 3.0]),
        (lambda x: x + 0 < 2.0**53 + 1, [2**53, 2**53 + 1, 2**53 + 2, 2**60]),
        (lambda x: x + 0 == 2**53 + 1, [2.0**53, 2.0**53 + 2, 2**53 + 1]),
        (lambda x: x * 0 != 0, [math.inf, math.nan, 1.0, 1]),
        (lambda x, nan=math.nan: x * 1 >= nan, [1, 1.0, math.nan]),
        (lambda x: x - 0.0, [-0.0, 0.0, 0, -0]),
        (lambda x: -1 * x + 0, [0.0, 0, 3.5]),
    ],
)
@pytest.mark.parametrize("batch", [False, True])
def test_python_semantics(func, items, batch):
    eager, compiled = run_both(lambda s: s.map(func), items, batch)
    assert compiled == eager
    assert compiled == [canon(func(x)) for x in items]


@pytest.mark.parametrize("batch", [False, True])
def test_division_by_zero_raises_at_the_same_step(batch):
    for compiled in (False, True):
        s, out = akayu.Stream(), []
        s.map(lambda x: x * 2 + 1).map(lambda x, z=0: x // z + 1).sink(out.append)
        if compiled:
            s.compile()
        with pytest.raises(RuntimeError, match="defined at") as info:
            if batch:
                s.emit_batch([1, 2, 3])
            else:
                s.emit(1)
        assert isinstance(info.value.__cause__, ZeroDivisionError)
        assert out == []


def test_profiling_sees_every_step():
    """Profiling runs the steps one by one, so each is attributed."""
    s, out = akayu.Stream(), []
    s.map(lambda x: x + 1).map(lambda x: x * 2).filter(lambda x: x > 4).sink(out.append)
    s.compile()
    s.start_profiling()
    for x in range(5):
        s.emit(x)
    nodes = s.stop_profiling().to_dict()["nodes"]
    assert out == [6, 8, 10]
    flow = [(n["items_in"], n["items_out"]) for n in nodes if n["name"].startswith(("map", "filter"))]
    assert flow == [(5, 5), (5, 5), (5, 3)], nodes
