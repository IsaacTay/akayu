"""Differential fuzzer for the native fast paths: random lambdas over awkward
values, run by akayu (uncompiled and compiled; map, filter and longer chains;
emit and emit_batch) and by plain Python, which must agree on every result
and every exception type.

compile() turns lambdas such as `x % 2 == 0`, `str(x).upper()` or
`acc + x` into native steps, with their own arithmetic, comparisons and
method calls; this checks them against what Python computes, including
overflow, NaN, -0.0, subclasses that override operators, and objects whose
operators return odd things.

AKAYU_OPS_FUZZ_SEEDS sets the number of seeds (default 30).
"""

import math
import os
import random
from decimal import Decimal
from fractions import Fraction

import pytest

import akayu

# Random expressions trip Python's own warnings (`~True`, `x is 0`, ...).
pytestmark = pytest.mark.filterwarnings("ignore")

SEEDS = int(os.environ.get("AKAYU_OPS_FUZZ_SEEDS", "30"))
CHUNK = 10
EXPRS_PER_SEED = 25


class Int(int):
    def __add__(self, other):
        return "Int+"

    def __gt__(self, other):
        return "Int>"

    def __repr__(self):
        return f"Int({int(self)})"


class Float(float):
    def __mul__(self, other):
        return "Float*"

    def __repr__(self):
        return f"Float({float(self)})"


class Str(str):
    __slots__ = ()

    def strip(self, *args):
        return "Str.strip"

    def __repr__(self):
        return f"Str({str.__repr__(self)})"


class Dict(dict):
    def get(self, key, default=None):
        return "Dict.get"

    def __repr__(self):
        return "Dict" + dict.__repr__(self)


class Odd:
    """Operators that return non-bools, refuse, or raise."""

    def __eq__(self, other):
        return "eq"

    def __lt__(self, other):
        return NotImplemented

    def __bool__(self):
        raise ValueError("no truth value")

    def __len__(self):
        return 3

    def __getitem__(self, key):
        return ("item", key)

    def __add__(self, other):
        return ("add", other)

    def __radd__(self, other):
        return ("radd", other)

    def __contains__(self, item):
        return True

    def __hash__(self):
        return 1

    def __repr__(self):
        return "Odd"


class BadLen:
    def __len__(self):
        return -1  # noqa: PLE0303 - deliberately invalid

    def __repr__(self):
        return "BadLen"


VALUES = [
    *(0, 1, -1, 2, -7, 7, 255, 2**31, 2**53 + 1, 2**62, 2**63 - 1, -(2**63), 2**63, -(2**63) - 1),
    *(2**64, 10**20, True, False),
    *(0.0, -0.0, 0.5, -2.5, 1e300, -1e300, 1e308, math.inf, -math.inf, math.nan, 2.0**53, 1e-320),
    *(Int(3), Float(2.5), Fraction(1, 3), Decimal("1.5"), complex(1, 2)),
    *("", "a", " ab ", "abc", Str(" s "), b"ab", bytearray(b"z")),
    *([1, 2], [], (1,), (), {"a": 1}, {}, Dict(a=2), {1, 2}, frozenset(), None, Odd(), BadLen(), range(3)),
]
INTS = ["0", "1", "2", "-1", "-3", "7", "255", "2**31", "2**53+1", "2**63-1", "-2**63", "2**63", "2**64", "10**20"]
FLOATS = ["0.0", "-0.0", "0.5", "-2.5", "1e300", "1e999", "-1e999", "2.0**53"]
OTHER = ["'a'", "''", "True", "False", "None", "(1, 2, 'a')", "{'a': 1}", "'abc'"]
ARITH = ["+", "-", "*", "/", "//", "%", "**", "<<", ">>", "&", "|", "^"]
# Huge exponents, shifts and repeats take forever, or all memory, in Python too.
SMALL = ["0", "1", "2", "-1", "3", "0.5", "-2.5"]
CMP = ["==", "!=", "<", "<=", ">", ">="]
CALLS = ["len", "str", "abs", "int", "float", "bool", "repr"]
SUBSCRIPTS = ["0", "-1", "'a'", "1:", ":2", "::-1"]
ATTRS = [
    *("strip()", "upper()", 'get("a", 0)', 'count("a")', 'startswith("a")', "split()"),
    *("bit_length()", "is_integer()", "real", "imag"),
]
CONTAINERS = ['(1, 2, "a")', '"abc"', '{"a": 1}', "[0, 0.0, None]", "range(5)"]


def const(rng):
    r = rng.random()
    if r < 0.5:
        return rng.choice(INTS)
    return rng.choice(FLOATS) if r < 0.8 else rng.choice(OTHER)


def wrap(rng, e):
    """`e` with one more step applied to it."""
    k = rng.randrange(14)
    if k < 4:
        op = rng.choice(ARITH)
        if op in ("**", "<<", "*"):
            return f"({e} {op} {rng.choice(SMALL)})"
        c = const(rng)
        return f"({e} {op} {c})" if rng.random() < 0.8 else f"({c} {op} {e})"
    if k < 6:
        return f"({e} {rng.choice(CMP)} {const(rng)})"
    if k == 6:
        return rng.choice([f"(not {e})", f"(-{e})", f"(+{e})", f"(~{e})"])
    if k == 7:
        return f"{rng.choice(CALLS)}({e})"
    if k == 8:
        return f"{e}[{rng.choice(SUBSCRIPTS)}]"
    if k == 9:
        return f"{e}.{rng.choice(ATTRS)}"
    if k == 10:
        return f"({e} {rng.choice(['in', 'not in'])} {rng.choice(CONTAINERS)})"
    if k == 11:
        return f"({e} {rng.choice(['is', 'is not'])} {rng.choice(['None', 'True', 'False'])})"
    if k == 12:
        return f"({e} {rng.choice(['and', 'or'])} {const(rng)})"
    return f"({const(rng)} {rng.choice(['and', 'or'])} {e})"


def chain(rng, steps):
    e = "x"
    for _ in range(rng.randint(1, steps)):
        e = wrap(rng, e)
    return e


def expression(rng):
    e = chain(rng, 4)
    if rng.random() < 0.12:
        e2 = chain(rng, 2)
        e = rng.choice([f"({e}, {e2})", f"[{e}, {e2}]", f"({e} and {e2})", f"({e} or {e2})"])
    return e


def canon(v):
    """A comparable form of a result: its type and repr (nan == nan)."""
    if isinstance(v, BaseException):
        return ("raise", type(v).__name__)
    if isinstance(v, float) and math.isnan(v):
        return (type(v).__name__, "nan")
    return (type(v).__name__, repr(v))


def innermost(e):
    while e.__cause__ is not None:
        e = e.__cause__
    return e


def call(f, *args):
    try:
        return canon(f(*args))
    except Exception as e:
        return canon(e)


def truth(f, v):
    try:
        return bool(f(v))
    except Exception as e:
        return e


def per_item(build, compiled, values):
    """What a graph made by `build(source, out)` delivers for each value."""
    s, out = akayu.Stream(), []
    build(s, out)
    if compiled:
        s.compile()
    got = []
    for v in values:
        out.clear()
        try:
            s.emit(v)
            got.append([canon(o) for o in out])
        except Exception as e:
            got.append(canon(innermost(e)))
    return got


def mismatches(seed, values=VALUES):
    rng = random.Random(seed)
    return [m for _ in range(EXPRS_PER_SEED) for m in expr_mismatches(expression(rng), values)]


def expr_mismatches(src, values=VALUES):
    """Where akayu and Python disagree on `lambda x: <src>` over `values`, as a
    map, a filter and a map.map, item by item and as a batch, uncompiled and
    compiled."""
    found = []
    f = eval(f"lambda x: {src}")
    want_map = [[r] if r[0] != "raise" else r for r in (call(f, v) for v in values)]
    want_filter = []
    for v in values:
        t = truth(f, v)
        want_filter.append(canon(t) if isinstance(t, BaseException) else [canon(v)] if t else [])
    graphs = {
        "map": (lambda s, out, g: s.map(g).sink(out.append), want_map),
        "filter": (lambda s, out, g: s.filter(g).sink(out.append), want_filter),
        "map.map": (lambda s, out, g: s.map(g).map(lambda y: y).sink(out.append), want_map),
    }
    for compiled in (False, True):
        for kind, (build, want) in graphs.items():
            # A fresh function per graph: analysis results are cached by code.
            g = eval(f"lambda x: {src}")
            got = per_item(lambda s, out, g=g, build=build: build(s, out, g), compiled, values)
            for v, w, a in zip(values, want, got, strict=True):
                if w != a:
                    found.append(f"{kind} compiled={compiled} {src} on {v!r}: python {w}, akayu {a}")
                    break
        # A batch of the values Python handles stops nowhere.
        ok = [v for v, r in zip(values, want_map, strict=True) if isinstance(r, list)]
        g = eval(f"lambda x: {src}")
        s, out = akayu.Stream(), []
        s.map(g).sink(out.append)
        if compiled:
            s.compile()
        try:
            s.emit_batch(ok)
            got = [canon(o) for o in out]
        except Exception as e:
            got = canon(innermost(e))
        want = [r[0] for r in want_map if isinstance(r, list)]
        if got != want:
            found.append(f"batch compiled={compiled} {src}: python {want[:5]}, akayu {got}")
    return found


ACC_SEQS = [
    *([1, 2, 3], [1, 2.5, -3], [2**62, 2**62, 2**62], [True, 1, False], ["a", "b"], [math.nan, 1.0]),
    *([1, "a", 2], [-(2**63), -1], [0, 0, 1], [1e308, 1e308], [2**53, 1], [[1], [2]], [None, 1], [3, 0, 2]),
]
ACC_STARTS = ["0", "1", "0.0", "-1", "''", "[]", "2**63-1", "-2**63", "None", "1e308", "10**20", "True"]


def accumulate_mismatches(seed, seqs=ACC_SEQS):
    rng = random.Random(seed)
    op = rng.choice(ARITH[:6])  # no **: 10**20 ** 2**62 never ends
    src = rng.choice([f"acc {op} x", f"x {op} acc", "max(acc, x)", "min(acc, x)", f"acc {op} {const(rng)}"])
    start = rng.choice(ACC_STARTS)
    found = []
    for seq in seqs:
        f = eval(f"lambda acc, x: {src}")
        want, acc = [], eval(start)
        for v in seq:
            try:
                acc = f(acc, v)
                want.append([canon(acc)])
            except Exception as e:
                want.append(canon(e))
        for compiled in (False, True):
            g = eval(f"lambda acc, x: {src}")
            got = per_item(lambda s, out, g=g: s.accumulate(g, eval(start)).sink(out.append), compiled, seq)
            if got != want:
                found.append(f"accumulate compiled={compiled} {src} from {start} over {seq}: {want} vs {got}")
    return found


@pytest.mark.parametrize("start", range(0, SEEDS, CHUNK))
def test_native_ops_match_python(start):
    found = [m for seed in range(start, min(start + CHUNK, SEEDS)) for m in mismatches(seed)]
    assert not found, "\n".join(found[:10])


@pytest.mark.parametrize("start", range(0, SEEDS * 4, CHUNK * 4))
def test_native_accumulate_matches_python(start):
    found = [m for seed in range(start, min(start + CHUNK * 4, SEEDS * 4)) for m in accumulate_mismatches(seed)]
    assert not found, "\n".join(found[:10])
