"""The lambda optimizer must keep recognizing common patterns.

It reads raw bytecode, which changes between Python versions. On 3.14 it
silently recognized nothing (LOAD_FAST_BORROW, LOAD_SMALL_INT, ...), so
everything still worked but unoptimized, and closure snapshots behaved
differently from 3.13. Result-only tests cannot see that; these can.
"""

import operator
import sys

import pytest
from akayu._helpers import _analyze_lambda
from support import calls_during, run_python

from akayu import Stream

threshold = 10
allowed = {"a", "b"}
old, new = "x", "y"

CASES = [
    (lambda x: x + 1, ("binop", "+", 1)),
    (lambda x: x * 2, ("binop", "*", 2)),
    (lambda x: x > 10, ("compare", ">", 10)),
    (lambda x: x > threshold, ("compare", ">", 10)),
    (lambda x: x["a"], ("itemgetter", "a")),
    (lambda x: x.name, ("attrgetter", "name")),
    (lambda x: len(x), ("len",)),
    (lambda x: str(x), ("str",)),
    (lambda x: x in allowed, ("contains", allowed)),
    (lambda x: x.strip(), ("method0", "strip")),
    (lambda x: x.replace(old, new), ("method2", "replace", "x", "y")),
    (lambda x: x is None, ("is", None)),
    (lambda x: not x, ("not",)),
]


@pytest.mark.parametrize(("func", "expected"), CASES, ids=[repr(e) for _, e in CASES])
def test_pattern_recognized(func, expected):
    assert _analyze_lambda(func) == expected


def test_compound_and_recognized():
    result = _analyze_lambda(lambda x: x > 1 and x < 9)
    assert result is not None and result[0] == "and"


def test_slice_recognized():
    """3.13 emits BINARY_SLICE; 3.14 folds the slice into a constant."""
    result = _analyze_lambda(lambda x: x[1:3])
    if sys.version_info >= (3, 14):
        assert result == ("itemgetter", slice(1, 3))
        assert operator.itemgetter(result[1])("abcdef") == "abcdef"[1:3]
    else:
        assert result == ("slice", 1, 3)


def test_unrecognized_lambda_is_left_alone():
    assert _analyze_lambda(lambda x: (x, x + 1)) is None


@pytest.mark.parametrize(
    ("func", "expected"),
    [
        (lambda x, k=3: x + k, ("binop", "+", 3)),
        (lambda x, lim=5: x > lim, ("compare", ">", 5)),
        (lambda x, key="a": x[key], ("itemgetter", "a")),
        (lambda x, y: x + y, None),  # no default: not a constant
        (lambda x, *, k=1: x + k, None),  # keyword-only
        (lambda x, k=3: k - x, ("rbinop", "-", 3)),  # the constant on the left
    ],
    ids=["binop", "compare", "itemgetter", "no-default", "kw-only", "reversed"],
)
def test_defaulted_parameters_are_constants(func, expected):
    assert _analyze_lambda(func) == expected


@pytest.mark.parametrize(
    ("func", "expected"),
    [
        (lambda x: x % 2 == 0, ("chain", ("binop", "%", 2), ("compare", "==", 0))),
        (lambda x: x * 2 + 1, ("chain", ("binop", "*", 2), ("binop", "+", 1))),
        (lambda x: x["a"]["b"], ("chain", ("itemgetter", "a"), ("itemgetter", "b"))),
        (lambda x: x.a.b, ("chain", ("attrgetter", "a"), ("attrgetter", "b"))),
        (lambda x: str(x).upper(), ("chain", ("str",), ("method0", "upper"))),
        (lambda x: x.strip().lower(), ("chain", ("method0", "strip"), ("method0", "lower"))),
        (lambda x: 0 < x < 10, None),  # comparison chains branch
    ],
    ids=["mod-eq", "mul-add", "item-item", "attr-attr", "str-method", "method-method", "cmp-chain"],
)
def test_step_chains_recognized(func, expected):
    assert _analyze_lambda(func) == expected


TABLE = {"a": 1}


@pytest.mark.parametrize(
    ("func", "expected"),
    [
        (lambda x: 2 * x, ("rbinop", "*", 2)),
        (lambda x: 0 < x, ("rcompare", "<", 0)),
        (lambda x: x * x, ("binop2", "*", ("item",), ("item",))),
        (lambda x: x.lo < x.hi, ("compare2", "<", ("attrgetter", "lo"), ("attrgetter", "hi"))),
        (lambda x: -x, ("unary", "-")),
        (lambda x: ~x, ("unary", "~")),
        (lambda x: abs(x - 3), ("chain", ("binop", "-", 3), ("unary", "abs"))),
        (lambda x: x & 0xFF, ("binop", "&", 255)),
        (lambda x: TABLE[x], ("getitem_from", TABLE)),
        (lambda x: TABLE[x.key], ("chain", ("attrgetter", "key"), ("getitem_from", TABLE))),
        (lambda x: TABLE[x]["a"], ("chain", ("getitem_from", TABLE), ("itemgetter", "a"))),
        (lambda x: 2 * x + 1, ("chain", ("rbinop", "*", 2), ("binop", "+", 1))),
        (lambda x: -(x + 1), ("chain", ("binop", "+", 1), ("unary", "-"))),
        (lambda x: len(x.split()), ("chain", ("method0", "split"), ("len",))),
        (lambda x: x[x.key], None),  # the item indexed by itself
        (lambda x: x @ x, None),  # no native @
    ],
    ids=[
        *("const-left", "const-left-compare", "item-twice", "two-attrs", "neg", "invert", "abs", "bitand"),
        *("table", "table-of-attr", "table-then-key", "const-left-then", "neg-of", "len-of", "self-index", "matmul"),
    ],
)
def test_operators_on_expressions_of_the_item(func, expected):
    assert _analyze_lambda(func) == expected


class Gt(int):
    def __gt__(self, other):
        return "not a bool"


def test_a_comparison_converted_to_bool_stays_converted():
    # 3.13 compiles `not not (x > 3)` to one comparison that converts its
    # result to bool; read as the bare comparison, it gave "not a bool".
    assert _analyze_lambda(lambda x: not not (x > 3)) == ("chain", ("compare", ">", 3), ("bool",))
    out = []
    s = Stream()
    s.map(lambda x: not not (x > 3)).sink(out.append)
    s.compile()
    s.emit(Gt(5))
    assert out == [True]


def run_ok(code, **env):
    """Run `code` in a fresh interpreter (the analyzer is set up at
    import): (stdout, stderr), after checking it succeeded."""
    status, out, err = run_python(code, **env)
    assert status == 0, err
    return out.strip(), err


COUNT_CALLS = """
import warnings
warnings.simplefilter("always")
import akayu
calls = []
k = 2
def f(x):
    calls.append(x)
    return x * k
s = akayu.Stream()
out = s.map(lambda x: f(x) and x * k).to_list()
native = s.map(lambda x: x * k).to_list()
s.compile()
k = 3
s.emit(5)
print(out, native, len(calls))
"""


def test_analyzer_can_be_switched_off():
    """AKAYU_ANALYZER=0: every lambda runs as Python (late binding), quietly."""
    stdout, stderr = run_ok(COUNT_CALLS, AKAYU_ANALYZER="0")
    assert stdout == "[15] [15] 1"
    assert "RuntimeWarning" not in stderr


def test_analyzer_runs_on_tested_versions():
    stdout, stderr = run_ok(COUNT_CALLS)
    assert stdout == "[15] [10] 1"  # the native lambda read k once, at compile()
    assert "RuntimeWarning" not in stderr


def test_a_python_without_the_bytecode_table_still_imports():
    """`dis._inline_cache_entries` is private: without it akayu used to fail
    at import. Now lambdas run as Python, with one warning."""
    stdout, stderr = run_ok("import dis\ndel dis._inline_cache_entries\n" + COUNT_CALLS)
    assert stdout == "[15] [15] 1"
    assert stderr.count("RuntimeWarning") == 1
    assert "can't read" in stderr


# (op, function, items, runs natively before compile()): every form the
# analyzer recognises. Before compile(), only lambdas that read nothing but
# their own code run natively; the rest, including those calling a builtin by
# name (a module may rebind `len`), keep late binding. After, all do.
NATIVE_FORMS = [
    ("map", lambda x: x + 1, [1, 2.5], True),
    ("map", lambda x: x * 2, [3, "ab"], True),
    ("map", lambda x: x // 2 % 3, [7, -9], True),
    ("map", lambda x: x["a"], [{"a": 1}], True),
    ("map", lambda x: x["a"]["b"], [{"a": {"b": 2}}], True),
    ("map", lambda x: x.real, [3, 2.5], True),
    ("map", lambda x: len(x), ["ab", (1,)], False),
    ("map", lambda x: str(x), [1, None], False),
    ("map", lambda x: int(x), ["7", 2.9], False),
    ("map", lambda x: float(x), ["1.5", 2], False),
    ("map", lambda x: bool(x), [0, "a"], False),
    ("map", lambda x: tuple(x), ["ab"], False),
    ("map", lambda x: type(x), [1], False),
    ("map", lambda x: x.strip(), [" a "], True),
    ("map", lambda x: x.startswith("a"), ["ab", "b"], True),
    ("map", lambda x: x.replace(old, new), ["xx"], False),
    ("map", lambda x: str(x).upper(), ["a"], False),
    ("map", lambda x: x[1:3], ["abcd"], True),
    ("map", lambda x: (x["a"], x["b"]), [{"a": 1, "b": 2}], True),
    ("map", lambda x: [x.real, x.imag], [3j], True),
    ("map", lambda x: x * threshold, [2], False),
    ("filter", lambda x: x > 10, [5, 15], True),
    ("filter", lambda x: x > threshold, [5, 15], False),
    ("filter", lambda x: x in allowed, ["a", "z"], False),
    ("filter", lambda x: x not in allowed, ["a", "z"], False),
    ("filter", lambda x: x is None, [None, 1], True),
    ("filter", lambda x: x is not None, [None, 1], True),
    ("filter", lambda x: not x, [0, 1], True),
    ("filter", lambda x: not x > 3, [1, 5], True),
    ("filter", lambda x: x > 1 and x < 9, [0, 5, 10], True),
    ("filter", lambda x: x < 1 or x > 9, [0, 5, 10], True),
    ("filter", lambda x: len(x) > 1, ["a", "ab"], False),
    ("filter", lambda x: isinstance(x, int), [1, "a"], False),
    ("filter", lambda x: hasattr(x, "real"), [1, "a"], False),
    ("filter", lambda x: x % 2 == 0, [1, 2], True),
]


@pytest.mark.parametrize(
    ("op", "func", "items", "eager"),
    NATIVE_FORMS,
    ids=[f"{op}-{i}" for i, (op, *_) in enumerate(NATIVE_FORMS)],
)
def test_recognised_forms_run_natively_with_python_results(op, func, items, eager, compiled):
    """A recognised lambda isn't called once compile() ran (nor before, if it
    reads nothing but its code), and its results are what Python computes.
    The analyzer hands its findings to Rust as tagged tuples: a misspelt tag
    silently fell back to calling the lambda, which every result test passed."""
    import akayu

    s, out = akayu.Stream(), []
    getattr(s, op)(func).sink(out.append)
    if compiled:
        s.compile()
    calls = calls_during(lambda: [s.emit(x) for x in items], func)
    assert calls == (0 if compiled or eager else len(items))
    assert out == ([func(x) for x in items] if op == "map" else [x for x in items if func(x)])


def test_plus_turns_a_comparison_into_an_int():
    # +x is the same number for ints and floats, so the unboxed program
    # leaves it out; after a comparison it makes the bool an int.
    out = []
    s = Stream()
    s.map(lambda x: +(x > 0)).sink(out.append)
    s.compile()
    s.emit_batch([5, -5])
    s.emit(7)
    assert out == [1, 0, 1] and all(type(v) is int for v in out)
