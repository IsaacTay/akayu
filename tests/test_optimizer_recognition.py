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
from support import run_python

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
        (lambda x, k=3: k - x, None),  # the item must come first
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
        (lambda x: x * x, None),  # the item twice
    ],
    ids=["mod-eq", "mul-add", "item-item", "attr-attr", "str-method", "method-method", "cmp-chain", "x-twice"],
)
def test_step_chains_recognized(func, expected):
    assert _analyze_lambda(func) == expected


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
