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
