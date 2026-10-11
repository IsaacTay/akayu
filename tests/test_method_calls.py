"""compile()'s native method calls: `x.strip()`, `x.get("k", 0)`, ...

For builtin types that define the method (str, dict, bytes, ...), compile()
resolves the C method once and calls it directly for items of exactly that
type; any other item (a subclass, which may override it, or any other
object) looks the method up by name. Both must behave exactly like Python.
"""

import pytest
from akayu._helpers import _analyze_lambda
from hypothesis import given, settings
from hypothesis import strategies as st

import akayu


class Str(str):
    __slots__ = ()

    def strip(self):
        return "overridden"


class Record:
    def __init__(self, v):
        self.v = v

    def get(self, key, default=None):
        return ("record", key, default, self.v)

    def __repr__(self):
        return f"Record({self.v!r})"


METHODS = [
    lambda x: x.strip(),
    lambda x: x.upper(),
    lambda x: x.startswith("a"),
    lambda x: x.replace("a", "b"),
    lambda x: x.split(","),
    lambda x: x.count("a"),
    lambda x: x.get("k"),
    lambda x: x.get("k", 0),
    lambda x: x.index(1),
    lambda x: x.is_integer(),
    lambda x: x.bit_length(),
    lambda x: x.keys(),
]

items = st.one_of(
    st.text(alphabet="ab ,\t", max_size=6),
    st.binary(max_size=4),
    st.dictionaries(st.sampled_from(["k", "j"]), st.integers(0, 3), max_size=2),
    st.lists(st.integers(0, 2), max_size=3),
    st.tuples(st.integers(0, 2)),
    st.integers(-5, 5),
    st.floats(allow_nan=False),
    st.builds(Str, st.text(alphabet="ab ", max_size=4)),
    st.builds(Record, st.integers(0, 3)),
    st.none(),
)


def python(func, x):
    try:
        return ("value", func(x))
    except Exception as e:
        return ("error", type(e), str(e))


def compiled(func, x, batch):
    s, out = akayu.Stream(), []
    s.map(func).sink(out.append)
    s.compile()
    try:
        if batch:
            s.emit_batch([x])
        else:
            s.emit(x)
    except RuntimeError as e:
        cause = e.__cause__
        return ("error", type(cause), str(cause))
    (value,) = out
    return ("value", value)


@pytest.mark.parametrize("func", METHODS)
def test_methods_are_native(func):
    assert _analyze_lambda(func)[0].startswith("method")


@settings(max_examples=300, deadline=None)
@given(func=st.sampled_from(METHODS), x=items, batch=st.booleans())
def test_matches_python(func, x, batch):
    expected = python(func, x)
    got = compiled(func, x, batch)
    if expected[0] == "value" and isinstance(expected[1], type({}.keys())):
        expected, got = ("value", list(expected[1])), ("value", list(got[1]))
    assert got == expected, (x, expected, got)


def test_a_subclass_override_is_called():
    s, out = akayu.Stream(), []
    s.map(lambda x: x.strip()).sink(out.append)
    s.compile()
    s.emit_batch([Str("  a  "), "  b  ", b"  c  "])
    assert out == ["overridden", "b", b"c"]
