"""Nodes record where the user created them.

Locations are read natively from the calling frame (they used to come from a
Python helper per node). Frames inside akayu's own wrapper module are skipped,
so nodes built by zip(), union(), combine_latest() and MultiStream point at the
user's line instead of akayu/__init__.py.
"""

import inspect
import re

import pytest

import akayu

HERE = __file__


def _report_locations(source, emit=1):
    with source.profile() as p:
        source.emit(emit)
    return {row["name"]: row["location"] for row in p.report.to_list()}


def _line():
    """The caller's current line number."""
    return inspect.currentframe().f_back.f_lineno


def test_method_nodes_point_at_the_calling_line():
    s = akayu.Stream()
    line = _line() + 1  # the next line
    s.map(abs).sink(lambda _: None)
    locs = _report_locations(s)
    assert locs["map(abs)"] == f"{HERE}:{line}"
    assert locs["sink(<lambda>)"] == f"{HERE}:{line}"


@pytest.mark.parametrize("combinator", ["zip", "union", "combine_latest"])
def test_wrapper_built_nodes_point_at_user_code(combinator):
    a, b = akayu.Stream(), akayu.Stream()
    line = _line() + 1  # the next line
    merged = getattr(akayu, combinator)(a, b)
    merged.sink(lambda _: None)
    locs = _report_locations(a)
    built = [loc for name, loc in locs.items() if name.startswith(("zip", "union", "combine", "tag"))]
    assert built, locs
    assert all(loc == f"{HERE}:{line}" for loc in built), locs
    assert not any("akayu/__init__.py" in loc for loc in locs.values())


def test_multistream_nodes_point_at_user_code():
    ms = akayu.MultiStream([akayu.Stream(), akayu.Stream()])
    line = _line() + 1  # the next line
    ms.map(abs).sink(lambda _: None)
    locs = _report_locations(ms[0])
    assert locs["map(abs)"] == f"{HERE}:{line}"


def test_error_message_names_the_user_line():
    s = akayu.Stream()
    line = _line() + 1  # the next line
    s.map(lambda x: 1 // x).sink(lambda _: None)
    with pytest.raises(RuntimeError, match=rf"defined at {re.escape(HERE)}:{line}"):
        s.emit(0)
