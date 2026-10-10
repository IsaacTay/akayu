"""The bytecode analysis behind compile() is cached by code object.

A request handler that builds a pipeline per call creates new lambdas every
time; keyed by function, every compile() re-analysed them. Keyed by code, a
result is reused only when the values the analysis resolved (closure cells,
globals) are the very same objects for the new function.
"""

import gc

import pytest
from akayu._helpers import _MAX_RESULTS_PER_CODE, _analysis_cache, _cached_results

import akayu


def run(func, *items, op="map"):
    s, out = akayu.Stream(), []
    getattr(s, op)(func).sink(out.append)
    s.compile()
    for x in items:
        s.emit(x)
    return out


def make_adder(k):
    return lambda x: x + k


def make_member_check(allowed):
    return lambda x: x in allowed


def test_fresh_lambdas_share_one_analysis():
    def handler():
        return run(lambda x: x + 1, 1)

    for _ in range(20):
        assert handler() == [2]
    code = handler.__code__.co_consts
    lambdas = [c for c in code if hasattr(c, "co_name") and c.co_name == "<lambda>"]
    (lam,) = lambdas
    assert len(_cached_results(lam)) == 1


@pytest.mark.parametrize("k", [1, 2, 3, 2, 1])
def test_closures_over_different_values_are_not_shared(k):
    assert run(make_adder(k), 10) == [10 + k]


def test_equal_but_distinct_closure_values_get_their_own_result():
    """Sharing would leave the op holding the first call's list."""
    first, second = ["a"], ["b"]
    assert run(make_member_check(first), "a", "b", op="filter") == ["a"]
    assert run(make_member_check(second), "a", "b", op="filter") == ["b"]


def test_rebound_global_is_resolved_again():
    module_globals = {"Kind": int}
    check = eval("lambda x: isinstance(x, Kind)", module_globals)
    assert run(check, 1, "s", op="filter") == [1]
    module_globals["Kind"] = str
    same_code = type(check)(check.__code__, module_globals)
    assert run(same_code, 1, "s", op="filter") == ["s"]


def test_results_per_code_are_bounded():
    for k in range(3 * _MAX_RESULTS_PER_CODE):
        assert run(make_adder(k), 0) == [k]
    assert len(_cached_results(make_adder(0).__code__)) <= _MAX_RESULTS_PER_CODE


def test_entries_die_with_their_code():
    lam = eval("lambda x: x - 1")
    key = id(lam.__code__)
    assert run(lam, 5) == [4]
    assert key in _analysis_cache
    del lam
    gc.collect()
    assert key not in _analysis_cache
