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


def test_loop_captured_defaults_stay_per_branch():
    """`lambda x, k=k:` is recognised (k is a constant: nodes pass one
    argument). Branches share the lambda's code but not its default, so each
    must get its own analysis."""
    for compile_graph in (False, True):
        s, out = akayu.Stream(), []
        for k in range(5):
            s.map(lambda x, k=k: x + k).sink(out.append)
        if compile_graph:
            s.compile()
        s.emit(100)
        assert out == [100, 101, 102, 103, 104]


def accumulate(func, *items):
    s, out = akayu.Stream(), []
    s.accumulate(func, 0).sink(out.append)
    s.compile()
    for x in items:
        s.emit(x)
    return out


def test_module_globals_shadow_builtins():
    """A module may define its own `len` or `max`; compile() must call it,
    as Python does, not the builtin of that name."""
    ns = {"len": lambda x: "mine", "max": lambda a, b: -1}
    exec("f = lambda x: len(x)\ng = lambda acc, x: max(acc, x)", ns)
    assert run(ns["f"], [1, 2]) == ["mine"]
    assert accumulate(ns["g"], 5, 7) == [-1, -1]


def test_replaced_builtins_are_not_the_real_ones():
    ns = {"__builtins__": {"len": lambda x: "theirs"}}
    exec("f = lambda x: len(x)", ns)
    assert run(ns["f"], [1, 2]) == ["theirs"]


def test_bound_methods_are_not_analysed_as_functions():
    """A bound method's code takes `self` first: analysed as a plain function,
    `self.factor` became the item's attribute."""

    class Scaler:
        factor = 10

        def scale(self, x=3):
            return self.factor

        def total(self, acc, x=0):
            return self.factor + acc

    obj = Scaler()
    assert run(obj.scale, 5) == [10]
    assert accumulate(obj.total, 5, 6) == [10, 20]


def test_accumulate_analysis_is_cached_per_binding():
    def handler(fn):
        return accumulate(fn, 3, 9, 4)

    plain = {}
    shadowed = {"max": lambda a, b: a - b}
    for ns in (plain, shadowed, plain):
        exec("f = lambda acc, x: max(acc, x)", ns)
    assert handler(plain["f"]) == [3, 9, 9]
    assert handler(shadowed["f"]) == [-3, -12, -16]


def test_a_reassigned_code_object_is_analysed_again():
    f = lambda x: x + 1  # noqa: E731
    assert run(f, 1) == [2]
    f.__code__ = (lambda x: x * 10).__code__
    assert run(f, 1) == [10]


def test_code_only_results_skip_the_python_analysis():
    """A result that depends only on the code (no closure, default or global
    value) is reused by compile() without calling the Python analyzer."""
    import sys

    def build():
        s = akayu.Stream()
        s.map(lambda x: x * 2 + 1).filter(lambda x: x % 3 == 0).accumulate(lambda a, x: a + x, 0).sink([].append)
        return s

    build().compile()
    calls = []

    def profile(frame, event, arg):
        if event == "call":
            calls.append(frame.f_code.co_name)

    s = build()
    sys.setprofile(profile)
    try:
        s.compile()
    finally:
        sys.setprofile(None)
    assert not [c for c in calls if c.startswith("_analyze")], calls


def test_cached_results_do_not_keep_captured_graphs_alive():
    """A result that captured a Stream (a sink's emit target) or a list is
    not cached: the cache used to keep up to 8 graphs per lambda alive."""
    import weakref

    def handler():
        s, t = akayu.Stream(), akayu.Stream()
        t.map(lambda x: x).sink([].append)
        s.sink(lambda x: t.emit(x))
        allowed = list(range(1000))
        s.filter(lambda x: x in allowed).sink([].append)
        s.compile()
        s.emit(1)
        return weakref.ref(s), weakref.ref(t)

    refs = [handler() for _ in range(20)]
    gc.collect()
    assert [r for pair in refs for r in pair if r() is not None] == []


def test_cached_results_keep_only_plain_values():
    """Numbers, strings, types and builtins are cached as before."""
    from akayu._helpers import _pinnable

    assert all(_pinnable(v) for v in (None, 1, 2.5, "s", b"b", int, len, (1, "a"), frozenset({1})))
    assert not any(_pinnable(v) for v in ([1], {1: 2}, {1}, akayu.Stream(), object(), (1, [2])))
