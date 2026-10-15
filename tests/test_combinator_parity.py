"""zip() and combine_latest() against reference implementations, on random
interleavings of their inputs, per item and in batches, eager and compiled.

Inputs reach these nodes through internal tag nodes. The tags used to wrap
every item in an (index, value) tuple; they now pass the index natively, and
this pins that every path still produces exactly the reference output.
"""

import itertools
from collections import deque

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import akayu

N_INPUTS = 3

events = st.lists(st.tuples(st.integers(0, N_INPUTS - 1), st.integers(-50, 50)), max_size=60)


def ref_zip(evs):
    bufs, out = [deque() for _ in range(N_INPUTS)], []
    for i, v in evs:
        bufs[i].append(v)
        if all(bufs):
            out.append(tuple(b.popleft() for b in bufs))
    return out


def ref_combine_latest(evs, emit_on=None):
    state, out = [None] * N_INPUTS, []
    seen = [False] * N_INPUTS
    for i, v in evs:
        state[i], seen[i] = v, True
        if all(seen) and (emit_on is None or i in emit_on):
            out.append(tuple(state))
    return out


def feed(sources, evs, batch):
    if not batch:
        for i, v in evs:
            sources[i].emit(v)
        return
    for i, run in itertools.groupby(evs, key=lambda e: e[0]):
        sources[i].emit_batch([v for _, v in run])


@settings(max_examples=150)
@given(evs=events, batch=st.booleans(), compiled=st.booleans())
def test_zip_matches_reference(evs, batch, compiled):
    sources = [akayu.Stream() for _ in range(N_INPUTS)]
    out = []
    akayu.zip(*sources).sink(out.append)
    if compiled:
        for s in sources:
            s.compile()
    feed(sources, evs, batch)
    assert out == ref_zip(evs)


@settings(max_examples=150)
@given(evs=events, batch=st.booleans(), compiled=st.booleans(), emit_on=st.sampled_from([None, (0,), (1, 2)]))
def test_combine_latest_matches_reference(evs, batch, compiled, emit_on):
    sources = [akayu.Stream() for _ in range(N_INPUTS)]
    out = []
    kw = {} if emit_on is None else {"emit_on": [sources[i] for i in emit_on]}
    akayu.combine_latest(*sources, **kw).sink(out.append)
    if compiled:
        for s in sources:
            s.compile()
    feed(sources, evs, batch)
    assert out == ref_combine_latest(evs, emit_on)


@settings(max_examples=50)
@given(evs=events)
def test_zip_downstream_of_par_matches_reference(evs):
    """Under par() the tag edges go through the locked path."""
    sources = [akayu.Stream() for _ in range(N_INPUTS)]
    out = []
    branches = [s.par() for s in sources]
    akayu.zip(*[b.map(lambda x: x) for b in branches]).sink(out.append)
    feed(sources, evs, batch=False)
    assert out == ref_zip(evs)


def test_combinator_profile_counts():
    a, b = akayu.Stream(), akayu.Stream()
    out = []
    akayu.zip(a, b).sink(out.append)
    with a.profile() as p:
        for i in range(5):
            a.emit(i)
    rows = {r["name"]: r for r in p.report.to_list()}
    assert (rows["tag(0)"]["items_in"], rows["tag(0)"]["items_out"]) == (5, 5)
    assert (rows["zip"]["items_in"], rows["zip"]["items_out"]) == (5, 0)


MERGES = ["union", "zip", "combine_latest"]


def build_merge(form, kind, first, second):
    if form == "function":
        return getattr(akayu, kind)(first, second)
    return getattr(first, kind)(second)


@pytest.mark.parametrize("kind", MERGES)
@pytest.mark.parametrize("form", ["function", "method"])
def test_a_merge_of_an_input_that_is_mid_emit_is_refused(form, kind):
    """Built from a callback while one input is emitting, the function form
    panicked; the method form built the wrong graph (union left the busy
    input out, zip and combine_latest paired the other input with itself)."""
    s, t = akayu.Stream(), akayu.Stream()
    built = []
    s.sink(lambda _: built.append(build_merge(form, kind, t, s)))
    with pytest.raises(akayu.CallbackError) as info:
        s.emit(1)
    assert isinstance(info.value.__cause__, akayu.BusyError)
    assert built == []
    # Nothing was attached to either input: they still work on their own.
    out = []
    t.sink(out.append)
    t.emit("t")
    assert out == ["t"]


@pytest.mark.parametrize("kind", MERGES)
def test_method_and_function_forms_build_the_same_graph(kind):
    results = []
    for form in ("function", "method"):
        a, b = akayu.Stream(), akayu.Stream()
        out = build_merge(form, kind, a, b).to_list()
        for src, x in [(a, 1), (b, "x"), (a, 2), (b, "y")]:
            src.emit(x)
        results.append(out)
    assert results[0] == results[1]


def test_a_merge_may_name_an_input_twice():
    a = akayu.Stream()
    out = a.zip(a).to_list()
    a.emit(1)
    a.emit(2)
    assert out == [(1, 1), (2, 2)]
