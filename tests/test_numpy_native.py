"""compile()'s numpy programs (src/arrays.rs): numpy expressions in map and
starmap lambdas run on small float64 arrays with numpy's own loops, without
numpy's per-call dispatch.

The results must be numpy's exactly (values, types, layouts, views' bases,
warnings, errors), and anything a program doesn't take must run the
function as before. numpy_native_fuzzer.py checks random expressions; the
tests here pin each form and each way out.

scripts/test-wheel-windows.sh skips this module: numpy's Windows build calls
a C runtime function Wine lacks.
"""

import gc
import inspect
import itertools
import re
import warnings
import weakref

import pytest
from support import UNDER_WINE, calls_during, innermost, seed_chunks, seed_ids

if UNDER_WINE:
    pytest.skip("numpy crashes under Wine (ucrtbase crealf)", allow_module_level=True)

import numpy as np
from akayu._helpers import _analyze_function, _analyze_star_for_compile
from numpy_native_fuzzer import CHUNK, SEEDS, describe, mismatches, outcome, same_outcome

import akayu

rng = np.random.default_rng(0)
C = rng.standard_normal((3, 3))
DT = 0.01
G = np.array([0.0, 0.0, 9.81])
R = rng.standard_normal((40, 3, 3))
R2 = rng.standard_normal((40, 3, 3))
VECS = list(rng.standard_normal((40, 3)))
MATS = list(R)  # views into R, as list(array) gives them
BYTES = [r.tobytes() for r in R]

# (lambda, items, starmap): each must run natively on these items.
FORMS = [
    (lambda r: C @ r, MATS, False),
    (lambda a, b: a.T @ b, list(zip(R, R2, strict=True)), True),
    (lambda v: C @ v, VECS, False),
    (lambda v: v @ C, VECS, False),
    (lambda v: v @ v, VECS, False),
    (lambda v: v * DT, VECS, False),
    (lambda v: DT * v, VECS, False),
    (lambda v: v - G, VECS, False),
    (lambda r: -r / 2, MATS, False),
    (lambda v: np.linalg.norm(v), VECS, False),
    (lambda r: np.linalg.norm(r), MATS, False),
    (lambda v: np.clip(v, -1.0, 1.0), VECS, False),
    (lambda r: np.arctan2(r[1, 0], r[0, 0]), MATS, False),
    (lambda r: np.arccos(np.clip((np.trace(r) - 1) / 2, -1.0, 1.0)), MATS, False),
    (lambda r: r[:, 2], MATS, False),
    (lambda r: r[1], MATS, False),
    (lambda r: r.T, MATS, False),
    (lambda r: (C @ r)[:, 0], MATS, False),
    (lambda r: (r.T @ C).T, MATS, False),
    (lambda x: np.degrees(x), [float(v[0]) for v in VECS], False),
    (lambda x: np.sqrt(x), [abs(np.float64(v[0])) for v in VECS], False),
    (lambda b: np.frombuffer(b).reshape(3, 3), BYTES, False),
    (lambda b: np.frombuffer(b), BYTES, False),
    (lambda b: (C @ np.frombuffer(b).reshape(3, 3)).tobytes(), BYTES, False),
    (
        lambda a, b: (np.frombuffer(a).reshape(3, 3).T @ np.frombuffer(b).reshape(3, 3)).tobytes(),
        list(itertools.pairwise(BYTES)),
        True,
    ),
]


def results(f, items, star, compiled=True):
    s, out = akayu.Stream(), []
    (s.starmap(f) if star else s.map(f)).sink(out.append)
    if compiled:
        s.compile()
    s.emit_batch(items)
    return out


def expected(f, items, star):
    return [f(*x) if star else f(x) for x in items]


def described(values, items, star):
    return [describe(v, x if star else (x,)) for v, x in zip(values, items, strict=True)]


@pytest.mark.parametrize(("f", "items", "star"), FORMS, ids=lambda v: getattr(v, "__name__", ""))
def test_each_form_runs_natively_with_numpys_results(f, items, star):
    out = []
    ran = calls_during(lambda: out.extend(results(f, items, star)), f)
    assert ran == 0
    got, want = described(out, items, star), described(expected(f, items, star), items, star)
    assert same_outcome(got, want, inspect.getsource(f))


def test_views_have_numpys_bases():
    r = MATS[0]  # a view: numpy bases its views on R, which owns the data
    (col,) = results(lambda r: r[:, 2], [r], False)
    assert col.base is R
    b = BYTES[0]
    (m,) = results(lambda b: np.frombuffer(b).reshape(3, 3), [b], False)
    # numpy's frombuffer array over the bytes, not the bytes themselves
    assert type(m.base) is np.ndarray and m.base.base is b and not m.flags.writeable
    (col,) = results(lambda r: (C @ r)[:, 0], [r], False)
    assert col.base is not None and col.base.flags.owndata and col.base.shape == (3, 3)


def test_results_keep_their_memory_order():
    f = np.asfortranarray(C)
    for fn in (lambda a: a * 2.0, lambda a: a.T * 2.0, lambda a: np.sqrt(np.abs(a)), lambda a: a @ a):
        for a in (C, f):
            (got,) = results(fn, [a], False)
            want = fn(a)
            assert (got.flags.c_contiguous, got.flags.f_contiguous) == (
                want.flags.c_contiguous,
                want.flags.f_contiguous,
            )
            assert got.strides == want.strides and got.tobytes() == want.tobytes()


def test_scalars_come_back_as_numpy_returns_them():
    (a, b, c) = results(lambda x: (x - 1) / 2, [0.5, np.float64(0.5), 3.0], False)
    assert (type(a), type(b), type(c)) == (float, np.float64, float)
    (d,) = results(lambda x: np.degrees(x), [0.5], False)
    assert type(d) is np.float64
    (t,) = results(lambda r: np.trace(r), [np.diag([-0.0, -0.0, -0.0])], False)
    assert type(t) is np.float64 and np.signbit(t) == np.signbit(np.trace(np.diag([-0.0, -0.0, -0.0])))


@pytest.mark.parametrize(
    "item",
    [
        np.arange(3),  # int64
        np.ones(3, dtype=np.float32),
        np.ones((2, 2, 2)),
        np.ones((5, 5)),  # too big
        np.asmatrix(np.eye(3)),  # a subclass, with its own @
        np.ones(3)[::-1],  # a negative stride
        np.frombuffer(b"\x00" + np.ones(3).tobytes(), offset=1),  # unaligned
        np.array(2.0),  # 0-d
        [1.0, 2.0, 3.0],
        3,
        None,
    ],
    ids=lambda v: type(v).__name__ + str(getattr(v, "shape", "")),
)
def test_items_the_programs_dont_take_run_the_function(item):
    for f in (lambda v: C @ v, lambda v: np.linalg.norm(v), lambda v: v * DT, lambda v: np.clip(v, 0, 1)):
        want = outcome(lambda f=f: f(item), (item,))
        got = outcome(lambda f=f: results(f, [item], False)[0], (item,))
        assert got == want


def test_floating_point_errors_warn_as_numpy_does():
    items = [np.array([2.0, 0.5, -3.0]), np.array([1e308, 1.0, 1.0]), np.array([0.0, 1.0, 1.0])]
    for f in (lambda v: np.arccos(v), lambda v: v * 1e10, lambda v: np.log(v), lambda v: 1.0 / v):
        for v in items:
            want = outcome(lambda f=f, v=v: f(v), (v,))
            got = outcome(lambda f=f, v=v: results(f, [v], False)[0], (v,))
            assert got == want


def test_errstate_raise_raises_as_numpy_does():
    f = lambda v: np.arccos(v)  # noqa: E731
    s, out = akayu.Stream(), []
    s.map(f).sink(out.append)
    s.compile()
    with np.errstate(all="raise"):
        with pytest.raises(akayu.CallbackError) as info:
            s.emit(np.array([2.0, 0.0, 0.0]))
        s.emit(np.array([0.5, 0.0, 0.0]))
    assert type(innermost(info.value)) is FloatingPointError
    assert out[0].tobytes() == np.arccos(np.array([0.5, 0.0, 0.0])).tobytes()


def test_numpys_errors_are_raised_as_before():
    cases = [
        (lambda b: np.frombuffer(b).reshape(3, 3), b"\x00" * 20),  # not a multiple of 8
        (lambda b: np.frombuffer(b).reshape(3, 3), b"\x00" * 64),  # 8 floats: no 3 x 3
        (lambda r: r[3, 0], np.eye(3)),
        (lambda v: C @ v, np.ones(4)),
        (lambda v: v.T @ G, 2.0),
    ]
    for f, item in cases:
        want = outcome(lambda f=f, item=item: f(item), (item,))
        got = outcome(lambda f=f, item=item: results(f, [item], False)[0], (item,))
        assert got == want and want[0][0] == "raise"


def test_starmap_takes_only_tuples_of_its_arity():
    f = lambda a, b: a.T @ b  # noqa: E731
    items = [(C, C), [C, C], (C,), (C, C, C), (C, np.ones(3, dtype=np.float32))]
    for item in items:
        want = outcome(lambda item=item: f(*item), tuple(item))
        got = outcome(lambda item=item: results(f, [item], True)[0], tuple(item))
        assert got == want


def test_a_global_array_is_read_as_the_function_reads_it():
    s, out = akayu.Stream(), []
    s.map(lambda v: v - G).sink(out.append)
    s.compile()
    v = np.ones(3)
    s.emit(v)
    G[2] = 1.0  # changed in place: the program reads it when it runs
    try:
        s.emit(v)
    finally:
        G[2] = 9.81
    assert out[0].tobytes() == (v - G).tobytes()
    assert out[1].tobytes() == (v - np.array([0.0, 0.0, 1.0])).tobytes()


def test_a_dropped_graph_releases_its_constants():
    a = np.eye(3)
    ref = weakref.ref(a)
    s = akayu.Stream()
    s.map(lambda r, a=a: a @ r).sink(lambda _: None)
    s.compile()
    s.emit(np.eye(3))
    del s, a
    gc.collect()
    assert ref() is None


def test_analysis_leaves_other_expressions_to_python():
    np_ = np  # noqa: F841 - only the lambdas' globals matter
    for f in (
        lambda v: v**2,
        lambda v: v // 2,
        lambda v: np.clip(v, a_min=-1, a_max=1),
        lambda v: v.shape,
        lambda v: v.sum(),
        lambda v: np.sum(v),
        lambda v: np.linalg.norm(v, ord=1),
        lambda v: v > 0,
        lambda v: v[::2],
        lambda v: v if v.size else -v,
    ):
        result = _analyze_function(f)
        assert not (isinstance(result, tuple) and result[0] == "arrays"), f
    assert _analyze_star_for_compile(lambda a, b: a < b) is None
    assert _analyze_star_for_compile(lambda a, b: a.T @ b) is not None


def explain_tags(compile_):
    s = akayu.Stream()
    s.map(lambda r: np.arccos(np.clip((np.trace(r) - 1) / 2, -1.0, 1.0))).map(lambda r: r * DT).map(
        lambda r: r**2
    ).starmap(lambda a, b: a @ b).sink(print)
    if compile_:
        s.compile()
    lines = s.explain().splitlines()[3:7]
    return [re.fullmatch(r".*?test_numpy_native\.py:\d+  (.*?)(?:  +chain \d+)?", line)[1] for line in lines]


def test_explain_tags_what_runs_numpys_loops():
    # numpy calls and @: numpy's loops; arithmetic with a float: numpy's loops
    # for arrays too, but no more numpy code than int code; ** isn't one.
    assert explain_tags(True) == ["native, numpy", "native", "native", "native, numpy"]
    later = "Python until compile()"
    assert explain_tags(False) == [later, later, "native", later]


def test_the_profile_report_tags_them_too():
    s = akayu.Stream()
    s.map(lambda r: C @ r).sink(lambda _: None)
    s.compile()
    with s.profile() as p:
        s.emit(np.eye(3))
    assert [n["how"] for n in p.report.to_dict()["nodes"]][1] == "native, numpy"


def test_uncompiled_graphs_run_the_function():
    f = lambda r: C @ r  # noqa: E731
    out = []
    assert calls_during(lambda: out.extend(results(f, MATS[:3], False, compiled=False)), f) == 3
    assert [o.tobytes() for o in out] == [(C @ r).tobytes() for r in MATS[:3]]


def test_numbers_still_take_the_numeric_path():
    # x * 2.0 has a numpy program too; plain floats must not use it.
    out = results(lambda x: x * 2.0 + 1.0, [1.5, 2.5], False)
    assert out == [4.0, 6.0] and all(type(v) is float for v in out)


@pytest.mark.parametrize("seeds", seed_chunks(SEEDS, CHUNK), ids=seed_ids)
def test_numpy_programs_fuzz(seeds):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # the matrix subclass's deprecation
        found = [m for seed in seeds for m in mismatches(seed)]
    assert not found, "\n".join(found[:5])
