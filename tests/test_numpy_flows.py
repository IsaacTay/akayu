"""numpy values and numpy-heavy work through akayu.

numpy's scalars aren't Python numbers to the native fast paths: np.float64
is a float subclass, while np.int64 and np.bool_ aren't ints at all, so
compile()'s native steps must hand them back to Python untouched, types
included. Arrays have no single truth value and compare element-wise. And
numpy releases the GIL in its loops, so par() branches and prefetched items
doing numpy work run at the same time.

scripts/test-wheel-windows.sh skips this module: numpy's Windows build calls
a C runtime function Wine lacks.
"""

import asyncio
import gc
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pytest
from test_native_ops_fuzz import (
    CHUNK,
    SEEDS,
    accumulate_mismatches,
    canon,
    expr_mismatches,
    innermost,
    mismatches,
    per_item,
)

import akayu

# numpy warns on overflow and invalid values, as Python's own expressions do.
pytestmark = pytest.mark.filterwarnings("ignore")

NP_VALUES = [
    *(np.int64(5), np.int64(2**62), np.int64(-(2**63)), np.int32(-7), np.int8(100), np.uint8(250)),
    *(np.uint64(2**64 - 1), np.int64(0)),
    *(np.float64(2.5), np.float64(-0.0), np.float64("nan"), np.float64(1e308), np.float32(1.5), np.float16(0.5)),
    *(np.bool_(True), np.bool_(False), np.complex128(1 + 2j)),
    *(np.array(3), np.array([1, 2]), np.array([]), np.arange(6.0).reshape(2, 3)),
]

NP_ACC_SEQS = [
    [np.int64(2**62)] * 3,
    [np.float32(1.5), np.float64(2.5), np.float16(0.5)],
    [np.uint8(250), np.uint8(10)],
    [np.int64(3), 2.5, np.bool_(True), 4],
    [np.float64("nan"), 1],
    [np.array([1, 2]), np.array([3, 4])],
]

# Forms compile() runs natively, each checked whatever the fuzzer draws.
NATIVE_FORMS = [
    *("x", "x + 1", "x - 1", "x * 2", "x * 4", "x / 2", "x // 3", "x % 2", "-x", "x ** 2", "x << 1"),
    *("x % 2 == 0", "x > 3", "x == 5", "x != x", "not x", "x is None", "x + 0.5", "(x + 1) * 3 - 1"),
    *("abs(x)", "int(x)", "float(x)", "bool(x)", "str(x)", "len(x)", "type(x)", "x[0]", "x.sum()"),
]


@pytest.mark.parametrize("src", NATIVE_FORMS)
def test_native_forms_on_numpy_values_match_python(src):
    found = expr_mismatches(src, NP_VALUES)
    assert not found, "\n".join(found)


@pytest.mark.parametrize("start", range(0, SEEDS, CHUNK))
def test_native_ops_fuzz_on_numpy_values(start):
    found = [m for seed in range(start, min(start + CHUNK, SEEDS)) for m in mismatches(seed, NP_VALUES)]
    assert not found, "\n".join(found[:10])


@pytest.mark.parametrize("start", range(0, SEEDS * 4, CHUNK * 4))
def test_native_accumulate_fuzz_on_numpy_values(start):
    seeds = range(start, min(start + CHUNK * 4, SEEDS * 4))
    found = [m for seed in seeds for m in accumulate_mismatches(seed, NP_ACC_SEQS)]
    assert not found, "\n".join(found[:10])


@pytest.mark.parametrize("compiled", [False, True])
def test_a_native_numeric_run_hands_numpy_scalars_back_unchanged(compiled):
    """Consecutive native steps run unboxed on exact ints and floats only."""

    def build(s, out):
        s.map(lambda x: x + 1).map(lambda x: x * 3).filter(lambda x: x % 2 == 0).map(lambda x: x - 1).sink(out.append)

    def python(x):
        x = (x + 1) * 3
        return [canon(x - 1)] if x % 2 == 0 else []

    values = [v for v in NP_VALUES if np.ndim(v) == 0 and not isinstance(v, np.complexfloating)]
    want = []
    for v in values:
        try:
            want.append(python(v))
        except Exception as e:
            want.append(canon(e))
    assert per_item(build, compiled, values) == want


@pytest.mark.parametrize("mode", ["eager", "compiled", "batch"])
def test_accumulating_arrays_never_aliases_them(mode):
    """`acc + x` must make a new array each time: an in-place add would
    change the start value, the inputs and every earlier result."""
    start, out = np.zeros(3), []
    s = akayu.Stream()
    s.accumulate(lambda acc, x: acc + x, start).sink(out.append)
    if mode != "eager":
        s.compile()
    xs = [np.full(3, float(i)) for i in range(1, 5)]
    if mode == "batch":
        s.emit_batch(xs)
    else:
        for x in xs:
            s.emit(x)
    assert np.array_equal(start, np.zeros(3))
    assert all(np.array_equal(x, np.full(3, float(i))) for i, x in enumerate(xs, 1))
    assert [o.tolist() for o in out] == [[1.0] * 3, [3.0] * 3, [6.0] * 3, [10.0] * 3]
    assert len({id(o) for o in out}) == len(out)


@pytest.mark.parametrize("compiled", [False, True])
@pytest.mark.parametrize("data", [np.arange(10), np.arange(12).reshape(4, 3), np.array([], dtype=float)])
def test_emit_batch_takes_an_array(compiled, data):
    """Its items are the array's rows (numpy scalars for a 1-D array)."""
    s, out = akayu.Stream(), []
    s.map(lambda x: x * 2).sink(out.append)
    if compiled:
        s.compile()
    s.emit_batch(data)
    want = [x * 2 for x in data]
    assert [canon(o) for o in out] == [canon(w) for w in want]


def test_emit_batch_of_a_0d_array_is_a_type_error():
    with pytest.raises(TypeError):
        akayu.Stream().emit_batch(np.array(5))


def test_a_batch_map_gets_the_whole_array_batch():
    data = np.arange(100_000)
    calls, out = [], []
    s = akayu.Stream()
    s.batch_map(lambda xs: (calls.append(len(xs)), np.asarray(xs) * 2)[1]).sink(out.append)
    s.compile()
    s.emit_batch(data)
    assert calls == [len(data)]
    assert type(out[0]) is np.int64
    assert np.array_equal(np.array(out), data * 2)


# --- numpy work under par() and prefetch() ---------------------------------


def sort_copy(a):
    """numpy work that releases the GIL: sort a copy of `a`."""
    return float(np.sort(a)[0])


BIG = np.random.default_rng(0).standard_normal(2_000_000)


def in_parallel(attempt, tries=3):
    """Whether `attempt()`, returning (CPU seconds summed over the threads
    that did the work, wall seconds), ever used more CPU than wall time: work
    taking turns never can. A loaded machine starts the pool's workers late,
    and the waiting thread then runs the unstarted work itself, one item
    after another, so one attempt alone may show no overlap."""
    seen = []
    for _ in range(tries):
        cpu, wall = attempt()
        if cpu > 1.5 * wall:
            return True
        seen.append(f"{cpu:.2f}s of CPU in {wall:.2f}s")
    pytest.fail("it took turns every time: " + ", ".join(seen))


def test_numpy_par_branches_run_at_the_same_time():
    """They do because numpy releases the GIL and akayu doesn't hold it."""

    def attempt():
        cpu, lock = [], threading.Lock()

        def branch(x):
            t = time.thread_time()
            for _ in range(3):
                sort_copy(BIG)
            with lock:
                cpu.append(time.thread_time() - t)
            return x

        s, out = akayu.Stream(), []
        p = s.par()
        akayu.union(*[p.map(branch) for _ in range(4)]).seq().sink(out.append)
        s.compile()
        start = time.perf_counter()
        s.emit(1)
        wall = time.perf_counter() - start
        assert out == [1] * 4
        return sum(cpu), wall

    assert in_parallel(attempt)


def test_prefetched_numpy_work_runs_at_the_same_time_and_stays_in_order():
    def attempt():
        cpu, lock = [], threading.Lock()

        def work(i):
            t = time.thread_time()
            r = sort_copy(BIG[i:])
            with lock:
                cpu.append(time.thread_time() - t)
            return r

        s, out = akayu.Stream(), []
        s.prefetch(4).map(work).sink(out.append)
        s.compile()
        start = time.perf_counter()
        for i in range(12):
            s.emit(i)
        s.flush()
        wall = time.perf_counter() - start
        assert out == [float(np.min(BIG[i:])) for i in range(12)]
        return sum(cpu), wall

    assert in_parallel(attempt)


def test_arrays_are_released_after_par_and_prefetch():
    refs = []

    def tracked(a):
        refs.append(weakref.ref(a))
        return a

    s = akayu.Stream()
    p = s.par()
    branches = [p.map(lambda a: tracked(a + 1)), p.map(lambda a: tracked(a * 2))]
    akayu.union(*branches).seq().map(lambda a: float(a[0])).sink(lambda _: None)
    pf = akayu.Stream()
    pf.prefetch(4).map(lambda a: tracked(a.copy())).map(len).sink(lambda _: None)
    s.compile()
    pf.compile()
    for _ in range(20):
        a = tracked(np.ones(10_000))
        s.emit(a)
        pf.emit(a)
    pf.flush()
    del a
    gc.collect()
    assert len(refs) == 20 * 4
    assert sum(r() is not None for r in refs) == 0


def test_a_numpy_error_in_a_par_branch_is_raised_after_the_others_finish():
    done = []

    def slow(x):
        time.sleep(0.2)
        done.append(x)
        return x

    s = akayu.Stream()
    p = s.par()
    akayu.union(p.map(lambda _: np.linalg.inv(np.zeros((3, 3)))), p.map(slow)).seq().sink(lambda _: None)
    s.compile()
    with pytest.raises(RuntimeError) as info:
        s.emit(1)
    assert isinstance(innermost(info.value), np.linalg.LinAlgError)
    assert done == [1]


def test_threads_emitting_and_flushing_a_numpy_par_graph():
    seen, lock = [], threading.Lock()

    def keep(batch):
        with lock:
            seen.extend(batch)

    s = akayu.Stream()
    p = s.par()
    akayu.union(
        p.map(lambda x: (x, float(np.sort(np.full(20_000, x))[0]))),
        p.map(lambda x: (x, float(np.ones(20_000).sum()) * 0 + x)),
    ).seq().collect().sink(keep)
    s.compile()
    stop = threading.Event()

    def flusher():
        while not stop.is_set():
            s.flush()
            time.sleep(0.001)

    t = threading.Thread(target=flusher)
    t.start()
    try:
        with ThreadPoolExecutor(6) as ex:
            list(ex.map(s.emit, range(1000)))
    finally:
        stop.set()
        t.join()
    s.flush()
    assert sorted(seen) == sorted([(i, float(i)) for i in range(1000)] * 2)


def test_emits_from_asyncio_executor_threads_through_numpy_par():
    out, lock = [], threading.Lock()

    def keep(x):
        with lock:
            out.append(x)

    s = akayu.Stream()
    p = s.par()
    akayu.union(p.map(lambda x: x + 1), p.map(lambda x: float(np.sum(np.ones(1000) * x)))).seq().sink(keep)
    s.compile()

    async def main():
        loop = asyncio.get_running_loop()
        await asyncio.gather(*[loop.run_in_executor(None, s.emit, i) for i in range(500)])

    asyncio.run(main())
    assert sorted(out) == sorted([i + 1 for i in range(500)] + [float(i * 1000) for i in range(500)])
