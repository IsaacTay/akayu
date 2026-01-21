import pytest
import rstreamz
import streamz
import asyncio

# --- Functional Equivalence Tests ---


def test_equivalence_basic():
    """
    Verifies that rstreamz matches streamz behavior for Map -> Filter -> Accumulate.
    """

    # Setup Logic
    def pipeline_setup(source_cls):
        s = source_cls()
        res = []

        # map(x*2) -> filter(x>10) -> accumulate(sum) -> sink
        # Note: streamz accumulate default emits state on every update, same as rstreamz impl.
        # streamz accumulate signature: accumulate(func, start=no_default)

        node = s.map(lambda x: x * 2).filter(lambda x: x > 10)

        # Streamz accumulate vs rstreamz accumulate
        # rstreamz: accumulate(func, start)
        # streamz: accumulate(func, start=...)
        # Both behave similarly for simple reductions.

        node.accumulate(lambda state, x: state + x, start=0).sink(res.append)
        return s, res

    # Run Streamz
    s_orig, res_orig = pipeline_setup(streamz.Stream)
    inputs = [2, 6, 4, 8, 10]
    # 2*2=4 (>10 False)
    # 6*2=12 (>10 True) -> acc=12 -> emit 12
    # 4*2=8 (>10 False)
    # 8*2=16 (>10 True) -> acc=12+16=28 -> emit 28
    # 10*2=20 (>10 True) -> acc=28+20=48 -> emit 48

    for i in inputs:
        s_orig.emit(i)

    # Run RStreamz
    s_new, res_new = pipeline_setup(rstreamz.Stream)
    for i in inputs:
        s_new.emit(i)

    assert res_orig == res_new
    assert res_new == [12, 28, 48]


def test_equivalence_union():
    """
    Verifies Union behavior.
    """

    def setup(source_cls):
        s1 = source_cls()
        s2 = source_cls()
        res = []
        s1.union(s2).sink(res.append)
        return s1, s2, res

    # Streamz
    s1_o, s2_o, res_o = setup(streamz.Stream)
    s1_o.emit(1)
    s2_o.emit(2)
    s1_o.emit(3)

    # RStreamz
    s1_n, s2_n, res_n = setup(rstreamz.Stream)
    s1_n.emit(1)
    s2_n.emit(2)
    s1_n.emit(3)

    assert res_o == res_n
    assert res_n == [1, 2, 3]


def test_equivalence_combine_latest():
    """
    Verifies CombineLatest behavior.
    """

    def setup(source_cls):
        s1 = source_cls()
        s2 = source_cls()
        res = []
        s1.combine_latest(s2).sink(res.append)
        return s1, s2, res

    # Streamz
    s1_o, s2_o, res_o = setup(streamz.Stream)
    s1_o.emit(10)  # s1=10
    s2_o.emit(20)  # s2=20 -> (10, 20)
    s1_o.emit(30)  # s1=30 -> (30, 20)

    # RStreamz
    s1_n, s2_n, res_n = setup(rstreamz.Stream)
    s1_n.emit(10)
    s2_n.emit(20)
    s1_n.emit(30)

    assert res_o == res_n
    assert res_n == [(10, 20), (30, 20)]


# --- Benchmarks ---

INPUT_SIZE = 500_000


def run_throughput_benchmark(source_cls):
    s = source_cls()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    # Pipeline: map -> filter -> map -> sink
    s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).map(lambda x: x / 2).sink(inc)

    for i in range(INPUT_SIZE):
        s.emit(i)
    return count


def test_perf_rstreamz(benchmark):
    benchmark(run_throughput_benchmark, rstreamz.Stream)


def test_perf_streamz(benchmark):
    benchmark(run_throughput_benchmark, streamz.Stream)
