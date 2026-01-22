"""
Benchmarks for complex stream topologies comparing akayu vs streamz.

Tests various patterns:
- Fan-out (split to multiple branches)
- Fan-in (union multiple streams)
- Diamond (split then merge)
- Deep pipelines
- Multiple joins (combine_latest, zip)
"""

import akayu
import streamz

N_ITEMS = 100_000


def run_fanout(source_cls, n_branches=5):
    """Fan-out: Single source splits to N branches, each with map->filter->sink."""
    s = source_cls()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    # Create N parallel branches from the same source
    for _ in range(n_branches):
        s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).sink(inc)

    for i in range(N_ITEMS):
        s.emit(i)

    return count


def test_benchmark_fanout_akayu(benchmark):
    """Fan-out: 1 source -> 5 branches"""
    result = benchmark(run_fanout, akayu.Stream, 5)
    assert result == N_ITEMS // 2 * 5


def test_benchmark_fanout_streamz(benchmark):
    """Fan-out: 1 source -> 5 branches"""
    result = benchmark(run_fanout, streamz.Stream, 5)
    assert result == N_ITEMS // 2 * 5


def run_deep_pipeline(source_cls, depth=10):
    """Deep pipeline: Chain of N map operations."""
    s = source_cls()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    # Build a deep chain of maps
    node = s
    for i in range(depth):
        node = node.map(lambda x, i=i: x + 1)

    node.sink(inc)

    for i in range(N_ITEMS):
        s.emit(i)

    return count


def test_benchmark_deep_pipeline_akayu(benchmark):
    """Deep pipeline: 10 chained maps"""
    result = benchmark(run_deep_pipeline, akayu.Stream, 10)
    assert result == N_ITEMS


def test_benchmark_deep_pipeline_streamz(benchmark):
    """Deep pipeline: 10 chained maps"""
    result = benchmark(run_deep_pipeline, streamz.Stream, 10)
    assert result == N_ITEMS


def run_diamond(source_cls):
    """Diamond pattern: split -> process differently -> union -> sink."""
    s = source_cls()
    results = []

    # Branch 1: multiply by 2
    branch1 = s.map(lambda x: x * 2)

    # Branch 2: multiply by 3
    branch2 = s.map(lambda x: x * 3)

    # Merge branches
    branch1.union(branch2).sink(results.append)

    for i in range(N_ITEMS):
        s.emit(i)

    return len(results)


def test_benchmark_diamond_akayu(benchmark):
    """Diamond: split -> 2 branches -> union"""
    result = benchmark(run_diamond, akayu.Stream)
    assert result == N_ITEMS * 2


def test_benchmark_diamond_streamz(benchmark):
    """Diamond: split -> 2 branches -> union"""
    result = benchmark(run_diamond, streamz.Stream)
    assert result == N_ITEMS * 2


def run_multi_union(source_cls, n_sources=4):
    """Multiple sources merged via union chain."""
    sources = [source_cls() for _ in range(n_sources)]
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    # Chain unions: s1.union(s2).union(s3).union(s4)...
    merged = sources[0]
    for s in sources[1:]:
        merged = merged.union(s)

    merged.map(lambda x: x + 1).sink(inc)

    # Emit to each source
    items_per_source = N_ITEMS // n_sources
    for i in range(items_per_source):
        for s in sources:
            s.emit(i)

    return count


def test_benchmark_multi_union_akayu(benchmark):
    """Multi-union: 4 sources merged"""
    result = benchmark(run_multi_union, akayu.Stream, 4)
    assert result == N_ITEMS


def test_benchmark_multi_union_streamz(benchmark):
    """Multi-union: 4 sources merged"""
    result = benchmark(run_multi_union, streamz.Stream, 4)
    assert result == N_ITEMS


def run_combine_latest(source_cls):
    """Combine latest from 3 sources."""
    s1 = source_cls()
    s2 = source_cls()
    s3 = source_cls()
    results = []

    s1.combine_latest(s2, s3).sink(results.append)

    # Emit to sources in round-robin to generate combine_latest outputs
    # First emit to all to "prime" them
    s1.emit(0)
    s2.emit(0)
    s3.emit(0)

    # Now each emit generates an output
    for i in range(1, N_ITEMS):
        s1.emit(i)

    return len(results)


def test_benchmark_combine_latest_akayu(benchmark):
    """Combine latest: 3 sources"""
    result = benchmark(run_combine_latest, akayu.Stream)
    assert result == N_ITEMS


def test_benchmark_combine_latest_streamz(benchmark):
    """Combine latest: 3 sources"""
    result = benchmark(run_combine_latest, streamz.Stream)
    assert result == N_ITEMS


def run_zip_streams(source_cls):
    """Zip 3 sources together."""
    s1 = source_cls()
    s2 = source_cls()
    s3 = source_cls()
    results = []

    s1.zip(s2, s3).sink(results.append)

    # Emit same number to each source
    for i in range(N_ITEMS):
        s1.emit(i)
        s2.emit(i * 2)
        s3.emit(i * 3)

    return len(results)


def test_benchmark_zip_akayu(benchmark):
    """Zip: 3 sources"""
    result = benchmark(run_zip_streams, akayu.Stream)
    assert result == N_ITEMS


def test_benchmark_zip_streamz(benchmark):
    """Zip: 3 sources"""
    result = benchmark(run_zip_streams, streamz.Stream)
    assert result == N_ITEMS


def run_complex_dag(source_cls):
    """
    Complex DAG:

    source -> map -> split -> branch1 -> map -> filter -----> union -> map -> sink
                  |                                             ^
                  +-> branch2 -> map -> flatten -> filter ------+
    """
    s = source_cls()
    count = 0

    def inc(x):
        nonlocal count
        count += 1

    # Branch 1: simple map + filter
    branch1 = s.map(lambda x: x + 1).map(lambda x: x * 2).filter(lambda x: x % 4 == 0)

    # Branch 2: map to list, flatten, filter
    branch2 = s.map(lambda x: [x, x + 1]).flatten().filter(lambda x: x % 3 == 0)

    # Union and final processing
    branch1.union(branch2).map(lambda x: x + 1).sink(inc)

    for i in range(N_ITEMS):
        s.emit(i)

    return count


def test_benchmark_complex_dag_akayu(benchmark):
    """Complex DAG with split, flatten, union"""
    benchmark(run_complex_dag, akayu.Stream)


def test_benchmark_complex_dag_streamz(benchmark):
    """Complex DAG with split, flatten, union"""
    benchmark(run_complex_dag, streamz.Stream)


def run_accumulate_branches(source_cls):
    """Multiple accumulate branches from single source."""
    s = source_cls()
    results = {"sum": 0, "count": 0, "max": None}

    def set_sum(x):
        results["sum"] = x

    def set_count(x):
        results["count"] = x

    def set_max(x):
        results["max"] = x

    # Branch 1: running sum
    s.accumulate(lambda acc, x: acc + x, start=0).sink(set_sum)

    # Branch 2: running count
    s.accumulate(lambda acc, x: acc + 1, start=0).sink(set_count)

    # Branch 3: running max
    s.accumulate(lambda acc, x: max(acc, x), start=float("-inf")).sink(set_max)

    for i in range(N_ITEMS):
        s.emit(i)

    return results["count"]


def test_benchmark_accumulate_branches_akayu(benchmark):
    """3 accumulate branches from single source"""
    result = benchmark(run_accumulate_branches, akayu.Stream)
    assert result == N_ITEMS


def test_benchmark_accumulate_branches_streamz(benchmark):
    """3 accumulate branches from single source"""
    result = benchmark(run_accumulate_branches, streamz.Stream)
    assert result == N_ITEMS
