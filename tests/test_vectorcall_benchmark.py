"""Micro-benchmark: vectorcall vs PyObject_Call overhead.

Uses a trivial identity function so call overhead dominates.
"""
import akayu


def noop(x):
    return x


N = 1_000_000


def test_benchmark_noop_map_no_compile(benchmark):
    """Map with noop, no compile — measures per-item Rust overhead."""
    def run():
        s = akayu.Stream(asynchronous=False)
        results = []
        s.map(noop).sink(results.append)
        for i in range(N):
            s.emit(i)
        return len(results)

    result = benchmark(run)
    assert result == N


def test_benchmark_noop_map_compiled(benchmark):
    """Map with noop, compiled — measures fused overhead."""
    def run():
        s = akayu.Stream(asynchronous=False)
        results = []
        s.map(noop).sink(results.append)
        s.compile()
        for i in range(N):
            s.emit(i)
        return len(results)

    result = benchmark(run)
    assert result == N


def test_benchmark_noop_sink_only_no_compile(benchmark):
    """Sink-only with noop — baseline Rust call overhead."""
    def run():
        s = akayu.Stream(asynchronous=False)
        s.sink(noop)
        for i in range(N):
            s.emit(i)

    benchmark(run)


def test_benchmark_noop_sink_only_compiled(benchmark):
    """Sink-only with noop, compiled."""
    def run():
        s = akayu.Stream(asynchronous=False)
        s.sink(noop)
        s.compile()
        for i in range(N):
            s.emit(i)

    benchmark(run)


def test_benchmark_noop_filter_no_compile(benchmark):
    """Filter with always-true noop — measures filter call overhead."""
    def always_true(x):
        return True

    def run():
        s = akayu.Stream(asynchronous=False)
        results = []
        s.filter(always_true).sink(results.append)
        for i in range(N):
            s.emit(i)
        return len(results)

    result = benchmark(run)
    assert result == N


def test_benchmark_noop_filter_compiled(benchmark):
    """Filter with always-true, compiled."""
    def always_true(x):
        return True

    def run():
        s = akayu.Stream(asynchronous=False)
        results = []
        s.filter(always_true).sink(results.append)
        s.compile()
        for i in range(N):
            s.emit(i)
        return len(results)

    result = benchmark(run)
    assert result == N


def test_benchmark_noop_3map_chain_no_compile(benchmark):
    """3 noops chained — measures per-node transition overhead."""
    def run():
        s = akayu.Stream(asynchronous=False)
        results = []
        s.map(noop).map(noop).map(noop).sink(results.append)
        for i in range(N):
            s.emit(i)
        return len(results)

    result = benchmark(run)
    assert result == N


def test_benchmark_noop_3map_chain_compiled(benchmark):
    """3 noops chained, compiled — fusion eliminates transitions."""
    def run():
        s = akayu.Stream(asynchronous=False)
        results = []
        s.map(noop).map(noop).map(noop).sink(results.append)
        s.compile()
        for i in range(N):
            s.emit(i)
        return len(results)

    result = benchmark(run)
    assert result == N


def test_benchmark_noop_batch_emit(benchmark):
    """Batch emit with noop — single borrow for entire batch."""
    def run():
        s = akayu.Stream(asynchronous=False)
        results = []
        s.map(noop).sink(results.append)
        s.compile()
        s.emit_batch(list(range(N)))
        return len(results)

    result = benchmark(run)
    assert result == N
