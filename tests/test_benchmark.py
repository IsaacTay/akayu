import akayu


def test_benchmark_throughput(benchmark):
    """
    Benchmarks passing 500,000 items through a simple map pipeline (Default Mode).
    """

    def run_pipeline():
        s = akayu.Stream()
        # Create a pipeline: map -> filter -> sink
        count = 0

        def inc_count(x):
            nonlocal count
            count += 1

        s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).map(lambda x: x / 2).sink(
            inc_count
        )

        for i in range(500000):
            s.emit(i)

        return count

    # pytest-benchmark will run this multiple times
    result = benchmark(run_pipeline)
    assert result == 250000


def test_benchmark_throughput_sync_flag(benchmark):
    """
    Benchmarks passing 500,000 items through a simple map pipeline (asynchronous=False).
    """

    def run_pipeline():
        # Set asynchronous=False to enable sync-only optimization
        s = akayu.Stream(asynchronous=False)
        # Create a pipeline: map -> filter -> sink
        count = 0

        def inc_count(x):
            nonlocal count
            count += 1

        s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).map(lambda x: x / 2).sink(
            inc_count
        )

        for i in range(500000):
            s.emit(i)

        return count

    result = benchmark(run_pipeline)
    assert result == 250000


def test_benchmark_batch_throughput(benchmark):
    """
    Benchmarks passing 500,000 items through a simple map pipeline using emit_batch.
    """

    def run_pipeline():
        # Set asynchronous=False to enable sync-only optimization + batching
        s = akayu.Stream(asynchronous=False)
        # Create a pipeline: map -> filter -> sink
        count = 0

        def inc_count(x):
            nonlocal count
            count += 1

        s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).map(lambda x: x / 2).sink(
            inc_count
        )

        # Emit in batches of 1000
        batch = list(range(1000))
        for _ in range(500):
            s.emit_batch(batch)

        return count

    # pytest-benchmark will run this multiple times
    result = benchmark(run_pipeline)
    assert result == 250000


def test_benchmark_throughput_streamz(benchmark):
    """
    Benchmarks passing 500,000 items through a simple map pipeline using streamz.
    """
    import streamz

    def run_pipeline():
        s = streamz.Stream()
        # Create a pipeline: map -> filter -> sink
        count = 0

        def inc_count(x):
            nonlocal count
            count += 1

        s.map(lambda x: x + 1).filter(lambda x: x % 2 == 0).map(lambda x: x / 2).sink(
            inc_count
        )

        for i in range(500000):
            s.emit(i)

        return count

    result = benchmark(run_pipeline)
    assert result == 250000


def test_benchmark_pure_python(benchmark):
    """
    Benchmark a pure Python for-loop implementing the same logic.
    Baseline for 'zero overhead'.
    """

    def run_loop():
        count = 0
        for i in range(500000):
            # map: x + 1
            x = i + 1
            # filter: x % 2 == 0
            if x % 2 == 0:
                # map: x / 2
                _ = x / 2
                # sink: count
                count += 1
        return count

    result = benchmark(run_loop)
    assert result == 250000


def test_benchmark_expansion_emit(benchmark):
    """
    Benchmark Expansion: Map creates array -> Flatten.
    Standard emit() item by item.
    """

    def run_pipeline():
        s = akayu.Stream(asynchronous=False)
        count = 0

        def inc_count(x):
            nonlocal count
            count += 1

        # Expansion: 1 item -> 100 items
        # Total: 5000 inputs * 100 expansion = 500,000 operations downstream
        # Pipeline: map(expand) -> flatten -> map -> filter -> map -> sink
        s.map(lambda x: [x] * 100).flatten().map(lambda x: x + 1).filter(
            lambda x: x % 2 == 0
        ).map(lambda x: x / 2).sink(inc_count)

        for i in range(5000):
            s.emit(i)

        return count

    result = benchmark(run_pipeline)
    assert result == 250000


def test_benchmark_expansion_batch(benchmark):
    """
    Benchmark Expansion: Map creates array -> Flatten.
    Using emit_batch().
    """

    def run_pipeline():
        s = akayu.Stream(asynchronous=False)
        count = 0

        def inc_count(x):
            nonlocal count
            count += 1

        # Expansion: 1 item -> 100 items
        # Total: 5000 inputs * 100 expansion = 500,000 operations downstream
        # Pipeline: map(expand) -> flatten -> map -> filter -> map -> sink
        s.map(lambda x: [x] * 100).flatten().map(lambda x: x + 1).filter(
            lambda x: x % 2 == 0
        ).map(lambda x: x / 2).sink(inc_count)

        # Single batch of 5000 items
        batch = list(range(5000))
        s.emit_batch(batch)

        return count

    result = benchmark(run_pipeline)
    assert result == 250000


def test_benchmark_python_map_filter(benchmark):
    """
    Benchmark using Python's built-in map and filter functions.
    """

    def run_pipeline():
        # Pipeline: map(x+1) -> filter(x%2==0) -> map(x/2)
        iterable = range(500000)

        # map: x + 1
        mapped1 = map(lambda x: x + 1, iterable)

        # filter: x % 2 == 0
        filtered = filter(lambda x: x % 2 == 0, mapped1)

        # map: x / 2
        mapped2 = map(lambda x: x / 2, filtered)

        # Sink: consume count
        count = sum(1 for _ in mapped2)
        return count

    result = benchmark(run_pipeline)
    assert result == 250000
