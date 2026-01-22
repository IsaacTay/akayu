import rstreamz

def test_benchmark_no_compile(benchmark):
    """
    Benchmark a pipeline WITHOUT explicit .compile().
    Only 'safe' optimizations (like parallel locking) run on first emit.
    Linear fusion (Map+Map) is DISABLED.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        count = 0
        def inc_count(x):
            nonlocal count
            count += 1

        # Pipeline with fusable nodes:
        # map(inc) -> map(double) -> filter(even) -> filter(divisible_by_4)
        s.map(lambda x: x + 1)\
         .map(lambda x: x * 2)\
         .filter(lambda x: x % 2 == 0)\
         .filter(lambda x: x % 4 == 0)\
         .sink(inc_count)

        for i in range(100_000):
            s.emit(i)
        
        return count

    result = benchmark(run_pipeline)
    assert result == 50000  # Half of them match? 
    # 0 -> 1 -> 2 (even, not div 4) -> X
    # 1 -> 2 -> 4 (even, div 4) -> KEEP
    # So every other item.

def test_benchmark_with_compile(benchmark):
    """
    Benchmark the SAME pipeline WITH explicit .compile().
    This enables 'unsafe' optimizations like linear fusion.
    Map+Map and Filter+Filter should merge.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        count = 0
        def inc_count(x):
            nonlocal count
            count += 1

        # Same pipeline
        s.map(lambda x: x + 1)\
         .map(lambda x: x * 2)\
         .filter(lambda x: x % 2 == 0)\
         .filter(lambda x: x % 4 == 0)\
         .sink(inc_count)

        # Explicitly compile to trigger fusion
        s.compile()

        for i in range(100_000):
            s.emit(i)

        return count

    result = benchmark(run_pipeline)
    assert result == 50000


def test_benchmark_batch_map_no_compile(benchmark):
    """
    Benchmark batch_map chain WITHOUT compile.
    No fusion occurs - intermediate lists are created between batch_maps.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        results = []

        # Pipeline with fusable batch_map nodes
        s.batch_map(lambda batch: [x + 1 for x in batch])\
         .batch_map(lambda batch: [x * 2 for x in batch])\
         .batch_map(lambda batch: [x + 10 for x in batch])\
         .sink(results.append)

        s.emit_batch(list(range(100_000)))
        return len(results)

    result = benchmark(run_pipeline)
    assert result == 100_000


def test_benchmark_batch_map_with_compile(benchmark):
    """
    Benchmark batch_map chain WITH compile.
    BatchMap fusion should compose all batch_maps into one.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        results = []

        # Same pipeline
        s.batch_map(lambda batch: [x + 1 for x in batch])\
         .batch_map(lambda batch: [x * 2 for x in batch])\
         .batch_map(lambda batch: [x + 10 for x in batch])\
         .sink(results.append)

        s.compile()
        s.emit_batch(list(range(100_000)))
        return len(results)

    result = benchmark(run_pipeline)
    assert result == 100_000


def test_benchmark_filter_map_no_compile(benchmark):
    """
    Benchmark filter→map chain WITHOUT compile.
    Map runs on all items, filter runs on all items.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        results = []

        # Pipeline: filter then map - without fusion, map runs even on filtered items
        s.filter(lambda x: x % 2 == 0)\
         .map(lambda x: x * 2)\
         .filter(lambda x: x % 4 == 0)\
         .map(lambda x: x + 1)\
         .sink(results.append)

        for i in range(100_000):
            s.emit(i)
        return len(results)

    result = benchmark(run_pipeline)
    assert result == 50000  # Every even number passes first filter, all pass second


def test_benchmark_filter_map_with_compile(benchmark):
    """
    Benchmark filter→map chain WITH compile.
    FilterMap fusion should skip map calls for filtered items.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        results = []

        # Same pipeline
        s.filter(lambda x: x % 2 == 0)\
         .map(lambda x: x * 2)\
         .filter(lambda x: x % 4 == 0)\
         .map(lambda x: x + 1)\
         .sink(results.append)

        s.compile()
        for i in range(100_000):
            s.emit(i)
        return len(results)

    result = benchmark(run_pipeline)
    assert result == 50000


def test_benchmark_passthrough_no_compile(benchmark):
    """
    Benchmark pipeline with seq() nodes WITHOUT compile.
    seq() nodes add propagation overhead.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        results = []

        # Pipeline with seq() nodes (Source nodes that just pass through)
        s.seq()\
         .map(lambda x: x + 1)\
         .seq()\
         .map(lambda x: x * 2)\
         .seq()\
         .sink(results.append)

        for i in range(100_000):
            s.emit(i)
        return len(results)

    result = benchmark(run_pipeline)
    assert result == 100_000


def test_benchmark_passthrough_with_compile(benchmark):
    """
    Benchmark pipeline with seq() nodes WITH compile.
    Passthrough elimination should bypass seq() nodes entirely.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        results = []

        # Same pipeline
        s.seq()\
         .map(lambda x: x + 1)\
         .seq()\
         .map(lambda x: x * 2)\
         .seq()\
         .sink(results.append)

        s.compile()
        for i in range(100_000):
            s.emit(i)
        return len(results)

    result = benchmark(run_pipeline)
    assert result == 100_000


def test_benchmark_map_sink_no_compile(benchmark):
    """
    Benchmark map→sink chain WITHOUT compile.
    Propagation overhead between map and sink.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        count = 0
        def inc_count(x):
            nonlocal count
            count += 1

        # Pipeline: map then sink
        s.map(lambda x: x * 2)\
         .map(lambda x: x + 1)\
         .sink(inc_count)

        for i in range(100_000):
            s.emit(i)
        return count

    result = benchmark(run_pipeline)
    assert result == 100_000


def test_benchmark_map_sink_with_compile(benchmark):
    """
    Benchmark map→sink chain WITH compile.
    MapSink fusion eliminates propagation overhead.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        count = 0
        def inc_count(x):
            nonlocal count
            count += 1

        # Same pipeline
        s.map(lambda x: x * 2)\
         .map(lambda x: x + 1)\
         .sink(inc_count)

        s.compile()
        for i in range(100_000):
            s.emit(i)
        return count

    result = benchmark(run_pipeline)
    assert result == 100_000


def test_benchmark_filter_sink_no_compile(benchmark):
    """
    Benchmark filter→sink chain WITHOUT compile.
    Sink called for every item that passes filter.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        count = 0
        def inc_count(x):
            nonlocal count
            count += 1

        # Pipeline: filter then sink
        s.filter(lambda x: x % 2 == 0)\
         .sink(inc_count)

        for i in range(100_000):
            s.emit(i)
        return count

    result = benchmark(run_pipeline)
    assert result == 50000


def test_benchmark_filter_sink_with_compile(benchmark):
    """
    Benchmark filter→sink chain WITH compile.
    FilterSink fusion combines filter and sink into one operation.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        count = 0
        def inc_count(x):
            nonlocal count
            count += 1

        # Same pipeline
        s.filter(lambda x: x % 2 == 0)\
         .sink(inc_count)

        s.compile()
        for i in range(100_000):
            s.emit(i)
        return count

    result = benchmark(run_pipeline)
    assert result == 50000


def test_benchmark_filter_map_sink_no_compile(benchmark):
    """
    Benchmark filter→map→sink chain WITHOUT compile.
    Full propagation overhead at each step.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        count = 0
        def inc_count(x):
            nonlocal count
            count += 1

        # Pipeline: filter then map then sink
        s.filter(lambda x: x % 2 == 0)\
         .map(lambda x: x * 2)\
         .sink(inc_count)

        for i in range(100_000):
            s.emit(i)
        return count

    result = benchmark(run_pipeline)
    assert result == 50000


def test_benchmark_filter_map_sink_with_compile(benchmark):
    """
    Benchmark filter→map→sink chain WITH compile.
    FilterMapSink fusion combines all three into one operation.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        count = 0
        def inc_count(x):
            nonlocal count
            count += 1

        # Same pipeline
        s.filter(lambda x: x % 2 == 0)\
         .map(lambda x: x * 2)\
         .sink(inc_count)

        s.compile()
        for i in range(100_000):
            s.emit(i)
        return count

    result = benchmark(run_pipeline)
    assert result == 50000


def test_benchmark_starmap_fusion_with_compile(benchmark):
    """
    Benchmark starmap fusion WITH compile.
    Starmap should be treated like Map for fusion purposes.
    """
    def run_pipeline():
        s = rstreamz.Stream(asynchronous=False)
        results = []

        # Pipeline: map produces tuple, starmap unpacks it, map processes result
        s.map(lambda x: (x, x * 2))\
         .starmap(lambda a, b: a + b)\
         .map(lambda x: x * 2)\
         .sink(results.append)

        s.compile()
        for i in range(100_000):
            s.emit(i)
        return len(results)

    result = benchmark(run_pipeline)
    assert result == 100_000
