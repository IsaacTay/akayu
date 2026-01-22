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
