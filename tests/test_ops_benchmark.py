import akayu

# Target 500,000 downstream operations/items
N_OPS = 500_000


def run_flatten(source_cls, n):
    s = source_cls()
    s.flatten().sink(lambda x: None)

    # Input: lists of length 100.
    # We need n / 100 emissions to get n total items downstream.
    chunk_size = 100
    n_chunks = n // chunk_size
    data = [1] * chunk_size

    for _ in range(n_chunks):
        s.emit(data)


def test_benchmark_flatten_akayu(benchmark):
    benchmark(run_flatten, akayu.Stream, N_OPS)


def run_collect(source_cls, n):
    s = source_cls()
    # Note: collect() accumulates everything into a list until flushed.
    # We won't flush, just benchmark the append overhead.
    s.collect().sink(lambda x: None)

    for i in range(n):
        s.emit(i)


def test_benchmark_collect_akayu(benchmark):
    benchmark(run_collect, akayu.Stream, N_OPS)
