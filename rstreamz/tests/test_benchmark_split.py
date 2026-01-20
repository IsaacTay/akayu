import rstreamz
import time
import psutil
import os
import pytest
import gc


def test_benchmark_split_expansion(benchmark):
    """
    Benchmark: Source -> Split -> Map(Expand) -> Flatten -> Sink.
    Tests if the expansion is processed efficiently after a split.
    """

    def run_pipeline():
        # asynchronous=False enables the Sync Fast Path
        s = rstreamz.Stream(asynchronous=False)
        count = 0

        def inc_count(x):
            nonlocal count
            count += 1

        # Branch 1
        s.map(lambda x: [x] * 100).flatten().map(lambda x: x + 1).filter(
            lambda x: x % 2 == 0
        ).map(lambda x: x / 2).sink(inc_count)
        # Branch 2 (Force split logic)
        s.map(lambda x: [x] * 100).flatten().map(lambda x: x + 1).filter(
            lambda x: x % 2 == 0
        ).map(lambda x: x / 2).sink(inc_count)

        # Emit 500 items.
        # Each splits to 2 branches.
        # Each branch expands to 100 items.
        # Total Ops: 500 * 2 * 100 = 100,000 downstream ops.

        # We use emit_batch, but the split will force single updates.
        # The optimization goal is for Flatten to re-batch.
        batch = list(range(500))
        s.emit_batch(batch)

        return count

    result = benchmark(run_pipeline)
    assert result == 50000
