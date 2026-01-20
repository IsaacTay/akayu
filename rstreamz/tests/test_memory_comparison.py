import pytest
import rstreamz
import streamz
import psutil
import os
import gc
import time


def get_process_memory():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # MB


def run_pipeline_and_measure(source_cls, items):
    # Force GC before starting
    gc.collect()
    time.sleep(0.1)

    baseline = get_process_memory()

    s = source_cls()
    # Create a pipeline: Source -> Map -> Filter -> Map -> Sink
    # This creates a few nodes.
    s.map(lambda x: x * 2).filter(lambda x: x % 2 == 0).map(lambda x: x + 1).sink(
        lambda x: None
    )

    # Measure memory after graph creation
    graph_mem = get_process_memory()

    peaks = []
    for i in range(items):
        s.emit(i)
        if i % 10000 == 0:
            peaks.append(get_process_memory())

    # Measure memory after processing
    end_mem = get_process_memory()

    peak = max(peaks) if peaks else end_mem

    return {
        "baseline": baseline,
        "graph_creation_overhead": graph_mem - baseline,
        "peak_processing": peak,
        "end_processing": end_mem,
        "leak": end_mem - graph_mem,
    }


@pytest.mark.parametrize(
    "library_name, stream_cls",
    [("streamz", streamz.Stream), ("rstreamz", rstreamz.Stream)],
)
def test_memory_footprint(library_name, stream_cls):
    items = 200_000
    stats = run_pipeline_and_measure(stream_cls, items)

    print(f"\n--- {library_name} Memory Stats ---")
    print(f"Baseline: {stats['baseline']:.2f} MB")
    print(f"Graph Overhead: {stats['graph_creation_overhead']:.2f} MB")
    print(f"Peak during {items} items: {stats['peak_processing']:.2f} MB")
    print(f"End after processing: {stats['end_processing']:.2f} MB")
    print(f"Potential Leak (End - Graph): {stats['leak']:.2f} MB")

    # Assertions
    # 1. No massive leaks (growth < 5MB for 200k items)
    assert stats["leak"] < 5.0, f"{library_name} leaked memory!"


if __name__ == "__main__":
    # Allow running manually to see print output easily
    test_memory_footprint("streamz", streamz.Stream)
    test_memory_footprint("rstreamz", rstreamz.Stream)
