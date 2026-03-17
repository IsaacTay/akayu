"""Benchmark propagation overhead across different pipeline shapes."""
import time
from akayu import Stream

def noop(x):
    return x

def bench(label, setup_fn, n_emits=1000, batch_size=100, use_batch=True):
    """Run a benchmark and print profiling results."""
    source = setup_fn()
    source.compile()

    # Warmup
    if use_batch:
        source.emit_batch(list(range(10)))
    else:
        for i in range(10):
            source.emit(i)

    # Benchmark
    data = list(range(batch_size))
    with source.profile() as p:
        if use_batch:
            for _ in range(n_emits):
                source.emit_batch(data)
        else:
            for _ in range(n_emits * batch_size):
                source.emit(1)

    r = p.report
    d = r.to_dict()
    total = d['wall_time_ms']
    func = d['total_func_time_ms']
    overhead = d['total_overhead_ms']
    pct = (overhead / total * 100) if total > 0 else 0
    items = n_emits * batch_size
    print(f"{label:45s}  {total:8.2f}ms total  {func:8.2f}ms func  {overhead:8.2f}ms overhead ({pct:5.1f}%)  {items} items")
    return d

def linear_5_batch():
    """5-node linear pipeline, batch path."""
    s = Stream(asynchronous=False)
    s.map(noop).map(noop).map(noop).map(noop).sink(lambda x: None)
    return s

def linear_10_batch():
    """10-node linear pipeline, batch path."""
    s = Stream(asynchronous=False)
    node = s
    for _ in range(9):
        node = node.map(noop)
    node.sink(lambda x: None)
    return s

def branching_2():
    """Source -> 2 branches, each with a sink."""
    s = Stream(asynchronous=False)
    m = s.map(noop)
    m.sink(lambda x: None)
    m.sink(lambda x: None)
    return s

def branching_convergence():
    """Source -> 2 branches -> union -> sink."""
    s = Stream(asynchronous=False)
    a = s.map(noop)
    b = s.map(noop)
    a.union(b).sink(lambda x: None)
    return s

def linear_5_single():
    """5-node linear pipeline, single-item path."""
    s = Stream(asynchronous=False)
    s.map(noop).map(noop).map(noop).map(noop).sink(lambda x: None)
    return s

print("=" * 120)
print("Propagation Overhead Benchmark")
print("=" * 120)

print("\n--- Batch path (emit_batch) ---")
bench("linear-5-batch", linear_5_batch, n_emits=1000, batch_size=100)
bench("linear-10-batch", linear_10_batch, n_emits=1000, batch_size=100)
bench("branching-2-batch", branching_2, n_emits=1000, batch_size=100)
bench("branch-converge-batch", branching_convergence, n_emits=1000, batch_size=100)

print("\n--- Single-item path (emit) ---")
bench("linear-5-single", linear_5_single, n_emits=1, batch_size=100000, use_batch=False)
bench("linear-10-single", lambda: linear_10_batch(), n_emits=1, batch_size=100000, use_batch=False)
bench("branching-2-single", branching_2, n_emits=1, batch_size=100000, use_batch=False)
bench("branch-converge-single", branching_convergence, n_emits=1, batch_size=100000, use_batch=False)
