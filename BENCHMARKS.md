# Benchmarks

Benchmarks comparing `rstreamz` performance against the original `streamz` library and pure Python implementations.

**Environment:**
- Python 3.13.11
- Linux (x86_64)
- pytest-benchmark 5.2.3

## Summary

| Comparison | Speedup |
|------------|---------|
| rstreamz vs streamz (map/filter pipeline) | **~24x faster** |
| rstreamz vs streamz (flatten) | **~62x faster** |
| rstreamz vs streamz (collect) | **~37x faster** |
| rstreamz vs streamz (complex topologies) | **~21-45x faster** |

## rstreamz vs streamz

### Map/Filter Pipeline (500k items)

| Library | Mean (ms) | Ops/sec | Speedup |
|---------|-----------|---------|---------|
| rstreamz | 235.88 | 4.24 | 1.0x |
| streamz | 5,752.09 | 0.17 | **24x slower** |

### Flatten Operation (500k items)

| Library | Mean (ms) | Ops/sec | Speedup |
|---------|-----------|---------|---------|
| rstreamz | 33.32 | 30.02 | 1.0x |
| streamz | 2,051.00 | 0.49 | **62x slower** |

### Collect Operation (500k items)

| Library | Mean (ms) | Ops/sec | Speedup |
|---------|-----------|---------|---------|
| rstreamz | 50.52 | 19.80 | 1.0x |
| streamz | 1,854.79 | 0.54 | **37x slower** |

## Complex Flow Topologies (100k items)

Benchmarks for complex stream patterns with multiple branches, joins, and deep pipelines.

| Topology | rstreamz (ms) | streamz (ms) | Speedup |
|----------|---------------|--------------|---------|
| Multi-union (4 sources) | 33.73 | 1,525.04 | **45x faster** |
| Combine latest (3 sources) | 41.63 | 863.77 | **21x faster** |
| Diamond (split → union) | 43.40 | 1,950.33 | **45x faster** |
| Accumulate (3 branches) | 84.45 | 1,786.54 | **21x faster** |
| Zip (3 sources) | 85.82 | 1,892.09 | **22x faster** |
| Deep pipeline (10 maps) | 110.06 | 3,775.07 | **34x faster** |
| Complex DAG | 139.95 | 3,585.44 | **26x faster** |
| Fan-out (5 branches) | 165.08 | 3,477.61 | **21x faster** |

**Topology descriptions:**
- **Multi-union**: 4 separate sources merged via union chain
- **Combine latest**: 3 sources combined, emitting tuples of latest values
- **Diamond**: Single source splits to 2 branches, then merges via union
- **Accumulate branches**: Single source with 3 parallel accumulate operations (sum, count, max)
- **Zip**: 3 sources zipped together element-by-element
- **Deep pipeline**: 10 chained map operations (benefits from map fusion optimization)
- **Complex DAG**: Split with one branch using flatten, then union
- **Fan-out**: Single source with 5 parallel map→filter→sink branches

## rstreamz vs Pure Python

Comparison against equivalent pure Python implementations:

| Implementation | Mean (ms) | Ops/sec | Notes |
|----------------|-----------|---------|-------|
| Pure Python loop | 55.82 | 17.92 | Direct list iteration |
| Python separated map+filter | 77.33 | 12.93 | Two separate list comps |
| Python map+filter combined | 138.35 | 7.23 | Generator chain |
| rstreamz batch | 142.76 | 7.00 | Stream pipeline |
| rstreamz emit | 229.91 | 4.35 | Per-item emit |

The overhead vs pure Python is due to:
1. Stream graph traversal
2. Python function call overhead per operation
3. Dynamic dispatch for node types

For batch operations, rstreamz approaches pure Python performance while providing the reactive programming model.

## Optimizations

### Parallel Execution (`par()` + `prefetch()`)

rstreamz provides two mechanisms for parallel execution that work efficiently together:

- **`par()`**: Executes all downstream branches concurrently in a shared thread pool
- **`prefetch(n)`**: Processes up to `n` items concurrently while preserving output order

**Shared Thread Pool**: Both `par()` and `prefetch()` share a global thread pool (16 workers), eliminating the overhead of creating separate pools. This means combining `par()` with `prefetch()` has minimal additional overhead:

| Configuration | Time (50 items, 10ms IO each) | Ratio |
|--------------|-------------------------------|-------|
| `prefetch(4)` alone | ~135ms | 1.0x |
| `par()` + `prefetch(4)` | ~136ms | ~1.01x |

**Lock Optimization**: When `prefetch()` is used inside `par()` branches, rstreamz only locks at convergence points (like `union`), not the entire branch. This minimizes lock contention while maintaining thread safety.

### Map/Filter Fusion

On first emit, rstreamz optimizes the stream graph by fusing consecutive operations:
- **Consecutive maps**: `map(f1) → map(f2) → map(f3)` becomes a single `map(compose(f1, f2, f3))`
- **Consecutive filters**: `filter(p1) → filter(p2)` becomes `filter(p1 and p2)`

This reduces node traversals and improves performance for deep pipelines. The "Deep pipeline (10 maps)" benchmark shows **34x speedup** over streamz, benefiting from fusing 10 map operations into one.

## Running Benchmarks

```bash
# Enter development environment
nix develop

# Run all benchmarks
just bench

# Or manually:
maturin develop --release
pytest tests/test_benchmark.py tests/test_ops_benchmark.py -v --benchmark-only
```

## Benchmark Details

All benchmarks process 500,000 items through a pipeline of:
- `map(lambda x: x + 1)`
- `filter(lambda x: x % 2 == 0)`
- `sink(results.append)`

Timing includes pipeline setup and all item processing.
