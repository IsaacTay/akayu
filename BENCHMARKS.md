# Benchmarks

Benchmarks comparing `rstreamz` performance against the original `streamz` library and pure Python implementations.

**Environment:**
- Python 3.13.11
- Linux (x86_64)
- pytest-benchmark 5.2.3

## Summary

| Comparison | Speedup |
|------------|---------|
| rstreamz vs streamz (map/filter pipeline) | **~25x faster** |
| rstreamz vs streamz (flatten) | **~60x faster** |
| rstreamz vs streamz (collect) | **~38x faster** |
| rstreamz vs streamz (complex topologies) | **~21-45x faster** |

## rstreamz vs streamz

### Map/Filter Pipeline (500k items)

| Library | Mean (ms) | Ops/sec | Speedup |
|---------|-----------|---------|---------|
| rstreamz | 234.10 | 4.27 | 1.0x |
| streamz | 5,724.34 | 0.17 | **24.5x slower** |

### Flatten Operation (500k items)

| Library | Mean (ms) | Ops/sec | Speedup |
|---------|-----------|---------|---------|
| rstreamz | 34.47 | 29.01 | 1.0x |
| streamz | 2,037.54 | 0.49 | **59x slower** |

### Collect Operation (500k items)

| Library | Mean (ms) | Ops/sec | Speedup |
|---------|-----------|---------|---------|
| rstreamz | 48.93 | 20.44 | 1.0x |
| streamz | 1,852.58 | 0.54 | **38x slower** |

## Complex Flow Topologies (100k items)

Benchmarks for complex stream patterns with multiple branches, joins, and deep pipelines.

| Topology | rstreamz (ms) | streamz (ms) | Speedup |
|----------|---------------|--------------|---------|
| Multi-union (4 sources) | 33.86 | 1,533.26 | **45x faster** |
| Combine latest (3 sources) | 39.67 | 863.27 | **22x faster** |
| Diamond (split → union) | 43.17 | 1,935.08 | **45x faster** |
| Accumulate (3 branches) | 83.25 | 1,787.17 | **21x faster** |
| Zip (3 sources) | 85.62 | 1,887.51 | **22x faster** |
| Deep pipeline (10 maps) | 132.76 | 3,763.74 | **28x faster** |
| Complex DAG | 145.00 | 3,591.57 | **25x faster** |
| Fan-out (5 branches) | 165.72 | 3,472.00 | **21x faster** |

**Topology descriptions:**
- **Multi-union**: 4 separate sources merged via union chain
- **Combine latest**: 3 sources combined, emitting tuples of latest values
- **Diamond**: Single source splits to 2 branches, then merges via union
- **Accumulate branches**: Single source with 3 parallel accumulate operations (sum, count, max)
- **Zip**: 3 sources zipped together element-by-element
- **Deep pipeline**: 10 chained map operations
- **Complex DAG**: Split with one branch using flatten, then union
- **Fan-out**: Single source with 5 parallel map→filter→sink branches

## rstreamz vs Pure Python

Comparison against equivalent pure Python implementations:

| Implementation | Mean (ms) | Ops/sec | Notes |
|----------------|-----------|---------|-------|
| Pure Python loop | 57.43 | 17.41 | Direct list iteration |
| Python separated map+filter | 78.13 | 12.80 | Two separate list comps |
| Python map+filter combined | 139.26 | 7.18 | Generator chain |
| rstreamz batch | 143.84 | 6.95 | Stream pipeline |
| rstreamz emit | 238.17 | 4.20 | Per-item emit |

The overhead vs pure Python is due to:
1. Stream graph traversal
2. Python function call overhead per operation
3. Dynamic dispatch for node types

For batch operations, rstreamz approaches pure Python performance while providing the reactive programming model.

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
