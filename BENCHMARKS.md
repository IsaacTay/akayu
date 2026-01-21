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

## rstreamz vs streamz

### Map/Filter Pipeline (100k items)

| Library | Mean (ms) | Ops/sec | Speedup |
|---------|-----------|---------|---------|
| rstreamz | 46.45 | 21.53 | 1.0x |
| streamz | 1,151.03 | 0.87 | **24.8x slower** |

### Flatten Operation (100k items)

| Library | Mean (ms) | Ops/sec | Speedup |
|---------|-----------|---------|---------|
| rstreamz | 6.77 | 147.70 | 1.0x |
| streamz | 408.37 | 2.45 | **60x slower** |

### Collect Operation (100k items)

| Library | Mean (ms) | Ops/sec | Speedup |
|---------|-----------|---------|---------|
| rstreamz | 9.86 | 101.39 | 1.0x |
| streamz | 374.00 | 2.67 | **38x slower** |

## rstreamz vs Pure Python

Comparison against equivalent pure Python implementations:

| Implementation | Mean (ms) | Ops/sec | Notes |
|----------------|-----------|---------|-------|
| Pure Python loop | 11.26 | 88.78 | Direct list iteration |
| Python separated map+filter | 15.73 | 63.58 | Two separate list comps |
| Python map+filter combined | 27.99 | 35.72 | Generator chain |
| rstreamz batch | 29.67 | 33.71 | Stream pipeline |
| rstreamz emit | 47.00 | 21.28 | Per-item emit |

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

All benchmarks process 100,000 items through a pipeline of:
- `map(lambda x: x + 1)`
- `filter(lambda x: x % 2 == 0)`
- `sink(results.append)`

Timing includes pipeline setup and all item processing.
