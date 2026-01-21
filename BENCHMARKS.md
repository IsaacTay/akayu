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
