# Migrating from streamz

akayu is a Rust-powered drop-in replacement for [streamz](https://github.com/python-streamz/streamz) with better performance. This guide covers the key differences.

## Quick Comparison

| streamz | akayu | Notes |
|---------|-------|-------|
| `source.emit(x)` | `source.emit(x)` | Same API |
| `source.map(f)` | `source.map(f)` | Same API |
| `source.filter(f)` | `source.filter(f)` | Same API |
| `source.accumulate(f)` | `source.accumulate(f)` | Same API |
| `source.sink(f)` | `source.sink(f)` | Same API |
| N/A | `source.compile()` | **New** — enables optimizations |
| `Stream()` | `akayu.Stream()` | Import via `import akayu` |
| N/A | `source.emit_batch([...])` | **New** — bulk emit |
| N/A | `source.batch_map(f)` | **New** — vectorized transform |
| N/A | `source.prefetch(n)` | **New** — ordered concurrency |
| `source.par()` | `source.par()` | Same API |

## Key Differences

### 1. Import Style

```python
# streamz
from streamz import Stream

# akayu
import akayu
source = akayu.Stream()
```

### 2. The `compile()` Step

**streamz** has no compile step — graphs are always dynamic.

**akayu** has an optional `compile()` step that:
- Fuses operator chains (`map.map.map` → single fused op)
- Locks the graph (prevents accidental mutation)
- Initializes state for accumulate nodes

```python
source = akayu.Stream()
source.map(f1).map(f2).map(f3).sink(print)

# Optional but recommended for production
source.compile()
source.emit_batch(data)
```

### 3. emit() to Non-Source Nodes

**streamz** allows emitting to any node in simple cases.

**akayu** only allows emit to **source nodes** (created with `Stream()`). This is an intentional design choice for clarity.

```python
# streamz: may work in simple cases
mapped = source.map(f)
mapped.emit(x)  # sometimes works, behavior varies

# akayu: explicit error
mapped.emit(x)  # ❌ RuntimeError: Cannot emit to non-source node
```

### 4. Performance Characteristics

| Scenario | streamz | akayu |
|----------|---------|-------|
| Simple pipeline | Baseline | ~3x faster |
| Deep chains (10+ ops) | Baseline | ~3.2x faster |
| Branching/merging | Baseline | ~2.5-3.7x faster |
| Batch processing | slower | optimized with `emit_batch` |

## What's the Same

- Core API philosophy (fluent chainable streams)
- `map`, `filter`, `sink`, `accumulate` semantics
- `union`, `zip`, `combine_latest` for combining streams
- `par()` for parallel branches
- Source → transforms → sink mental model

## Porting Checklist

- [ ] Change `from streamz import Stream` to `import akayu`
- [ ] Replace `Stream()` with `akayu.Stream()`
- [ ] Add `source.compile()` after pipeline construction (optional but recommended)
- [ ] Replace any `emit()` calls to non-source nodes with `union()` injection pattern
- [ ] Consider using `emit_batch()` + `batch_map()` for bulk operations
- [ ] Test performance — you may not need `par()` anymore due to Rust speedup

## Example: Before and After

### streamz

```python
from streamz import Stream

source = Stream()
results = []

source.map(lambda x: x * 2).filter(lambda x: x > 5).sink(results.append)

for i in range(10):
    source.emit(i)

print(results)
```

### akayu

```python
import akayu

source = akayu.Stream()
results = []

source.map(lambda x: x * 2).filter(lambda x: x > 5).sink(results.append)

source.compile()  # Enable optimizations

source.emit_batch(range(10))  # or emit() one-by-one

print(results)
```
