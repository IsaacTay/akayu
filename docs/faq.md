# FAQ

## Do I need Rust installed?

**No.** akayu distributes pre-built wheels via pip. Just run:

```bash
pip install akayu
```

No Rust toolchain, no compilation step.

## When should I call `compile()`?

`compile()` is **optional** but recommended for production use:

| Scenario | compile()? | Why |
|----------|------------|-----|
| Production pipelines | ✅ Yes | Enables operator fusion, reduces overhead |
| Hot loops | ✅ Yes | Minimizes per-item overhead |
| Debugging | ❌ No | Uncompiled graphs are easier to inspect |
| Dynamic pipelines | ❌ No | You need to modify the graph at runtime |
| Prototyping | ❌ No | Faster iteration without recompiling |

## What's the difference between `emit()` and `emit_batch()`?

- `emit(x)` — Emit a **single item** through the pipeline
- `emit_batch([...])` — Emit a **collection of items** more efficiently

```python
# Single item (higher per-item overhead)
source.emit(1)
source.emit(2)
source.emit(3)

# Batch (lower overhead, recommended for bulk data)
source.emit_batch([1, 2, 3])
```

For large datasets (>1000 items), prefer `emit_batch()` with `batch_map()` for optimal performance.

## How do I debug my pipeline?

### 1. Name your streams

```python
source = akayu.Stream(name="input")
processed = source.map(process, name="process_step")
```

### 2. Skip compile() while debugging

Compiled graphs are harder to inspect. Debug with an uncompiled graph first:

```python
source = akayu.Stream()
# ... build pipeline ...
# source.compile()  # Comment out for debugging
```

### 3. Add print sinks temporarily

```python
source.map(transform).sink(print)  # Inspect values at any stage
```

### 4. Profile hot paths

```python
import cProfile
cProfile.run('source.emit_batch(data)', 'profile.stats')
```

## Can I use async functions?

Yes! akayu supports async functions throughout the pipeline:

```python
import asyncio
import akayu

async def fetch_url(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

source = akayu.Stream()
source.map(fetch_url).sink(print)

source.compile()
source.emit_batch(urls)
```

## Is it thread-safe?

**Yes**, with caveats:

- A single source can be emitted from multiple threads
- Each `Stream()` source is independent
- After `compile()`, the graph is locked (no thread safety issues)
- For true parallelism, use `par()` which manages its own thread pool

```python
# Safe: Multiple threads emitting to one source
source = akayu.Stream()
source.map(heavy_task).sink(print)
source.compile()

with ThreadPoolExecutor(4) as ex:
    for item in items:
        ex.submit(source.emit, item)
```

## How does it compare to streamz/RxPY/aiostream?

| Library | Performance | Async | Rust Backend | Learning Curve |
|---------|-------------|-------|--------------|----------------|
| **akayu** | High (~3x vs streamz) | ✅ | ✅ | Low |
| **streamz** | Medium | ✅ | ❌ | Low |
| **RxPY** | Medium | ❌ | ❌ | Medium |
| **aiostream** | Medium | ✅ | ❌ | Low |

**akayu** is the fastest pure-Python streams library while maintaining streamz's simple API.

## Why is my pipeline slow?

### Common causes:

1. **Too much parallelism on cheap ops** — `par()` overhead only pays off for ops >1ms
2. **Not using `compile()`** — Missing operator fusion
3. **Item-by-item processing** — Use `emit_batch()` + `batch_map()` for bulk data
4. **Python function overhead** — Consider numba or vectorized ops in `batch_map()`

### Debugging steps:

```python
# 1. Profile to find the bottleneck
cProfile.run('source.emit_batch(data)')

# 2. Try compile()
source.compile()

# 3. Use batch operations
source.emit_batch(large_chunk)  # instead of loop + emit()
```

## What does "partially vibed" mean?

It's a playful nod to the project's origin — akayu was initially developed for internal use, so not every feature has been battle-tested in production environments. The core stream processing is solid; some advanced features (like certain async patterns) may need more real-world testing.

> "Partially vibed" = the author's team used it and it worked, but you might find edge cases. Report them!

## Still have questions?

- Check the [Patterns & Best Practices](patterns.md) guide
- Review the [API Reference](api.md)
- Look at [benchmarks](benchmarks.md) for performance expectations
