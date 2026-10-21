# FAQ

## Do I need Rust installed?

**No**, on Linux (x86_64 and aarch64, glibc 2.17+), macOS (arm64 and x86_64) and Windows x64:
each release has wheels for CPython 3.13 and 3.14 there, which install from akayu's package index:

<!--pytest.mark.skip-->

```bash
uv add akayu --index akayu=https://isaactay.github.io/akayu/simple/
```

On other platforms, or to install an unreleased commit from GitHub, installing builds akayu from
source and needs a Rust toolchain ([rustup](https://rustup.rs/)). See
[Installation](getting-started.md#installation).

## When should I call `compile()`?

`compile()` is **optional** but recommended for production use:

| Scenario             | compile()? | Why                                       |
| -------------------- | ---------- | ----------------------------------------- |
| Production pipelines | ✅ Yes     | Enables operator fusion, reduces overhead |
| Hot loops            | ✅ Yes     | Minimizes per-item overhead               |
| Debugging            | ❌ No      | Uncompiled graphs are easier to inspect   |
| Dynamic pipelines    | ❌ No      | You need to modify the graph at runtime   |
| Prototyping          | ❌ No      | Faster iteration without recompiling      |

## What's the difference between `emit()` and `emit_batch()`?

- `emit(x)` — Emit a **single item** through the pipeline
- `emit_batch([...])` — Emit a **collection of items** more efficiently

```python
import akayu

source = akayu.Stream()
source.sink(print)

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
import akayu

source = akayu.Stream(name="input")
processed = source.map(lambda x: x * 2, stream_name="double")
```

### 2. See how each node runs

`explain()` lists the graph, a line per node, with where it was built and how its work runs:
`Python` (your function is called), `native` (akayu runs the lambda or builtin itself),
`native, unboxed` (arithmetic run on machine numbers) or `threads` (`par()`, `prefetch()`). After
`compile()` it also shows which nodes were merged into one loop (`chain 1`, ...). A map or filter
that runs as Python says why, when it can tell: `Python: calls lookup()`, `Python: 2 statements`,
`Python until compile()`:

```python
import akayu

source = akayu.Stream(name="input")
source.map(lambda x: x * 2, stream_name="double").map(str).sink([].append)
print(source.explain())
```

A compiled graph is frozen: add the print sinks below before calling `compile()`.

### 3. Add print sinks temporarily

<!-- pytest-codeblocks:cont -->

```python
source.map(lambda x: x + 1).sink(print)  # Inspect values at any stage
```

### 4. Profile hot paths

`profile()` times each node's function and counts the items in and out of it, with the same tags
as `explain()`:

<!-- pytest-codeblocks:cont -->

```python
with source.profile() as p:
    for x in range(3):
        source.emit(x)
print(p.report)  # or p.report.print(sort_by="time"), slowest first
```

## How does akayu handle concurrency?

With threads: `par()` runs branches and `prefetch()` runs calls on a shared pool of worker threads
(one per CPU). There is no asyncio support. Callbacks run with the GIL held, so only work that
releases it overlaps: network and disk I/O, `time.sleep()`, C code that releases the GIL.
Pure-Python computation doesn't run in parallel, and free-threaded Python doesn't change that
(importing akayu turns the GIL back on). See [Concurrency](features/concurrency.md).

## Is it thread-safe?

**Yes**:

- A source can be emitted from multiple threads: each `emit()` holds the source's lock.
- Streams merged with `union`, `zip` or `combine_latest` can be emitted from different threads:
  the merged node and everything after it are locked per item.
- `compile()` replaces those per-item locks with one lock for the whole connected graph (unless it
  contains `par()` or `prefetch()`), so a compiled graph is entered by one thread at a time.
- A thread waiting for a lock releases the GIL, so other Python threads keep running.
- For parallel I/O, use `par()` or `prefetch()`, which run work on akayu's thread pool.

With limits: two threads whose callbacks emit into each other's sources get an error instead of
waiting for each other forever, and in an uncompiled graph some cross-thread flushes fail instead
of waiting. See
[Callbacks, Re-entrancy and Limits](features/concurrency.md#callbacks-re-entrancy-and-limits).

```python
from concurrent.futures import ThreadPoolExecutor

import akayu

results = []
source = akayu.Stream()
source.map(lambda x: x * x).sink(results.append)
source.compile()

# Safe: multiple threads emitting to one source
with ThreadPoolExecutor(4) as ex:
    for item in range(100):
        ex.submit(source.emit, item)

assert sorted(results) == [x * x for x in range(100)]
```

## How does it compare to streamz/RxPY/aiostream?

| Library       | Performance               | Async                                 | Rust Backend | Learning Curve |
| ------------- | ------------------------- | ------------------------------------- | ------------ | -------------- |
| **akayu**     | High (28-142x vs streamz) | ❌ (threads via `par()`/`prefetch()`) | ✅           | Low            |
| **streamz**   | Medium                    | ✅                                    | ❌           | Low            |
| **RxPY**      | Medium                    | ❌                                    | ❌           | Medium         |
| **aiostream** | Medium                    | ✅                                    | ❌           | Low            |

**akayu** keeps streamz's simple API with a Rust core; see [Benchmarks](benchmarks.md) for measured speedups.

## Why is my pipeline slow?

### Common causes:

1. **`par()` around work that holds the GIL** — `par()` only speeds up branches that wait with the GIL released (I/O, sleeps); pure-Python computation runs one branch at a time, plus the dispatch cost
1. **Not using `compile()`** — Missing operator fusion
1. **Item-by-item processing** — Use `emit_batch()` + `batch_map()` for bulk data
1. **Python function overhead** — Consider numba or vectorized ops in `batch_map()`

### Debugging steps:

<!--pytest.mark.skip-->

```python
# 1. Profile to find the bottleneck
with source.profile() as p:
    source.emit_batch(data)
print(p.report)

# 2. Try compile(), then see which nodes still call Python
source.compile()
print(source.explain())

# 3. Use batch operations
source.emit_batch(large_chunk)  # instead of loop + emit()
```

## What does "partially vibed" mean?

It's a playful nod to the project's origin — akayu was initially developed for internal use, so not every feature has been battle-tested in production environments. The core stream processing is solid; some advanced features may need more real-world testing.

> "Partially vibed" = the author's team used it and it worked, but you might find edge cases. Report them!

## Still have questions?

- Check the [Patterns & Best Practices](patterns.md) guide
- Review the [API Reference](api.md)
- Look at [benchmarks](benchmarks.md) for performance expectations
