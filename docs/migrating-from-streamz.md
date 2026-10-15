# Migrating from streamz

akayu is a Rust-powered alternative to [streamz](https://github.com/python-streamz/streamz) with a similar API and much better performance. It isn't a drop-in replacement: this guide covers what changes when you port a pipeline.

## Quick Comparison

| streamz                      | akayu                                     | Notes                                              |
| ---------------------------- | ----------------------------------------- | -------------------------------------------------- |
| `source.emit(x)`             | `source.emit(x)`                          | Source nodes only (see below)                      |
| `source.map(f)`              | `source.map(f)`                           | Same API                                           |
| `source.filter(f)`           | `source.filter(f)`                        | `filter(None)` isn't supported: use `filter(bool)` |
| `source.accumulate(f)`       | `source.accumulate(f, start)`             | `start` is required                                |
| `source.sink(f)`             | `source.sink(f)`                          | Same API                                           |
| `source.sink_to_list()`      | `source.to_list()`                        | Renamed                                            |
| `Stream.from_textfile(path)` | `akayu.from_text_file(path)` + `.start()` | Reads the file once; doesn't follow it             |
| `source.sink_to_textfile(f)` | `source.sink(akayu.to_text_file, path=p)` | Appends `str(x)` and a newline per item            |
| `Stream(stream_name="s")`    | `akayu.Stream(name="s")`                  | Operations still take `stream_name=`               |
| `Stream()`                   | `akayu.Stream()`                          | Import via `import akayu`                          |
| N/A                          | `source.compile()`                        | **New** — enables optimizations                    |
| N/A                          | `source.emit_batch([...])`                | **New** — bulk emit                                |
| N/A                          | `source.batch_map(f)`                     | **New** — vectorized transform                     |
| N/A                          | `source.prefetch(n)`                      | **New** — ordered concurrency                      |
| N/A                          | `source.par()`                            | **New** — parallel branches                        |

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

- Fuses each chain of steps into one node, and runs simple lambdas natively
- Locks the graph (prevents accidental mutation)

```python
import akayu

source = akayu.Stream()
source.map(lambda x: x + 1).map(lambda x: x * 2).map(str).sink(print)

# Optional but recommended for production
source.compile()
source.emit_batch([1, 2, 3])
```

### 3. emit() to Non-Source Nodes

**streamz** lets you emit into any node: `mapped.emit(x)` sends `x` to `mapped`'s downstreams.

**akayu** only allows emit to **source nodes** (created with `Stream()`). This is an intentional design choice for clarity. To inject data mid-graph, create another source and `union()` it in.

<!--pytest.mark.skip-->

```python
import akayu

# streamz: x goes to mapped's downstreams
mapped = source.map(f)
mapped.emit(x)

# akayu: explicit error
mapped.emit(x)  # ❌ akayu.NotASourceError: Emit called on non-source node
```

### 4. Errors from Callbacks

**streamz** raises a callback's exception as it is: a failing `map(lambda x: 1 / x)` raises `ZeroDivisionError`.

**akayu** raises `akayu.CallbackError` naming the node (`.node`) and the line that created it (`.location`), with the original exception as its `__cause__`. `CallbackError` is a `RuntimeError`, like every error akayu raises about the graph (see [Errors](api.md#errors)). `KeyboardInterrupt`, `SystemExit` and other exceptions that aren't `Exception` subclasses pass through unchanged.

```python
import akayu

source = akayu.Stream()
source.map(lambda x: 1 / x).sink(print)

try:
    source.emit(0)
except akayu.CallbackError as e:
    assert isinstance(e.__cause__, ZeroDivisionError)
```

### 5. Performance Characteristics

| Scenario              | streamz  | akayu (compiled)            |
| --------------------- | -------- | --------------------------- |
| Deep chains (10 maps) | Baseline | ~142x faster                |
| Branching             | Baseline | ~44-54x faster              |
| Merging               | Baseline | ~32-61x faster              |
| Batch processing      | slower   | optimized with `emit_batch` |

See [Benchmarks](benchmarks.md) for the measurements.

## What's the Same

- Core API philosophy (fluent chainable streams)
- `map`, `filter`, `sink`, `starmap` and `flatten` semantics, and `collect()` emitting its buffer
  as a tuple on `flush()`
- `union`, `zip`, `combine_latest` for combining streams
- Source → transforms → sink mental model

## Different or Unsupported

- `stream_name` and `returns_state` are keyword-only.
- No asyncio integration: there is no `asynchronous=`, `loop=` or `ensure_io_loop=`, and `async def`
  callbacks raise `TypeError`. Use `prefetch()` or `par()` for concurrent I/O.
- No `metadata=` argument to `emit()` (streamz's reference-counted metadata).
- Graphs only grow: there is no `connect()`, `disconnect()`, `destroy()` or `remove()`. Build a new
  graph instead.
- Not available: `sliding_window`, `partition`, `timed_window`, `timed_window_unique`, `buffer`,
  `delay`, `rate_limit`, `unique`, `pluck`, `slice`, `latest`, `zip_latest`, `frequencies`,
  `scatter`/`gather` (Dask), `to_batch`/`to_dataframe`, `visualize`, and the sources other than
  text files (`from_iterable`, `from_kafka`, `filenames`, ...). Some are one-liners in akayu:
  `pluck(k)` is `map(operator.itemgetter(k))`, and `from_iterable(it)` is `emit_batch(list(it))`.

## Porting Checklist

- [ ] Change `from streamz import Stream` to `import akayu`
- [ ] Replace `Stream()` with `akayu.Stream()` (and `stream_name=` on sources with `name=`)
- [ ] Give every `accumulate()` a `start` value
- [ ] Replace `filter(None)` with `filter(bool)`, and `sink_to_list()` with `to_list()`
- [ ] Where you caught a callback's exception, catch `akayu.CallbackError` and check `__cause__`
- [ ] Replace any `emit()` calls to non-source nodes with `union()` injection pattern
- [ ] Add `source.compile()` after pipeline construction (optional but recommended)
- [ ] Consider using `emit_batch()` + `batch_map()` for bulk operations

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
