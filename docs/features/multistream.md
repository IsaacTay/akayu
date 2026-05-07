# MultiStream — Batch Operations on Streams

`MultiStream` wraps a list or dict of `Stream` instances and applies operations element-wise across the entire batch. It supports broadcasting so a single `Stream` can be paired with every stream in a batch.

## When to Use It

- **Multi-sensor pipelines**: same transform applied to N sensor streams
- **Multi-document processing**: independent streams for different documents/channels
- **Per-server log aggregation**: one stream per server, shared enrichment logic
- **Batched combinators**: pairwise `union`, `combine_latest`, or `zip` across groups of streams

## Creating a MultiStream

### From a list

```python
from akayu import Stream, MultiStream

sources = [Stream() for _ in range(4)]
ms = MultiStream(sources)
```

### From a dict

```python
sensors = {"temp": Stream(), "humid": Stream(), "pressure": Stream()}
ms = MultiStream(sensors)
```

Dict mode preserves keys through transforms and returns dict-shaped results from collectors.

`MultiStream` accepts a `dict`, `list`, or `tuple` of `Stream` instances.

```python
MultiStream([])           # empty list batch — OK
MultiStream({})           # empty dict batch — OK
MultiStream([Stream()])   # single stream — OK
MultiStream(Stream())     # TypeError — must be list/tuple/dict
MultiStream([1, 2])       # TypeError — elements must be Stream
MultiStream({"a": 1})     # TypeError — values must be Stream
```

## Dict Mode vs List Mode

| Feature | List mode | Dict mode |
|---------|-----------|-----------|
| Construction | `MultiStream([s1, s2])` | `MultiStream({"a": s1, "b": s2})` |
| Access by key | `ms[0]` (int) | `ms["a"]` (str) or `ms[0]` (int) |
| `to_list()` returns | `[list_0, list_1]` | `{"a": list_0, "b": list_1}` |
| `to_set()` returns | `[set_0, set_1]` | `{"a": set_0, "b": set_1}` |
| `keys()` / `items()` | raises TypeError | available |
| `to_dict()` | raises TypeError | `{"a": stream_a, "b": stream_b}` |
| Key propagation | through transforms | through transforms |
| After combinators | list mode | dict mode (keys preserved) |

## Transforms

All transforms return a new `MultiStream` and apply to each stream independently:

```python
ms = MultiStream([Stream(), Stream(), Stream()])

doubled = ms.map(lambda x: x * 2)
positives = doubled.filter(lambda x: x > 0)
collected = positives.collect()
results = collected.map(lambda tup: sum(tup)).to_list()
```

Supported transforms: `map`, `filter`, `sink`, `starmap`, `accumulate`, `flatten`, `collect`, `batch_map`, `chunk_by`.

Concurrency/branching: `par`, `parallel`, `seq`, `prefetch` — applied per-stream (each stream gets its own branch point or prefetch buffer).

### Chaining

```python
results = (
    MultiStream(sources)
    .map(lambda x: x.strip())
    .filter(lambda x: len(x) > 0)
    .map(lambda x: int(x) * 2)
    .to_list()
)
```

## Collectors

Collectors return a **list of results** (list mode) or a **dict of results** (dict mode), one per stream:

```python
# List mode
ms.to_list()  # → [list_0, list_1, ..., list_N]
ms.to_set()   # → [set_0, set_1, ..., set_N]

# Dict mode
ms.to_list()  # → {"a": list_a, "b": list_b}
ms.to_set()   # → {"a": set_a, "b": set_b}
```

```python
# List mode
sources = [Stream() for _ in range(3)]
ms = MultiStream(sources)
results = ms.map(lambda x: x + 1).to_list()

for i, s in enumerate(sources):
    s.emit(i)

# results[0] → [1], results[1] → [2], results[2] → [3]
```

```python
# Dict mode
sensors = {"temp": Stream(), "humid": Stream()}
ms = MultiStream(sensors)
results = ms.map(lambda x: x + 1).to_list()

sensors["temp"].emit(22)
sensors["humid"].emit(65)

# results["temp"] → [23], results["humid"] → [66]
```

## Actions

Actions call the corresponding method on every stream:

```python
ms.compile()          # compile all streams, returns self for chaining
ms.flush()            # flush all streams
ms.drain()            # drain all streams
```

## Broadcasting

Broadcasting is the core power of `MultiStream`. Combinators (`union`, `combine_latest`, `zip`) accept mixed `Stream` and `MultiStream` arguments:

- **Same keys (dict mode)**: pairs by key name, keys preserved in result
- **Same-length MultiStreams**: pairwise combination (stream 0 with stream 0, stream 1 with stream 1, etc.)
- **Length-1 MultiStream or plain Stream**: expanded to match the batch size
- **Mismatched lengths (>1)**: raises `ValueError`
- **Mismatched dict keys**: raises `ValueError`
- **Dict + list-backed MultiStream**: raises `TypeError`

```
Dict mode, same keys:      MultiStream({"a":s1,"b":s2}) + MultiStream({"a":s3,"b":s4}) → dict mode preserved
Dict mode, different keys: MultiStream({"a":s1,"b":s2}) + MultiStream({"x":s3,"y":s4}) → ValueError
Dict mode + list-backed:   MultiStream({"a":s1,"b":s2}) + MultiStream([s3, s4])        → TypeError
Dict mode + plain Stream:  MultiStream({"a":s1,"b":s2}) + Stream()                     → dict mode (broadcast)
List mode:                 MultiStream(3) + MultiStream(3) → list mode
Broadcast:                 MultiStream(3) + Stream()       → list mode
```

### union

Merge corresponding streams pairwise:

```python
sensors = MultiStream([Stream(), Stream(), Stream()])
thresholds = Stream()  # broadcast to all 3

alarms = sensors.map(check_value).union(thresholds.map(check_threshold))
```

### combine_latest

Combine latest values pairwise, with optional `emit_on`:

```python
temps = MultiStream([Stream(), Stream()])
humids = MultiStream([Stream(), Stream()])

readings = temps.combine_latest(humids)
# readings[0] = combine_latest(temps[0], humids[0])
# readings[1] = combine_latest(temps[1], humids[1])
```

With `emit_on`:

```python
readings = temps.combine_latest(humids, emit_on=temps)
# Only emit when the corresponding temp stream updates
```

### zip

Pair values strictly one-to-one across streams:

```python
timestamps = MultiStream([Stream(), Stream()])
values = MultiStream([Stream(), Stream()])

pairs = timestamps.zip(values)
# pairs[0] = zip(timestamps[0], values[0])
# pairs[1] = zip(timestamps[1], values[1])
```

### Multi-way combinators

All combinators accept multiple arguments:

```python
a = MultiStream(sources_a).map(transform_a)
b = MultiStream(sources_b).map(transform_b)
c = Stream()  # shared enrichment

merged = a.union(b, c)  # 3-way union, c broadcast to all
```

## Module-Level Functions

`akayu.union`, `akayu.combine_latest`, and `akayu.zip` also accept `MultiStream` arguments:

```python
# All Stream inputs → returns Stream
akayu.union(s1, s2)

# Any MultiStream input → returns MultiStream (with broadcasting)
akayu.union(MultiStream([s1, s2]), s3)  # → MultiStream(2)
```

## Accessing Individual Streams

`MultiStream` supports indexing, slicing, and iteration:

```python
# List mode
ms = MultiStream([Stream(), Stream(), Stream()])

ms[0]       # → Stream
ms[-1]      # → Stream
ms[1:3]     # → MultiStream(2)
ms[::2]     # → MultiStream(2)

for stream in ms:
    stream.sink(handler)
```

```python
# Dict mode
ms = MultiStream({"temp": Stream(), "humid": Stream(), "pressure": Stream()})

ms["temp"]              # → Stream (by key)
ms[0]                   # → Stream (by index, same as "temp")
ms["humid"]             # → Stream
ms[1:]                  # → MultiStream with keys ("humid", "pressure")

for key, stream in ms.items():
    stream.sink(handler)

ms.to_dict()            # → {"temp": Stream(), "humid": Stream(), "pressure": Stream()}
```

## Unsupported Operations

These operations only make sense on a single `Stream` and are blocked on `MultiStream`:

| Operation | Reason |
|-----------|--------|
| `emit` / `emit_batch` | Data injection is per-source, not per-batch |
| `emit_profiled` / `emit_batch_profiled` | Profiled emit is per-source |
| `start_profiling` / `stop_profiling` | Global profiling state |

If you need one of these, iterate over the `MultiStream` and call on each `Stream` individually:

```python
for s in ms:
    s.emit(value)
```

## Profiling

`profile()` returns a profiler context manager for each source stream — a list in list mode, a dict in dict mode:

```python
# List mode
ms = MultiStream([Stream(), Stream()])
profilers = ms.profile()
with profilers[0]:
    ms[0].emit(42)
print(profilers[0].report)

# Dict mode
ms = MultiStream({"temp": Stream(), "humid": Stream()})
profilers = ms.profile()
with profilers["temp"]:
    ms["temp"].emit(22)
print(profilers["temp"].report)
```

## stream_name with MultiStream

When you pass `stream_name` to a transform, `MultiStream` auto-indexes it:

```python
ms.map(transform, stream_name="enrich")
# Creates streams named "enrich_0", "enrich_1", "enrich_2", ...
```

## Real-World Example: Multi-Sensor Alarm Pipeline

```python
from akayu import Stream, MultiStream

sensors = {"temp": Stream(), "humid": Stream(), "pressure": Stream()}
ms = MultiStream(sensors)

alarms = (
    ms.filter(lambda reading: reading.value > THRESHOLD)
    .map(format_alarm)
    .collect()
)

ms.map(lambda r: r.value).sink(log_reading)
ms.flush()

for name, sensor in sensors.items():
    for reading in data[name]:
        sensor.emit(reading)

ms.flush()
# alarms.to_list()["temp"] contains all alarm tuples for temp sensor
```

## API Summary

| Method | Returns | Description |
|--------|---------|-------------|
| `map(fn)` | `MultiStream` | Apply `fn` to each stream |
| `filter(fn)` | `MultiStream` | Filter each stream |
| `sink(fn)` | `MultiStream` | Attach sink to each stream |
| `starmap(fn)` | `MultiStream` | Starmap each stream |
| `accumulate(fn, start)` | `MultiStream` | Running aggregation per stream |
| `flatten()` | `MultiStream` | Flatten lists per stream |
| `collect()` | `MultiStream` | Buffer items per stream |
| `batch_map(fn)` | `MultiStream` | Batch transform per stream |
| `chunk_by(fn)` | `MultiStream` | Chunk each stream |
| `par()` | `MultiStream` | Parallel branch per stream |
| `parallel()` | `MultiStream` | Alias for `par()` |
| `seq()` | `MultiStream` | Sequential branch per stream |
| `prefetch(n)` | `MultiStream` | Prefetch buffer per stream |
| `to_list()` | `list[list]` or `dict[str, list]` | Collect each stream to a list |
| `to_set()` | `list[set]` or `dict[str, set]` | Collect each stream to a set |
| `compile()` | `MultiStream` | Compile all streams, returns self |
| `flush()` | `None` | Flush all streams |
| `drain()` | `None` | Drain all streams |
| `profile()` | `list` or `dict[str, Profiler]` | Profiler context manager per source stream |
| `union(*others)` | `MultiStream` | Pairwise union with broadcasting |
| `combine_latest(*others, emit_on=None)` | `MultiStream` | Pairwise combine_latest with broadcasting |
| `zip(*others)` | `MultiStream` | Pairwise zip with broadcasting |
| `keys()` | `Iterator[str]` | Dict mode: iterate over keys |
| `values()` | `Iterator[Stream]` | Dict mode: iterate over streams |
| `items()` | `Iterator[(str, Stream)]` | Dict mode: iterate over key-stream pairs |
| `to_dict()` | `dict[str, Stream]` | Dict mode: return streams as dict |
| `keyed` | `bool` | `True` if constructed with a dict |
