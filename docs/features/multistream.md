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
import akayu

from akayu import Stream, MultiStream

sources = [Stream() for _ in range(4)]
ms = MultiStream(sources)
```

### From a dict

```python
from akayu import MultiStream, Stream

sensors = {"temp": Stream(), "humid": Stream(), "pressure": Stream()}
ms = MultiStream(sensors)
```

Dict mode preserves keys through transforms and returns dict-shaped results from collectors.

`MultiStream` accepts a `dict`, `list`, or `tuple` of `Stream` instances, or of `MultiStream`s
(see [Nested MultiStreams](#nested-multistreams)).

<!--pytest.mark.xfail(strict=True)-->

```python
from akayu import MultiStream, Stream

MultiStream([])           # empty list batch — OK
MultiStream({})           # empty dict batch — OK
MultiStream([Stream()])   # single stream — OK
MultiStream(Stream())     # TypeError — must be list/tuple/dict
MultiStream([1, 2])       # TypeError — elements must be Stream or MultiStream
MultiStream({"a": 1})     # TypeError — values must be Stream or MultiStream
```

## Dict Mode vs List Mode

| Feature              | List mode               | Dict mode                         |
| -------------------- | ----------------------- | --------------------------------- |
| Construction         | `MultiStream([s1, s2])` | `MultiStream({"a": s1, "b": s2})` |
| Access by key        | `ms[0]` (int)           | `ms["a"]` (str) or `ms[0]` (int)  |
| `to_list()` returns  | `[list_0, list_1]`      | `{"a": list_0, "b": list_1}`      |
| `to_set()` returns   | `[set_0, set_1]`        | `{"a": set_0, "b": set_1}`        |
| `keys()` / `items()` | raises TypeError        | available                         |
| `values()`           | the streams             | the streams                       |
| `to_dict()`          | raises TypeError        | `{"a": stream_a, "b": stream_b}`  |
| Key propagation      | through transforms      | through transforms                |
| After combinators    | list mode               | dict mode (keys preserved)        |

An `int` is always a position, also in dict mode: with `MultiStream({1: s1, 0: s2})`, `ms[1]`
is `s2`. Iterating a `MultiStream` yields its streams, not its keys, so test for a key with
`"temp" in ms.keys()`.

## Transforms

All transforms return a new `MultiStream` and apply to each stream independently:

```python
from akayu import MultiStream, Stream

ms = MultiStream([Stream(), Stream(), Stream()])

doubled = ms.map(lambda x: x * 2)
positives = doubled.filter(lambda x: x > 0)
collected = positives.collect()
results = collected.map(lambda tup: sum(tup)).to_list()
```

Supported transforms: `map`, `filter`, `sink`, `starmap`, `accumulate`, `flatten`, `collect`, `batch_map`, `chunk_by`, `chunks_between`.

Concurrency/branching: `par`, `parallel`, `seq`, `prefetch` — applied per-stream (each stream gets its own branch point or prefetch buffer).

### Chaining

```python
from akayu import MultiStream, Stream

sources = [Stream(), Stream()]
results = (
    MultiStream(sources)
    .map(lambda x: x.strip())
    .filter(lambda x: len(x) > 0)
    .map(lambda x: int(x) * 2)
    .to_list()
)

sources[0].emit(" 21 ")
sources[1].emit("   ")
assert results == [[42], []]
```

## Collectors

Collectors return a **list of results** (list mode) or a **dict of results** (dict mode), one per stream:

<!--pytest.mark.skip-->

```python
# List mode
ms.to_list()  # → [list_0, list_1, ..., list_N]
ms.to_set()   # → [set_0, set_1, ..., set_N]

# Dict mode
ms.to_list()  # → {"a": list_a, "b": list_b}
ms.to_set()   # → {"a": set_a, "b": set_b}
```

```python
from akayu import MultiStream, Stream

# List mode
sources = [Stream() for _ in range(3)]
ms = MultiStream(sources)
results = ms.map(lambda x: x + 1).to_list()

for i, s in enumerate(sources):
    s.emit(i)

assert results == [[1], [2], [3]]
```

```python
from akayu import MultiStream, Stream

# Dict mode
sensors = {"temp": Stream(), "humid": Stream()}
ms = MultiStream(sensors)
results = ms.map(lambda x: x + 1).to_list()

sensors["temp"].emit(22)
sensors["humid"].emit(65)

assert results == {"temp": [23], "humid": [66]}
```

## Actions

Actions call the corresponding method on every stream:

<!-- pytest-codeblocks:cont -->

```python
ms.compile()          # compile all streams, returns self for chaining
ms.flush()            # flush all streams
ms.drain()            # drain all streams
```

## Broadcasting

Broadcasting is the core power of `MultiStream`. Combinators (`union`, `combine_latest`, `zip`) accept mixed `Stream` and `MultiStream` arguments:

- **Same keys in the same order (dict mode)**: pairs by key, keys preserved in result
- **Same-length MultiStreams**: pairwise combination (stream 0 with stream 0, stream 1 with stream 1, etc.)
- **Length-1 MultiStream or plain Stream**: expanded to match the batch size
- **Mismatched lengths (>1)**: raises `ValueError`
- **Mismatched dict keys**, or the same keys in another order: raises `ValueError`
- **Dict + list-backed MultiStream**: raises `TypeError`

```
Dict mode, same keys:      MultiStream({"a":s1,"b":s2}) + MultiStream({"a":s3,"b":s4}) → dict mode preserved
Dict mode, different keys: MultiStream({"a":s1,"b":s2}) + MultiStream({"x":s3,"y":s4}) → ValueError
Dict mode, key order:      MultiStream({"a":s1,"b":s2}) + MultiStream({"b":s3,"a":s4}) → ValueError
Dict mode + list-backed:   MultiStream({"a":s1,"b":s2}) + MultiStream([s3, s4])        → TypeError
Dict mode + plain Stream:  MultiStream({"a":s1,"b":s2}) + Stream()                     → dict mode (broadcast)
List mode:                 MultiStream(3) + MultiStream(3) → list mode
Broadcast:                 MultiStream(3) + Stream()       → list mode
```

### union

Merge corresponding streams pairwise:

```python
from akayu import MultiStream, Stream

sensors = MultiStream([Stream(), Stream(), Stream()])
thresholds = Stream()  # broadcast to all 3

alarms = sensors.map(lambda v: v > 100).union(thresholds.map(lambda t: t > 50))
```

### combine_latest

Combine latest values pairwise, with optional `emit_on`:

```python
from akayu import MultiStream, Stream

temps = MultiStream([Stream(), Stream()])
humids = MultiStream([Stream(), Stream()])

readings = temps.combine_latest(humids)
# readings[0] = combine_latest(temps[0], humids[0])
# readings[1] = combine_latest(temps[1], humids[1])
```

With `emit_on`:

<!-- pytest-codeblocks:cont -->

```python
readings = temps.combine_latest(humids, emit_on=temps)
# Only emit when the corresponding temp stream updates
```

### zip

Pair values strictly one-to-one across streams:

```python
from akayu import MultiStream, Stream

timestamps = MultiStream([Stream(), Stream()])
values = MultiStream([Stream(), Stream()])

pairs = timestamps.zip(values)
# pairs[0] = zip(timestamps[0], values[0])
# pairs[1] = zip(timestamps[1], values[1])
```

### Multi-way combinators

All combinators accept multiple arguments:

```python
from akayu import MultiStream, Stream

a = MultiStream([Stream(), Stream()]).map(lambda x: x + 1)
b = MultiStream([Stream(), Stream()]).map(lambda x: x * 10)
c = Stream()  # shared enrichment

merged = a.union(b, c)  # 3-way union, c broadcast to all
assert isinstance(merged, MultiStream) and len(merged) == 2
```

## Module-Level Functions

`akayu.union`, `akayu.combine_latest`, and `akayu.zip` also accept `MultiStream` arguments:

```python
import akayu
from akayu import MultiStream, Stream

s1, s2, s3 = Stream(), Stream(), Stream()

# All Stream inputs → returns Stream
assert isinstance(akayu.union(s1, s2), Stream)

# Any MultiStream input → returns MultiStream (with broadcasting)
assert len(akayu.union(MultiStream([s1, s2]), s3)) == 2
```

## Accessing Individual Streams

`MultiStream` supports indexing, slicing, and iteration:

```python
from akayu import MultiStream, Stream

# List mode
ms = MultiStream([Stream(), Stream(), Stream()])

ms[0]       # → Stream
ms[-1]      # → Stream
ms[1:3]     # → MultiStream(2)
ms[::2]     # → MultiStream(2)

for stream in ms:
    stream.sink(print)
```

```python
from akayu import MultiStream, Stream

# Dict mode
ms = MultiStream({"temp": Stream(), "humid": Stream(), "pressure": Stream()})

ms["temp"]              # → Stream (by key)
ms[0]                   # → Stream (by index, same as "temp")
ms["humid"]             # → Stream
ms[1:]                  # → MultiStream with keys ("humid", "pressure")

for key, stream in ms.items():
    stream.sink(print)

ms.to_dict()            # → {"temp": Stream(), "humid": Stream(), "pressure": Stream()}
```

## Unsupported Operations

These operations only make sense on a single `Stream` and are blocked on `MultiStream`:

| Operation                               | Reason                                      |
| --------------------------------------- | ------------------------------------------- |
| `emit` / `emit_batch`                   | Data injection is per-source, not per-batch |
| `emit_profiled` / `emit_batch_profiled` | Profiled emit is per-source                 |
| `start_profiling` / `stop_profiling`    | Global profiling state                      |

If you need one of these, iterate over the `MultiStream` and call on each `Stream` individually:

<!-- pytest-codeblocks:cont -->

```python
for s in ms:
    s.emit(0)
```

## Profiling

`profile()` returns a profiler context manager for each source stream — a list in list mode, a dict in dict mode:

```python
from akayu import MultiStream, Stream

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

When you pass `stream_name` to `map`, `filter`, `sink`, `starmap`, `batch_map` or `prefetch`,
`MultiStream` appends each stream's position to it (in dict mode too: the position, not the key):

<!-- pytest-codeblocks:cont -->

```python
ms.map(lambda x: x, stream_name="enrich")
# Creates streams named "enrich_0", "enrich_1", "enrich_2", ...
```

`accumulate`, `flatten`, `collect`, `chunk_by` and `chunks_between` give every stream the same
name. `par()`, `parallel()` and `seq()` take no `stream_name`.

## Replacing and Adding Streams

Assigning to a key replaces the stream under it; assigning to a new key (dict mode) appends it.
An `int` key replaces by position:

```python
from akayu import MultiStream, Stream

ms = MultiStream({"temp": Stream()})
ms["humid"] = Stream()   # new key: appended
ms["temp"] = Stream()    # replaces the stream under "temp"
ms[0] = Stream()         # replaces by position ("temp" again)

assert list(ms.keys()) == ["temp", "humid"]
```

A list-backed `MultiStream` only takes `int` positions (a `str` key raises `TypeError`, a position
past the end `IndexError`), and the value must be a `Stream` or `MultiStream` (`TypeError`
otherwise). Assignment only changes this `MultiStream`: transforms built from it earlier keep the
streams they were built on.

## Nested MultiStreams

A `MultiStream` can hold `MultiStream`s. Transforms, collectors and actions (`compile()`,
`flush()`, `drain()`) apply to every stream inside, and collectors return nested results:

```python
from akayu import MultiStream, Stream

g1 = MultiStream([Stream(), Stream()])
g2 = MultiStream([Stream()])
groups = MultiStream({"g1": g1, "g2": g2})

results = groups.map(lambda v: v + 1).to_list()
groups.compile()

g1[0].emit(1)
g1[1].emit(2)
g2[0].emit(3)

assert results == {"g1": [[2], [3]], "g2": [[4]]}
```

Combinators (`union`, `combine_latest`, `zip`) don't support nested `MultiStream`s yet and raise
`TypeError`: call them on each inner `MultiStream`.

## Real-World Example: Multi-Sensor Alarm Pipeline

```python
from collections import namedtuple

from akayu import MultiStream, Stream

Reading = namedtuple("Reading", "value")
THRESHOLD = 50

sensors = {"temp": Stream(), "humid": Stream(), "pressure": Stream()}
ms = MultiStream(sensors)

# Attach collectors before data flows
alarms = ms.filter(lambda r: r.value > THRESHOLD).map(lambda r: f"ALARM {r.value}").collect().to_list()
values = ms.map(lambda r: r.value).to_list()

data = {"temp": [20, 55, 70], "humid": [40, 45], "pressure": [90]}
for name, sensor in sensors.items():
    for value in data[name]:
        sensor.emit(Reading(value))

ms.flush()  # each sensor's collect() emits its alarms as one tuple

assert alarms == {"temp": [("ALARM 55", "ALARM 70")], "humid": [()], "pressure": [("ALARM 90",)]}
assert values["temp"] == [20, 55, 70]
```

## API Summary

| Method                                  | Returns                           | Description                                |
| --------------------------------------- | --------------------------------- | ------------------------------------------ |
| `map(fn)`                               | `MultiStream`                     | Apply `fn` to each stream                  |
| `filter(predicate)`                     | `MultiStream`                     | Filter each stream                         |
| `sink(fn)`                              | `MultiStream`                     | Attach sink to each stream                 |
| `starmap(fn)`                           | `MultiStream`                     | Starmap each stream                        |
| `accumulate(fn, start)`                 | `MultiStream`                     | Running aggregation per stream             |
| `flatten()`                             | `MultiStream`                     | Flatten lists per stream                   |
| `collect()`                             | `MultiStream`                     | Buffer items per stream                    |
| `batch_map(fn)`                         | `MultiStream`                     | Batch transform per stream                 |
| `chunk_by(fn)`                          | `MultiStream`                     | Chunk each stream                          |
| `chunks_between(open, close)`           | `MultiStream`                     | Extract delimited chunks per stream        |
| `par()`                                 | `MultiStream`                     | Parallel branch per stream                 |
| `parallel()`                            | `MultiStream`                     | Alias for `par()`                          |
| `seq()`                                 | `MultiStream`                     | Sequential branch per stream               |
| `prefetch(n)`                           | `MultiStream`                     | Prefetch buffer per stream                 |
| `to_list()`                             | `list[list]` or `dict[str, list]` | Collect each stream to a list              |
| `to_set()`                              | `list[set]` or `dict[str, set]`   | Collect each stream to a set               |
| `compile()`                             | `MultiStream`                     | Compile all streams, returns self          |
| `flush()`                               | `None`                            | Flush all streams                          |
| `drain()`                               | `None`                            | Drain all streams                          |
| `profile()`                             | `list` or `dict[str, Profiler]`   | Profiler context manager per source stream |
| `union(*others)`                        | `MultiStream`                     | Pairwise union with broadcasting           |
| `combine_latest(*others, emit_on=None)` | `MultiStream`                     | Pairwise combine_latest with broadcasting  |
| `zip(*others)`                          | `MultiStream`                     | Pairwise zip with broadcasting             |
| `keys()`                                | `Iterator[str]`                   | Dict mode: iterate over keys               |
| `values()`                              | `Iterator[Stream]`                | Iterate over streams (both modes)          |
| `items()`                               | `Iterator[(str, Stream)]`         | Dict mode: iterate over key-stream pairs   |
| `to_dict()`                             | `dict[str, Stream]`               | Dict mode: return streams as dict          |
| `keyed`                                 | `bool`                            | `True` if constructed with a dict          |
