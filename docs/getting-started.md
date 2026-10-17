# Getting Started

## System Requirements

- CPython 3.13 or 3.14, on Linux, macOS or Windows

Each [release](https://github.com/IsaacTay/akayu/releases) has wheels for Linux x86_64 and
aarch64 (glibc 2.17+), macOS arm64 and x86_64, and Windows x64, tested on each of those
platforms. akayu isn't on PyPI: uv, pip and pixi install the wheels from akayu's own package
index, `https://isaactay.github.io/akayu/simple/`.

On other platforms (Alpine's musl, Windows on Arm, ...), or to install a commit that isn't
released yet, installing builds akayu from source, which takes a minute or two and needs a Rust
toolchain from [rustup](https://rustup.rs/) (on Windows, with the Visual Studio C++ build tools it
asks for). akayu also builds for free-threaded Python (3.13t, 3.14t), but importing it turns the
GIL back on.

## Installation

/// tab | uv

<!--pytest.mark.skip-->

```bash
uv add akayu --index akayu=https://isaactay.github.io/akayu/simple/
```

This writes the index into `pyproject.toml` and takes akayu only from it. Add `explicit = true`
so uv looks up nothing else there:

<!--pytest.mark.skip-->

```toml
[project]
requires-python = ">=3.13"
dependencies = ["akayu>=2.0.0"]

[tool.uv.sources]
akayu = { index = "akayu" }

[[tool.uv.index]]
name = "akayu"
url = "https://isaactay.github.io/akayu/simple/"
explicit = true
```

`uv.lock` then lists akayu's wheels for every platform, each with its sha256, so `uv sync`
installs the right one on Linux, macOS or Windows and refuses a file that changed. A project that
also supports Python 3.12 or older depends on `"akayu>=2.0.0; python_version >= '3.13'"`
instead.

///

/// tab | pip

<!--pytest.mark.skip-->

```bash
pip install akayu --index-url https://isaactay.github.io/akayu/simple/
```

akayu has no dependencies, so its index can replace PyPI for this command.

///

/// tab | pixi

Add the index to `pixi.toml`:

<!--pytest.mark.skip-->

```toml
[pypi-options]
extra-index-urls = ["https://isaactay.github.io/akayu/simple/"]
```

<!--pytest.mark.skip-->

```bash
pixi add --pypi akayu
```

///

/// tab | from source

<!--pytest.mark.skip-->

```bash
pip install "akayu @ git+https://github.com/IsaacTay/akayu"
```

Needs a Rust toolchain (see above). Add `@v2.0.0` (a release's tag) or a commit for a specific
version.

///

## Verify Installation

<!--pytest.mark.skip-->

```bash
python -c "import akayu; print('akayu installed!')"
```

## Basic Usage

### Creating a Stream

Every pipeline starts with a source `Stream`:

```python
import akayu

source = akayu.Stream()
```

### Building Pipelines

Chain operations to build your processing pipeline:

```python
import akayu

source = akayu.Stream()

# Transform values
doubled = source.map(lambda x: x * 2)

# Filter values
filtered = doubled.filter(lambda x: x > 10)

# Consume values
results = []
filtered.sink(results.append)
```

### Emitting Data

!!! warning "Source Nodes Only"
    Data can only be emitted into **source** nodes (nodes created with `Stream()`). Trying to emit data into derived nodes (like those returned by `.map()` or `.filter()`) raises `akayu.NotASourceError`.

```mermaid
flowchart LR
    User((User))
    S[Source]:::source
    M[map]:::error
    
    S --> M
    
    User -->|emit| S
    User -.-x|emit| M
    
    linkStyle 2 stroke:#dc2626,stroke-width:2px,color:red
```

```python
import akayu

source = akayu.Stream()
source.sink(print)

# Single item
source.emit(5)

# Multiple items (more efficient)
source.emit_batch([1, 2, 3, 4, 5])
```

### To compile or not to compile?

The `compile()` step enables graph optimizations but is **optional**.

**What compile() does:**

- **Chain fusion** — A run of steps like `map.filter.map` becomes one node, and simple lambdas run natively
- **Graph locking** — Prevents accidental modifications after optimization

It freezes and optimizes the graph below the stream you call it on: in a graph with several
sources, compile each of them.

**When to use it:**

- Production pipelines processing high volumes
- Hot loops where overhead matters
- When you want protection against accidental graph mutation

**When to skip it:**

- Debugging — uncompiled graphs are easier to inspect
- Dynamic pipelines — you need to add nodes at runtime (there is no way to remove one)
- Rapid prototyping — faster iteration without recompiling

```python
import akayu

source = akayu.Stream()
source.map(lambda x: x * 2).sink(print)

# Optional but recommended for production
source.compile()
source.emit_batch([1, 2, 3])
```

## Common Mistakes

### Emitting to a non-source node

<!--pytest.mark.xfail(strict=True)-->

```python
import akayu

source = akayu.Stream()
mapped = source.map(lambda x: x * 2)

mapped.emit(5)  # ❌ akayu.NotASourceError: Emit called on non-source node
```

**Error:**

```
akayu.NotASourceError: Emit called on non-source node 'map(<lambda>)'. Data can only be emitted into source nodes.
```

**Fix:** Always emit to the original source node:

```python
import akayu

source = akayu.Stream()
mapped = source.map(lambda x: x * 2)

source.emit(5)  # ✅ Correct
```

## Naming Streams for Debugging

Every operation accepts an optional `stream_name` parameter. Names appear in error messages, making it easy to trace problems in complex pipelines.

```python
import akayu

source = akayu.Stream(name="input")
doubled = source.map(lambda x: x * 2, stream_name="double")
filtered = doubled.filter(lambda x: x > 10, stream_name="filter_gt_10")
```

If an error occurs, the name is included:

```
akayu.NotASourceError: Emit called on non-source node 'double'. ...
```

All transform methods support `stream_name`: `map`, `filter`, `sink`, `flatten`,
`collect`, `prefetch`, `accumulate`, `chunk_by`, `chunks_between`, `starmap`, `batch_map`.

If not provided, a default name is generated from the function name (e.g. `"map(my_func)"`).

## Available Operations

### Transformations

- [`#!python map(func)`](api.md#akayu.Stream.map) — Transform each element
- [`#!python filter(predicate)`](api.md#akayu.Stream.filter) — Keep matching elements
- [`#!python flatten()`](api.md#akayu.Stream.flatten) — Expand iterables into individual items
- [`#!python accumulate(func, start)`](api.md#akayu.Stream.accumulate) — Running aggregation
- [`#!python batch_map(func)`](api.md#akayu.Stream.batch_map) — Vectorized batch transformation
- [`#!python starmap(func)`](api.md#akayu.Stream.starmap) — Map with argument unpacking
- [`#!python chunk_by(predicate)`](api.md#akayu.Stream.chunk_by) — Buffer until delimiter, emit as tuple
- [`#!python chunks_between(open, close)`](api.md#akayu.Stream.chunks_between) — Emit the items between two delimiters as a tuple
- [`#!python prefetch(n)`](api.md#akayu.Stream.prefetch) — Run the next `map`, `filter` or `batch_map` on up to `n` items at once (call `flush()` at the end)
- [`#!python sink(func)`](api.md#akayu.Stream.sink) — Terminal side-effect operation

### Collecting

- [`#!python collect()`](api.md#akayu.Stream.collect) — Buffer elements until `flush()`
- [`#!python to_list()`](api.md#akayu.Stream.to_list) — Collect into a Python list
- [`#!python to_set()`](api.md#akayu.Stream.to_set) — Collect into a Python set
- [`#!python flush()`](api.md#akayu.Stream.flush) — Emit buffered items, up to the first buffering node on each path
- [`#!python drain()`](api.md#akayu.Stream.drain) — Flush every buffering node downstream

### Combining Streams

- [`#!python union(*streams)`](api.md#akayu.Stream.union) — Merge multiple streams
- [`#!python zip(*streams)`](api.md#akayu.Stream.zip) — Pair items from multiple streams
- [`#!python combine_latest(*streams)`](api.md#akayu.Stream.combine_latest) — Latest value from each stream

## Example: Processing Pipeline

```python
import akayu

def process_record(record):
    return {"id": record["id"], "value": record["value"] * 2}

def is_valid(record):
    return record["value"] > 0

# Build pipeline
source = akayu.Stream()
results = []

source.filter(is_valid).map(process_record).sink(results.append)

# Compile for performance
source.compile()

# Process data
records = [
    {"id": 1, "value": 10},
    {"id": 2, "value": -5},
    {"id": 3, "value": 20},
]
source.emit_batch(records)

print(results)
# [{'id': 1, 'value': 20}, {'id': 3, 'value': 40}]
```

## Next Steps

- [Starter Patterns](patterns.md#starter-patterns) — Common patterns with examples
- [Patterns & Best Practices](patterns.md) — Concurrency, batching, anti-patterns
- [API Reference](api.md) — Full method documentation
