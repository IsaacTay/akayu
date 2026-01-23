# Getting Started

## Installation

/// tab | pip

```bash
pip install akayu
```

///

/// tab | uv

```bash
uv add akayu
```

///

/// tab | pixi

```bash
pixi add akayu
```

///

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

Push data through the pipeline using `emit()` or `emit_batch()`:

```python
# Single item
source.emit(5)

# Multiple items (more efficient)
source.emit_batch([1, 2, 3, 4, 5])
```

### Compiling for Performance

Call `.compile()` before processing to enable optimizations like operator fusion:

```python
source = akayu.Stream()
source.map(f1).map(f2).map(f3).sink(print)

# Freeze topology and apply optimizations
source.compile()

# Now emit data through the optimized pipeline
source.emit_batch(data)
```

!!! warning
    **Graph Locking**: Once `.compile()` is called, the stream graph is **locked** and becomes immutable. Attempting to add any new operations (like `.map()`, `.filter()`, etc.) to any part of the compiled graph will raise a `RuntimeError`. Build your entire pipeline first, then compile it once. See [Graph Locking & Immutability](features/compile-optimizations.md#graph-locking-immutability) for details.

## Available Operations

### Transformations

- [`#!python map(func)`](api.md#akayu.Stream.map): Transform each element using the provided function.
- [`#!python filter(predicate)`](api.md#akayu.Stream.filter): Keep only elements that satisfy the predicate.
- [`#!python flatten()`](api.md#akayu.Stream.flatten): Expand iterable elements into individual items downstream.
- [`#!python accumulate(func, start)`](api.md#akayu.Stream.accumulate): Accumulate values (like reduce) and emit every intermediate result.
- [`#!python sink(func)`](api.md#akayu.Stream.sink): Execute a side-effect function for each element (terminal operation).

### Combining Streams

- [`#!python union(*streams)`](api.md#akayu.Stream.union): Merge multiple streams into one, emitting items as they arrive.
- [`#!python zip(*streams)`](api.md#akayu.Stream.zip): Wait for one item from each stream and emit them as a tuple.
- [`#!python combine_latest(*streams)`](api.md#akayu.Stream.combine_latest): Emit the latest value from each stream whenever any of them updates.

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
