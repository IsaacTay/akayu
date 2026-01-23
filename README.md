# Akayu

rust + streamz = akayu (red hot springs)

A rust powered python library for stream data processing. Takes heavy inspiration from the [streamz](https://github.com/python-streamz/streamz) library.

> [!warning]
> This project was partially vibed for internal use, use at own risk. Async functionality is currently undertested

## Features

- High Performance: Rust-powered core with automatic optimizations
- Compile-time Optimizations: Chain fusion, async check elimination, and more
- Concurrent Processing: `par()` for parallel branches, `prefetch()` for ordered concurrency
- Batch Operations: Efficient vectorized processing with `batch_map()` and `emit_batch()`

## Quick Example

```python
import akayu

# Create a source stream
source = akayu.Stream()

# Build a processing pipeline
results = []
source.map(lambda x: x * 2).filter(lambda x: x > 5).sink(results.append)

# Compile for maximum performance
source.compile()

# Process data
for i in range(10):
    source.emit(i)

print(results)  # [6, 8, 10, 12, 14, 16, 18]
```

## Performance

akayu consistently has lower overhead when benchmarked against streamz:

| Topology Type | Scenario                              | Typical Speedup  |
| ------------- | ------------------------------------- | ---------------- |
| Deep Chains   | Long chains of operations (10+ nodes) | **~3.2x**        |
| Branching     | Fan-out, diamond patterns             | **~2.5x - 3.7x** |
| Merging       | Union, Zip, CombineLatest             | **~2.5x - 3.6x** |
| Complex DAG   | Split, flatten, union mixed           | **~3.9x**        |

See the [Benchmarks](https://isaactay.github.io/akayu/benchmarks/) page for detailed performance comparisons.

## Documentation

- **[Full Documentation](https://isaactay.github.io/akayu/)**
- **[Getting Started](https://isaactay.github.io/akayu/getting-started/)**
- **[API Reference](https://isaactay.github.io/akayu/api/)**
