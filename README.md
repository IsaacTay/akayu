# Akayu

rust + streamz = akayu (red hot springs)

A rust powered python library for stream data processing. Takes heavy inspiration from the [streamz](https://github.com/python-streamz/streamz) library.

> [!warning]
> This project was partially vibed for internal use, use at own risk.

## Features

- High Performance: Rust-powered core with automatic optimizations
- Compile-time Optimizations: Chain fusion, builtin fast paths for simple lambdas, and more
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

Compiled pipelines against streamz, 500,000 items:

| Topology Type | Scenario                    | Speedup over streamz |
| ------------- | --------------------------- | -------------------- |
| Deep Chains   | 10 chained maps             | **~58x**             |
| Branching     | Fan-out, diamond            | **~34x - 45x**       |
| Merging       | Union, zip, combine_latest  | **~32x - 63x**       |
| Complex DAG   | Split, flatten, union mixed | **~31x**             |

See the [Benchmarks](https://isaactay.github.io/akayu/benchmarks/) page for detailed performance comparisons.

## Documentation

- **[Full Documentation](https://isaactay.github.io/akayu/)**
- **[Getting Started](https://isaactay.github.io/akayu/getting-started/)**
- **[API Reference](https://isaactay.github.io/akayu/api/)**
