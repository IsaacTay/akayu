# Akayu

A Rust-powered Python library for high-performance stream data processing. Built as a faster alternative to [streamz](https://github.com/python-streamz/streamz), with graph optimizations, concurrent processing, and batch operations.

## Features

- **Rust-powered core** — Minimal overhead, maximum performance
- **Compile-time optimizations** — Chain fusion, and simple lambdas run natively
- **Concurrent processing** — `par()` for parallel branches, `prefetch()` for I/O concurrency
- **Batch operations** — Vectorized processing with `batch_map()` and `emit_batch()`
- **Dynamic pipelines** — Keep adding to streams at runtime when uncompiled

## Quick Install + Hello World

akayu installs from GitHub and builds from source: you need CPython 3.13 or 3.14 and a Rust
toolchain ([details](getting-started.md#system-requirements)).

<!--pytest.mark.skip-->

```bash
pip install "akayu @ git+https://github.com/IsaacTay/akayu"
```

```python
import akayu

source = akayu.Stream()
results = []
source.map(lambda x: x * 2).filter(lambda x: x > 5).sink(results.append)

source.compile()

for i in range(10):
    source.emit(i)

print(results)  # [6, 8, 10, 12, 14, 16, 18]
```

## Next Steps

- [Getting Started Guide](getting-started.md) — Installation, basic usage, and core concepts
- [Patterns & Best Practices](patterns.md) — Common architectures, concurrency strategies, and anti-patterns
- [API Reference](api.md) — Full method documentation
- [MultiStream](features/multistream.md) — Batch operations across multiple streams
- [Benchmarks](benchmarks.md) — Performance comparisons vs streamz
- [Migrating from streamz](migrating-from-streamz.md) — Porting guide for streamz users
- [FAQ](faq.md) — Common questions and troubleshooting
