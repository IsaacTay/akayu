# Changelog

## 2.0.0

### Breaking changes

- Python 3.13 or newer is required. Earlier versions could not import akayu at all, although the
  package metadata claimed support back to 3.8.
- Async functions are not supported: every operation that takes a callable now raises `TypeError`
  for `async def` functions, which used to be accepted and silently emitted un-awaited coroutines.
  Use `prefetch()` or `par()` for concurrent I/O.
- `from_text_file()` returns a `TextFileSource` that reads nothing until `start()` is called;
  `join()` waits for the last line and re-raises any error.
- `stream_name` and `returns_state` are keyword-only on every operation, and
  `MultiStream.filter()`'s argument is named `predicate`, like `Stream.filter()`.
- `Stream.update()` / `update_batch()` are internal and renamed `_update()` / `_update_batch()`.
- numpy and scipy are no longer installed with akayu; it never used them.
- `prefetch()` functions run in a copy of the caller's `contextvars` context (they used to see
  the worker thread's), like `par()` branches. The last `par()` branch runs on the calling thread.

### Added

- `Stream.chunks_between(open, close)` and `MultiStream.chunks_between()`.
- Dict-backed and nested `MultiStream`s, and `MultiStream.__setitem__`.
- Python 3.14 support, including the lambda optimizer.
- `akayu.__version__`.
- Faster `compile()` through cached lambda analysis.
- Linux aarch64 wheels from `just publish`.

### Performance

Measured in instructions per item with `scripts/bench_cost.py` (`just bench-cost`), which is
independent of machine load:

- `emit()` took a Python `threading.RLock` through by-name method calls: ~3,600 instructions per
  emit, more than the rest of a short pipeline. A Rust lock replaces it: a map-filter-sink
  pipeline costs 55% less per item.
- `par()` and `prefetch()` use a Rust thread pool instead of `ThreadPoolExecutor`: ~95% fewer
  instructions per item (compiled `par()`: 199,000 -> 6,100).
- Module-level `union()` and `par()` convergence points lock in Rust instead of through a
  Python helper per item: `union()` costs 87-92% less per item.
- `zip()` / `combine_latest()` no longer pack and unpack an `(index, value)` tuple per item:
  78% / 71% less per item, 65% less in batches.
- `map`/`filter`/`sink`/`batch_map(func, *args, **kwargs)` bind the extra arguments natively instead
  of calling a Python wrapper per item: 81% less per item with keyword arguments, 40% with
  positional ones. `starmap(func)` star-calls natively: 13-17% less.
- `compile()` recognizes chains of simple steps such as `x % 2 == 0`, `x * 2 + 1`, `x["a"]["b"]`,
  `x.a.b`, `str(x).upper()` and `x.strip().lower()` (each step was already supported alone):
  20-34% less per item for pipelines that use them.
- `compile()` also optimizes lambdas with defaulted extra parameters, the `lambda x, k=k:`
  idiom for capturing a loop variable (a fan-out built in a loop: 21% faster).
- `compile()` runs common accumulate functions natively: `acc + x`, `acc + 1` (any binary
  operator), `max(acc, x)`, `min(acc, x)`: 27% less per item, 50% less in batches.
- Building a graph costs 65-70% less, and `compile()` reuses bytecode analysis across fresh
  lambdas with the same code (a request handler building a pipeline per call).
- 45% less memory per node (565 -> 313 bytes); `compile()` frees the nodes it fuses into chains
  unless you hold them; `emit_batch()` map/filter/accumulate stages reuse one buffer instead of
  allocating one per stage.
- Wall clock, `tests/bench_complex.py` on the same machine: compiled pipelines are 2.3x - 15.9x
  faster per item than before this work, and 22x - 60x faster than streamz.
- `to_text_file()` writes each line as one append with the GIL released, so other threads keep
  running during the file I/O and concurrent writers never interleave within a line.

### Fixed

- Stream graphs are garbage-collected. Previously every graph leaked, along with every function,
  closure and buffered item it referenced.
- After a function was freed, a new function at the same address could be optimized as the old one.
- `flush()` / `drain()` on a node fused into a chain by `compile()` dropped the data buffered below it.
- `import akayu` no longer shadows a user module named `helpers`.
- No more spurious `flush() called on ''` warnings after `compile()`.
- `from_text_file()` no longer loses lines emitted before the pipeline is attached, or swallows errors.
- par()/prefetch() per-node locks are released with their node.
- The type stub matches the runtime API (checked with stubtest), and every documentation example runs.
- A failed `prefetch()` item raises once; it used to be re-raised by every later `emit()`/`flush()`.
- Nested `par()` much wider than the thread pool deadlocked (40 outer branches on 16 workers,
  each waiting on inner branches queued behind the others); it now completes.
- Nodes created by `zip()`, `union()`, `combine_latest()` and `MultiStream` report the user's
  source line (in errors and profiles) instead of a line inside akayu.

## 1.0.0

First release.
