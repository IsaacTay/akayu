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

### Added

- `Stream.chunks_between(open, close)` and `MultiStream.chunks_between()`.
- Dict-backed and nested `MultiStream`s, and `MultiStream.__setitem__`.
- Python 3.14 support, including the lambda optimizer.
- `akayu.__version__`.
- Faster `compile()` through cached lambda analysis.
- Linux aarch64 wheels from `just publish`.

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

## 1.0.0

First release.
