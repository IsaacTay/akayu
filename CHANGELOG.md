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
  Python helper per item: `union()` costs 87-92% less per item. `compile()` gives a graph
  without `par()` one shared lock and drops the per-item merge locks (union: 14% less).
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
- `compile()` runs arithmetic and comparisons on ints and floats unboxed, keeping the item a
  machine number across consecutive steps: 28-75% less per item for numeric map/filter
  pipelines, 64-86% in batches (ten `x + 1` maps: 5,100 -> 1,270 per item; in batches
  3,580 -> 510).
- Lambdas run natively without `compile()` when their native form depends only on their own
  code (`lambda x: x + 1`, `lambda x: x["k"]`, `lambda x: x.strip()`): 27-51% less per item
  in uncompiled pipelines. `def` functions and lambdas that read closure, default or global values
  are still called, so late binding, coverage and breakpoints are unchanged.
- Numeric filters (`x % 3 == 0`) test their result without making a Python bool: splitting a
  stream into lists costs 5-13% less.
- `compile()` calls methods of builtin types directly (`x.strip()`, `x.get("k", 0)`,
  `x.startswith("a")`): 165-185 fewer instructions per call, 14-27% less per item in batches.
- `compile()` runs common accumulate functions natively: `acc + x`, `acc + 1` (any binary
  operator), `max(acc, x)`, `min(acc, x)`: 27% less per item, 50% less in batches.
- `compile()` turns `sink(lst.append)` on a list into a direct append (2-5% per item), and when
  it proves the branches below a branch point independent (native steps only, each branch
  appending to its own list), `emit_batch()` gives each branch the whole batch: 39% less per
  item when splitting a stream into lists.
- Building a graph costs 65-70% less, and `compile()` reuses bytecode analysis across fresh
  lambdas with the same code (a request handler building a pipeline per call).
- `compile()` itself costs about half as much (a 4-node graph: 29,500 -> 14,000 instructions):
  map functions were analysed twice, accumulate functions disassembled on every call, results
  that depend only on the code no longer call into Python at all, and the graph walks hash
  addresses with a multiply instead of SipHash.
- 45% less memory per node (565 -> 309 bytes; compiled 518 -> 304); `compile()` frees the nodes
  it fuses into chains unless you hold them; `emit_batch()` map/filter/accumulate stages reuse one buffer instead of
  allocating one per stage.
- Wall clock, `tests/bench_complex.py` on the same machine: compiled pipelines are 3.3x - 16.3x
  faster per item than before this work, and 28x - 142x faster than streamz.
- `to_text_file()` writes each line as one append with the GIL released, so other threads keep
  running during the file I/O and concurrent writers never interleave within a line.

### Fixed

- Stream graphs are garbage-collected. Previously every graph leaked, along with every function,
  closure and buffered item it referenced.
- After a function was freed, a new function at the same address could be optimized as the old one.
- `compile()` called the builtin of a name that the function's module redefines (its own `len`
  or `max`) instead of the module's function.
- `compile()` analysed bound methods (`map(obj.method)`) as plain functions, with the item in
  place of `self`, so the compiled graph could compute something else or raise.
- `flush()` / `drain()` on a node fused into a chain by `compile()` dropped the data buffered below it.
- `import akayu` no longer shadows a user module named `helpers`.
- No more spurious `flush() called on ''` warnings after `compile()`.
- `from_text_file()` no longer loses lines emitted before the pipeline is attached, or swallows errors.
- par()/prefetch() per-node locks are released with their node.
- The type stub matches the runtime API (checked with stubtest), and every documentation example runs.
- Streams merged with `a.union(b)`, `zip()` or `combine_latest()` and emitted from different
  threads raised "Already borrowed", lost updates, or crashed (compiled `combine_latest`); only
  `akayu.union()` locked its shared node. Every merge now does, and a `sink(target.emit)`
  shortcut takes the target's lock like `target.emit()`.
- A failed `prefetch()` item raises once; it used to be re-raised by every later `emit()`/`flush()`.
- Nested `par()` much wider than the thread pool deadlocked (40 outer branches on 16 workers,
  each waiting on inner branches queued behind the others); it now completes.
- Nodes created by `zip()`, `union()`, `combine_latest()` and `MultiStream` report the user's
  source line (in errors and profiles) instead of a line inside akayu.
- `batch_map()` under `emit()` passed on only the first of the results its function returned (also
  after `prefetch()`, which raised `StopIteration` for none).
- One `flush()` reached a node below a merge once per path: a `collect()` there emitted an extra
  empty tuple per path.
- `chunk_by()` kept an item whose predicate raised in its buffer.
- `compile()` fused `collect()` plus `filter(p).sink(lambda _: c.flush())` wrongly: the delimiter
  always ended the chunk whatever the branch order, items collected before `compile()` were
  dropped, and so were other branches below the filter.
- Deep graphs (thousands of nodes, long chains of sources feeding each other, threads with small
  stacks such as musl's 128 KiB default) crashed the interpreter with a native stack overflow;
  they raise `RecursionError`, as do `flush()` and `drain()` of such a graph. `compile()` handles
  graphs of 100,000+ nodes, and no longer takes exponential time on stacked diamonds.
- Adding nodes, compiling or profiling from a callback mid-emit raised `PanicException` (which
  `except Exception` doesn't catch) and could leave the source refusing every later `emit()`;
  these are `RuntimeError`s now and the source recovers. `compile()` of a graph whose sink emits
  into or flushes its own source panicked.
- Compiled graphs: a feedback loop through `sink(lambda x: other.emit(x))` skipped the
  re-entrancy check and recursed until it crashed, and an item coming back into a node still
  processing another ran through aliased mutable state. Both raise, as in uncompiled graphs.
- `KeyboardInterrupt`, `SystemExit` and `GeneratorExit` raised in a callback came out as
  `RuntimeError`: `except KeyboardInterrupt` didn't see them, and `sys.exit(3)` exited with 1.
- `prefetch()`: a `flush()` that raised for a failed item dropped the good results before it;
  `prefetch(-1)` raised `OverflowError` instead of `ValueError`.
- `flush()` / `drain()` from another thread while others emitted raised "Already borrowed" (and
  could lose prefetched items), and hung for good when the emitting callbacks released the GIL
  (`time.sleep()`, I/O), as an `atexit` flush with a daemon producer thread does; they wait for
  the running emit. An `emit()` from another thread during a `flush()` raised "Already mutably
  borrowed"; it waits for the flush. In uncompiled graphs, items reaching a `collect()` that a
  timer thread was flushing failed as having "re-entered" it, losing nearly all of them; they wait
  for the flush.
- The lambda optimizer computed something else for `(a or b) * 10`, `not (a or b)`, `not not x`,
  and tuples whose later elements re-read the item after an attribute or method chain; its
  comparisons turned any result into a bool and treated identical objects as equal (numpy arrays,
  query builders, objects with their own `__eq__`).
- Errors from `flatten()` (an item that isn't iterable), from iterating `batch_map()` results, and
  from a predicate result whose `__bool__` raises name the node, like every other step.
- A thread-pool worker started during interpreter shutdown could panic.
- `compile()` dropped a call to a builtin it doesn't know (`abs`, `repr`, `sorted`) at the head of
  a chain of steps: `lambda x: abs(x) + 1 + 2` computed `x + 1 + 2`.
- Compiled graphs called a `Stream` subclass's target through the base `emit()` for
  `sink(target.emit)` and `sink(lambda x: target.emit(x))`, skipping an override.
- Compiled `map(operator.attrgetter("a.b"))` looked up an attribute named `"a.b"`.
- `prefetch(n).batch_map(f)` called `f` once per item even under `emit_batch()`; it takes the
  whole batch, as without `prefetch()`.
- A `chunk_by()`, `chunks_between()` or prefetched node flushed from one of its own downstream
  callbacks was flushed mid-update in compiled graphs; it is refused, as uncompiled.
- `combine_latest(..., emit_on=...)` ignored streams that aren't inputs (and an empty list) and
  emitted on every input; it raises `ValueError`.
- Slicing a keyed `MultiStream` with negative or reversed slices paired streams with the wrong keys.
- `accumulate(..., returns_state=True)` raised a bare conversion error for a function that doesn't
  return a pair; the error names the node.
- A thread that exited mid-profiling left every other thread checking for a profiling session.
- `prefetch(2**64)` raised `OverflowError`; it is a `ValueError` like other invalid sizes. An object
  with an `async def __call__` is refused like an async function.
- `import akayu` failed without package metadata (vendored copies, zipapps, PyInstaller).
- The extension didn't build for free-threaded Python (3.13t, 3.14t).

## 1.0.0

First release.
