# Optimization Ideas

Measure before and after with `just bench-cost --save before.json` / `--compare before.json`
(instructions per item, which hold up on a busy machine; `--event cycles:u` for cycles). For cache
effects, cycles are too noisy under load: count L2 misses
(`l2_cache_req_stat.ic_dc_miss_in_l2` on AMD) instead.

## Open ideas

Not built. The estimates are rough, from profiles of the current code.

- **Wider lambda analysis**: chained comparisons (`0 < x < 10`) and `a if x else b` are still
  called as Python functions; they need a native form that branches.
- **More numpy**: comparisons, so filters can be native (`np.linalg.norm(v) > 1`); float32 and
  int arrays; broadcasting a row against a matrix; ndarray subclasses (numpy picks their result's
  type by `__array_priority__`); `np.dot`, `np.cross`, `.sum()`. Beyond that, items one at a time
  are the limit: whole arrays at once is another 2.5× (re-stacking per-item arrays) to 20× (kept
  stacked from the source), which needs batches to survive branch points (next idea).
- **Batch fan-out with Python steps**: `emit_batch()` hands each branch the whole batch only when
  every branch below the branch point is native steps ending in its own list. Otherwise the
  branches take one item at a time, because running each branch over the whole batch changes the
  order in which side effects in different branches happen.
- **Fewer `par()` locks**: `compile()` could probably prove a branch's lock redundant when the
  branches never merge again; at most 2-4% of `par()`'s cost.
- **Estimated small or narrow**: tag folding for `zip()` (~5%), common subexpressions across
  branches (only safe for functions known to be pure), batch interleaving through diamonds.
- **`compile()` cost**: about 85 µs per fan-out branch, and compiling each of thousands of
  sources joined along a chain takes quadratic time.
- **Memory**: `flatten()` materializes each input before emitting it, and `zip()` buffers without
  bound when one input runs ahead.

## Tried and rejected

Each was built or profiled and measured; the numbers are why it isn't in the code.

- **numpy maths written in Rust** (instead of calling numpy's own loops): a few ns less per step,
  but the results could differ from numpy's: `@` and norms go through BLAS (its own summation
  order and FMA), and on AVX-512 machines numpy computes `arccos`, `arctan2` and others with its
  own SIMD code. Calling numpy's loops gives its bits on every machine.
- **A flag on uncompiled native ops** (whether the analysis also had a numpy program): one `bool`
  on `EagerOp` made uncompiled int pipelines 1.5-4.7% slower, numpy or not. `compile()` looks for
  numpy in `sys.modules` instead.

- **Pre-computed dispatch tables** (flattening a compiled graph into an array of function
  pointers instead of matching on each node's logic): after the 2.0.0 performance work akayu's
  own code (`apply_logic`, `propagate`, `update`, `emit`) was about 17% of the instructions per
  item in a plain pipeline, and much less now that common lambdas run natively; the rest is
  CPython calling user functions and allocating their results. A table can only win part of
  that, at the cost of node names in errors and profiles.
- **Generating Python code for fused chains**: 0% on a 3-step chain, 18% on 10 chained plain
  functions, 13% in batches. Not worth a second execution model.
- **Item-major batches for native chains** (each item through the whole chain, instead of each
  step over the whole batch): 28-78% more instructions. Per-step loops amortize the dispatch.
- **Chunked batches and a consuming sink**: L2 misses were already below one per item, so there
  was nothing to win.
- **Software prefetch** of the next item: no measurable gain.
- **A context field for uncompiled native ops**: 0.5-1% slower on compiled paths, which don't
  use it.
- **A manual borrow check in `emit()`** instead of PyO3's `&self`: 19 more instructions per emit
  (`PyRef` increfs). Cross-thread `flush()` takes the source's lock plus a shared borrow instead.
- **Proving the per-emit lock away**: other threads can always reach a source, so it stays
  (~78 instructions per emit); `NodeLock::acquire` must stay `#[inline]` with a cold wait path
  (losing the inline cost `par()` 1.5%).
