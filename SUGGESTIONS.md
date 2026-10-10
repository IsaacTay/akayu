# Optimization Suggestions

> **Measured first (`just bench-cost`, `perf record -e instructions:u`).** After the 2.0.0
> performance work, akayu's own code (`apply_logic`, `propagate`, `update`, `emit`) is about 17% of
> the instructions per item in a plain pipeline. The rest is CPython calling the user's functions
> and allocating their results. A dispatch table can only win part of that 17%. Generating Python
> code for fused chains was also tried: 0% on a 3-step chain, 18% on 10 chained plain functions,
> and 13% in batches, so it was not adopted.

## Pre-Computed Dispatch Tables

### Problem

During stream execution, every node hit in the propagation path runs through a `match` on `NodeLogic` to determine what to do. Even after `compile()` + `freeze_all()`, the hot path in `update()` still branches on:

```rust
// execution.rs:update() — per-item, every time
if self.compiled_chainable || self.compiled_filter_fast {
    match apply_logic(py, &mut self.logic, &ctx, x, profiling_active)? { ... }
}
```

And inside `apply_logic()`, there's another `match` on the `NodeLogic` variant (Map, Filter, Sink, Accumulate, Flatten, BatchMap, Chain). For `Chain` nodes, there's *another* per-step `match`.

Each `match` branch is cheap individually, but they add up across the full propagation path: if a value flows through a Chain of 5 steps + 3 downstream nodes, that's ~8 match dispatches per item. With high-throughput streams (millions of items/sec), branch mispredictions on the `match` arms start to matter.

### Proposed Solution

After `freeze_all()`, pre-compute a **flat dispatch table** — a `Vec<DispatchEntry>` — that represents the entire propagation path from any source node to its terminal sinks. Each entry contains a raw function pointer + the data it needs, eliminating all `match` dispatch during execution.

```rust
/// Pre-computed dispatch entry. Fixed-size, no enum tag branch.
struct DispatchEntry {
    /// Raw function pointer: takes (py, data_ptr, value) → StepResult
    /// One function pointer per operation kind, resolved at compile time.
    func: unsafe fn(Python, &DispatchData, Py<PyAny>) -> PyResult<StepResult>,
    /// Opaque data for this step (func, predicate, builtin_op, etc.)
    data: DispatchData,
    /// What to do with the result:
    ///   0..N = index of next entry (propagate value)
    ///   PROPAGATE_DOWNSTREAM = exit this chain, propagate to downstream node
    ///   TERMINAL = stop propagation
    next: u32,
}
```

The key insight (borrowed from ik_llama's approach): **at compile time, you already know the full graph topology and every node's logic variant**. There's no reason to re-discover this at runtime via `match` — you can bake it into a flat array of typed function pointers.

### How ik_llama does this (for reference)

ik_llama pre-builds a computation graph with ~3000 nodes at model load. During generation, it iterates this flat array and dispatches each node by type — but critically, it also **chains** consecutive MUL_MAT operations that share the same input, skipping intermediate RESHAPE/VIEW ops. The chain is encoded directly into the graph structure, so at runtime it's just "call matmul, advance N indices" — no branching, no matching.

### Concrete approach for akayu

**Phase 1: Flatten Chains into dispatch arrays**

Current Chain execution:
```rust
// apply_logic() for Chain
NodeLogic::Chain { steps } => {
    let mut val = x;
    for step in steps.iter_mut() {
        match apply_logic(py, &mut step.logic, &step_ctx, val, profiling)? {
            StepResult::Value(v) => val = v,
            StepResult::Filtered => return Ok(StepResult::Filtered),
            StepResult::Terminal => return Ok(StepResult::Terminal),
            // ...
        }
    }
}
```

Proposed: each Chain step becomes a `DispatchEntry`. The `next` field encodes the step index (for Value results) or a sentinel (for Filtered/Terminal). No `match` on `NodeLogic` — just indirect call through `func` pointer, then index into the next entry.

**Phase 2: Flatten across node boundaries**

After Phase 1, each node's dispatch table is independent. Phase 2 stitches them together: when a Chain's output flows into another chainable downstream, merge their dispatch entries into one contiguous array. The `next` field just points to the next entry in the same array.

This is analogous to ik_llama's MUL_MAT chaining — it merges consecutive operations that share a dependency into a single flat execution sequence.

**Phase 3: Inline builtins as function pointers**

For `BuiltinOp` variants, instead of the current recursive `apply_builtin()` match, pre-compute a function pointer at compile time:

```rust
// At compile time:
let func_ptr = match builtin_op {
    BuiltinOp::Len => builtin_len as unsafe fn(...) -> ...,
    BuiltinOp::Str => builtin_str as unsafe fn(...) -> ...,
    BuiltinOp::IsInstance(t) => builtin_isinstance(t) as unsafe fn(...) -> ...,
    // ...
};
entry.func = func_ptr;
```

At runtime: one indirect call, zero branching.

### Expected gains

- **Branch misprediction reduction**: Current hot path has ~8 conditional branches per item (match arms, `is_truthy` checks, `compiled_chainable` flags). Flat dispatch table has 1 indirect call + 1 index jump per step.
- **Cache locality**: Dispatch entries are fixed-size and stored contiguously. Current approach hops between `NodeLogic` enum variants of different sizes, scattered across heap allocations.
- **Compile-time invariant hoisting**: `compiled_chainable`, `compiled_filter_fast`, `builtin_op.is_some()`, `cached_vectorcall.is_some()` — all checked at runtime per-item — become compile-time decisions baked into the function pointer selection.

### Caveats / Open Questions

1. **Mutable state**: `Accumulate` nodes carry mutable state. The dispatch entry would need a `&mut` reference to the state, which complicates the ownership model. One approach: store state as `Py<PyAny>` in the dispatch data, borrow_mut at runtime (same as current, just moved to the function pointer).

2. **Non-chainable nodes**: `Parallel`, `Collect`, `ChunkBy`, `CombineLatest`, `Zip` can't be flattened into a linear dispatch table because they have non-linear downstream semantics. These would remain as "barrier" entries that break the flat array and force a return to the normal propagation path.

3. **Error handling**: Current `match` arms produce nice error messages with node name + source location. Function pointers would need to carry this metadata somehow — either as a field in `DispatchEntry` (small overhead) or via a side table indexed by entry position.

4. **Debugging ergonomics**: A flat array of function pointers is harder to inspect than a graph of named nodes. Consider keeping the current `NodeLogic` representation around for debugging/profiling, only using the dispatch table in release mode.

5. **PyO3 borrow safety**: The current code is careful about `borrow()` / `borrow_mut()` lifetimes on `Py<Stream>`. A dispatch table approach would need to ensure that the same borrow safety is maintained — pre-computed raw pointers to Stream nodes must remain valid across GC operations. Using `Py::as_ref()` at dispatch time (not at compile time) would be safe.

### Rough sketch

```rust
// In optimization.rs, new method on Stream:
fn build_dispatch_table(&mut self, py: Python) -> Vec<DispatchEntry> {
    let mut entries = Vec::new();
    // Walk downstreams, flattening chains and chainable nodes
    self.flatten_into(py, &mut entries);
    entries
}

fn flatten_into(&self, py: Python, entries: &mut Vec<DispatchEntry>) {
    match &self.logic {
        NodeLogic::Chain { steps } => {
            for step in steps {
                entries.push(DispatchEntry::from_step(py, step));
            }
        }
        NodeLogic::Map { .. } | NodeLogic::Filter { .. } | NodeLogic::Sink { .. } => {
            entries.push(DispatchEntry::from_node(py, self));
        }
        // Non-chainable: barrier entry
        _ => {
            entries.push(DispatchEntry::barrier(self));
        }
    }
    for ds in &self.downstreams {
        ds.borrow(py).flatten_into(py, entries);
    }
}
```
