//! Steps: what each kind of node does with an item or a batch
//! (`apply_logic`, `apply_logic_batch`), including fused chains and the
//! buffering nodes' state machines. Propagation is `execution`'s.

use std::time::{Duration, Instant};

use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::helpers::{cached_vectorcall1, vectorcall1, vectorcall2, wrap_error};
use crate::native::{
    apply_accumulate, apply_builtin, apply_builtin_as_filter, iter_all, iter_into, list_append,
    star_call, truth,
};
use crate::node::{BuiltinOp, ChainNode, ChunkByState, ChunkOrder, NodeLogic};
use crate::numeric::RunOutcome;
use crate::profiling;

/// Check for pending signals (e.g. KeyboardInterrupt).
/// Cost: ~7ns when no signal pending (atomic flag read).
#[inline(always)]
pub(crate) fn check_signals(py: Python) -> PyResult<()> {
    unsafe {
        if pyo3::ffi::PyErr_CheckSignals() == -1 {
            return Err(PyErr::fetch(py));
        }
    }
    Ok(())
}

/// Start a profiling timer if profiling is active.
#[inline(always)]
pub(crate) fn prof_start(profiling: bool) -> Option<Instant> {
    if profiling {
        Some(Instant::now())
    } else {
        None
    }
}

/// Result of processing a single item through a chainable NodeLogic variant.
pub(crate) enum StepResult {
    /// Transformed value — pass downstream
    Value(Py<PyAny>),
    /// Item was filtered out — stop propagation
    Filtered,
    /// Terminal node (Sink) consumed the value
    Terminal,
    /// Flatten produced a batch — switch to batch propagation
    Batch(Vec<Py<PyAny>>),
}

/// Result of processing a batch through a chainable NodeLogic variant.
pub(crate) enum BatchStepResult {
    /// Output items to propagate downstream
    Items(Vec<Py<PyAny>>),
    /// Terminal node (Sink) consumed all items
    Terminal,
}

/// Per-node metadata that always travels together through apply_logic/apply_logic_batch.
/// Zero-cost: just references, no allocation.
pub(crate) struct NodeCtx<'a> {
    pub builtin_op: &'a Option<std::sync::Arc<BuiltinOp>>,
    pub cached_vc: &'a Option<pyo3::ffi::vectorcallfunc>,
    pub name: &'a str,
    pub source_location: &'a str,
    pub node_id: usize,
    pub flush_target: &'a Option<Py<crate::Stream>>,
    pub emit_target: &'a Option<Py<crate::Stream>>,
}

/// The `NodeCtx` of a node or a chain step: borrows its fields one by one,
/// so the caller can still borrow its `logic` mutably.
macro_rules! node_ctx {
    ($node:expr, $node_id:expr) => {
        $crate::steps::NodeCtx {
            builtin_op: &$node.builtin_op,
            cached_vc: &$node.cached_vectorcall,
            name: &$node.name,
            source_location: &$node.source_location,
            node_id: $node_id,
            flush_target: &$node.flush_target,
            emit_target: &$node.emit_target,
        }
    };
}
pub(crate) use node_ctx;

/// A map step on one item: its native op, or its function.
#[inline(always)]
fn map_one(py: Python, func: &Py<PyAny>, ctx: &NodeCtx, x: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    let res = match ctx.builtin_op {
        Some(op) => apply_builtin(py, op, x),
        None => vcall1(py, func, x, ctx.cached_vc),
    };
    wrap_error(py, res, ctx.name, ctx.source_location)
}

/// A filter step on one item: whether it passes.
#[inline(always)]
fn filter_one(py: Python, predicate: &Py<PyAny>, ctx: &NodeCtx, x: &Py<PyAny>) -> PyResult<bool> {
    let res = match ctx.builtin_op {
        Some(op) => apply_builtin_as_filter(py, op, x),
        None => vcall1(py, predicate, x, ctx.cached_vc).and_then(|r| truth(py, r)),
    };
    wrap_error(py, res, ctx.name, ctx.source_location)
}

/// A sink step on one item, for a sink without a flush or emit target.
#[inline(always)]
fn sink_one(py: Python, func: &Py<PyAny>, ctx: &NodeCtx, x: &Py<PyAny>) -> PyResult<()> {
    let res = match ctx.builtin_op.as_deref() {
        Some(BuiltinOp::ListAppend(list)) => list_append(py, list, x),
        Some(BuiltinOp::Print) => print_one(py, x),
        _ => vcall1(py, func, x, ctx.cached_vc).map(drop),
    };
    wrap_error(py, res, ctx.name, ctx.source_location)
}

/// `print(x)`, through the cached builtin.
#[inline(never)]
fn print_one(py: Python, x: &Py<PyAny>) -> PyResult<()> {
    crate::helpers::get_builtin_print()?.call1(py, (x,))?;
    Ok(())
}

/// A sink's `collect.flush()` shortcut: what `collect.flush()` called from
/// Python does, without the call.
#[inline(never)]
fn flush_target_node(py: Python, target: &Py<crate::Stream>) -> PyResult<()> {
    crate::helpers::with_node_mut(target.bind(py), py, |node| {
        node.flush_node(py, crate::operations::FlushMode::Flush, true)
    })
}

/// Call a Python callable, using the cached vectorcall pointer if available.
#[inline(always)]
fn vcall1(
    py: Python,
    func: &Py<PyAny>,
    arg: &Py<PyAny>,
    cached: &Option<pyo3::ffi::vectorcallfunc>,
) -> PyResult<Py<PyAny>> {
    if let Some(ptr) = cached {
        cached_vectorcall1(py, *ptr, func, arg)
    } else {
        vectorcall1(py, func, arg)
    }
}

/// Advance an `ExtractBetween` state machine by one item, returning the
/// completed chunk when `close` fires on a non-empty buffer.
///
/// Only the predicate for the current state is called: `open` while outside,
/// `close` while inside. This lets one predicate serve as both delimiters.
#[allow(clippy::too_many_arguments)]
pub(crate) fn extract_between_step(
    py: Python,
    x: &Py<PyAny>,
    buffer: &mut Vec<Py<PyAny>>,
    inside: &mut bool,
    (open, open_vc): (&Py<PyAny>, &Option<pyo3::ffi::vectorcallfunc>),
    (close, close_vc): (&Py<PyAny>, &Option<pyo3::ffi::vectorcallfunc>),
    name: &str,
    source_location: &str,
) -> PyResult<Option<Py<PyAny>>> {
    let (pred, vc) = if *inside {
        (close, close_vc)
    } else {
        (open, open_vc)
    };
    let res = vcall1(py, pred, x, vc).and_then(|r| truth(py, r));
    if !wrap_error(py, res, name, source_location)? {
        if *inside {
            buffer.push(x.clone_ref(py));
        }
        return Ok(None);
    }
    *inside = !*inside;
    if *inside || buffer.is_empty() {
        return Ok(None);
    }
    let chunk = std::mem::take(buffer);
    Ok(Some(PyTuple::new(py, chunk)?.into_any().unbind()))
}

/// Buffer `val` on zip input `index`; return the next tuple once every input has one.
pub(crate) fn zip_push(
    py: Python,
    buffers: &mut [std::collections::VecDeque<Py<PyAny>>],
    index: usize,
    val: Py<PyAny>,
) -> PyResult<Option<Py<PyAny>>> {
    if let Some(buf) = buffers.get_mut(index) {
        buf.push_back(val);
    }
    if !buffers.iter().all(|buf| !buf.is_empty()) {
        return Ok(None);
    }
    // Every buffer is non-empty, so each pop yields a value.
    let values = buffers.iter_mut().map(|buf| buf.pop_front().unwrap());
    Ok(Some(PyTuple::new(py, values)?.into_any().unbind()))
}

/// Record `val` as the latest on input `index`; return the combined tuple when
/// every input has a value and `index` triggers emission.
pub(crate) fn combine_latest_push(
    py: Python,
    state: &mut [Option<Py<PyAny>>],
    emit_on_indices: &[usize],
    index: usize,
    val: Py<PyAny>,
) -> PyResult<Option<Py<PyAny>>> {
    if let Some(slot) = state.get_mut(index) {
        *slot = Some(val);
    }
    let should_emit = state.iter().all(Option::is_some)
        && (emit_on_indices.is_empty() || emit_on_indices.contains(&index));
    if !should_emit {
        return Ok(None);
    }
    let values = state.iter().map(|v| v.as_ref().unwrap().clone_ref(py));
    Ok(Some(PyTuple::new(py, values)?.into_any().unbind()))
}

/// A sink's `target.emit(x)` shortcut: take the target's lock, as
/// `target.emit()` does, then propagate. Out of line: it is rare, and inline
/// it made the hot `update()` path measurably slower.
#[inline(never)]
fn emit_into_target(
    py: Python,
    target: &Py<crate::Stream>,
    items: impl IntoIterator<Item = Py<PyAny>>,
) -> PyResult<()> {
    let t = target
        .try_borrow(py)
        .map_err(|_| PyErr::from(crate::error::StreamError::BusySource))?;
    // Entered like target.emit() does: a feedback loop back into a source
    // that is mid-emit is rejected, not run as unbounded recursion.
    let _entered = t.enter_source(py, false)?;
    for x in items {
        crate::profiling::record_emitted(&t, 1);
        t.propagate(py, x)?;
    }
    Ok(())
}

/// The (state, output) pair an accumulate function with `returns_state=True`
/// returns.
fn split_state(py: Python, result: Py<PyAny>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
    result.extract(py).map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(format!(
            "accumulate(returns_state=True) needs the function to return a (state, output) \
             pair, got {}",
            result
                .bind(py)
                .get_type()
                .name()
                .map_or_else(|_| "?".into(), |n| n.to_string())
        ))
    })
}

/// Process a single item through a chainable NodeLogic variant.
///
/// Handles: Map, Filter (simple path), Sink, Accumulate, Flatten, BatchMap.
/// Profiling is recorded internally per variant.
///
/// Note: returns PyResult because it calls Python functions
/// (vectorcall1, is_truthy, etc.) which can raise exceptions.
#[allow(clippy::cognitive_complexity)]
pub(crate) fn apply_logic(
    py: Python,
    logic: &mut NodeLogic,
    ctx: &NodeCtx,
    x: Py<PyAny>,
    profiling: bool,
) -> PyResult<StepResult> {
    // The common steps, unprofiled, stay in this small function; everything
    // else lives out of line, so the per-step call stays cheap (a fused chain
    // calls this once per step, per item).
    if !profiling {
        match logic {
            NodeLogic::Map { func } => {
                return Ok(StepResult::Value(map_one(py, func, ctx, &x)?));
            }
            NodeLogic::Filter { predicate } => {
                return Ok(if filter_one(py, predicate, ctx, &x)? {
                    StepResult::Value(x)
                } else {
                    StepResult::Filtered
                });
            }
            NodeLogic::Sink { func } if ctx.flush_target.is_none() && ctx.emit_target.is_none() => {
                sink_one(py, func, ctx, &x)?;
                return Ok(StepResult::Terminal);
            }
            NodeLogic::Chain { steps } => return apply_chain(py, steps, x, false),
            _ => {}
        }
    }
    apply_logic_general(py, logic, ctx, x, profiling)
}

/// Run a fused chain's steps on one item.
#[inline(never)]
fn apply_chain(
    py: Python,
    steps: &mut [ChainNode],
    x: Py<PyAny>,
    profiling: bool,
) -> PyResult<StepResult> {
    let mut val = x;
    let mut i = 0;
    while i < steps.len() {
        if !profiling && let Some(run) = steps[i].numeric_run.as_deref() {
            match run.run(py, val) {
                RunOutcome::Value(v) => {
                    val = v;
                    i += run.len();
                    continue;
                }
                RunOutcome::Filtered => return Ok(StepResult::Filtered),
                RunOutcome::Fallback(v) => val = v, // the ordinary steps below
            }
        }
        let step = &mut steps[i];
        i += 1;
        let step_ctx = node_ctx!(step, step.node_id);
        match apply_logic(py, &mut step.logic, &step_ctx, val, profiling)? {
            StepResult::Value(v) => val = v,
            StepResult::Filtered => return Ok(StepResult::Filtered),
            StepResult::Terminal => return Ok(StepResult::Terminal),
            StepResult::Batch(items) => {
                // Flatten mid-chain: switch remaining steps to batch mode
                let mut batch = items;
                for s in &mut steps[i..] {
                    let s_ctx = node_ctx!(s, s.node_id);
                    match apply_logic_batch(py, &mut s.logic, &s_ctx, batch, profiling)? {
                        BatchStepResult::Items(out) => batch = out,
                        BatchStepResult::Terminal => return Ok(StepResult::Terminal),
                    }
                }
                return Ok(StepResult::Batch(batch));
            }
        }
    }
    Ok(StepResult::Value(val))
}

/// Every step kind, with profiling: the out-of-line rest of `apply_logic`.
#[inline(never)]
fn apply_logic_general(
    py: Python,
    logic: &mut NodeLogic,
    ctx: &NodeCtx,
    x: Py<PyAny>,
    profiling: bool,
) -> PyResult<StepResult> {
    match logic {
        NodeLogic::Map { func } => {
            let t = prof_start(profiling);
            let result = map_one(py, func, ctx, &x)?;
            if let Some(t) = t {
                profiling::record_node(ctx.node_id, t.elapsed(), 1, 1);
            }
            Ok(StepResult::Value(result))
        }

        NodeLogic::StarMap { func } => {
            let t = prof_start(profiling);
            let result = wrap_error(py, star_call(py, func, &x), ctx.name, ctx.source_location)?;
            if let Some(t) = t {
                profiling::record_node(ctx.node_id, t.elapsed(), 1, 1);
            }
            Ok(StepResult::Value(result))
        }

        NodeLogic::Filter { predicate } => {
            let t = prof_start(profiling);
            let passes = filter_one(py, predicate, ctx, &x)?;
            if let Some(t) = t {
                profiling::record_node(ctx.node_id, t.elapsed(), 1, u64::from(passes));
            }
            if passes {
                Ok(StepResult::Value(x))
            } else {
                Ok(StepResult::Filtered)
            }
        }

        NodeLogic::Sink { func } => {
            // Fast path: direct Rust flush (no Python dispatch)
            if let Some(target) = ctx.flush_target {
                if profiling {
                    profiling::record_node(ctx.node_id, Duration::ZERO, 1, 0);
                }
                flush_target_node(py, target)?;
                return Ok(StepResult::Terminal);
            }
            // Fast path: direct Rust emit (no Python dispatch)
            if let Some(target) = ctx.emit_target {
                if profiling {
                    profiling::record_node(ctx.node_id, Duration::ZERO, 1, 0);
                }
                emit_into_target(py, target, [x])?;
                return Ok(StepResult::Terminal);
            }
            let t = prof_start(profiling);
            sink_one(py, func, ctx, &x)?;
            if let Some(t) = t {
                profiling::record_node(ctx.node_id, t.elapsed(), 1, 0);
            }
            Ok(StepResult::Terminal)
        }

        NodeLogic::Accumulate {
            func,
            state,
            returns_state,
        } => {
            let t = prof_start(profiling);
            let result = ctx
                .builtin_op
                .as_deref()
                .and_then(|op| apply_accumulate(py, op, state, &x))
                .unwrap_or_else(|| vectorcall2(py, func, state, &x));
            let ft = if let Some(t) = t {
                t.elapsed()
            } else {
                Duration::ZERO
            };
            let result = wrap_error(py, result, ctx.name, ctx.source_location)?;
            let output = if *returns_state {
                let (new_state, output) =
                    wrap_error(py, split_state(py, result), ctx.name, ctx.source_location)?;
                *state = new_state;
                output
            } else {
                *state = result.clone_ref(py);
                result
            };
            if profiling {
                profiling::record_node(ctx.node_id, ft, 1, 1);
            }
            Ok(StepResult::Value(output))
        }

        NodeLogic::Flatten => {
            let batch = wrap_error(py, iter_all(x.bind(py)), ctx.name, ctx.source_location)?;
            if profiling {
                profiling::record_node(ctx.node_id, Duration::ZERO, 1, batch.len() as u64);
            }
            Ok(StepResult::Batch(batch))
        }

        NodeLogic::BatchMap { func } => {
            let py_list = PyList::new(py, &[x])?;
            let t = prof_start(profiling);
            let result = func.call1(py, (py_list,));
            let ft = if let Some(t) = t {
                t.elapsed()
            } else {
                Duration::ZERO
            };
            let result = wrap_error(py, result, ctx.name, ctx.source_location)?;
            // Every result goes on, however many the function returned for
            // the one item (it used to keep only the first).
            let mut results =
                wrap_error(py, iter_all(result.bind(py)), ctx.name, ctx.source_location)?;
            if profiling {
                profiling::record_node(ctx.node_id, ft, 1, results.len() as u64);
            }
            Ok(match results.len() {
                0 => StepResult::Filtered,
                1 => StepResult::Value(results.pop().expect("one result")),
                _ => StepResult::Batch(results),
            })
        }

        NodeLogic::Chain { steps } => apply_chain(py, steps, x, profiling),

        // Not steps (`NodeLogic::is_step`): update() runs them itself.
        NodeLogic::Source
        | NodeLogic::Collect { .. }
        | NodeLogic::Tag { .. }
        | NodeLogic::CombineLatest(_)
        | NodeLogic::Zip { .. }
        | NodeLogic::Parallel
        | NodeLogic::Prefetch { .. }
        | NodeLogic::Prefetched { .. }
        | NodeLogic::ChainMember { .. }
        | NodeLogic::ChunkBy(_)
        | NodeLogic::ExtractBetween(_) => unreachable!("apply_logic called with a non-step node"),
    }
}

/// Process a batch of items through a chainable NodeLogic variant.
///
/// Handles: Map, Filter, Sink, Accumulate, Flatten, BatchMap.
/// Profiling is recorded internally per variant.
#[allow(clippy::cognitive_complexity)]
pub(crate) fn apply_logic_batch(
    py: Python,
    logic: &mut NodeLogic,
    ctx: &NodeCtx,
    items: Vec<Py<PyAny>>,
    profiling: bool,
) -> PyResult<BatchStepResult> {
    let items_in = items.len() as u64;

    match logic {
        // Map and Filter work in place: no new buffer per stage, so a batch
        // costs one buffer however long the chain.
        NodeLogic::Map { func } => {
            let t = prof_start(profiling);
            let mut output = items;
            match ctx.builtin_op {
                Some(op) => {
                    for (i, x) in output.iter_mut().enumerate() {
                        if i & 1023 == 0 {
                            check_signals(py)?;
                        }
                        let res = apply_builtin(py, op, x);
                        *x = wrap_error(py, res, ctx.name, ctx.source_location)?;
                    }
                }
                None => {
                    for x in &mut output {
                        let res = vcall1(py, func, x, ctx.cached_vc);
                        *x = wrap_error(py, res, ctx.name, ctx.source_location)?;
                    }
                }
            }
            if let Some(t) = t {
                profiling::record_node(ctx.node_id, t.elapsed(), items_in, output.len() as u64);
            }
            Ok(BatchStepResult::Items(output))
        }

        NodeLogic::StarMap { func } => {
            let t = prof_start(profiling);
            let mut output = items;
            for x in &mut output {
                *x = wrap_error(py, star_call(py, func, x), ctx.name, ctx.source_location)?;
            }
            if let Some(t) = t {
                profiling::record_node(ctx.node_id, t.elapsed(), items_in, output.len() as u64);
            }
            Ok(BatchStepResult::Items(output))
        }

        NodeLogic::Filter { predicate } => {
            let t = prof_start(profiling);
            // Compact the kept items to the front, then cut the rest off.
            let mut output = items;
            let mut kept = 0;
            match ctx.builtin_op {
                Some(op) => {
                    for i in 0..output.len() {
                        if i & 1023 == 0 {
                            check_signals(py)?;
                        }
                        let res = apply_builtin_as_filter(py, op, &output[i]);
                        if wrap_error(py, res, ctx.name, ctx.source_location)? {
                            output.swap(kept, i);
                            kept += 1;
                        }
                    }
                }
                None => {
                    for i in 0..output.len() {
                        let res = vcall1(py, predicate, &output[i], ctx.cached_vc);
                        if wrap_error(
                            py,
                            res.and_then(|r| truth(py, r)),
                            ctx.name,
                            ctx.source_location,
                        )? {
                            output.swap(kept, i);
                            kept += 1;
                        }
                    }
                }
            }
            output.truncate(kept);
            if let Some(t) = t {
                profiling::record_node(ctx.node_id, t.elapsed(), items_in, output.len() as u64);
            }
            Ok(BatchStepResult::Items(output))
        }

        NodeLogic::Flatten => {
            let mut output = Vec::new();
            for x in items {
                wrap_error(
                    py,
                    iter_into(x.bind(py), &mut output),
                    ctx.name,
                    ctx.source_location,
                )?;
            }
            if profiling {
                profiling::record_node(ctx.node_id, Duration::ZERO, items_in, output.len() as u64);
            }
            Ok(BatchStepResult::Items(output))
        }

        NodeLogic::BatchMap { func } => {
            let py_list = PyList::new(py, &items)?;
            let t = prof_start(profiling);
            let result = func.call1(py, (py_list,));
            let ft = if let Some(t) = t {
                t.elapsed()
            } else {
                Duration::ZERO
            };
            let result = wrap_error(py, result, ctx.name, ctx.source_location)?;
            let output = wrap_error(py, iter_all(result.bind(py)), ctx.name, ctx.source_location)?;
            if profiling {
                profiling::record_node(ctx.node_id, ft, items_in, output.len() as u64);
            }
            Ok(BatchStepResult::Items(output))
        }

        NodeLogic::Accumulate {
            func,
            state,
            returns_state,
        } => {
            let t = prof_start(profiling);
            let mut output = items;
            for x in &mut output {
                let result = ctx
                    .builtin_op
                    .as_deref()
                    .and_then(|op| apply_accumulate(py, op, state, x))
                    .unwrap_or_else(|| vectorcall2(py, func, state, x));
                let result = wrap_error(py, result, ctx.name, ctx.source_location)?;
                if *returns_state {
                    let (new_state, output) =
                        wrap_error(py, split_state(py, result), ctx.name, ctx.source_location)?;
                    *state = new_state;
                    *x = output;
                } else {
                    *state = result.clone_ref(py);
                    *x = result;
                }
            }
            if let Some(t) = t {
                profiling::record_node(ctx.node_id, t.elapsed(), items_in, output.len() as u64);
            }
            Ok(BatchStepResult::Items(output))
        }

        NodeLogic::Sink { func } => {
            // Fast path: direct Rust flush (no Python dispatch)
            if let Some(target) = ctx.flush_target {
                if profiling {
                    profiling::record_node(ctx.node_id, Duration::ZERO, items_in, 0);
                }
                for _x in items {
                    flush_target_node(py, target)?;
                }
                return Ok(BatchStepResult::Terminal);
            }
            // Fast path: direct Rust emit (no Python dispatch)
            if let Some(target) = ctx.emit_target {
                if profiling {
                    profiling::record_node(ctx.node_id, Duration::ZERO, items_in, 0);
                }
                emit_into_target(py, target, items)?;
                return Ok(BatchStepResult::Terminal);
            }
            let t = prof_start(profiling);
            // The common list sink keeps its own loop.
            if let Some(BuiltinOp::ListAppend(list)) = ctx.builtin_op.as_deref() {
                for x in &items {
                    wrap_error(py, list_append(py, list, x), ctx.name, ctx.source_location)?;
                }
            } else {
                // By value: each item is freed as soon as the sink is done
                // with it, not when the whole batch is.
                for x in items {
                    sink_one(py, func, ctx, &x)?;
                }
            }
            if let Some(t) = t {
                profiling::record_node(ctx.node_id, t.elapsed(), items_in, 0);
            }
            Ok(BatchStepResult::Terminal)
        }

        NodeLogic::Chain { steps } => {
            let mut batch = items;
            let mut i = 0;
            while i < steps.len() {
                if !profiling && let Some(run) = steps[i].numeric_run.as_deref() {
                    match run.run_batch(py, batch) {
                        Ok(out) => {
                            batch = out;
                            i += run.len();
                            continue;
                        }
                        Err(untouched) => batch = untouched,
                    }
                }
                let step = &mut steps[i];
                i += 1;
                let step_ctx = node_ctx!(step, step.node_id);
                match apply_logic_batch(py, &mut step.logic, &step_ctx, batch, profiling)? {
                    BatchStepResult::Items(out) => batch = out,
                    BatchStepResult::Terminal => return Ok(BatchStepResult::Terminal),
                }
            }
            Ok(BatchStepResult::Items(batch))
        }

        // Not steps (`NodeLogic::is_step`): update_batch() runs them itself.
        NodeLogic::Source
        | NodeLogic::Collect { .. }
        | NodeLogic::Tag { .. }
        | NodeLogic::CombineLatest(_)
        | NodeLogic::Zip { .. }
        | NodeLogic::Parallel
        | NodeLogic::Prefetch { .. }
        | NodeLogic::Prefetched { .. }
        | NodeLogic::ChainMember { .. }
        | NodeLogic::ChunkBy(_)
        | NodeLogic::ExtractBetween(_) => {
            unreachable!("apply_logic_batch called with a non-step node")
        }
    }
}

/// (the chunk an item completed, the item to buffer after it went on)
type ChunkStep = (Option<Py<PyAny>>, Option<Py<PyAny>>);

/// One item into a chunk_by node, up to the chunk it completes: (chunk,
/// item to buffer after). The caller buffers that item (`chunk_buffer`)
/// once the chunk has gone downstream (see `ChunkOrder`).
#[inline(always)]
pub(crate) fn chunk_by_item(
    py: Python,
    state: &mut ChunkByState,
    op: Option<&BuiltinOp>,
    cached_vc: &Option<pyo3::ffi::vectorcallfunc>,
    name: &str,
    source_location: &str,
    x: Py<PyAny>,
) -> PyResult<ChunkStep> {
    // Inline: it runs once per item.
    #[inline(always)]
    fn test(
        py: Python,
        state: &ChunkByState,
        op: Option<&BuiltinOp>,
        cached_vc: &Option<pyo3::ffi::vectorcallfunc>,
        name: &str,
        source_location: &str,
        x: &Py<PyAny>,
    ) -> PyResult<bool> {
        let res = match op {
            Some(op) => apply_builtin_as_filter(py, op, x),
            None => vcall1(py, &state.predicate, x, cached_vc).and_then(|r| truth(py, r)),
        };
        wrap_error(py, res, name, source_location)
    }
    let is_delimiter = |state: &ChunkByState, x: &Py<PyAny>| {
        test(py, state, op, cached_vc, name, source_location, x)
    };
    match state.order {
        ChunkOrder::Operator { include_delimiter } => {
            if !is_delimiter(state, &x)? {
                chunk_buffer(py, state, x)?;
                return Ok((None, None));
            }
            if include_delimiter {
                chunk_buffer(py, state, x)?;
            }
            Ok((chunk_take(py, &mut state.buffer, false)?, None))
        }
        ChunkOrder::Fused { buffer_first: true } => {
            chunk_buffer(py, state, x.clone_ref(py))?;
            let chunk = if is_delimiter(state, &x)? {
                chunk_take(py, &mut state.buffer, true)?
            } else {
                None
            };
            Ok((chunk, None))
        }
        ChunkOrder::Fused {
            buffer_first: false,
        } => {
            let chunk = if is_delimiter(state, &x)? {
                chunk_take(py, &mut state.buffer, true)?
            } else {
                None
            };
            Ok((chunk, Some(x)))
        }
    }
}

/// Buffer an item in a chunk_by node, through the steps fused before it,
/// as a chain runs them: a flatten (or batch_map) among them buffers every
/// item it yields.
#[inline]
pub(crate) fn chunk_buffer(py: Python, state: &mut ChunkByState, x: Py<PyAny>) -> PyResult<()> {
    if state.pre_buffer_chain.is_empty() {
        state.buffer.push(x);
        return Ok(());
    }
    match apply_chain(py, &mut state.pre_buffer_chain, x, false)? {
        StepResult::Value(v) => state.buffer.push(v),
        StepResult::Batch(items) => state.buffer.extend(items),
        StepResult::Filtered | StepResult::Terminal => {}
    }
    Ok(())
}

/// A chunk_by node's buffer as a tuple, emptying it; None for an empty
/// buffer unless `emit_empty`.
pub(crate) fn chunk_take(
    py: Python,
    buffer: &mut Vec<Py<PyAny>>,
    emit_empty: bool,
) -> PyResult<Option<Py<PyAny>>> {
    if buffer.is_empty() && !emit_empty {
        return Ok(None);
    }
    Ok(Some(
        PyTuple::new(py, std::mem::take(buffer))?
            .into_any()
            .unbind(),
    ))
}
