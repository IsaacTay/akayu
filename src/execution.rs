//! Propagation: a node's update for an item or a batch, and handing the
//! results to its children (locked, through compiled pointers, or in
//! parallel). What each node does with an item is `steps`'.

use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::Stream;
use crate::helpers::wrap_error;
use crate::node::{CombineLatestState, ExtractBetweenState, NodeLogic};
use crate::pool::{self, Latch};
use crate::profiling;
use crate::steps::{
    BatchStepResult, StepResult, apply_logic, apply_logic_batch, chunk_buffer, chunk_by_item,
    combine_latest_push, extract_between_step, node_ctx, prof_start, zip_push,
};
use std::sync::Arc;

/// Entry points for the compiled fast path, which reaches a child through a
/// raw pointer and so bypasses PyO3's borrow flag. An item coming back into
/// a node that is still mid-update (through a callback's emit into another
/// source that leads back here) would alias it: it is refused, as the borrow
/// refuses it on the uncompiled path. Uncompiled hops enter through them too:
/// when only one of the sources feeding a node is compiled, the node is
/// reached both ways, and each must see the other mid-update.
macro_rules! compiled_entry {
    ($name:ident, $inner:ident, ($($arg:ident: $ty:ty),*)) => {
        #[inline(always)]
        pub(crate) fn $name(&mut self, py: Python, $($arg: $ty),*) -> PyResult<()> {
            if !self.guard_reentry {
                return self.$inner(py, $($arg),*);
            }
            if self.busy {
                return Err(reentered());
            }
            self.busy = true;
            let result = self.$inner(py, $($arg),*);
            self.busy = false;
            result
        }
    };
}

/// An item reached a node compile() folded into a chain of steps.
#[cold]
#[inline(never)]
fn chain_member_reached(name: &str) -> PyErr {
    crate::error::StreamError::ChainMemberReached(name.to_string()).into()
}

/// The error for a re-entered node: out of line, it is never the hot path.
#[cold]
#[inline(never)]
fn reentered() -> PyErr {
    crate::error::StreamError::Reentered.into()
}

impl Stream {
    compiled_entry!(update_compiled, update, (val: Py<PyAny>));
    compiled_entry!(update_batch_compiled, update_batch, (items: Vec<Py<PyAny>>));
    compiled_entry!(update_tagged_compiled, update_tagged, (index: usize, val: Py<PyAny>));
    compiled_entry!(update_batch_tagged_compiled, update_batch_tagged, (index: usize, items: Vec<Py<PyAny>>));
}

// A node's update, for one item or a batch. Not methods Python can call:
// compiled updates reach a node through a raw pointer (`StreamPtr`), and a
// call from Python could start a second one on a node mid-update.
impl Stream {
    /// A step (`chainable`) on one item.
    #[inline(always)]
    fn update_step(
        &mut self,
        py: Python,
        x: Py<PyAny>,
        profiling_active: bool,
        node_id: usize,
    ) -> PyResult<()> {
        let ctx = node_ctx!(self, node_id);
        match apply_logic(py, &mut self.logic, &ctx, x, profiling_active)? {
            StepResult::Value(v) => self.propagate(py, v),
            StepResult::Filtered | StepResult::Terminal => Ok(()),
            StepResult::Batch(items) => self.propagate_batch(py, items),
        }
    }

    /// A step (`chainable`) on a batch.
    #[inline(always)]
    fn update_batch_step(
        &mut self,
        py: Python,
        items: Vec<Py<PyAny>>,
        profiling_active: bool,
        node_id: usize,
    ) -> PyResult<()> {
        let ctx = node_ctx!(self, node_id);
        match apply_logic_batch(py, &mut self.logic, &ctx, items, profiling_active)? {
            BatchStepResult::Items(output) => self.propagate_batch(py, output),
            BatchStepResult::Terminal => Ok(()),
        }
    }

    // Complexity is inherent to dispatching across 20+ NodeLogic variants;
    // each arm is simple and independent.
    #[allow(clippy::cognitive_complexity)]
    pub(crate) fn update_batch(&mut self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<()> {
        let profiling_active = profiling::is_profiling();
        let node_id = if profiling_active {
            std::ptr::from_ref(&*self) as usize
        } else {
            0
        };

        if self.chainable {
            return self.update_batch_step(py, items, profiling_active, node_id);
        }

        // Non-chainable variants
        let items_in = items.len() as u64;
        let mut output_batch = Vec::with_capacity(items.len());

        match &mut self.logic {
            // -- Passthrough --
            NodeLogic::Source | NodeLogic::Prefetch { .. } => {
                output_batch = items;
                if profiling_active {
                    profiling::record_node(node_id, Duration::ZERO, items_in, items_in);
                }
            }

            NodeLogic::Collect { buffer } => {
                if profiling_active {
                    profiling::record_node(node_id, Duration::ZERO, items_in, 0);
                }
                buffer.extend(items);
                return Ok(());
            }

            NodeLogic::ChunkBy(state) => {
                let t = prof_start(profiling_active);
                let mut emitted = Vec::new();
                for x in items {
                    let (chunk, buffer_after) = chunk_by_item(
                        py,
                        state,
                        self.builtin_op.as_deref(),
                        &self.cached_vectorcall,
                        &self.name,
                        &self.source_location,
                        x,
                    )?;
                    emitted.extend(chunk);
                    if let Some(x) = buffer_after {
                        chunk_buffer(py, state, x)?;
                    }
                }
                let items_out = emitted.len() as u64;
                if let Some(t) = t
                    && profiling_active
                {
                    profiling::record_node(node_id, t.elapsed(), items_in, items_out);
                }
                if !emitted.is_empty() {
                    return self.propagate_batch(py, emitted);
                }
                return Ok(());
            }
            NodeLogic::ExtractBetween(boxed) => {
                let ExtractBetweenState {
                    buffer,
                    open,
                    close,
                    inside,
                    close_vectorcall,
                } = &mut **boxed;
                let t = prof_start(profiling_active);
                let mut emitted = Vec::new();
                for x in items {
                    if let Some(chunk) = extract_between_step(
                        py,
                        &x,
                        buffer,
                        inside,
                        (open, &self.cached_vectorcall),
                        (close, close_vectorcall),
                        &self.name,
                        &self.source_location,
                    )? {
                        emitted.push(chunk);
                    }
                }
                let items_out = emitted.len() as u64;
                if let Some(t) = t
                    && profiling_active
                {
                    profiling::record_node(node_id, t.elapsed(), items_in, items_out);
                }
                if !emitted.is_empty() {
                    return self.propagate_batch(py, emitted);
                }
                return Ok(());
            }

            // -- Multi-stream --
            NodeLogic::Tag { index } => {
                let index = *index;
                if profiling_active {
                    profiling::record_node(node_id, Duration::ZERO, items_in, items_in);
                }
                return self.propagate_batch_tagged(py, index, items);
            }
            NodeLogic::CombineLatest(boxed) => {
                let CombineLatestState {
                    state,
                    emit_on_indices,
                } = &mut **boxed;
                for x in items {
                    let (idx, val): (usize, Py<PyAny>) = x.extract(py)?;
                    output_batch.extend(combine_latest_push(py, state, emit_on_indices, idx, val)?);
                }
                if profiling_active {
                    profiling::record_node(
                        node_id,
                        Duration::ZERO,
                        items_in,
                        output_batch.len() as u64,
                    );
                }
            }
            NodeLogic::Zip { buffers } => {
                for x in items {
                    let (idx, val): (usize, Py<PyAny>) = x.extract(py)?;
                    output_batch.extend(zip_push(py, buffers, idx, val)?);
                }
                if profiling_active {
                    profiling::record_node(
                        node_id,
                        Duration::ZERO,
                        items_in,
                        output_batch.len() as u64,
                    );
                }
            }

            // -- Concurrent (self-propagating) --
            NodeLogic::Parallel => {
                for x in items {
                    self.propagate_parallel(py, x)?;
                }
                if profiling_active {
                    profiling::record_node(node_id, Duration::ZERO, items_in, items_in);
                }
                return Ok(());
            }
            NodeLogic::Prefetched { prefetcher } => {
                let t = prof_start(profiling_active);
                let (results, error) = prefetcher.update_batch(py, items);
                let ft = t.map_or(Duration::ZERO, |t| t.elapsed());
                self.dispatch_state_results(py, results, profiling_active, node_id, ft, items_in)?;
                return match error {
                    Some(e) => wrap_error(py, Err(e), &self.name, &self.source_location),
                    None => Ok(()),
                };
            }

            // Steps run above; `chainable` only caches `is_step()`.
            NodeLogic::Map { .. }
            | NodeLogic::StarMap { .. }
            | NodeLogic::Filter { .. }
            | NodeLogic::Flatten
            | NodeLogic::Accumulate { .. }
            | NodeLogic::Sink { .. }
            | NodeLogic::BatchMap { .. }
            | NodeLogic::Chain { .. } => {
                return self.update_batch_step(py, items, profiling_active, node_id);
            }
            // A chain member only gets here from a node compile() bypassed
            // (an emit into a seq() node it skipped): its chain does its work.
            NodeLogic::ChainMember { .. } => return Err(chain_member_reached(&self.name)),
        }

        self.propagate_batch(py, output_batch)
    }

    pub(crate) fn update(&mut self, py: Python, x: Py<PyAny>) -> PyResult<()> {
        let profiling_active = profiling::is_profiling();
        let node_id = if profiling_active {
            std::ptr::from_ref(&*self) as usize
        } else {
            0
        };

        if self.chainable {
            return self.update_step(py, x, profiling_active, node_id);
        }

        let mut ft = Duration::ZERO;

        let result = match &mut self.logic {
            NodeLogic::Source => Some(x),

            NodeLogic::Collect { buffer } => {
                if profiling_active {
                    profiling::record_node(node_id, Duration::ZERO, 1, 0);
                }
                buffer.push(x);
                return Ok(());
            }
            NodeLogic::ChunkBy(state) => {
                let t = prof_start(profiling_active);
                let (chunk, buffer_after) = chunk_by_item(
                    py,
                    state,
                    self.builtin_op.as_deref(),
                    &self.cached_vectorcall,
                    &self.name,
                    &self.source_location,
                    x,
                )?;
                if let Some(t) = t {
                    profiling::record_node(node_id, t.elapsed(), 1, u64::from(chunk.is_some()));
                }
                if let Some(chunk) = chunk {
                    self.propagate(py, chunk)?;
                }
                if let Some(x) = buffer_after
                    && let NodeLogic::ChunkBy(state) = &mut self.logic
                {
                    chunk_buffer(py, state, x)?;
                }
                return Ok(());
            }
            NodeLogic::ExtractBetween(boxed) => {
                let ExtractBetweenState {
                    buffer,
                    open,
                    close,
                    inside,
                    close_vectorcall,
                } = &mut **boxed;
                let t = prof_start(profiling_active);
                let chunk = extract_between_step(
                    py,
                    &x,
                    buffer,
                    inside,
                    (open, &self.cached_vectorcall),
                    (close, close_vectorcall),
                    &self.name,
                    &self.source_location,
                )?;
                if let Some(t) = t {
                    ft = t.elapsed();
                }
                if profiling_active {
                    let items_out = chunk.is_some() as u64;
                    profiling::record_node(node_id, ft, 1, items_out);
                }
                return match chunk {
                    Some(chunk) => self.propagate(py, chunk),
                    None => Ok(()),
                };
            }
            NodeLogic::Tag { index } => {
                let index = *index;
                if profiling_active {
                    profiling::record_node(node_id, Duration::ZERO, 1, 1);
                }
                return self.propagate_tagged(py, index, x);
            }
            NodeLogic::CombineLatest(boxed) => {
                let CombineLatestState {
                    state,
                    emit_on_indices,
                } = &mut **boxed;
                let (idx, val): (usize, Py<PyAny>) = x.extract(py)?;
                combine_latest_push(py, state, emit_on_indices, idx, val)?
            }
            NodeLogic::Zip { buffers } => {
                let (idx, val): (usize, Py<PyAny>) = x.extract(py)?;
                zip_push(py, buffers, idx, val)?
            }
            NodeLogic::Parallel => {
                if profiling_active {
                    profiling::record_node(node_id, Duration::ZERO, 1, 1);
                }
                return self.propagate_parallel(py, x);
            }
            NodeLogic::Prefetch { .. } => Some(x),
            NodeLogic::Prefetched { prefetcher } => {
                let t = prof_start(profiling_active);
                let results = prefetcher.update(py, x);
                if let Some(t) = t {
                    ft = t.elapsed();
                }
                let results = wrap_error(py, results, &self.name, &self.source_location)?;
                self.dispatch_state_results(py, results, profiling_active, node_id, ft, 1)?;
                return Ok(());
            }

            // Steps run above; `chainable` only caches `is_step()`.
            NodeLogic::Map { .. }
            | NodeLogic::StarMap { .. }
            | NodeLogic::Filter { .. }
            | NodeLogic::Flatten
            | NodeLogic::Accumulate { .. }
            | NodeLogic::Sink { .. }
            | NodeLogic::BatchMap { .. }
            | NodeLogic::Chain { .. } => {
                return self.update_step(py, x, profiling_active, node_id);
            }
            // A chain member only gets here from a node compile() bypassed
            // (an emit into a seq() node it skipped): its chain does its work.
            NodeLogic::ChainMember { .. } => return Err(chain_member_reached(&self.name)),
        };

        // Record for nodes that fall through (non-early-return, non-terminal)
        if profiling_active {
            let has_output = result.is_some();
            profiling::record_node(node_id, ft, 1, if has_output { 1 } else { 0 });
        }

        if let Some(val) = result {
            return self.propagate(py, val);
        }
        Ok(())
    }
}

// Propagation methods (non-pymethods)
impl Stream {
    pub fn propagate_batch(&self, py: Python, output_batch: Vec<Py<PyAny>>) -> PyResult<()> {
        if output_batch.is_empty() {
            return Ok(());
        }

        let n = self.downstreams.len();
        if n == 0 {
            return Ok(());
        }
        crate::stack::check()?;

        // If THIS node needs locking (par() branches, union inputs), take each
        // child's lock before updating it: the child may be shared with another
        // thread's branch.
        let use_lock = self.locks_children;

        if n == 1 {
            if use_lock {
                return self.downstreams[0].locked_update_batch(py, output_batch);
            } else if let Some(ptr) = self.downstreams[0].ptr {
                let child = unsafe { ptr.get_mut() };
                return child.update_batch_compiled(py, output_batch);
            }
            return crate::helpers::enter_node(&self.downstreams[0], py)?
                .update_batch_compiled(py, output_batch);
        }

        if self.batch_fanout {
            // compile() proved the branches independent: each takes the whole
            // batch, on the per-step batch path.
            let ptr = |d: &crate::lock::Downstream| d.ptr.expect("compiled edge");
            for d in self.downstreams.iter().take(n - 1) {
                let batch = output_batch.iter().map(|v| v.clone_ref(py)).collect();
                unsafe { ptr(d).get_mut() }.update_batch_compiled(py, batch)?;
            }
            return unsafe { ptr(&self.downstreams[n - 1]).get_mut() }
                .update_batch_compiled(py, output_batch);
        }
        if use_lock {
            for val in output_batch {
                for child in self.downstreams.iter().take(n - 1) {
                    child.locked_update(py, val.clone_ref(py))?;
                }
                self.downstreams[n - 1].locked_update(py, val)?;
            }
        } else if self.downstreams[0].ptr.is_some() {
            // Compiled: every edge has its pointer.
            let ptr = |d: &crate::lock::Downstream| d.ptr.expect("compiled edge");
            for val in output_batch {
                for d in self.downstreams.iter().take(n - 1) {
                    let child = unsafe { ptr(d).get_mut() };
                    child.update_compiled(py, val.clone_ref(py))?;
                }
                let child = unsafe { ptr(&self.downstreams[n - 1]).get_mut() };
                child.update_compiled(py, val)?;
            }
        } else {
            for val in output_batch {
                for child in self.downstreams.iter().take(n - 1) {
                    crate::helpers::enter_node(child, py)?
                        .update_compiled(py, val.clone_ref(py))?;
                }
                crate::helpers::enter_node(&self.downstreams[n - 1], py)?
                    .update_compiled(py, val)?;
            }
        }
        Ok(())
    }

    pub fn propagate(&self, py: Python, val: Py<PyAny>) -> PyResult<()> {
        let n = self.downstreams.len();
        if n == 0 {
            return Ok(());
        }
        crate::stack::check()?;

        // If THIS node needs locking (par() branches, union inputs), take each
        // child's lock before updating it: the child may be shared with another
        // thread's branch.
        if self.locks_children {
            for child in self.downstreams.iter().take(n - 1) {
                child.locked_update(py, val.clone_ref(py))?;
            }
            self.downstreams[n - 1].locked_update(py, val)?;
        } else if let Some(first) = self.downstreams[0].ptr {
            // Compiled fast path: bypass PyCell borrow checks using cached raw pointers.
            // Safe because: DAG (no cycles), sequential processing (no aliasing),
            // frozen topology (pointers remain valid while Py<Stream> refs in downstreams live).
            if n == 1 {
                let child = unsafe { first.get_mut() };
                return child.update_compiled(py, val);
            }
            let ptr = |d: &crate::lock::Downstream| d.ptr.expect("compiled edge");
            for d in self.downstreams.iter().take(n - 1) {
                let child = unsafe { ptr(d).get_mut() };
                child.update_compiled(py, val.clone_ref(py))?;
            }
            let child = unsafe { ptr(&self.downstreams[n - 1]).get_mut() };
            child.update_compiled(py, val)?;
        } else {
            // Pre-compile fallback: use PyCell borrow_mut
            if n == 1 {
                return crate::helpers::enter_node(&self.downstreams[0], py)?
                    .update_compiled(py, val);
            }
            for child in self.downstreams.iter().take(n - 1) {
                crate::helpers::enter_node(child, py)?.update_compiled(py, val.clone_ref(py))?;
            }
            crate::helpers::enter_node(&self.downstreams[n - 1], py)?.update_compiled(py, val)?;
        }
        Ok(())
    }

    /// A Tag's only downstream is its zip/combine_latest node: pass the input
    /// index alongside the value instead of packing an `(index, value)` tuple
    /// that the combinator would unpack again.
    fn propagate_tagged(&self, py: Python, index: usize, val: Py<PyAny>) -> PyResult<()> {
        let [child] = self.downstreams.as_slice() else {
            return self.propagate(
                py,
                PyTuple::new(py, [index.into_pyobject(py)?.into_any().unbind(), val])?
                    .into_any()
                    .unbind(),
            );
        };
        if self.locks_children {
            let _guard = child.lock.acquire(py)?;
            return crate::helpers::enter_node(child, py)?.update_tagged_compiled(py, index, val);
        }
        if let Some(ptr) = child.ptr {
            return unsafe { ptr.get_mut() }.update_tagged_compiled(py, index, val);
        }
        crate::helpers::enter_node(child, py)?.update_tagged_compiled(py, index, val)
    }

    fn propagate_batch_tagged(
        &self,
        py: Python,
        index: usize,
        items: Vec<Py<PyAny>>,
    ) -> PyResult<()> {
        let [child] = self.downstreams.as_slice() else {
            let tagged = items
                .into_iter()
                .map(|v| {
                    Ok(
                        PyTuple::new(py, [index.into_pyobject(py)?.into_any().unbind(), v])?
                            .into_any()
                            .unbind(),
                    )
                })
                .collect::<PyResult<Vec<_>>>()?;
            return self.propagate_batch(py, tagged);
        };
        if self.locks_children {
            let _guard = child.lock.acquire(py)?;
            return crate::helpers::enter_node(child, py)?
                .update_batch_tagged_compiled(py, index, items);
        }
        if let Some(ptr) = child.ptr {
            return unsafe { ptr.get_mut() }.update_batch_tagged_compiled(py, index, items);
        }
        crate::helpers::enter_node(child, py)?.update_batch_tagged_compiled(py, index, items)
    }

    /// Receive `val` on input `index` of a zip/combine_latest node.
    fn update_tagged(&mut self, py: Python, index: usize, val: Py<PyAny>) -> PyResult<()> {
        let out = match &mut self.logic {
            NodeLogic::Zip { buffers } => zip_push(py, buffers, index, val)?,
            NodeLogic::CombineLatest(combine) => {
                combine_latest_push(py, &mut combine.state, &combine.emit_on_indices, index, val)?
            }
            _ => {
                let tuple = PyTuple::new(py, [index.into_pyobject(py)?.into_any().unbind(), val])?;
                return self.update(py, tuple.into_any().unbind());
            }
        };
        if profiling::is_profiling() {
            let node_id = std::ptr::from_ref(&*self) as usize;
            let n_out = u64::from(out.is_some());
            profiling::record_node(node_id, Duration::ZERO, 1, n_out);
        }
        match out {
            Some(tuple) => self.propagate(py, tuple),
            None => Ok(()),
        }
    }

    fn update_batch_tagged(
        &mut self,
        py: Python,
        index: usize,
        items: Vec<Py<PyAny>>,
    ) -> PyResult<()> {
        let items_in = items.len() as u64;
        let mut output = Vec::new();
        match &mut self.logic {
            NodeLogic::Zip { buffers } => {
                for val in items {
                    output.extend(zip_push(py, buffers, index, val)?);
                }
            }
            NodeLogic::CombineLatest(boxed) => {
                let CombineLatestState {
                    state,
                    emit_on_indices,
                } = &mut **boxed;
                for val in items {
                    output.extend(combine_latest_push(py, state, emit_on_indices, index, val)?);
                }
            }
            _ => {
                let tagged = items
                    .into_iter()
                    .map(|v| {
                        Ok(
                            PyTuple::new(py, [index.into_pyobject(py)?.into_any().unbind(), v])?
                                .into_any()
                                .unbind(),
                        )
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                return self.update_batch(py, tagged);
            }
        }
        if profiling::is_profiling() {
            let node_id = std::ptr::from_ref(&*self) as usize;
            profiling::record_node(node_id, Duration::ZERO, items_in, output.len() as u64);
        }
        self.propagate_batch(py, output)
    }

    /// Iterate results from a stateful node (Prefetch*), propagate each,
    /// and record profiling if active.
    fn dispatch_state_results(
        &self,
        py: Python,
        results: Vec<Py<PyAny>>,
        profiling_active: bool,
        node_id: usize,
        ft: Duration,
        items_in: u64,
    ) -> PyResult<()> {
        let items_out = results.len() as u64;
        for result in results {
            self.propagate(py, result)?;
        }
        if profiling_active {
            profiling::record_node(node_id, ft, items_in, items_out);
        }
        Ok(())
    }

    /// Propagate value to downstreams in parallel using thread pool.
    pub(crate) fn propagate_parallel(&self, py: Python, val: Py<PyAny>) -> PyResult<()> {
        let n = self.downstreams.len();
        if n == 0 {
            return Ok(());
        }

        // For single downstream, no parallelism needed - just use normal propagation
        if n == 1 {
            return crate::helpers::enter_node(&self.downstreams[0], py)?.update_compiled(py, val);
        }

        // Every branch but the last goes to the pool; the last runs here.
        // Each runs in its own copy of this thread's context.
        let (last, others) = self.downstreams.split_last().expect("n > 1");
        let latch = Latch::new(others.len());
        let mut tasks = Vec::with_capacity(others.len());
        for (index, edge) in others.iter().enumerate() {
            let (edge, val, ctx) = (
                edge.clone_edge(py),
                val.clone_ref(py),
                pool::copy_context(py)?,
            );
            let latch = Arc::clone(&latch);
            tasks.push(pool::submit(Box::new(move |py| {
                let result = pool::catch_panic(|| {
                    pool::run_in_context(py, &ctx, || edge.locked_update(py, val))
                });
                latch.count_down(index, result);
            })));
        }
        let ctx = pool::copy_context(py)?;
        let last_result = pool::run_in_context(py, &ctx, || last.locked_update(py, val));
        pool::wait_for(py, latch.as_ref(), &tasks)?;
        // Like before: every branch has finished; raise the first branch's error.
        match latch.take_error() {
            Some(e) => Err(e),
            None => last_result,
        }
    }
}
