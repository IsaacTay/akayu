//! `prefetch(n)`: run the next map/filter/batch_map function up to `n` items
//! ahead on the thread pool, handing results back in input order.
//!
//! Each `update()` starts one item; once `n` are in flight it waits for the
//! oldest and returns its result. `flush()` waits for the rest. If the oldest
//! task hasn't been picked up by a worker yet, the waiting thread runs it.

use std::collections::VecDeque;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::helpers::vectorcall1;
use crate::pool::{self, Slot, Task};

#[derive(Clone, Copy)]
pub(crate) enum PrefetchKind {
    Map,
    /// Keep the item when the predicate is true.
    Filter,
    /// Call the function with a list: one item's, or a whole emit_batch()'s,
    /// as batch_map() without prefetch() does. Every result goes on.
    BatchMap,
}

/// What one item turned into: nothing (filtered out), one result, or (for a
/// batch function) any number.
type Results = smallvec::SmallVec<[Py<PyAny>; 1]>;
/// A task in flight, and what it was given (for the error if it fails).
type Pending = (Arc<Slot<Results>>, Arc<Task>, Py<PyAny>);

pub struct Prefetcher {
    pub(crate) func: Py<PyAny>,
    kind: PrefetchKind,
    size: usize,
    pending: VecDeque<Pending>,
    /// What the function was given when it last failed (see `take_failed`).
    failed: Option<Py<PyAny>>,
}

impl Prefetcher {
    pub(crate) fn new(func: Py<PyAny>, kind: PrefetchKind, size: usize) -> Self {
        Self {
            func,
            kind,
            size,
            pending: VecDeque::new(),
            failed: None,
        }
    }

    /// For the GC (see `gc.rs`): the function. Items in flight change on
    /// every update, so they are not visited.
    pub(crate) fn traverse(&self, visit: &pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        let Self {
            func,
            kind: _,
            size: _,
            pending: _,
            failed: _,
        } = self;
        visit.call(func)
    }

    /// Start `item`; once `size` items are in flight, finish the oldest.
    /// Errors are the function's own (the caller names the node).
    pub(crate) fn update(&mut self, py: Python, item: Py<PyAny>) -> PyResult<Vec<Py<PyAny>>> {
        let mut out = Vec::new();
        self.push(py, item, &mut out)?;
        Ok(out)
    }

    /// Start `items`: the results finished meanwhile, and the error that
    /// stopped the batch, if one did. Those results still go on, as a
    /// flush's do: they may be earlier emits' items.
    pub(crate) fn update_batch(
        &mut self,
        py: Python,
        items: Vec<Py<PyAny>>,
    ) -> (Vec<Py<PyAny>>, Option<PyErr>) {
        let mut out = Vec::new();
        let started = if matches!(self.kind, PrefetchKind::BatchMap) {
            if items.is_empty() {
                Ok(())
            } else {
                PyList::new(py, items)
                    .and_then(|batch| self.submit(py, batch.into_any().unbind(), &mut out))
            }
        } else {
            items
                .into_iter()
                .try_for_each(|item| self.push(py, item, &mut out))
        };
        (out, started.err())
    }

    /// Finish everything in flight, in order: the results up to the first
    /// error, and that error. The items after it stay in flight for the next
    /// flush, so their results aren't lost with the error.
    pub(crate) fn flush(&mut self, py: Python) -> (Vec<Py<PyAny>>, Option<PyErr>) {
        let mut out = Vec::new();
        while !self.pending.is_empty() {
            match self.finish_oldest(py) {
                Ok(results) => out.extend(results),
                Err(e) => return (out, Some(e)),
            }
        }
        (out, None)
    }

    fn push(&mut self, py: Python, item: Py<PyAny>, out: &mut Vec<Py<PyAny>>) -> PyResult<()> {
        let arg = if matches!(self.kind, PrefetchKind::BatchMap) {
            PyList::new(py, [item])?.into_any().unbind()
        } else {
            item
        };
        self.submit(py, arg, out)
    }

    /// Start the function on `arg` (an item, or a list for a batch function).
    fn submit(&mut self, py: Python, arg: Py<PyAny>, out: &mut Vec<Py<PyAny>>) -> PyResult<()> {
        let slot = Slot::new();
        let given = arg.clone_ref(py);
        let (job_slot, func, kind) = (Arc::clone(&slot), self.func.clone_ref(py), self.kind);
        let ctx = pool::copy_context(py)?;
        let task = pool::submit(Box::new(move |py| {
            let result = pool::catch_panic(|| {
                pool::run_in_context(py, &ctx, || apply(py, kind, &func, arg))
            });
            job_slot.set(result);
        }));
        self.pending.push_back((slot, task, given));
        if self.pending.len() >= self.size {
            out.extend(self.finish_oldest(py)?);
        }
        Ok(())
    }

    /// What the function was given when it raised the error the last call
    /// returned: the item, or a batch function's list.
    pub(crate) fn take_failed(&mut self) -> Option<Py<PyAny>> {
        self.failed.take()
    }

    fn finish_oldest(&mut self, py: Python) -> PyResult<Results> {
        let (slot, task, _) = self.pending.front().expect("nothing in flight");
        // If interrupted (Ctrl+C), the item stays in flight for the next call.
        pool::wait_for(py, slot.as_ref(), std::slice::from_ref(task))?;
        let (slot, _, given) = self.pending.pop_front().expect("nothing in flight");
        let result = slot.take();
        if result.is_err() {
            self.failed = Some(given);
        }
        result
    }
}

fn apply(py: Python, kind: PrefetchKind, func: &Py<PyAny>, item: Py<PyAny>) -> PyResult<Results> {
    match kind {
        PrefetchKind::Map => Ok(smallvec::smallvec![vectorcall1(py, func, &item)?]),
        PrefetchKind::Filter => {
            let keep = vectorcall1(py, func, &item)?.is_truthy(py)?;
            Ok(if keep {
                smallvec::smallvec![item]
            } else {
                Results::new()
            })
        }
        PrefetchKind::BatchMap => {
            let results = vectorcall1(py, func, &item)?;
            results
                .bind(py)
                .try_iter()?
                .map(|r| r.map(Bound::unbind))
                .collect()
        }
    }
}
