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
    /// Call the function with a one-item list; every result goes on.
    BatchMap,
}

/// What one item turned into: nothing (filtered out), one result, or (for a
/// batch function) any number.
type Results = smallvec::SmallVec<[Py<PyAny>; 1]>;
type Pending = (Arc<Slot<Results>>, Arc<Task>);

pub struct Prefetcher {
    pub(crate) func: Py<PyAny>,
    kind: PrefetchKind,
    size: usize,
    pending: VecDeque<Pending>,
}

impl Prefetcher {
    pub(crate) fn new(func: Py<PyAny>, kind: PrefetchKind, size: usize) -> Self {
        Self {
            func,
            kind,
            size,
            pending: VecDeque::new(),
        }
    }

    /// Start `item`; once `size` items are in flight, finish the oldest.
    /// Errors are the function's own (the caller names the node).
    pub(crate) fn update(&mut self, py: Python, item: Py<PyAny>) -> PyResult<Vec<Py<PyAny>>> {
        let mut out = Vec::new();
        self.push(py, item, &mut out)?;
        Ok(out)
    }

    pub(crate) fn update_batch(
        &mut self,
        py: Python,
        items: Vec<Py<PyAny>>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let mut out = Vec::new();
        for item in items {
            self.push(py, item, &mut out)?;
        }
        Ok(out)
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
        let slot = Slot::new();
        let (job_slot, func, kind) = (Arc::clone(&slot), self.func.clone_ref(py), self.kind);
        let ctx = pool::copy_context(py)?;
        let task = pool::submit(Box::new(move |py| {
            let result = pool::catch_panic(|| {
                pool::run_in_context(py, &ctx, || apply(py, kind, &func, item))
            });
            job_slot.set(result);
        }));
        self.pending.push_back((slot, task));
        if self.pending.len() >= self.size {
            out.extend(self.finish_oldest(py)?);
        }
        Ok(())
    }

    fn finish_oldest(&mut self, py: Python) -> PyResult<Results> {
        let (slot, task) = self.pending.front().expect("nothing in flight");
        // If interrupted (Ctrl+C), the item stays in flight for the next call.
        pool::wait_for(py, slot.as_ref(), std::slice::from_ref(task))?;
        let (slot, _) = self.pending.pop_front().expect("nothing in flight");
        slot.take()
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
            let batch = PyList::new(py, [item])?.into_any().unbind();
            let results = vectorcall1(py, func, &batch)?;
            results
                .bind(py)
                .try_iter()?
                .map(|r| r.map(Bound::unbind))
                .collect()
        }
    }
}
