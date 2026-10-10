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
    /// Call the function with a one-item list; take the first result.
    BatchMap,
}

type Pending = (Arc<Slot<Option<Py<PyAny>>>>, Arc<Task>);

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

    /// Finish everything in flight, in order.
    pub(crate) fn flush(&mut self, py: Python) -> PyResult<Vec<Py<PyAny>>> {
        let mut out = Vec::new();
        while !self.pending.is_empty() {
            out.extend(self.finish_oldest(py)?);
        }
        Ok(out)
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

    fn finish_oldest(&mut self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let (slot, task) = self.pending.front().expect("nothing in flight");
        // If interrupted (Ctrl+C), the item stays in flight for the next call.
        pool::wait_for(py, slot.as_ref(), std::slice::from_ref(task))?;
        let (slot, _) = self.pending.pop_front().expect("nothing in flight");
        slot.take()
    }
}

fn apply(
    py: Python,
    kind: PrefetchKind,
    func: &Py<PyAny>,
    item: Py<PyAny>,
) -> PyResult<Option<Py<PyAny>>> {
    match kind {
        PrefetchKind::Map => vectorcall1(py, func, &item).map(Some),
        PrefetchKind::Filter => {
            let keep = vectorcall1(py, func, &item)?.is_truthy(py)?;
            Ok(keep.then_some(item))
        }
        PrefetchKind::BatchMap => {
            let batch = PyList::new(py, [item])?.into_any().unbind();
            let results = vectorcall1(py, func, &batch)?;
            let first = results.bind(py).try_iter()?.next();
            match first {
                Some(value) => Ok(Some(value?.unbind())),
                None => Err(pyo3::exceptions::PyStopIteration::new_err(())),
            }
        }
    }
}
