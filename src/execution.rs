//! Stream execution logic (update, propagate)

use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::helpers::{
    get_filter_async, get_gather, get_is_awaitable, get_parallel_execute, get_process_async,
    get_safe_update, get_safe_update_batch, wrap_error,
};
use crate::node::NodeLogic;
use crate::Stream;

#[pymethods]
impl Stream {
    pub fn update_batch(&mut self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<Option<Py<PyAny>>> {
        // skip_async_check is pre-computed in self.skip_async_check
        let mut output_batch = Vec::with_capacity(items.len());

        match &mut self.logic {
            NodeLogic::Source => {
                output_batch = items;
            }
            NodeLogic::Map { func } | NodeLogic::Starmap { func } => {
                for x in items {
                    let res = wrap_error(py, func.call1(py, (x,)), &self.name)?;
                    output_batch.push(res);
                }
            }
            NodeLogic::Filter { predicate } => {
                for x in items {
                    let res = wrap_error(py, predicate.call1(py, (x.clone_ref(py),)), &self.name)?;
                    if res.is_truthy(py)? {
                        output_batch.push(x);
                    }
                }
            }
            NodeLogic::Flatten => {
                for x in items {
                    let x_bound = x.bind(py);
                    let iter = x_bound.try_iter()?;
                    for item_res in iter {
                        output_batch.push(item_res?.into());
                    }
                }
            }
            NodeLogic::Collect { buffer } => {
                buffer.extend(items);
                return Ok(None);
            }
            NodeLogic::Sink { func } => {
                for x in items {
                    wrap_error(py, func.call1(py, (x,)), &self.name)?;
                }
            }
            NodeLogic::Accumulate {
                func,
                state,
                returns_state,
            } => {
                for x in items {
                    let result =
                        wrap_error(py, func.call1(py, (state.clone_ref(py), x)), &self.name)?;
                    if *returns_state {
                        // func returns (new_state, emit_value)
                        let tuple: (Py<PyAny>, Py<PyAny>) = result.extract(py)?;
                        *state = tuple.0;
                        output_batch.push(tuple.1);
                    } else {
                        // func returns single value that is both state and emit value
                        *state = result.clone_ref(py);
                        output_batch.push(result);
                    }
                }
            }
            NodeLogic::Tag { index } => {
                let idx_obj: Py<PyAny> = (*index).into_pyobject(py)?.into();
                for x in items {
                    let tuple = PyTuple::new(py, &[idx_obj.clone_ref(py), x])?;
                    output_batch.push(tuple.into());
                }
            }
            NodeLogic::CombineLatest {
                state,
                emit_on_indices,
            } => {
                for x in items {
                    let tuple: (usize, Py<PyAny>) = x.extract(py)?;
                    let (idx, val) = tuple;
                    if idx < state.len() {
                        state[idx] = Some(val);
                    }
                    // Check if we should emit: all values present AND (emit_on empty OR idx in emit_on)
                    let should_emit = state.iter().all(std::option::Option::is_some)
                        && (emit_on_indices.is_empty() || emit_on_indices.contains(&idx));
                    if should_emit {
                        let values: Vec<Py<PyAny>> = state
                            .iter()
                            .map(|s| s.as_ref().unwrap().clone_ref(py))
                            .collect();
                        let tuple_out = PyTuple::new(py, values)?;
                        output_batch.push(tuple_out.into());
                    }
                }
            }
            NodeLogic::Zip { buffers } => {
                for x in items {
                    let tuple: (usize, Py<PyAny>) = x.extract(py)?;
                    let (idx, val) = tuple;
                    if idx < buffers.len() {
                        buffers[idx].push_back(val);
                    }
                }
                while buffers.iter().all(|buf| !buf.is_empty()) {
                    let mut values = Vec::with_capacity(buffers.len());
                    for buf in buffers.iter_mut() {
                        if let Some(v) = buf.pop_front() {
                            values.push(v);
                        }
                    }
                    let tuple_out = PyTuple::new(py, values)?;
                    output_batch.push(tuple_out.into());
                }
            }
            NodeLogic::BatchMap { func } => {
                // Convert Vec<Py<PyAny>> to Python list
                let py_list = PyList::new(py, &items)?;
                // Call function once with entire batch
                let result = wrap_error(py, func.call1(py, (py_list,)), &self.name)?;
                // Convert result back to Vec
                let result_list = result.bind(py);
                for item in result_list.try_iter()? {
                    output_batch.push(item?.into());
                }
            }
            NodeLogic::Parallel => {
                // Parallel node processes batch items sequentially through parallel propagation
                // Each item is propagated to all downstreams in parallel
                let mut futures = Vec::new();
                for x in items {
                    if let Some(f) = self.propagate_parallel(py, x)? {
                        futures.push(f);
                    }
                }
                if !futures.is_empty() {
                    let gather = get_gather()?;
                    let gathered = gather.call1(py, (futures,))?;
                    return Ok(Some(gathered));
                }
                return Ok(None);
            }
            NodeLogic::Prefetch { .. } => {
                // Prefetch without a following Map just passes through
                output_batch = items;
            }
            NodeLogic::PrefetchMap { state, .. } | NodeLogic::PrefetchFilter { state, .. } => {
                // Get ready results from Python state, collect them first
                let mut to_propagate: Vec<Py<PyAny>> = Vec::new();
                for x in items {
                    let results =
                        wrap_error(py, state.call_method1(py, "update", (x,)), &self.name)?;
                    let results_list = results.bind(py);
                    for result in results_list.try_iter()? {
                        to_propagate.push(result?.into());
                    }
                }
                // Propagate after releasing the borrow on self.logic
                for val in to_propagate {
                    self.propagate(py, val)?;
                }
                return Ok(None);
            }
        }

        self.propagate_batch(py, output_batch)
    }

    pub fn update(&mut self, py: Python, x: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        let result = match &mut self.logic {
            NodeLogic::Source => Some(x),
            NodeLogic::Map { func } | NodeLogic::Starmap { func } => {
                Some(wrap_error(py, func.call1(py, (x,)), &self.name)?)
            }
            NodeLogic::Filter { predicate } => {
                let x_clone = x.clone_ref(py);
                let res = wrap_error(py, predicate.call1(py, (x_clone,)), &self.name)?;
                if !self.skip_async_check {
                    let is_awaitable_fn = get_is_awaitable()?;
                    let is_awaitable = is_awaitable_fn.call1(py, (&res,))?.is_truthy(py)?;
                    if is_awaitable {
                        let filter_async = get_filter_async()?;
                        let downstreams_list: Vec<Py<Self>> =
                            self.downstreams.iter().map(|s| s.clone_ref(py)).collect();
                        let dl = PyList::new(py, downstreams_list)?;
                        let coro = filter_async.call1(py, (res, x, dl))?;
                        return Ok(Some(coro));
                    }
                }
                if res.is_truthy(py)? { Some(x) } else { None }
            }
            NodeLogic::Flatten => {
                let x_bound = x.bind(py);
                let iter = x_bound.try_iter()?;
                let mut batch = Vec::new();
                for item_res in iter {
                    let item = item_res?;
                    batch.push(item.into());
                }
                return self.propagate_batch(py, batch);
            }
            NodeLogic::Collect { buffer } => {
                buffer.push(x);
                return Ok(None);
            }
            NodeLogic::Sink { func } => {
                wrap_error(py, func.call1(py, (x,)), &self.name)?;
                None
            }
            NodeLogic::Accumulate {
                func,
                state,
                returns_state,
            } => {
                let result = wrap_error(py, func.call1(py, (state.clone_ref(py), x)), &self.name)?;
                if *returns_state {
                    // func returns (new_state, emit_value)
                    let tuple: (Py<PyAny>, Py<PyAny>) = result.extract(py)?;
                    *state = tuple.0;
                    Some(tuple.1)
                } else {
                    // func returns single value that is both state and emit value
                    *state = result.clone_ref(py);
                    Some(result)
                }
            }
            NodeLogic::Tag { index } => {
                let idx_obj: Py<PyAny> = (*index).into_pyobject(py)?.into();
                let elements = vec![idx_obj, x];
                let tuple = PyTuple::new(py, elements)?;
                Some(tuple.into())
            }
            NodeLogic::CombineLatest {
                state,
                emit_on_indices,
            } => {
                let tuple: (usize, Py<PyAny>) = x.extract(py)?;
                let (idx, val) = tuple;
                if idx < state.len() {
                    state[idx] = Some(val);
                }
                // Check if we should emit: all values present AND (emit_on empty OR idx in emit_on)
                let should_emit = state.iter().all(std::option::Option::is_some)
                    && (emit_on_indices.is_empty() || emit_on_indices.contains(&idx));
                if should_emit {
                    let values: Vec<Py<PyAny>> = state
                        .iter()
                        .map(|s| s.as_ref().unwrap().clone_ref(py))
                        .collect();
                    let tuple_out = PyTuple::new(py, values)?;
                    Some(tuple_out.into())
                } else {
                    None
                }
            }
            NodeLogic::Zip { buffers } => {
                let tuple: (usize, Py<PyAny>) = x.extract(py)?;
                let (idx, val) = tuple;
                if idx < buffers.len() {
                    buffers[idx].push_back(val);
                }
                if buffers.iter().all(|buf| !buf.is_empty()) {
                    let mut values = Vec::with_capacity(buffers.len());
                    for buf in buffers.iter_mut() {
                        if let Some(v) = buf.pop_front() {
                            values.push(v);
                        }
                    }
                    let tuple_out = PyTuple::new(py, values)?;
                    Some(tuple_out.into())
                } else {
                    None
                }
            }
            NodeLogic::BatchMap { func } => {
                // Wrap single item in list, call func, extract single result
                let py_list = PyList::new(py, &[x])?;
                let result = wrap_error(py, func.call1(py, (py_list,)), &self.name)?;
                let result_list = result.bind(py);
                let mut iter = result_list.try_iter()?;
                match iter.next() {
                    Some(item) => Some(item?.into()),
                    None => return Ok(None),
                }
            }
            NodeLogic::Parallel => {
                // Parallel node passes value through, but uses parallel propagation
                return self.propagate_parallel(py, x);
            }
            NodeLogic::Prefetch { .. } => {
                // Prefetch without a following Map just passes through
                // (this shouldn't happen after optimization, but handle it gracefully)
                Some(x)
            }
            NodeLogic::PrefetchMap { state, .. } => {
                // Get ready results from Python state (handles threading and ordering)
                let results = wrap_error(py, state.call_method1(py, "update", (x,)), &self.name)?;
                // Propagate each ready result to downstreams (done in Rust for thread safety)
                let results_list = results.bind(py);
                for result in results_list.try_iter()? {
                    self.propagate(py, result?.into())?;
                }
                return Ok(None);
            }
            NodeLogic::PrefetchFilter { state, .. } => {
                // Get ready results from Python state (handles threading, ordering, and filtering)
                let results = wrap_error(py, state.call_method1(py, "update", (x,)), &self.name)?;
                // Propagate each ready result to downstreams (done in Rust for thread safety)
                let results_list = results.bind(py);
                for result in results_list.try_iter()? {
                    self.propagate(py, result?.into())?;
                }
                return Ok(None);
            }
        };
        if let Some(val) = result {
            return self.propagate(py, val);
        }
        Ok(None)
    }
}

// Propagation methods (non-pymethods)
impl Stream {
    pub fn propagate_batch(
        &self,
        py: Python,
        output_batch: Vec<Py<PyAny>>,
    ) -> PyResult<Option<Py<PyAny>>> {
        if output_batch.is_empty() {
            return Ok(None);
        }

        let n = self.downstreams.len();
        if n == 0 {
            return Ok(None);
        }

        // If THIS node needs locking (e.g., PrefetchMap inside par), use safe_update
        let use_lock = self.needs_lock;

        if n == 1 {
            let batch_list = PyList::new(py, &output_batch)?;
            let result = if use_lock {
                let safe_update_batch = get_safe_update_batch()?;
                safe_update_batch.call1(py, (&self.downstreams[0], batch_list))?
            } else {
                self.downstreams[0].call_method1(py, "update_batch", (batch_list,))?
            };
            if result.is_none(py) {
                return Ok(None);
            }
            return Ok(Some(result));
        }

        let mut futures = Vec::new();
        if use_lock {
            let safe_update = get_safe_update()?;
            for val in output_batch {
                for child in self.downstreams.iter().take(n - 1) {
                    let result = safe_update.call1(py, (child, val.clone_ref(py)))?;
                    if !result.is_none(py) {
                        futures.push(result);
                    }
                }
                let result = safe_update.call1(py, (&self.downstreams[n - 1], &val))?;
                if !result.is_none(py) {
                    futures.push(result);
                }
            }
        } else {
            for val in output_batch {
                for child in self.downstreams.iter().take(n - 1) {
                    let result = child.call_method1(py, "update", (val.clone_ref(py),))?;
                    if !result.is_none(py) {
                        futures.push(result);
                    }
                }
                let result = self.downstreams[n - 1].call_method1(py, "update", (&val,))?;
                if !result.is_none(py) {
                    futures.push(result);
                }
            }
        }

        if !futures.is_empty() {
            let gather = get_gather()?;
            let gathered = gather.call1(py, (futures,))?;
            return Ok(Some(gathered));
        }
        Ok(None)
    }

    pub fn propagate(&self, py: Python, val: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        if !self.skip_async_check {
            let is_awaitable_fn = get_is_awaitable()?;
            let is_awaitable = is_awaitable_fn.call1(py, (&val,))?.is_truthy(py)?;
            if is_awaitable {
                let process_async = get_process_async()?;
                let downstreams_list: Vec<Py<Self>> =
                    self.downstreams.iter().map(|s| s.clone_ref(py)).collect();
                let dl = PyList::new(py, downstreams_list)?;
                let coro = process_async.call1(py, (val, dl))?;
                return Ok(Some(coro));
            }
        }

        let n = self.downstreams.len();
        if n == 0 {
            return Ok(None);
        }

        // If THIS node needs locking (e.g., PrefetchMap inside par), use safe_update
        // for all downstream calls to prevent borrow conflicts at convergence points.
        let use_lock = self.needs_lock;
        let mut futures = Vec::new();

        if use_lock {
            let safe_update = get_safe_update()?;
            if n == 1 {
                let result = safe_update.call1(py, (&self.downstreams[0], &val))?;
                if !result.is_none(py) {
                    futures.push(result);
                }
            } else {
                for child in self.downstreams.iter().take(n - 1) {
                    let result = safe_update.call1(py, (child, val.clone_ref(py)))?;
                    if !result.is_none(py) {
                        futures.push(result);
                    }
                }
                let result = safe_update.call1(py, (&self.downstreams[n - 1], &val))?;
                if !result.is_none(py) {
                    futures.push(result);
                }
            }
        } else {
            // Fast path: no locking needed, use direct method calls
            if n == 1 {
                let result = self.downstreams[0].call_method1(py, "update", (&val,))?;
                if !result.is_none(py) {
                    futures.push(result);
                }
            } else {
                for child in self.downstreams.iter().take(n - 1) {
                    let result = child.call_method1(py, "update", (val.clone_ref(py),))?;
                    if !result.is_none(py) {
                        futures.push(result);
                    }
                }
                let result = self.downstreams[n - 1].call_method1(py, "update", (&val,))?;
                if !result.is_none(py) {
                    futures.push(result);
                }
            }
        }

        if !futures.is_empty() {
            let gather = get_gather()?;
            let gathered = gather.call1(py, (futures,))?;
            return Ok(Some(gathered));
        }
        Ok(None)
    }

    /// Propagate value to downstreams in parallel using thread pool.
    pub(crate) fn propagate_parallel(&self, py: Python, val: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        let n = self.downstreams.len();
        if n == 0 {
            return Ok(None);
        }

        // For single downstream, no parallelism needed - just use normal propagation
        if n == 1 {
            let mut child_ref = self.downstreams[0].borrow_mut(py);
            return child_ref.update(py, val);
        }

        // Multiple downstreams: use thread pool for parallel execution
        let parallel_execute = get_parallel_execute()?;
        let downstreams_list: Vec<Py<Self>> =
            self.downstreams.iter().map(|s| s.clone_ref(py)).collect();
        let dl = PyList::new(py, downstreams_list)?;
        let result = parallel_execute.call1(py, (val, dl))?;

        // _parallel_execute returns None or an asyncio.gather coroutine
        if result.is_none(py) {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }
}
