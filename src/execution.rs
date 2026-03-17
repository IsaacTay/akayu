//! Stream execution logic (update, propagate)

use std::time::{Duration, Instant};

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyList, PyTuple};

use crate::Stream;
use crate::helpers::{
    get_filter_async, get_gather, get_is_awaitable, get_parallel_execute, get_process_async,
    get_safe_update, get_safe_update_batch, vectorcall1, wrap_error,
};
use crate::node::{BuiltinOp, NodeLogic};
use crate::profiling;

/// Start a profiling timer if profiling is active.
#[inline(always)]
fn prof_start(profiling: bool) -> Option<Instant> {
    if profiling { Some(Instant::now()) } else { None }
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
    builtin_op: &Option<BuiltinOp>,
    name: &str,
    source_location: &str,
    node_id: usize,
    x: Py<PyAny>,
    profiling: bool,
) -> PyResult<StepResult> {
    match logic {
        NodeLogic::Map { func } => {
            let t = prof_start(profiling);
            let result = match builtin_op {
                Some(BuiltinOp::Len) => {
                    let len = x.bind(py).len()?;
                    len.into_pyobject(py)?.into_any().unbind()
                }
                Some(BuiltinOp::Str) => x.bind(py).str()?.into_any().unbind(),
                Some(BuiltinOp::Int) => unsafe {
                    let ptr = pyo3::ffi::PyNumber_Long(x.as_ptr());
                    if ptr.is_null() { return Err(PyErr::fetch(py)); }
                    Py::from_owned_ptr(py, ptr)
                },
                Some(BuiltinOp::Float) => unsafe {
                    let ptr = pyo3::ffi::PyNumber_Float(x.as_ptr());
                    if ptr.is_null() { return Err(PyErr::fetch(py)); }
                    Py::from_owned_ptr(py, ptr)
                },
                Some(BuiltinOp::Bool) => {
                    let r = unsafe { pyo3::ffi::PyObject_IsTrue(x.as_ptr()) };
                    if r == -1 { return Err(PyErr::fetch(py)); }
                    PyBool::new(py, r != 0).to_owned().into_any().unbind()
                }
                Some(BuiltinOp::Not) => {
                    let r = unsafe { pyo3::ffi::PyObject_IsTrue(x.as_ptr()) };
                    if r == -1 { return Err(PyErr::fetch(py)); }
                    PyBool::new(py, r == 0).to_owned().into_any().unbind()
                }
                Some(BuiltinOp::Truth) => {
                    let r = unsafe { pyo3::ffi::PyObject_IsTrue(x.as_ptr()) };
                    if r == -1 { return Err(PyErr::fetch(py)); }
                    PyBool::new(py, r != 0).to_owned().into_any().unbind()
                }
                Some(BuiltinOp::ItemGetter(key)) => {
                    x.bind(py).get_item(key)?.unbind()
                }
                Some(BuiltinOp::AttrGetter(attr_name)) => {
                    x.bind(py).getattr(attr_name)?.unbind()
                }
                _ => {
                    let res = vectorcall1(py, func, &x);
                    wrap_error(py, res, name, source_location)?
                }
            };
            let ft = if let Some(t) = t { t.elapsed() } else { Duration::ZERO };
            if profiling {
                profiling::record_node(node_id, ft, 1, 1, name, source_location);
            }
            Ok(StepResult::Value(result))
        }

        NodeLogic::Filter { predicate } => {
            let (passes, ft) = match builtin_op {
                Some(BuiltinOp::IsInstance(type_obj)) => {
                    let t = prof_start(profiling);
                    let passes = x.bind(py).is_instance(type_obj.bind(py))?;
                    let ft = if let Some(t) = t { t.elapsed() } else { Duration::ZERO };
                    (passes, ft)
                }
                Some(BuiltinOp::Is(sentinel)) => {
                    let t = prof_start(profiling);
                    let passes = x.is(sentinel);
                    let ft = if let Some(t) = t { t.elapsed() } else { Duration::ZERO };
                    (passes, ft)
                }
                Some(BuiltinOp::IsNot(sentinel)) => {
                    let t = prof_start(profiling);
                    let passes = !x.is(sentinel);
                    let ft = if let Some(t) = t { t.elapsed() } else { Duration::ZERO };
                    (passes, ft)
                }
                _ => {
                    let t = prof_start(profiling);
                    let res = vectorcall1(py, predicate, &x);
                    let ft = if let Some(t) = t { t.elapsed() } else { Duration::ZERO };
                    let res = wrap_error(py, res, name, source_location)?;
                    let passes = res.is_truthy(py)?;
                    (passes, ft)
                }
            };
            if profiling {
                profiling::record_node(node_id, ft, 1, u64::from(passes), name, source_location);
            }
            if passes { Ok(StepResult::Value(x)) } else { Ok(StepResult::Filtered) }
        }

        NodeLogic::Sink { func } => {
            let t = prof_start(profiling);
            match builtin_op {
                Some(BuiltinOp::Print) => {
                    let print_fn = crate::helpers::get_builtin_print()?;
                    print_fn.call1(py, (&x,))?;
                }
                _ => {
                    let res = vectorcall1(py, func, &x);
                    wrap_error(py, res, name, source_location)?;
                }
            }
            let ft = if let Some(t) = t { t.elapsed() } else { Duration::ZERO };
            if profiling {
                profiling::record_node(node_id, ft, 1, 0, name, source_location);
            }
            Ok(StepResult::Terminal)
        }

        NodeLogic::Accumulate { func, state, returns_state } => {
            let t = prof_start(profiling);
            let result = func.call1(py, (state.clone_ref(py), x));
            let ft = if let Some(t) = t { t.elapsed() } else { Duration::ZERO };
            let result = wrap_error(py, result, name, source_location)?;
            let output = if *returns_state {
                let tuple: (Py<PyAny>, Py<PyAny>) = result.extract(py)?;
                *state = tuple.0;
                tuple.1
            } else {
                *state = result.clone_ref(py);
                result
            };
            if profiling {
                profiling::record_node(node_id, ft, 1, 1, name, source_location);
            }
            Ok(StepResult::Value(output))
        }

        NodeLogic::Flatten => {
            let x_bound = x.bind(py);
            let iter = x_bound.try_iter()?;
            let mut batch = Vec::new();
            for item_res in iter {
                batch.push(item_res?.into());
            }
            if profiling {
                profiling::record_node(node_id, Duration::ZERO, 1, batch.len() as u64, name, source_location);
            }
            Ok(StepResult::Batch(batch))
        }

        NodeLogic::BatchMap { func } => {
            let py_list = PyList::new(py, &[x])?;
            let t = prof_start(profiling);
            let result = func.call1(py, (py_list,));
            let ft = if let Some(t) = t { t.elapsed() } else { Duration::ZERO };
            let result = wrap_error(py, result, name, source_location)?;
            let result_list = result.bind(py);
            let mut iter = result_list.try_iter()?;
            match iter.next() {
                Some(item) => {
                    if profiling {
                        profiling::record_node(node_id, ft, 1, 1, name, source_location);
                    }
                    Ok(StepResult::Value(item?.into()))
                }
                None => {
                    if profiling {
                        profiling::record_node(node_id, ft, 1, 0, name, source_location);
                    }
                    Ok(StepResult::Filtered)
                }
            }
        }

        NodeLogic::Chain { steps } => {
            let mut val = x;
            let mut i = 0;
            while i < steps.len() {
                let step = &mut steps[i];
                match apply_logic(py, &mut step.logic, &step.builtin_op, &step.name,
                                  &step.source_location, step.node_id, val, profiling)? {
                    StepResult::Value(v) => { val = v; i += 1; }
                    StepResult::Filtered => return Ok(StepResult::Filtered),
                    StepResult::Terminal => return Ok(StepResult::Terminal),
                    StepResult::Batch(items) => {
                        // Flatten mid-chain: switch remaining steps to batch mode
                        let mut batch = items;
                        for j in (i + 1)..steps.len() {
                            let s = &mut steps[j];
                            match apply_logic_batch(py, &mut s.logic, &s.builtin_op,
                                                    &s.name, &s.source_location, s.node_id,
                                                    batch, profiling)? {
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

        NodeLogic::ChainMember => unreachable!("ChainMember should never be executed directly"),

        _ => unreachable!("apply_logic called with non-chainable variant"),
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
    builtin_op: &Option<BuiltinOp>,
    name: &str,
    source_location: &str,
    node_id: usize,
    items: Vec<Py<PyAny>>,
    profiling: bool,
) -> PyResult<BatchStepResult> {
    let items_in = items.len() as u64;

    match logic {
        NodeLogic::Map { func } => {
            let t = prof_start(profiling);
            let mut output = Vec::with_capacity(items.len());
            match builtin_op {
                Some(BuiltinOp::Len) => {
                    for x in items {
                        let len = x.bind(py).len()?;
                        output.push(len.into_pyobject(py)?.into_any().unbind());
                    }
                }
                Some(BuiltinOp::Str) => {
                    for x in items {
                        output.push(x.bind(py).str()?.into_any().unbind());
                    }
                }
                Some(BuiltinOp::Int) => {
                    for x in items {
                        let result: Py<PyAny> = unsafe {
                            let ptr = pyo3::ffi::PyNumber_Long(x.as_ptr());
                            if ptr.is_null() { return Err(PyErr::fetch(py)); }
                            Py::from_owned_ptr(py, ptr)
                        };
                        output.push(result);
                    }
                }
                Some(BuiltinOp::Float) => {
                    for x in items {
                        let result: Py<PyAny> = unsafe {
                            let ptr = pyo3::ffi::PyNumber_Float(x.as_ptr());
                            if ptr.is_null() { return Err(PyErr::fetch(py)); }
                            Py::from_owned_ptr(py, ptr)
                        };
                        output.push(result);
                    }
                }
                Some(BuiltinOp::Bool) => {
                    for x in items {
                        let r = unsafe { pyo3::ffi::PyObject_IsTrue(x.as_ptr()) };
                        if r == -1 { return Err(PyErr::fetch(py)); }
                        output.push(PyBool::new(py, r != 0).to_owned().into_any().unbind());
                    }
                }
                Some(BuiltinOp::Not) => {
                    for x in items {
                        let r = unsafe { pyo3::ffi::PyObject_IsTrue(x.as_ptr()) };
                        if r == -1 { return Err(PyErr::fetch(py)); }
                        output.push(PyBool::new(py, r == 0).to_owned().into_any().unbind());
                    }
                }
                Some(BuiltinOp::Truth) => {
                    for x in items {
                        let r = unsafe { pyo3::ffi::PyObject_IsTrue(x.as_ptr()) };
                        if r == -1 { return Err(PyErr::fetch(py)); }
                        output.push(PyBool::new(py, r != 0).to_owned().into_any().unbind());
                    }
                }
                Some(BuiltinOp::ItemGetter(key)) => {
                    for x in items {
                        let result = x.bind(py).get_item(key)?;
                        output.push(result.unbind());
                    }
                }
                Some(BuiltinOp::AttrGetter(attr_name)) => {
                    for x in items {
                        let result = x.bind(py).getattr(attr_name)?;
                        output.push(result.unbind());
                    }
                }
                _ => {
                    for x in items {
                        let res = vectorcall1(py, func, &x);
                        let res = wrap_error(py, res, name, source_location)?;
                        output.push(res);
                    }
                }
            }
            if let Some(t) = t { profiling::record_node(node_id, t.elapsed(), items_in, output.len() as u64, name, source_location); }
            Ok(BatchStepResult::Items(output))
        }

        NodeLogic::Filter { predicate } => {
            let t = prof_start(profiling);
            let mut output = Vec::with_capacity(items.len());
            match builtin_op {
                Some(BuiltinOp::IsInstance(type_obj)) => {
                    let type_bound = type_obj.bind(py);
                    for x in items {
                        if x.bind(py).is_instance(type_bound)? {
                            output.push(x);
                        }
                    }
                }
                Some(BuiltinOp::Is(sentinel)) => {
                    for x in items {
                        if x.is(sentinel) {
                            output.push(x);
                        }
                    }
                }
                Some(BuiltinOp::IsNot(sentinel)) => {
                    for x in items {
                        if !x.is(sentinel) {
                            output.push(x);
                        }
                    }
                }
                _ => {
                    for x in items {
                        let res = vectorcall1(py, predicate, &x);
                        let res = wrap_error(py, res, name, source_location)?;
                        if res.is_truthy(py)? {
                            output.push(x);
                        }
                    }
                }
            }
            if let Some(t) = t { profiling::record_node(node_id, t.elapsed(), items_in, output.len() as u64, name, source_location); }
            Ok(BatchStepResult::Items(output))
        }

        NodeLogic::Flatten => {
            let mut output = Vec::new();
            for x in items {
                let x_bound = x.bind(py);
                for item_res in x_bound.try_iter()? {
                    output.push(item_res?.into());
                }
            }
            if profiling { profiling::record_node(node_id, Duration::ZERO, items_in, output.len() as u64, name, source_location); }
            Ok(BatchStepResult::Items(output))
        }

        NodeLogic::BatchMap { func } => {
            let py_list = PyList::new(py, &items)?;
            let t = prof_start(profiling);
            let result = func.call1(py, (py_list,));
            let ft = if let Some(t) = t { t.elapsed() } else { Duration::ZERO };
            let result = wrap_error(py, result, name, source_location)?;
            let result_list = result.bind(py);
            let mut output = Vec::new();
            for item in result_list.try_iter()? {
                output.push(item?.into());
            }
            if profiling { profiling::record_node(node_id, ft, items_in, output.len() as u64, name, source_location); }
            Ok(BatchStepResult::Items(output))
        }

        NodeLogic::Accumulate { func, state, returns_state } => {
            let t = prof_start(profiling);
            let mut output = Vec::with_capacity(items.len());
            for x in items {
                let result = func.call1(py, (state.clone_ref(py), x));
                let result = wrap_error(py, result, name, source_location)?;
                if *returns_state {
                    let tuple: (Py<PyAny>, Py<PyAny>) = result.extract(py)?;
                    *state = tuple.0;
                    output.push(tuple.1);
                } else {
                    *state = result.clone_ref(py);
                    output.push(result);
                }
            }
            if let Some(t) = t { profiling::record_node(node_id, t.elapsed(), items_in, output.len() as u64, name, source_location); }
            Ok(BatchStepResult::Items(output))
        }

        NodeLogic::Sink { func } => {
            let t = prof_start(profiling);
            match builtin_op {
                Some(BuiltinOp::Print) => {
                    let print_fn = crate::helpers::get_builtin_print()?;
                    for x in items {
                        print_fn.call1(py, (&x,))?;
                    }
                }
                _ => {
                    for x in items {
                        let res = vectorcall1(py, func, &x);
                        wrap_error(py, res, name, source_location)?;
                    }
                }
            }
            if let Some(t) = t {
                profiling::record_node(node_id, t.elapsed(), items_in, 0, name, source_location);
            }
            Ok(BatchStepResult::Terminal)
        }

        NodeLogic::Chain { steps } => {
            let mut batch = items;
            for step in steps.iter_mut() {
                match apply_logic_batch(py, &mut step.logic, &step.builtin_op,
                                        &step.name, &step.source_location, step.node_id,
                                        batch, profiling)? {
                    BatchStepResult::Items(out) => batch = out,
                    BatchStepResult::Terminal => return Ok(BatchStepResult::Terminal),
                }
            }
            Ok(BatchStepResult::Items(batch))
        }

        NodeLogic::ChainMember => unreachable!("ChainMember should never be executed directly"),

        _ => unreachable!("apply_logic_batch called with non-chainable variant"),
    }
}

#[pymethods]
impl Stream {
    // Complexity is inherent to dispatching across 20+ NodeLogic variants;
    // each arm is simple and independent.
    #[allow(clippy::cognitive_complexity)]
    pub fn update_batch(
        &mut self,
        py: Python,
        items: Vec<Py<PyAny>>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let profiling_active = profiling::is_profiling();
        let node_id = if profiling_active { std::ptr::from_ref(&*self) as usize } else { 0 };

        // Chainable variants: delegate to apply_logic_batch
        if matches!(&self.logic,
            NodeLogic::Map { .. } | NodeLogic::Filter { .. } | NodeLogic::Sink { .. }
            | NodeLogic::Accumulate { .. } | NodeLogic::Flatten | NodeLogic::BatchMap { .. }
            | NodeLogic::Chain { .. }
        ) {
            match apply_logic_batch(py, &mut self.logic, &self.builtin_op, &self.name,
                                    &self.source_location, node_id, items, profiling_active)? {
                BatchStepResult::Items(output) => return self.propagate_batch(py, output),
                BatchStepResult::Terminal => return Ok(None),
            }
        }

        // Non-chainable variants
        let items_in = items.len() as u64;
        let mut output_batch = Vec::with_capacity(items.len());

        match &mut self.logic {
            // -- Passthrough --
            NodeLogic::Source | NodeLogic::Prefetch { .. } => {
                output_batch = items;
                if profiling_active { profiling::record_node(node_id, Duration::ZERO, items_in, items_in, &self.name, &self.source_location); }
            }

            NodeLogic::Collect { buffer } => {
                if profiling_active {
                    profiling::record_node(node_id, Duration::ZERO, items_in, 0, &self.name, &self.source_location);
                }
                buffer.extend(items);
                return Ok(None);
            }

            // -- Multi-stream --
            NodeLogic::Tag { index } => {
                let idx_obj: Py<PyAny> = (*index).into_pyobject(py)?.into();
                for x in items {
                    let tuple = PyTuple::new(py, &[idx_obj.clone_ref(py), x])?;
                    output_batch.push(tuple.into());
                }
                if profiling_active { profiling::record_node(node_id, Duration::ZERO, items_in, output_batch.len() as u64, &self.name, &self.source_location); }
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
                if profiling_active { profiling::record_node(node_id, Duration::ZERO, items_in, output_batch.len() as u64, &self.name, &self.source_location); }
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
                if profiling_active { profiling::record_node(node_id, Duration::ZERO, items_in, output_batch.len() as u64, &self.name, &self.source_location); }
            }

            // -- Concurrent (self-propagating) --
            NodeLogic::Parallel => {
                let mut futures = Vec::new();
                for x in items {
                    if let Some(f) = self.propagate_parallel(py, x)? {
                        futures.push(f);
                    }
                }
                if profiling_active {
                    profiling::record_node(node_id, Duration::ZERO, items_in, items_in, &self.name, &self.source_location);
                }
                if !futures.is_empty() {
                    let gather = get_gather()?;
                    let gathered = gather.call1(py, (futures,))?;
                    return Ok(Some(gathered));
                }
                return Ok(None);
            }
            NodeLogic::PrefetchMap { state, .. } | NodeLogic::PrefetchFilter { state, .. } => {
                let mut to_propagate: Vec<Py<PyAny>> = Vec::new();
                let t = prof_start(profiling_active);
                for x in items {
                    let results = state.call_method1(py, "update", (x,));
                    let results = wrap_error(py, results, &self.name, &self.source_location)?;
                    let results_list = results.bind(py);
                    for result in results_list.try_iter()? {
                        to_propagate.push(result?.into());
                    }
                }
                let items_out = to_propagate.len() as u64;
                if let Some(t) = t {
                    profiling::record_node(node_id, t.elapsed(), items_in, items_out, &self.name, &self.source_location);
                }
                for val in to_propagate {
                    self.propagate(py, val)?;
                }
                return Ok(None);
            }
            NodeLogic::PrefetchBatchMap { state, .. } => {
                let py_list = PyList::new(py, &items)?;
                let t = prof_start(profiling_active);
                let results = state.call_method1(py, "update_batch", (py_list,));
                let ft = if let Some(t) = t { t.elapsed() } else { Duration::ZERO };
                let results = wrap_error(py, results, &self.name, &self.source_location)?;
                let results_list = results.bind(py);
                let mut to_propagate: Vec<Py<PyAny>> = Vec::new();
                for result in results_list.try_iter()? {
                    to_propagate.push(result?.into());
                }
                let items_out = to_propagate.len() as u64;
                if profiling_active {
                    profiling::record_node(node_id, ft, items_in, items_out, &self.name, &self.source_location);
                }
                for val in to_propagate {
                    self.propagate(py, val)?;
                }
                return Ok(None);
            }
            NodeLogic::AsyncMap { func, state } => {
                let mut to_propagate: Vec<Py<PyAny>> = Vec::new();
                let t = prof_start(profiling_active);
                for x in items {
                    let coro = func.call1(py, (x,));
                    let coro = wrap_error(py, coro, &self.name, &self.source_location)?;
                    let results =
                        wrap_error(py, state.call_method1(py, "submit", (coro,)), &self.name, &self.source_location)?;
                    let results_list = results.bind(py);
                    for result in results_list.try_iter()? {
                        to_propagate.push(result?.into());
                    }
                }
                let items_out = to_propagate.len() as u64;
                if let Some(t) = t {
                    profiling::record_node(node_id, t.elapsed(), items_in, items_out, &self.name, &self.source_location);
                }
                for val in to_propagate {
                    self.propagate(py, val)?;
                }
                return Ok(None);
            }

            // Chainable variants handled above
            _ => unreachable!("chainable variant should have been handled by apply_logic_batch"),
        }

        self.propagate_batch(py, output_batch)
    }

    pub fn update(&mut self, py: Python, x: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        let profiling_active = profiling::is_profiling();
        let node_id = if profiling_active { std::ptr::from_ref(&*self) as usize } else { 0 };

        // Chainable variants: delegate to apply_logic
        // (except Filter when async check is needed — that stays here)
        let is_chainable = matches!(&self.logic,
            NodeLogic::Map { .. } | NodeLogic::Sink { .. }
            | NodeLogic::Accumulate { .. } | NodeLogic::Flatten | NodeLogic::BatchMap { .. }
            | NodeLogic::Chain { .. }
        );
        let is_chainable_filter = matches!(&self.logic, NodeLogic::Filter { .. })
            && (self.skip_async_check || self.builtin_op.is_some());

        if is_chainable || is_chainable_filter {
            match apply_logic(py, &mut self.logic, &self.builtin_op, &self.name,
                              &self.source_location, node_id, x, profiling_active)? {
                StepResult::Value(v) => return self.propagate(py, v),
                StepResult::Filtered => return Ok(None),
                StepResult::Terminal => return Ok(None),
                StepResult::Batch(items) => return self.propagate_batch(py, items),
            }
        }

        // Non-chainable variants + Filter with async check
        let mut ft = Duration::ZERO;

        let result = match &mut self.logic {
            NodeLogic::Source => Some(x),

            // Filter: async-check path (generic predicate, !skip_async_check)
            NodeLogic::Filter { predicate } => {
                // This path only runs when !skip_async_check && builtin_op.is_none()
                // (builtin ops and skip_async_check cases handled above via apply_logic)
                let t = prof_start(profiling_active);
                let res = vectorcall1(py, predicate, &x);
                if let Some(t) = t { ft = t.elapsed(); }
                let res = wrap_error(py, res, &self.name, &self.source_location)?;
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
                let passes = res.is_truthy(py)?;
                if passes { Some(x) } else { None }
            }

            NodeLogic::Collect { buffer } => {
                if profiling_active {
                    profiling::record_node(node_id, Duration::ZERO, 1, 0, &self.name, &self.source_location);
                }
                buffer.push(x);
                return Ok(None);
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
            NodeLogic::Parallel => {
                if profiling_active {
                    profiling::record_node(node_id, Duration::ZERO, 1, 1, &self.name, &self.source_location);
                }
                return self.propagate_parallel(py, x);
            }
            NodeLogic::Prefetch { .. } => {
                Some(x)
            }
            NodeLogic::PrefetchMap { state, .. } => {
                let t = prof_start(profiling_active);
                let results = state.call_method1(py, "update", (x,));
                if let Some(t) = t { ft = t.elapsed(); }
                let results = wrap_error(py, results, &self.name, &self.source_location)?;
                let results_list = results.bind(py);
                let mut items_out: u64 = 0;
                for result in results_list.try_iter()? {
                    self.propagate(py, result?.into())?;
                    items_out += 1;
                }
                if profiling_active {
                    profiling::record_node(node_id, ft, 1, items_out, &self.name, &self.source_location);
                }
                return Ok(None);
            }
            NodeLogic::PrefetchFilter { state, .. } => {
                let t = prof_start(profiling_active);
                let results = state.call_method1(py, "update", (x,));
                if let Some(t) = t { ft = t.elapsed(); }
                let results = wrap_error(py, results, &self.name, &self.source_location)?;
                let results_list = results.bind(py);
                let mut items_out: u64 = 0;
                for result in results_list.try_iter()? {
                    self.propagate(py, result?.into())?;
                    items_out += 1;
                }
                if profiling_active {
                    profiling::record_node(node_id, ft, 1, items_out, &self.name, &self.source_location);
                }
                return Ok(None);
            }
            NodeLogic::PrefetchBatchMap { state, .. } => {
                let t = prof_start(profiling_active);
                let results = state.call_method1(py, "update", (x,));
                if let Some(t) = t { ft = t.elapsed(); }
                let results = wrap_error(py, results, &self.name, &self.source_location)?;
                let results_list = results.bind(py);
                let mut items_out: u64 = 0;
                for result in results_list.try_iter()? {
                    self.propagate(py, result?.into())?;
                    items_out += 1;
                }
                if profiling_active {
                    profiling::record_node(node_id, ft, 1, items_out, &self.name, &self.source_location);
                }
                return Ok(None);
            }
            NodeLogic::AsyncMap { func, state } => {
                let t = prof_start(profiling_active);
                let coro = func.call1(py, (x,));
                if let Some(t) = t { ft = t.elapsed(); }
                let coro = wrap_error(py, coro, &self.name, &self.source_location)?;
                let results =
                    wrap_error(py, state.call_method1(py, "submit", (coro,)), &self.name, &self.source_location)?;
                let results_list = results.bind(py);
                let mut items_out: u64 = 0;
                for result in results_list.try_iter()? {
                    self.propagate(py, result?.into())?;
                    items_out += 1;
                }
                if profiling_active {
                    profiling::record_node(node_id, ft, 1, items_out, &self.name, &self.source_location);
                }
                return Ok(None);
            }

            // Chainable variants handled above
            _ => unreachable!("chainable variant should have been handled by apply_logic"),
        };

        // Record for nodes that fall through (non-early-return, non-terminal)
        if profiling_active {
            let has_output = result.is_some();
            profiling::record_node(node_id, ft, 1, if has_output { 1 } else { 0 }, &self.name, &self.source_location);
        }

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
            if use_lock {
                let batch_list = PyList::new(py, &output_batch)?;
                let safe_update_batch = get_safe_update_batch()?;
                let result = safe_update_batch.call1(py, (&self.downstreams[0], batch_list))?;
                if result.is_none(py) {
                    return Ok(None);
                }
                return Ok(Some(result));
            } else {
                let mut child = self.downstreams[0].borrow_mut(py);
                let result = child.update_batch(py, output_batch)?;
                drop(child);
                return Ok(result);
            }
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
                    let mut child_ref = child.borrow_mut(py);
                    if let Some(result) = child_ref.update(py, val.clone_ref(py))? {
                        futures.push(result);
                    }
                    drop(child_ref);
                }
                let mut child_ref = self.downstreams[n - 1].borrow_mut(py);
                if let Some(result) = child_ref.update(py, val)? {
                    futures.push(result);
                }
                drop(child_ref);
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
            // Fast path: no locking needed, use direct Rust calls
            if n == 1 {
                let mut child = self.downstreams[0].borrow_mut(py);
                let result = child.update(py, val)?;
                drop(child);
                if let Some(r) = result {
                    return Ok(Some(r));
                }
                return Ok(None);
            } else {
                for child in self.downstreams.iter().take(n - 1) {
                    let mut child_ref = child.borrow_mut(py);
                    if let Some(result) = child_ref.update(py, val.clone_ref(py))? {
                        futures.push(result);
                    }
                    drop(child_ref);
                }
                let mut child_ref = self.downstreams[n - 1].borrow_mut(py);
                if let Some(result) = child_ref.update(py, val)? {
                    futures.push(result);
                }
                drop(child_ref);
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
    pub(crate) fn propagate_parallel(
        &self,
        py: Python,
        val: Py<PyAny>,
    ) -> PyResult<Option<Py<PyAny>>> {
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
