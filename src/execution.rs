//! Stream execution logic (update, propagate)

use std::time::{Duration, Instant};

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyList, PyTuple};

use crate::Stream;
use crate::helpers::{cached_vectorcall1, vectorcall1, vectorcall2, wrap_error};
use crate::node::{
    BinaryOp, BuiltinOp, ChainNode, ChunkByState, ChunkOrder, CombineLatestState,
    ExtractBetweenState, MethodCall, NodeLogic,
};
use crate::numeric::RunOutcome;
use crate::pool::{self, Latch};
use crate::profiling;
use std::sync::Arc;

/// Check for pending signals (e.g. KeyboardInterrupt).
/// Cost: ~7ns when no signal pending (atomic flag read).
#[inline(always)]
fn check_signals(py: Python) -> PyResult<()> {
    unsafe {
        if pyo3::ffi::PyErr_CheckSignals() == -1 {
            return Err(PyErr::fetch(py));
        }
    }
    Ok(())
}

/// Start a profiling timer if profiling is active.
#[inline(always)]
fn prof_start(profiling: bool) -> Option<Instant> {
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
        NodeCtx {
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
    crate::helpers::with_node_mut(target.bind(py), py, |node| node.flush_guarded(py, true))
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
fn extract_between_step(
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
fn zip_push(
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
fn combine_latest_push(
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

/// The truth of a predicate's result. Released as a `Bound`: dropping a
/// `Py` looks up pyo3's thread-local GIL count.
#[inline(always)]
fn truth(py: Python, result: Py<PyAny>) -> PyResult<bool> {
    result.into_bound(py).is_truthy()
}

/// Everything `obj` iterates over, appended to `out` (flatten's items).
fn iter_into(obj: &Bound<'_, PyAny>, out: &mut Vec<Py<PyAny>>) -> PyResult<()> {
    for item in obj.try_iter()? {
        out.push(item?.unbind());
    }
    Ok(())
}

/// Everything `obj` iterates over (flatten's item, batch_map's result).
fn iter_all(obj: &Bound<'_, PyAny>) -> PyResult<Vec<Py<PyAny>>> {
    // A loop, not `collect()`: that asks for a size hint, which calls
    // `__length_hint__` through a bound method made for the purpose.
    let mut items = Vec::new();
    for item in obj.try_iter()? {
        items.push(item?.unbind());
    }
    Ok(items)
}

/// `list.append(x)` for an exact list, without the method call.
#[inline]
fn list_append(py: Python, list: &Py<PyAny>, x: &Py<PyAny>) -> PyResult<()> {
    // SAFETY: GIL held; `list` is an exact list (checked by compile()).
    if unsafe { pyo3::ffi::PyList_Append(list.as_ptr(), x.as_ptr()) } != 0 {
        return Err(PyErr::fetch(py));
    }
    Ok(())
}

/// `x.name(*args)`: an item of a builtin type compile() resolved the method
/// for calls it directly, anything else looks it up by name.
fn method_call(py: Python, call: &MethodCall, x: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    let mut argv = [x.as_ptr(); 3];
    for (slot, arg) in argv[1..].iter_mut().zip(&call.args) {
        *slot = arg.as_ptr();
    }
    let nargs = 1 + call.args.len();
    // SAFETY: GIL held; argv holds `x` and the call's live arguments; both
    // calls return a new reference, or NULL with an exception set.
    unsafe {
        let tp = pyo3::ffi::Py_TYPE(x.as_ptr()).cast::<pyo3::ffi::PyObject>();
        let result = match call.direct.iter().find(|(t, _)| t.as_ptr() == tp) {
            Some((_, method)) => pyo3::ffi::PyObject_Vectorcall(
                method.as_ptr(),
                argv.as_ptr(),
                nargs,
                std::ptr::null_mut(),
            ),
            None => pyo3::ffi::PyObject_VectorcallMethod(
                call.name.as_ptr(),
                argv.as_ptr(),
                nargs,
                std::ptr::null_mut(),
            ),
        };
        Bound::from_owned_ptr_or_err(py, result).map(Bound::unbind)
    }
}

/// `func(*x)`. A tuple is passed through as the argument tuple; anything else
/// is converted to one first (raising like `*x` for non-iterables).
fn star_call(py: Python, func: &Py<PyAny>, x: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    let args = x.bind(py);
    let args = match args.cast::<PyTuple>() {
        Ok(tuple) => tuple.clone(),
        Err(_) => args
            .try_iter()?
            .collect::<PyResult<Vec<_>>>()
            .and_then(|v| PyTuple::new(py, v))?,
    };
    // SAFETY: GIL held; func and args are live; returns a new reference or NULL.
    unsafe {
        Bound::from_owned_ptr_or_err(
            py,
            pyo3::ffi::PyObject_Call(func.as_ptr(), args.as_ptr(), std::ptr::null_mut()),
        )
    }
    .map(Bound::unbind)
}

/// `a <op> b` through the number protocol, as the `BINARY_OP` bytecode does.
fn binary_op(py: Python, op: &BinaryOp, a: &Py<PyAny>, b: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    use pyo3::ffi;
    let (a, b) = (a.as_ptr(), b.as_ptr());
    // SAFETY: GIL held; both operands are live; each returns a new reference or NULL.
    let result = unsafe {
        match op {
            BinaryOp::Add => ffi::PyNumber_Add(a, b),
            BinaryOp::Sub => ffi::PyNumber_Subtract(a, b),
            BinaryOp::Mul => ffi::PyNumber_Multiply(a, b),
            BinaryOp::TrueDiv => ffi::PyNumber_TrueDivide(a, b),
            BinaryOp::FloorDiv => ffi::PyNumber_FloorDivide(a, b),
            BinaryOp::Mod => ffi::PyNumber_Remainder(a, b),
            BinaryOp::Pow => ffi::PyNumber_Power(a, b, ffi::Py_None()),
        }
    };
    // SAFETY: as above.
    unsafe { Bound::from_owned_ptr_or_err(py, result) }.map(Bound::unbind)
}

/// The next accumulate state from a recognised accumulate function, or
/// `None` for ops that aren't accumulate patterns.
fn apply_accumulate(
    py: Python,
    op: &BuiltinOp,
    state: &Py<PyAny>,
    x: &Py<PyAny>,
) -> Option<PyResult<Py<PyAny>>> {
    Some(match op {
        BuiltinOp::AccumBinOp(bin_op) => binary_op(py, bin_op, state, x),
        BuiltinOp::AccumBinOpConst(bin_op, c) => binary_op(py, bin_op, state, c),
        // As builtins.max(state, x): x replaces state only if strictly greater.
        BuiltinOp::AccumMax => x
            .bind(py)
            .gt(state.bind(py))
            .map(|greater| if greater { x } else { state }.clone_ref(py)),
        BuiltinOp::AccumMin => x
            .bind(py)
            .lt(state.bind(py))
            .map(|less| if less { x } else { state }.clone_ref(py)),
        _ => return None,
    })
}

/// Apply a `BuiltinOp` to a single value, returning the result.
///
/// This is the recursive workhorse for Chain, And, Or, Negate.
/// For leaf ops it performs the same C-API calls as the inline match arms
/// in `apply_logic`, but factored out so compound ops can compose them.
#[inline]
fn apply_builtin(py: Python, op: &BuiltinOp, x: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    match op {
        BuiltinOp::Len => {
            let len = x.bind(py).len()?;
            Ok(len.into_pyobject(py)?.into_any().unbind())
        }
        BuiltinOp::Str => Ok(x.bind(py).str()?.into_any().unbind()),
        BuiltinOp::Int => unsafe {
            let ptr = pyo3::ffi::PyNumber_Long(x.as_ptr());
            if ptr.is_null() {
                return Err(PyErr::fetch(py));
            }
            Ok(Py::from_owned_ptr(py, ptr))
        },
        BuiltinOp::Float => unsafe {
            let ptr = pyo3::ffi::PyNumber_Float(x.as_ptr());
            if ptr.is_null() {
                return Err(PyErr::fetch(py));
            }
            Ok(Py::from_owned_ptr(py, ptr))
        },
        BuiltinOp::Bool => {
            let r = unsafe { pyo3::ffi::PyObject_IsTrue(x.as_ptr()) };
            if r == -1 {
                return Err(PyErr::fetch(py));
            }
            Ok(PyBool::new(py, r != 0).to_owned().into_any().unbind())
        }
        BuiltinOp::Not => {
            let r = unsafe { pyo3::ffi::PyObject_IsTrue(x.as_ptr()) };
            if r == -1 {
                return Err(PyErr::fetch(py));
            }
            Ok(PyBool::new(py, r == 0).to_owned().into_any().unbind())
        }
        BuiltinOp::Truth => {
            let r = unsafe { pyo3::ffi::PyObject_IsTrue(x.as_ptr()) };
            if r == -1 {
                return Err(PyErr::fetch(py));
            }
            Ok(PyBool::new(py, r != 0).to_owned().into_any().unbind())
        }
        BuiltinOp::ItemGetter(key) => Ok(x.bind(py).get_item(key)?.unbind()),
        BuiltinOp::AttrGetter(attr_name) => Ok(x.bind(py).getattr(attr_name)?.unbind()),
        BuiltinOp::IsInstance(type_obj) => {
            let r = x.bind(py).is_instance(type_obj.bind(py))?;
            Ok(PyBool::new(py, r).to_owned().into_any().unbind())
        }
        BuiltinOp::Is(sentinel) => Ok(PyBool::new(py, x.is(sentinel))
            .to_owned()
            .into_any()
            .unbind()),
        BuiltinOp::IsNot(sentinel) => Ok(PyBool::new(py, !x.is(sentinel))
            .to_owned()
            .into_any()
            .unbind()),
        BuiltinOp::Print => {
            let print_fn = crate::helpers::get_builtin_print()?;
            print_fn.call1(py, (x,))?;
            Ok(py.None())
        }
        BuiltinOp::Type => Ok(x.bind(py).get_type().into_any().unbind()),
        BuiltinOp::HasAttr(attr_name) => {
            let r = unsafe { pyo3::ffi::PyObject_HasAttr(x.as_ptr(), attr_name.as_ptr()) };
            Ok(PyBool::new(py, r == 1).to_owned().into_any().unbind())
        }
        BuiltinOp::Tuple => unsafe {
            let ptr = pyo3::ffi::PySequence_Tuple(x.as_ptr());
            if ptr.is_null() {
                return Err(PyErr::fetch(py));
            }
            Ok(Py::from_owned_ptr(py, ptr))
        },
        BuiltinOp::List => unsafe {
            let ptr = pyo3::ffi::PySequence_List(x.as_ptr());
            if ptr.is_null() {
                return Err(PyErr::fetch(py));
            }
            Ok(Py::from_owned_ptr(py, ptr))
        },
        BuiltinOp::Set => unsafe {
            let ptr = pyo3::ffi::PySet_New(x.as_ptr());
            if ptr.is_null() {
                return Err(PyErr::fetch(py));
            }
            Ok(Py::from_owned_ptr(py, ptr))
        },
        BuiltinOp::FrozenSet => unsafe {
            let ptr = pyo3::ffi::PyFrozenSet_New(x.as_ptr());
            if ptr.is_null() {
                return Err(PyErr::fetch(py));
            }
            Ok(Py::from_owned_ptr(py, ptr))
        },

        // --- Phase 2 variants ---
        BuiltinOp::BinOp(bin_op, rhs) => binary_op(py, bin_op, x, rhs),
        BuiltinOp::Numeric(program, generic) => match program.run(py, x) {
            Some(v) => Ok(v),
            None => apply_builtin(py, generic, x),
        },
        // The comparison's own result, as `x < c` gives it: not
        // RichCompareBool, which treats identical objects as equal and turns
        // any result (a numpy array, a query expression) into a bool.
        BuiltinOp::Compare(cmp_op, rhs) => unsafe {
            Bound::from_owned_ptr_or_err(
                py,
                pyo3::ffi::PyObject_RichCompare(x.as_ptr(), rhs.as_ptr(), cmp_op.to_ffi()),
            )
            .map(Bound::unbind)
        },
        BuiltinOp::Contains(coll) => {
            let r = unsafe { pyo3::ffi::PySequence_Contains(coll.as_ptr(), x.as_ptr()) };
            if r == -1 {
                return Err(PyErr::fetch(py));
            }
            Ok(PyBool::new(py, r != 0).to_owned().into_any().unbind())
        }
        BuiltinOp::NotContains(coll) => {
            let r = unsafe { pyo3::ffi::PySequence_Contains(coll.as_ptr(), x.as_ptr()) };
            if r == -1 {
                return Err(PyErr::fetch(py));
            }
            Ok(PyBool::new(py, r == 0).to_owned().into_any().unbind())
        }
        BuiltinOp::MethodCall(call) => method_call(py, call, x),
        BuiltinOp::Eager(eager) if eager.current() => apply_builtin(py, &eager.op, x),
        BuiltinOp::Eager(eager) => vectorcall1(py, &eager.func, x),
        BuiltinOp::Slice(start, stop) => unsafe {
            let slice = pyo3::ffi::PySlice_New(start.as_ptr(), stop.as_ptr(), std::ptr::null_mut());
            if slice.is_null() {
                return Err(PyErr::fetch(py));
            }
            let result = pyo3::ffi::PyObject_GetItem(x.as_ptr(), slice);
            pyo3::ffi::Py_DECREF(slice);
            if result.is_null() {
                return Err(PyErr::fetch(py));
            }
            Ok(Py::from_owned_ptr(py, result))
        },
        BuiltinOp::TupleConstruct(ops) => {
            let mut elems = Vec::with_capacity(ops.len());
            for op in ops {
                elems.push(apply_builtin(py, op, x)?);
            }
            Ok(PyTuple::new(py, &elems)?.into_any().unbind())
        }
        BuiltinOp::ListConstruct(ops) => {
            let mut elems = Vec::with_capacity(ops.len());
            for op in ops {
                elems.push(apply_builtin(py, op, x)?);
            }
            Ok(PyList::new(py, &elems)?.into_any().unbind())
        }
        BuiltinOp::Chain(first, second) => {
            let mid = apply_builtin(py, first, x)?;
            apply_builtin(py, second, &mid)
        }
        BuiltinOp::Negate(inner) => {
            let result = apply_builtin(py, inner, x)?;
            let r = unsafe { pyo3::ffi::PyObject_IsTrue(result.as_ptr()) };
            if r == -1 {
                return Err(PyErr::fetch(py));
            }
            Ok(PyBool::new(py, r == 0).to_owned().into_any().unbind())
        }
        BuiltinOp::And(a, b) => {
            let ra = apply_builtin(py, a, x)?;
            let truthy = unsafe { pyo3::ffi::PyObject_IsTrue(ra.as_ptr()) };
            if truthy == -1 {
                return Err(PyErr::fetch(py));
            }
            if truthy == 0 {
                Ok(ra)
            } else {
                apply_builtin(py, b, x)
            }
        }
        BuiltinOp::Or(a, b) => {
            let ra = apply_builtin(py, a, x)?;
            let truthy = unsafe { pyo3::ffi::PyObject_IsTrue(ra.as_ptr()) };
            if truthy == -1 {
                return Err(PyErr::fetch(py));
            }
            if truthy != 0 {
                Ok(ra)
            } else {
                apply_builtin(py, b, x)
            }
        }
        // Detected only for accumulate nodes, which use apply_accumulate.
        BuiltinOp::AccumBinOp(_)
        | BuiltinOp::AccumBinOpConst(..)
        | BuiltinOp::AccumMax
        | BuiltinOp::AccumMin
        | BuiltinOp::ListAppend(_) => Err(crate::error::exc::StreamError::new_err(
            "akayu internal error: accumulate/sink op applied to a single value",
        )),
    }
}

/// Apply a `BuiltinOp` as a filter predicate, returning whether the item passes.
#[inline]
fn apply_builtin_as_filter(py: Python, op: &BuiltinOp, x: &Py<PyAny>) -> PyResult<bool> {
    // For certain ops we can skip the intermediate PyObject and go straight to bool
    match op {
        BuiltinOp::IsInstance(type_obj) => x.bind(py).is_instance(type_obj.bind(py)),
        BuiltinOp::Is(sentinel) => Ok(x.is(sentinel)),
        BuiltinOp::IsNot(sentinel) => Ok(!x.is(sentinel)),
        BuiltinOp::HasAttr(attr_name) => {
            let r = unsafe { pyo3::ffi::PyObject_HasAttr(x.as_ptr(), attr_name.as_ptr()) };
            Ok(r == 1)
        }
        BuiltinOp::And(a, b) => {
            if !apply_builtin_as_filter(py, a, x)? {
                Ok(false)
            } else {
                apply_builtin_as_filter(py, b, x)
            }
        }
        BuiltinOp::Or(a, b) => {
            if apply_builtin_as_filter(py, a, x)? {
                Ok(true)
            } else {
                apply_builtin_as_filter(py, b, x)
            }
        }
        BuiltinOp::Negate(inner) => Ok(!apply_builtin_as_filter(py, inner, x)?),
        BuiltinOp::Numeric(program, generic) => match program.truth(x) {
            Some(keep) => Ok(keep),
            None => apply_builtin_as_filter(py, generic, x),
        },
        BuiltinOp::Eager(eager) if eager.current() => apply_builtin_as_filter(py, &eager.op, x),
        _ => {
            let result = apply_builtin(py, op, x)?;
            result.is_truthy(py)
        }
    }
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
                for x in &items {
                    sink_one(py, func, ctx, x)?;
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

/// (the chunk an item completed, the item to buffer after it went on)
type ChunkStep = (Option<Py<PyAny>>, Option<Py<PyAny>>);

/// One item into a chunk_by node, up to the chunk it completes: (chunk,
/// item to buffer after). The caller buffers that item (`chunk_buffer`)
/// once the chunk has gone downstream (see `ChunkOrder`).
#[inline(always)]
fn chunk_by_item(
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
fn chunk_buffer(py: Python, state: &mut ChunkByState, x: Py<PyAny>) -> PyResult<()> {
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
            let _guard = child.lock.acquire(py);
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
            let _guard = child.lock.acquire(py);
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
