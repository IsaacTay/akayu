//! `map(func, *args, **kwargs)` (and filter, sink, batch_map): extra
//! arguments bound after the item.
//!
//! This used to be a Python wrapper, `def wrapper(x): return func(x, *args,
//! **kwargs)`: a frame per item, and with keyword arguments a new dict per
//! item, 4x the cost of the map itself. It is now a builtin function whose
//! call is one vectorcall of `func(x, *args, **kwargs)`.

use std::ptr::null_mut;
use std::sync::OnceLock;

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use smallvec::SmallVec;

/// The bound function and its extra arguments.
#[pyclass(frozen)]
pub(crate) struct BoundCall {
    pub(crate) func: Py<PyAny>,
    /// The extra positional arguments, then the keyword values.
    extra: Vec<Py<PyAny>>,
    n_args: usize,
    kwnames: Option<Py<PyTuple>>,
}

impl BoundCall {
    /// The extra positional arguments.
    pub(crate) fn args(&self) -> &[Py<PyAny>] {
        &self.extra[..self.n_args]
    }

    pub(crate) fn has_kwargs(&self) -> bool {
        self.kwnames.is_some()
    }
}

#[pymethods]
impl BoundCall {
    fn __traverse__(&self, visit: pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        visit.call(&self.func)?;
        self.extra.iter().try_for_each(|v| visit.call(v))
    }
}

/// Bind `args` and `kwargs` after the item: `bind(f, (a,), {"k": v})(x)`
/// calls `f(x, a, k=v)`.
pub(crate) fn bind(
    py: Python,
    func: Py<PyAny>,
    args: &Bound<'_, PyTuple>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<Py<PyAny>> {
    let mut extra: Vec<Py<PyAny>> = args.iter().map(Bound::unbind).collect();
    let n_args = extra.len();
    let kwnames = match kwargs.filter(|k| !k.is_empty()) {
        None => None,
        Some(kwargs) => {
            let mut names = Vec::with_capacity(kwargs.len());
            for (name, value) in kwargs.iter() {
                names.push(name);
                extra.push(value.unbind());
            }
            Some(PyTuple::new(py, names)?.unbind())
        }
    };
    let data = Py::new(
        py,
        BoundCall {
            func,
            extra,
            n_args,
            kwnames,
        },
    )?;
    // SAFETY: GIL held; the method def lives forever; PyCFunction_NewEx takes
    // its own reference to `data` and returns a new reference or NULL.
    unsafe {
        Bound::from_owned_ptr_or_err(
            py,
            ffi::PyCFunction_NewEx(method_def(), data.as_ptr(), null_mut()),
        )
    }
    .map(Bound::unbind)
}

/// The `BoundCall` behind a callable made by `bind`, if it is one.
pub(crate) fn bound_parts<'py>(py: Python<'py>, func: &Py<PyAny>) -> Option<Bound<'py, BoundCall>> {
    // SAFETY: GIL held; a type check on a live object.
    if unsafe { ffi::PyCFunction_Check(func.as_ptr()) } == 0 {
        return None;
    }
    let slf = func
        .bind(py)
        .getattr_opt(pyo3::intern!(py, "__self__"))
        .ok()??;
    slf.cast_into::<BoundCall>().ok()
}

struct MethodDef(ffi::PyMethodDef);
// SAFETY: the def only holds pointers to static data.
unsafe impl Sync for MethodDef {}
unsafe impl Send for MethodDef {}

fn method_def() -> *mut ffi::PyMethodDef {
    static DEF: OnceLock<&'static MethodDef> = OnceLock::new();
    let def = DEF.get_or_init(|| {
        Box::leak(Box::new(MethodDef(ffi::PyMethodDef {
            ml_name: c"bound_call".as_ptr(),
            ml_meth: ffi::PyMethodDefPointer {
                PyCFunctionFastWithKeywords: call_bound,
            },
            ml_flags: ffi::METH_FASTCALL | ffi::METH_KEYWORDS,
            ml_doc: std::ptr::null(),
        })))
    });
    std::ptr::from_ref(&def.0).cast_mut()
}

/// METH_FASTCALL | METH_KEYWORDS entry point: `slf` is the `BoundCall`.
unsafe extern "C" fn call_bound(
    slf: *mut ffi::PyObject,
    args: *const *mut ffi::PyObject,
    nargs: ffi::Py_ssize_t,
    kwnames: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    // SAFETY: CPython calls this with the GIL held.
    let py = unsafe { Python::assume_attached() };
    if nargs != 1 || !kwnames.is_null() {
        pyo3::exceptions::PyTypeError::new_err(
            "a bound call takes exactly one positional argument",
        )
        .restore(py);
        return null_mut();
    }
    // SAFETY: `slf` is the BoundCall this function was created with in `bind`.
    let data = unsafe { Bound::from_borrowed_ptr(py, slf).cast_into_unchecked::<BoundCall>() };
    let call = data.get();
    let mut stack: SmallVec<[*mut ffi::PyObject; 8]> =
        SmallVec::with_capacity(1 + call.extra.len());
    // SAFETY: nargs == 1, so args[0] is the item.
    stack.push(unsafe { *args });
    stack.extend(call.extra.iter().map(Py::as_ptr));
    let kwnames = call.kwnames.as_ref().map_or(null_mut(), Py::as_ptr);
    // SAFETY: GIL held; every pointer in `stack` is kept alive by the caller or `call`.
    unsafe {
        ffi::PyObject_Vectorcall(call.func.as_ptr(), stack.as_ptr(), 1 + call.n_args, kwnames)
    }
}
