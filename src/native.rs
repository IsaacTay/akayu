//! Native operations: what compile() runs instead of calling a recognised
//! function (`BuiltinOp`), and the C-API helpers the steps share.

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyList, PyTuple};

use crate::helpers::vectorcall1;
use crate::node::{BinaryOp, BuiltinOp, MethodCall, UnaryOp};

/// The truth of a predicate's result. Released as a `Bound`: dropping a
/// `Py` looks up pyo3's thread-local GIL count.
#[inline(always)]
pub(crate) fn truth(py: Python, result: Py<PyAny>) -> PyResult<bool> {
    result.into_bound(py).is_truthy()
}

/// Everything `obj` iterates over, appended to `out` (flatten's items).
pub(crate) fn iter_into(obj: &Bound<'_, PyAny>, out: &mut Vec<Py<PyAny>>) -> PyResult<()> {
    for item in obj.try_iter()? {
        out.push(item?.unbind());
    }
    Ok(())
}

/// Everything `obj` iterates over (flatten's item, batch_map's result).
pub(crate) fn iter_all(obj: &Bound<'_, PyAny>) -> PyResult<Vec<Py<PyAny>>> {
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
pub(crate) fn list_append(py: Python, list: &Py<PyAny>, x: &Py<PyAny>) -> PyResult<()> {
    // SAFETY: GIL held; `list` is an exact list (checked by compile()).
    if unsafe { pyo3::ffi::PyList_Append(list.as_ptr(), x.as_ptr()) } != 0 {
        return Err(PyErr::fetch(py));
    }
    Ok(())
}

/// `x.name(*args)`: an item of a builtin type compile() resolved the method
/// for calls it directly, anything else looks it up by name.
pub(crate) fn method_call(py: Python, call: &MethodCall, x: &Py<PyAny>) -> PyResult<Py<PyAny>> {
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
pub(crate) fn star_call(py: Python, func: &Py<PyAny>, x: &Py<PyAny>) -> PyResult<Py<PyAny>> {
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
pub(crate) fn binary_op(
    py: Python,
    op: &BinaryOp,
    a: &Py<PyAny>,
    b: &Py<PyAny>,
) -> PyResult<Py<PyAny>> {
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
            BinaryOp::LShift => ffi::PyNumber_Lshift(a, b),
            BinaryOp::RShift => ffi::PyNumber_Rshift(a, b),
            BinaryOp::And => ffi::PyNumber_And(a, b),
            BinaryOp::Or => ffi::PyNumber_Or(a, b),
            BinaryOp::Xor => ffi::PyNumber_Xor(a, b),
        }
    };
    // SAFETY: as above.
    unsafe { Bound::from_owned_ptr_or_err(py, result) }.map(Bound::unbind)
}

/// `-x`, `+x`, `~x` or `abs(x)` through the number protocol, as the bytecode
/// and builtin do.
pub(crate) fn unary_op(py: Python, op: UnaryOp, x: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    use pyo3::ffi;
    let x = x.as_ptr();
    // SAFETY: GIL held; `x` is live; each returns a new reference or NULL.
    let result = unsafe {
        match op {
            UnaryOp::Neg => ffi::PyNumber_Negative(x),
            UnaryOp::Pos => ffi::PyNumber_Positive(x),
            UnaryOp::Invert => ffi::PyNumber_Invert(x),
            UnaryOp::Abs => ffi::PyNumber_Absolute(x),
        }
    };
    // SAFETY: as above.
    unsafe { Bound::from_owned_ptr_or_err(py, result) }.map(Bound::unbind)
}

/// The next accumulate state from a recognised accumulate function, or
/// `None` for ops that aren't accumulate patterns.
pub(crate) fn apply_accumulate(
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
pub(crate) fn apply_builtin(py: Python, op: &BuiltinOp, x: &Py<PyAny>) -> PyResult<Py<PyAny>> {
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
        BuiltinOp::RBinOp(bin_op, lhs) => binary_op(py, bin_op, lhs, x),
        BuiltinOp::BinOpOf(bin_op, left, right) => {
            let a = apply_builtin(py, left, x)?;
            let b = apply_builtin(py, right, x)?;
            binary_op(py, bin_op, &a, &b)
        }
        BuiltinOp::Item => Ok(x.clone_ref(py)),
        BuiltinOp::Unary(op) => unary_op(py, *op, x),
        BuiltinOp::GetItemFrom(container) => unsafe {
            Bound::from_owned_ptr_or_err(
                py,
                pyo3::ffi::PyObject_GetItem(container.as_ptr(), x.as_ptr()),
            )
            .map(Bound::unbind)
        },
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
        BuiltinOp::RCompare(cmp_op, lhs) => unsafe {
            Bound::from_owned_ptr_or_err(
                py,
                pyo3::ffi::PyObject_RichCompare(lhs.as_ptr(), x.as_ptr(), cmp_op.to_ffi()),
            )
            .map(Bound::unbind)
        },
        BuiltinOp::CompareOf(cmp_op, left, right) => {
            let a = apply_builtin(py, left, x)?;
            let b = apply_builtin(py, right, x)?;
            unsafe {
                Bound::from_owned_ptr_or_err(
                    py,
                    pyo3::ffi::PyObject_RichCompare(a.as_ptr(), b.as_ptr(), cmp_op.to_ffi()),
                )
                .map(Bound::unbind)
            }
        }
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
pub(crate) fn apply_builtin_as_filter(py: Python, op: &BuiltinOp, x: &Py<PyAny>) -> PyResult<bool> {
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
