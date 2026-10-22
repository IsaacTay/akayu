//! Native numpy expressions on small float64 arrays.
//!
//! numpy costs a few hundred ns to a few µs per call whatever the array's
//! size, so a map over 3x3 rotations or 3-vectors spends nearly all its time
//! in numpy's dispatch: checking for overrides, resolving dtypes, setting up
//! an iterator, allocating each result. When compile() finds a lambda made of
//! numpy operations it knows (`C @ r`, `a.T @ b`, `v * dt`,
//! `np.linalg.norm(v)`, `np.arccos(np.clip((np.trace(r) - 1) / 2, -1, 1))`,
//! `np.frombuffer(b).reshape(3, 3)`, ...), the analyzer also translates it
//! into a `Program`. Per item, the program reads the arrays in place and runs
//! every step with numpy's own inner loop: the loop numpy itself would pick
//! on this machine (from `ufunc._get_strided_loop`, numpy's hook for numba),
//! or the dtype's dot function for norms. Intermediate results stay in
//! buffers on the stack, and only the result becomes a Python object.
//!
//! Results must equal numpy's exactly: the values (numpy's own loops compute
//! them, so BLAS and SIMD choices are numpy's), the type, shape and memory
//! order, and for a view its base and flags. The program only handles what it
//! can do identically, and returns None otherwise; the caller then runs the
//! function (or the item's ordinary native op), which is safe because the
//! program has no effects. None is returned for: anything but exact ndarrays
//! of float64 (aligned, non-negative strides, 1 or 2 dimensions, 1 to 16
//! elements), np.float64 and float scalars, and bytes for np.frombuffer;
//! shapes that would broadcast; 2-D operands of different memory orders; a
//! NaN operand of scalar arithmetic (which of two NaNs comes back is up to
//! the compiler); float division by zero; and any floating-point error flag
//! an op raises, as numpy may warn or raise for it (np.errstate).
//!
//! The layouts read here are numpy 2's C ABI; on another numpy, or if
//! `_get_strided_loop`'s capsule changes, no program is built.

use std::ffi::{c_char, c_int, c_void};
use std::mem::MaybeUninit;

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyString, PyTuple};

use crate::node::BuiltinOp;

type Intp = isize;

/// numpy's `PyArrayMethod_StridedLoop`.
type StridedLoop = unsafe extern "C" fn(
    *mut c_void,
    *const *mut c_char,
    *const Intp,
    *const Intp,
    *mut c_void,
) -> c_int;
/// numpy's `PyArray_DotFunc`.
type DotFunc =
    unsafe extern "C" fn(*mut c_char, Intp, *mut c_char, Intp, *mut c_char, Intp, *mut c_void);
type NewFromDescr = unsafe extern "C" fn(
    *mut ffi::PyTypeObject,
    *mut ffi::PyObject,
    c_int,
    *const Intp,
    *const Intp,
    *mut c_void,
    c_int,
    *mut ffi::PyObject,
) -> *mut ffi::PyObject;
type SetBaseObject = unsafe extern "C" fn(*mut ffi::PyObject, *mut ffi::PyObject) -> c_int;

/// The start of numpy 2's `PyArrayObject_fields`.
#[repr(C)]
struct ArrayFields {
    ob_base: ffi::PyObject,
    data: *mut c_char,
    nd: c_int,
    dimensions: *const Intp,
    strides: *const Intp,
    base: *mut ffi::PyObject,
    descr: *mut ffi::PyObject,
    flags: c_int,
}

/// numpy's `ufunc_call_info`, from `ufunc._resolve_dtypes_and_context` and
/// filled in by `ufunc._get_strided_loop`.
#[repr(C)]
struct CallInfo {
    strided_loop: Option<StridedLoop>,
    context: *mut c_void,
    auxdata: *mut c_void,
    requires_pyapi: u8,
    no_floatingpoint_errors: u8,
}

const CALL_INFO: &std::ffi::CStr = c"numpy_1.24_ufunc_call_info";
const ABI_VERSION: u32 = 0x0200_0000;
const NPY_DOUBLE: c_int = 12;
const C_CONTIGUOUS: c_int = 0x0001;
const F_CONTIGUOUS: c_int = 0x0002;
const OWNDATA: c_int = 0x0004;
const ALIGNED: c_int = 0x0100;
const WRITEBACKIFCOPY: c_int = 0x2000;
/// `PyArray_ArrFuncs.dotfunc`: after the 21 casts and six other functions.
const DOTFUNC_SLOT: usize = 27;

/// Elements in an array a program works on.
const MAX_SIZE: usize = 16;
/// Ops in a program, and so buffers for their results.
const MAX_OPS: usize = 24;
/// Values on a program's stack.
const MAX_DEPTH: usize = 8;

/// The ufuncs programs call, by numpy name, with their number of inputs.
/// The first seven are what operators and the other ops use.
const UFUNCS: &[(&str, usize)] = &[
    ("add", 2),
    ("subtract", 2),
    ("multiply", 2),
    ("divide", 2),
    ("negative", 1),
    ("matmul", 2),
    ("clip", 3),
    ("sqrt", 1),
    ("cbrt", 1),
    ("square", 1),
    ("reciprocal", 1),
    ("absolute", 1),
    ("fabs", 1),
    ("sign", 1),
    ("positive", 1),
    ("degrees", 1),
    ("radians", 1),
    ("rad2deg", 1),
    ("deg2rad", 1),
    ("sin", 1),
    ("cos", 1),
    ("tan", 1),
    ("arcsin", 1),
    ("arccos", 1),
    ("arctan", 1),
    ("sinh", 1),
    ("cosh", 1),
    ("tanh", 1),
    ("arcsinh", 1),
    ("arccosh", 1),
    ("arctanh", 1),
    ("exp", 1),
    ("exp2", 1),
    ("expm1", 1),
    ("log", 1),
    ("log2", 1),
    ("log10", 1),
    ("log1p", 1),
    ("floor", 1),
    ("ceil", 1),
    ("trunc", 1),
    ("rint", 1),
    ("arctan2", 2),
    ("hypot", 2),
    ("maximum", 2),
    ("minimum", 2),
    ("fmax", 2),
    ("fmin", 2),
    ("power", 2),
    ("copysign", 2),
];
const ADD: u8 = 0;
const SUBTRACT: u8 = 1;
const MULTIPLY: u8 = 2;
const DIVIDE: u8 = 3;
const NEGATIVE: u8 = 4;
const MATMUL: u8 = 5;
const CLIP: u8 = 6;
const SQRT: u8 = 7;

/// A ufunc's float64 loop.
struct Loop {
    func: StridedLoop,
    info: *const CallInfo,
    /// Owns `info` (and the loop's data, which numpy ties to it).
    _capsule: Py<PyAny>,
}

/// What programs use of numpy's C API.
struct Numpy {
    ndarray: *mut ffi::PyTypeObject,
    float64: *mut ffi::PyTypeObject,
    /// The float64 dtype (a reference kept for good).
    f8: *mut ffi::PyObject,
    new_from_descr: NewFromDescr,
    set_base_object: SetBaseObject,
    clear_fpe: unsafe extern "C" fn(),
    /// Returns the flags raised, and clears them.
    get_fpe: unsafe extern "C" fn() -> c_int,
    dot: DotFunc,
    /// By `UFUNCS` index; None where numpy has no float64 loop.
    loops: Box<[Option<Loop>]>,
}

// SAFETY: only used with the GIL held (programs run in steps, which hold it).
unsafe impl Send for Numpy {}
unsafe impl Sync for Numpy {}

static NUMPY: PyOnceLock<Option<Numpy>> = PyOnceLock::new();

/// numpy's C API, bound the first time a program is built (numpy is
/// imported by then: the analyzer only translates once it is). None on a
/// numpy whose layouts differ from numpy 2's.
fn numpy(py: Python) -> Option<&'static Numpy> {
    NUMPY.get_or_init(py, || bind(py).ok().flatten()).as_ref()
}

/// Whether numpy has been imported (without importing it): only then can
/// an item be an ndarray, and the analyzer translate numpy expressions.
pub(crate) fn numpy_imported(py: Python) -> bool {
    // SAFETY: GIL held; a borrowed lookup in sys.modules (NULL if absent).
    unsafe {
        let modules = ffi::PyImport_GetModuleDict();
        let module = ffi::PyDict_GetItemWithError(modules, pyo3::intern!(py, "numpy").as_ptr());
        if module.is_null() {
            ffi::PyErr_Clear();
        }
        !module.is_null()
    }
}

/// A numpy C API table from its capsule.
fn api_table(module: &Bound<'_, PyAny>, name: &str) -> PyResult<*const *mut c_void> {
    let capsule = module.getattr(name)?;
    // SAFETY: GIL held; numpy's API capsules have no name.
    let table = unsafe { ffi::PyCapsule_GetPointer(capsule.as_ptr(), std::ptr::null()) };
    if table.is_null() {
        return Err(PyErr::fetch(module.py()));
    }
    Ok(table.cast())
}

/// Entry `i` of a numpy C API table, as the function its headers declare.
///
/// # Safety
/// `table` is a numpy API table whose entry `i` is a function of type `F`.
unsafe fn entry<F: Copy>(table: *const *mut c_void, i: usize) -> F {
    const { assert!(size_of::<F>() == size_of::<*mut c_void>()) };
    // SAFETY: the caller's; a function pointer the size of a data pointer.
    unsafe { std::mem::transmute_copy(&*table.add(i)) }
}

fn bind(py: Python) -> PyResult<Option<Numpy>> {
    type DescrFromType = unsafe extern "C" fn(c_int) -> *mut ffi::PyObject;
    type ArrFuncs = unsafe extern "C" fn(*mut ffi::PyObject) -> *const *mut c_void;
    let umath = py.import("numpy._core._multiarray_umath")?.into_any();
    let array_api = api_table(&umath, "_ARRAY_API")?;
    let ufunc_api = api_table(&umath, "_UFUNC_API")?;
    // SAFETY: GIL held; the tables' entries are numpy's functions and types,
    // at the indexes numpy 2's headers give them (checked by its ABI version).
    unsafe {
        if entry::<unsafe extern "C" fn() -> u32>(array_api, 0)() != ABI_VERSION {
            return Ok(None);
        }
        let f8 = entry::<DescrFromType>(array_api, 45)(NPY_DOUBLE);
        if f8.is_null() {
            return Err(PyErr::fetch(py));
        }
        let arrfuncs = entry::<ArrFuncs>(array_api, 365)(f8);
        if (*arrfuncs.add(DOTFUNC_SLOT)).is_null() {
            return Ok(None);
        }
        let f8_obj = Bound::from_borrowed_ptr(py, f8);
        let loops = UFUNCS
            .iter()
            .map(|(name, nin)| float64_loop(&umath, &f8_obj, name, *nin).ok())
            .collect();
        Ok(Some(Numpy {
            ndarray: (*array_api.add(2)).cast(),
            float64: (*array_api.add(31)).cast(),
            f8,
            new_from_descr: entry(array_api, 94),
            set_base_object: entry(array_api, 282),
            clear_fpe: entry(ufunc_api, 27),
            get_fpe: entry(ufunc_api, 28),
            dot: entry(arrfuncs, DOTFUNC_SLOT),
            loops,
        }))
    }
}

/// The loop numpy runs for `name` on float64 operands, from the private
/// hook numpy keeps for numba.
fn float64_loop(
    umath: &Bound<'_, PyAny>,
    f8: &Bound<'_, PyAny>,
    name: &str,
    nin: usize,
) -> PyResult<Loop> {
    let py = umath.py();
    let ufunc = umath.getattr(name)?;
    let mut dtypes = vec![f8.clone(); nin];
    dtypes.push(py.None().into_bound(py));
    let resolved =
        ufunc.call_method1("_resolve_dtypes_and_context", (PyTuple::new(py, dtypes)?,))?;
    let (dtypes, capsule): (Bound<PyTuple>, Bound<PyAny>) = resolved.extract()?;
    // Every operand float64, the output included.
    for dtype in dtypes.iter() {
        if !dtype.is(f8) {
            return Err(pyo3::exceptions::PyTypeError::new_err("not a float64 loop"));
        }
    }
    ufunc.call_method1("_get_strided_loop", (&capsule,))?;
    // SAFETY: GIL held; the capsule's name says its layout (`CallInfo`).
    let info = unsafe { ffi::PyCapsule_GetPointer(capsule.as_ptr(), CALL_INFO.as_ptr()) };
    if info.is_null() {
        return Err(PyErr::fetch(py));
    }
    let info: *const CallInfo = info.cast();
    // SAFETY: as above; filled in by _get_strided_loop.
    let func = unsafe { (*info).strided_loop }
        .ok_or_else(|| pyo3::exceptions::PyTypeError::new_err("no loop"))?;
    Ok(Loop {
        func,
        info,
        _capsule: capsule.unbind(),
    })
}

impl Loop {
    /// Run the loop over `dims[0]` elements (a gufunc's core dimensions follow).
    ///
    /// # Safety
    /// GIL held; `data` and `strides` describe valid float64 operands and
    /// output for the loop's arity and `dims`.
    unsafe fn call(&self, data: &[*mut c_char], dims: &[Intp], strides: &[Intp]) -> Option<()> {
        // SAFETY: the caller's.
        let info = unsafe { &*self.info };
        let status = unsafe {
            (self.func)(
                info.context,
                data.as_ptr(),
                dims.as_ptr(),
                strides.as_ptr(),
                info.auxdata,
            )
        };
        (status == 0).then_some(())
    }
}

/// Where an operand comes from.
#[derive(Clone, Copy)]
enum Operand {
    /// The value an earlier op left on the stack.
    Stack,
    /// The function's argument.
    Arg(u8),
    /// A float or int constant (a Python scalar).
    Num(f64),
    /// A constant ndarray or np.float64, read when the program runs.
    Obj(u16),
}

/// An index into an array.
#[derive(Clone, Copy)]
enum Key {
    /// a[i]
    Int(Intp),
    /// a[i, j]
    Pair(Intp, Intp),
    /// a[:, j]
    Col(Intp),
    /// a[i, :]
    Row(Intp),
    /// a[:]
    All,
}

#[derive(Clone, Copy)]
enum Kind {
    /// a + b, a - b, a * b, a / b (by ufunc)
    Arith(u8),
    MatMul,
    Neg,
    /// a.T
    T,
    Index(Key),
    /// a.reshape(shape), one or two dimensions (-1 is worked out)
    Reshape(u8, [Intp; 2]),
    /// np.frombuffer(b)
    FromBuffer,
    /// a.tobytes(), the last op
    ToBytes,
    /// np.linalg.norm(a)
    Norm,
    /// np.trace(a)
    Trace,
    /// np.<ufunc>(...) by `UFUNCS` index: np.clip is CLIP
    Ufunc(u8),
}

struct Op {
    kind: Kind,
    args: [Operand; 3],
    nargs: u8,
}

/// A numpy expression of a function's arguments, as the analyzer translated
/// it (see `_analyze_arrays`).
pub struct Program {
    nargs: u8,
    ops: Box<[Op]>,
    objs: Box<[Py<PyAny>]>,
    /// numpy's ndarray and float64 types, to tell what a program takes
    /// from other items quickly.
    ndarray: *mut ffi::PyTypeObject,
    float64: *mut ffi::PyTypeObject,
}

// SAFETY: `ndarray` is a static type object; the rest is Python objects
// used with the GIL held.
unsafe impl Send for Program {}
unsafe impl Sync for Program {}

/// A program, and what runs when an item doesn't fit it.
pub struct ArrayOp {
    pub(crate) program: Program,
    pub(crate) fallback: Fallback,
}

pub enum Fallback {
    /// A map's ordinary native op.
    Op(Box<BuiltinOp>),
    /// A map's function.
    Call(Py<PyAny>),
    /// A starmap's function.
    StarCall(Py<PyAny>),
}

impl ArrayOp {
    /// The op to run instead when `x` is no array: a map's ordinary op,
    /// which has its own fast paths for everything else (see `apply_builtin`).
    #[inline(always)]
    pub(crate) fn op_for(&self, x: &Py<PyAny>) -> Option<&BuiltinOp> {
        match &self.fallback {
            Fallback::Op(op) if !self.program.is_array(x) => Some(op),
            _ => None,
        }
    }

    /// Whether a starmap's item could fit: an exact tuple of the program's
    /// arity, of types it takes. Inline, so others go straight to the call.
    #[inline(always)]
    pub(crate) fn star_fits(&self, x: &Py<PyAny>) -> bool {
        let x = x.as_ptr();
        // SAFETY: GIL held; a type check, then the live tuple's own items.
        unsafe {
            ffi::PyTuple_CheckExact(x) != 0
                && ffi::PyTuple_GET_SIZE(x) == self.program.nargs as isize
                && (0..self.program.nargs as isize)
                    .all(|i| self.program.takes(ffi::PyTuple_GET_ITEM(x, i)))
        }
    }

    #[inline(never)]
    pub(crate) fn apply(&self, py: Python, x: &Py<PyAny>) -> PyResult<Py<PyAny>> {
        let result = match self.fallback {
            Fallback::StarCall(_) => self.program.run_star(py, x),
            // The op's own fast paths are for everything but arrays.
            Fallback::Op(_) => self.program.run_array(py, x),
            Fallback::Call(_) => self.program.run(py, &[x.as_ptr()]),
        };
        if let Some(result) = result {
            return Ok(result);
        }
        match &self.fallback {
            Fallback::Op(op) => crate::native::apply_builtin(py, op, x),
            Fallback::Call(func) => crate::helpers::vectorcall1(py, func, x),
            Fallback::StarCall(func) => crate::native::star_call(py, func, x),
        }
    }

    /// The objects the op holds, for the garbage collector.
    pub(crate) fn objects(&self) -> impl Iterator<Item = &Py<PyAny>> {
        let func = match &self.fallback {
            Fallback::Call(func) | Fallback::StarCall(func) => Some(func),
            Fallback::Op(_) => None,
        };
        self.program.objs.iter().chain(func)
    }
}

impl Program {
    /// Parse the analyzer's ops (see `_analyze_arrays`); None if numpy can't
    /// run them here.
    pub(crate) fn parse(py: Python, nargs: usize, ops: &Bound<'_, PyAny>) -> Option<Self> {
        let np = numpy(py)?;
        let ops = ops.cast::<PyTuple>().ok()?;
        if nargs == 0 || nargs > 8 || ops.is_empty() || ops.len() > MAX_OPS {
            return None;
        }
        let mut objs = Vec::new();
        let mut parsed = Vec::with_capacity(ops.len());
        let mut depth = 0usize;
        for (i, op) in ops.iter().enumerate() {
            let op = op.cast_into::<PyTuple>().ok()?;
            let tag: String = op.get_item(0).ok()?.extract().ok()?;
            // Ops with a parameter before their operands.
            let (kind, first) = match tag.as_str() {
                "+" => (Kind::Arith(ADD), 1),
                "-" => (Kind::Arith(SUBTRACT), 1),
                "*" => (Kind::Arith(MULTIPLY), 1),
                "/" => (Kind::Arith(DIVIDE), 1),
                "@" => (Kind::MatMul, 1),
                "neg" => (Kind::Neg, 1),
                "T" => (Kind::T, 1),
                "frombuffer" => (Kind::FromBuffer, 1),
                "tobytes" => (Kind::ToBytes, 1),
                "norm" => (Kind::Norm, 1),
                "trace" => (Kind::Trace, 1),
                "clip" => (Kind::Ufunc(CLIP), 1),
                "index" => (Kind::Index(parse_key(&op.get_item(1).ok()?)?), 2),
                "reshape" => {
                    let shape = op.get_item(1).ok()?.cast_into::<PyTuple>().ok()?;
                    let (nd, dims) = match shape.len() {
                        1 => (1, [shape.get_item(0).ok()?.extract().ok()?, 1]),
                        2 => (
                            2,
                            [
                                shape.get_item(0).ok()?.extract().ok()?,
                                shape.get_item(1).ok()?.extract().ok()?,
                            ],
                        ),
                        _ => return None,
                    };
                    (Kind::Reshape(nd, dims), 2)
                }
                "ufunc" => {
                    let name = op.get_item(1).ok()?.cast_into::<PyString>().ok()?;
                    let name = name.to_str().ok()?;
                    let index = UFUNCS.iter().position(|(n, _)| *n == name)?;
                    (Kind::Ufunc(index as u8), 2)
                }
                _ => return None,
            };
            let expected = match kind {
                Kind::Arith(_) | Kind::MatMul => 2,
                Kind::Ufunc(index) => UFUNCS[index as usize].1,
                _ => 1,
            };
            if op.len() != first + expected {
                return None;
            }
            if let Kind::Ufunc(index) | Kind::Arith(index) = kind {
                np.loops[index as usize].as_ref()?;
            }
            if matches!(kind, Kind::MatMul) {
                np.loops[MATMUL as usize].as_ref()?;
            }
            if matches!(kind, Kind::Neg) {
                np.loops[NEGATIVE as usize].as_ref()?;
            }
            if matches!(kind, Kind::Norm) {
                np.loops[SQRT as usize].as_ref()?;
            }
            if matches!(kind, Kind::Trace) {
                np.loops[ADD as usize].as_ref()?;
            }
            // tobytes() makes the result: nothing can follow it.
            if matches!(kind, Kind::ToBytes) && i + 1 != ops.len() {
                return None;
            }
            let mut args = [Operand::Stack; 3];
            for (slot, arg) in args.iter_mut().zip(op.iter().skip(first)) {
                *slot = parse_operand(&arg, nargs, &mut objs)?;
            }
            let popped = args[..expected]
                .iter()
                .filter(|a| matches!(a, Operand::Stack))
                .count();
            depth = depth.checked_sub(popped)? + 1;
            if depth > MAX_DEPTH {
                return None;
            }
            parsed.push(Op {
                kind,
                args,
                nargs: expected as u8,
            });
        }
        (depth == 1).then(|| Program {
            nargs: nargs as u8,
            ops: parsed.into_boxed_slice(),
            objs: objs.into_boxed_slice(),
            ndarray: np.ndarray,
            float64: np.float64,
        })
    }

    /// Whether `x` is an ndarray, or bytes (for np.frombuffer).
    #[inline(always)]
    fn is_array(&self, x: &Py<PyAny>) -> bool {
        // SAFETY: GIL held; type checks on a live object.
        unsafe {
            let tp = ffi::Py_TYPE(x.as_ptr());
            tp == self.ndarray || tp == &raw mut ffi::PyBytes_Type
        }
    }

    /// Whether `o` has a type a program takes (see `load`).
    #[inline(always)]
    fn takes(&self, o: *mut ffi::PyObject) -> bool {
        // SAFETY: GIL held; a type check on a live object.
        let tp = unsafe { ffi::Py_TYPE(o) };
        tp == self.ndarray
            || tp == self.float64
            || tp == &raw mut ffi::PyFloat_Type
            || tp == &raw mut ffi::PyBytes_Type
    }

    fn run_array(&self, py: Python, x: &Py<PyAny>) -> Option<Py<PyAny>> {
        if !self.is_array(x) {
            return None;
        }
        self.run(py, &[x.as_ptr()])
    }

    /// A starmap's item: an exact tuple of the program's arity.
    fn run_star(&self, py: Python, x: &Py<PyAny>) -> Option<Py<PyAny>> {
        let x = x.as_ptr();
        // SAFETY: GIL held; a type check, then the live tuple's own items.
        unsafe {
            if ffi::PyTuple_CheckExact(x) == 0 || ffi::PyTuple_GET_SIZE(x) != self.nargs as isize {
                return None;
            }
            let items = (&raw const (*x.cast::<ffi::PyTupleObject>()).ob_item).cast();
            self.run(py, std::slice::from_raw_parts(items, self.nargs as usize))
        }
    }

    /// The result for these arguments, or None to run the function instead.
    #[inline(always)]
    pub(crate) fn run(&self, py: Python, args: &[*mut ffi::PyObject]) -> Option<Py<PyAny>> {
        // An item of another type costs no more than this: the program's
        // frame, with its buffers, is large.
        if !args.iter().all(|&arg| self.takes(arg)) {
            return None;
        }
        self.execute(py, args)
    }

    #[inline(never)]
    fn execute(&self, py: Python, args: &[*mut ffi::PyObject]) -> Option<Py<PyAny>> {
        let np = NUMPY.get(py)?.as_ref()?;
        let mut loaded = [Val::Num { v: 0.0, np: false }; 8];
        let mut temps = MaybeUninit::<Temps>::uninit();
        // SAFETY: every field but `used` is MaybeUninit.
        let temps = unsafe {
            (&raw mut (*temps.as_mut_ptr()).used).write(0);
            temps.assume_init_mut()
        };
        let mut stack = [Val::Num { v: 0.0, np: false }; MAX_DEPTH];
        let mut depth = 0;
        // SAFETY: GIL held throughout; values point into arguments, constants
        // and `temps`, all alive until the result is boxed.
        unsafe {
            for (slot, &arg) in loaded.iter_mut().zip(args) {
                *slot = load(np, arg)?;
            }
            (np.clear_fpe)();
            for op in &self.ops {
                let mut vals = [Val::Num { v: 0.0, np: false }; 3];
                let n = op.nargs as usize;
                for i in (0..n).rev() {
                    vals[i] = match op.args[i] {
                        Operand::Stack => {
                            depth -= 1;
                            stack[depth]
                        }
                        Operand::Arg(k) => loaded[k as usize],
                        Operand::Num(v) => Val::Num { v, np: false },
                        Operand::Obj(k) => load(np, self.objs[k as usize].as_ptr())?,
                    };
                }
                stack[depth] = step(np, op.kind, &vals[..n], temps)?;
                depth += 1;
                // After every op, as numpy checks after every call: a later
                // loop may clear the flags (numpy's min, max and clip loops
                // drop the ones NaN comparisons raise).
                if (np.get_fpe)() != 0 {
                    return None;
                }
            }
            let result = finish(np, stack[0], temps);
            if result.is_null() {
                // Out of memory: the function will say so.
                ffi::PyErr_Clear();
                return None;
            }
            Some(Py::from_owned_ptr(py, result))
        }
    }
}

fn parse_key(key: &Bound<'_, PyAny>) -> Option<Key> {
    let key = key.cast::<PyTuple>().ok()?;
    let tag: String = key.get_item(0).ok()?.extract().ok()?;
    let int = |i: usize| key.get_item(i).ok()?.extract::<Intp>().ok();
    match tag.as_str() {
        "int" => Some(Key::Int(int(1)?)),
        "pair" => Some(Key::Pair(int(1)?, int(2)?)),
        "col" => Some(Key::Col(int(1)?)),
        "row" => Some(Key::Row(int(1)?)),
        "all" => Some(Key::All),
        _ => None,
    }
}

fn parse_operand(
    arg: &Bound<'_, PyAny>,
    nargs: usize,
    objs: &mut Vec<Py<PyAny>>,
) -> Option<Operand> {
    let arg = arg.cast::<PyTuple>().ok()?;
    let tag: String = arg.get_item(0).ok()?.extract().ok()?;
    match tag.as_str() {
        "s" => Some(Operand::Stack),
        "arg" => {
            let i: usize = arg.get_item(1).ok()?.extract().ok()?;
            (i < nargs).then_some(Operand::Arg(i as u8))
        }
        "num" => Some(Operand::Num(arg.get_item(1).ok()?.extract().ok()?)),
        "obj" => {
            objs.push(arg.get_item(1).ok()?.unbind());
            Some(Operand::Obj(u16::try_from(objs.len() - 1).ok()?))
        }
        _ => None,
    }
}

/// Where an array's elements live.
#[derive(Clone, Copy, PartialEq)]
enum Src {
    /// In this ndarray (an argument or constant): the value is a view of it.
    Array(*mut ffi::PyObject),
    /// In this bytes object, through np.frombuffer.
    Bytes(*mut ffi::PyObject),
    /// In a buffer of `Temps`: an op's result.
    Temp(u8),
}

#[derive(Clone, Copy, PartialEq)]
struct Arr {
    data: *mut c_char,
    nd: u8,
    shape: [Intp; 2],
    strides: [Intp; 2],
    src: Src,
    /// A view of `src` (numpy makes a new array object for every view,
    /// a.T of a 1-D array included), not `src`'s own array.
    view: bool,
}

#[derive(Clone, Copy)]
enum Val {
    /// A float: a numpy float64 if `np`, else a Python float.
    Num {
        v: f64,
        np: bool,
    },
    Arr(Arr),
    /// A bytes object, for np.frombuffer.
    Bytes(*mut ffi::PyObject),
    /// These elements' bytes: the result of tobytes().
    Packed(Arr),
}

impl Arr {
    fn size(&self) -> Intp {
        self.shape[0] * if self.nd == 2 { self.shape[1] } else { 1 }
    }

    /// numpy's C-contiguity: dimensions of length 1 have any stride.
    fn c_contiguous(&self) -> bool {
        let mut expected = 8;
        for i in (0..self.nd as usize).rev() {
            if self.shape[i] != 1 {
                if self.strides[i] != expected {
                    return false;
                }
                expected *= self.shape[i];
            }
        }
        true
    }

    fn f_contiguous(&self) -> bool {
        let mut expected = 8;
        for i in 0..self.nd as usize {
            if self.shape[i] != 1 {
                if self.strides[i] != expected {
                    return false;
                }
                expected *= self.shape[i];
            }
        }
        true
    }

    /// The element at byte offset `offset`.
    ///
    /// # Safety
    /// `offset` within the array.
    unsafe fn at(&self, offset: Intp) -> f64 {
        // SAFETY: the caller's; arrays here are aligned.
        unsafe { *self.data.offset(offset).cast::<f64>() }
    }
}

/// C or F order strides for a fresh array.
fn fresh_strides(nd: u8, shape: [Intp; 2], fortran: bool) -> [Intp; 2] {
    match (nd, fortran) {
        (1, _) => [8, 0],
        (_, false) => [8 * shape[1], 8],
        (_, true) => [8, 8 * shape[0]],
    }
}

/// Buffers for op results: one per op, so no result is overwritten while a
/// later value may still point into it.
#[repr(C, align(64))]
struct Temps {
    bufs: [[MaybeUninit<f64>; MAX_SIZE]; MAX_OPS],
    /// Each buffer's whole array, as it was made.
    whole: [MaybeUninit<Arr>; MAX_OPS],
    used: usize,
}

impl Temps {
    /// A fresh C or F order array's buffer.
    fn alloc(&mut self, nd: u8, shape: [Intp; 2], fortran: bool) -> Option<Arr> {
        let slot = self.used;
        if slot >= MAX_OPS {
            return None;
        }
        self.used += 1;
        let arr = Arr {
            data: self.bufs[slot].as_mut_ptr().cast(),
            nd,
            shape,
            strides: fresh_strides(nd, shape, fortran),
            src: Src::Temp(slot as u8),
            view: false,
        };
        self.whole[slot] = MaybeUninit::new(arr);
        Some(arr)
    }

    fn whole(&self, slot: u8) -> Arr {
        // SAFETY: a Temp source names a slot `alloc` filled in.
        unsafe { self.whole[slot as usize].assume_init() }
    }
}

/// An argument or constant as a value: None for anything a program doesn't
/// handle.
///
/// # Safety
/// GIL held; `o` alive while the value is used.
unsafe fn load(np: &Numpy, o: *mut ffi::PyObject) -> Option<Val> {
    // SAFETY: the caller's.
    unsafe {
        let tp = ffi::Py_TYPE(o);
        if tp == np.ndarray {
            let a = &*o.cast::<ArrayFields>();
            if a.descr != np.f8 || a.flags & ALIGNED == 0 || !(1..=2).contains(&a.nd) {
                return None;
            }
            let nd = a.nd as usize;
            let (mut shape, mut strides) = ([1; 2], [0; 2]);
            for i in 0..nd {
                shape[i] = *a.dimensions.add(i);
                strides[i] = *a.strides.add(i);
                if shape[i] < 1 || strides[i] < 0 {
                    return None;
                }
            }
            let arr = Arr {
                data: a.data,
                nd: nd as u8,
                shape,
                strides,
                src: Src::Array(o),
                view: true,
            };
            (arr.size() as usize <= MAX_SIZE).then_some(Val::Arr(arr))
        } else if tp == &raw mut ffi::PyFloat_Type {
            Some(Val::Num {
                v: ffi::PyFloat_AS_DOUBLE(o),
                np: false,
            })
        } else if tp == np.float64 {
            // np.float64 is a float subclass: its value is where a float's is.
            Some(Val::Num {
                v: ffi::PyFloat_AS_DOUBLE(o),
                np: true,
            })
        } else if tp == &raw mut ffi::PyBytes_Type {
            Some(Val::Bytes(o))
        } else {
            None
        }
    }
}

/// One op on its operands.
///
/// # Safety
/// GIL held; array values point to live elements.
unsafe fn step(np: &Numpy, kind: Kind, vals: &[Val], temps: &mut Temps) -> Option<Val> {
    // SAFETY: the caller's.
    unsafe {
        match kind {
            Kind::Arith(ufunc) => match (vals[0], vals[1]) {
                (Val::Num { v: a, np: na }, Val::Num { v: b, np: nb }) => {
                    scalar_arith(ufunc, a, b, na || nb)
                }
                _ => elementwise(np, ufunc, vals, temps),
            },
            Kind::Neg => match vals[0] {
                Val::Num { v, np } => Some(Val::Num { v: -v, np }),
                _ => elementwise(np, NEGATIVE, vals, temps),
            },
            Kind::Ufunc(ufunc) => elementwise(np, ufunc, vals, temps),
            Kind::MatMul => matmul(np, vals[0], vals[1], temps),
            Kind::T => match vals[0] {
                // A 1-D array's transpose is a view of it as it is.
                Val::Arr(mut a) => {
                    if a.nd == 2 {
                        a.shape.swap(0, 1);
                        a.strides.swap(0, 1);
                    }
                    a.view = true;
                    Some(Val::Arr(a))
                }
                // np.float64.T is the scalar itself; a float has no .T.
                _ => None,
            },
            Kind::Index(key) => index(vals[0], key),
            Kind::Reshape(nd, dims) => reshape(vals[0], nd, dims),
            Kind::FromBuffer => from_buffer(vals[0]),
            Kind::ToBytes => match vals[0] {
                Val::Arr(a) => Some(Val::Packed(a)),
                _ => None,
            },
            Kind::Norm => norm(np, vals[0], temps),
            Kind::Trace => trace(np, vals[0]),
        }
    }
}

/// `a <op> b` on two floats, as Python (both Python floats) or numpy's
/// scalar math does it: one IEEE operation.
fn scalar_arith(ufunc: u8, a: f64, b: f64, np: bool) -> Option<Val> {
    if a.is_nan() || b.is_nan() {
        return None;
    }
    let v = match ufunc {
        ADD => a + b,
        SUBTRACT => a - b,
        MULTIPLY => a * b,
        // Python raises ZeroDivisionError; numpy warns (a flag the run checks).
        _ if b == 0.0 && !np => return None,
        _ => a / b,
    };
    Some(Val::Num { v, np })
}

/// A ufunc over operands of one shape (scalars broadcast), allocated and
/// looped as numpy does: a result in the arrays' common memory order, one
/// inner loop over every element. All-scalar operands give a numpy scalar.
///
/// # Safety
/// GIL held; array values point to live elements.
unsafe fn elementwise(np: &Numpy, ufunc: u8, vals: &[Val], temps: &mut Temps) -> Option<Val> {
    let lp = np.loops[ufunc as usize].as_ref()?;
    let mut layout: Option<(u8, [Intp; 2])> = None;
    let (mut c, mut f) = (true, true);
    for v in vals {
        match v {
            Val::Num { .. } => {}
            Val::Arr(a) => {
                match layout {
                    None => layout = Some((a.nd, a.shape)),
                    Some((nd, shape)) if nd == a.nd && shape == a.shape => {}
                    Some(_) => return None,
                }
                c &= a.c_contiguous();
                f &= a.f_contiguous();
            }
            _ => return None,
        }
    }
    let mut scalars = [0.0f64; 4];
    let mut data = [std::ptr::null_mut::<c_char>(); 4];
    let mut strides = [0 as Intp; 4];
    let n = vals.len();
    for (i, v) in vals.iter().enumerate() {
        match v {
            Val::Num { v, .. } => {
                scalars[i] = *v;
                data[i] = (&raw mut scalars[i]).cast();
            }
            Val::Arr(a) => {
                data[i] = a.data;
                // 1-D: the array's own stride; 2-D: contiguous (checked below).
                strides[i] = if a.nd == 1 { a.strides[0] } else { 8 };
            }
            _ => return None,
        }
    }
    match layout {
        None => {
            data[n] = (&raw mut scalars[3]).cast();
            // SAFETY: the caller's; every operand is a local float.
            unsafe { lp.call(&data[..=n], &[1], &strides[..=n]) }?;
            Some(Val::Num {
                v: scalars[3],
                np: true,
            })
        }
        Some((nd, shape)) => {
            // numpy keeps a shared memory order (C if every operand has it).
            let fortran = match (nd, c, f) {
                (1, ..) | (_, true, _) => false,
                (_, false, true) => true,
                _ => return None,
            };
            let out = temps.alloc(nd, shape, fortran)?;
            data[n] = out.data;
            strides[n] = 8;
            // SAFETY: the caller's; `out` has room for every element.
            unsafe { lp.call(&data[..=n], &[out.size()], &strides[..=n]) }?;
            Some(Val::Arr(out))
        }
    }
}

/// `a @ b`: numpy's matmul loop with the operands' own strides (which decide
/// how it calls BLAS), into a C-order result. 1-D @ 1-D is a numpy scalar.
///
/// # Safety
/// GIL held; array values point to live elements.
unsafe fn matmul(np: &Numpy, a: Val, b: Val, temps: &mut Temps) -> Option<Val> {
    let (Val::Arr(a), Val::Arr(b)) = (a, b) else {
        return None;
    };
    let lp = np.loops[MATMUL as usize].as_ref()?;
    // A missing dimension (1-D operand) has length 1 and stride 0.
    let (m, n, is1_m, is1_n) = match a.nd {
        2 => (a.shape[0], a.shape[1], a.strides[0], a.strides[1]),
        _ => (1, a.shape[0], 0, a.strides[0]),
    };
    let (bn, p, is2_n, is2_p) = match b.nd {
        2 => (b.shape[0], b.shape[1], b.strides[0], b.strides[1]),
        _ => (b.shape[0], 1, b.strides[0], 0),
    };
    if bn != n || (m * p) as usize > MAX_SIZE {
        return None;
    }
    let mut scalar = 0.0f64;
    let (out, os_m, os_p) = match (a.nd, b.nd) {
        (2, 2) => (Some(temps.alloc(2, [m, p], false)?), 8 * p, 8),
        (2, _) => (Some(temps.alloc(1, [m, 1], false)?), 8, 0),
        (_, 2) => (Some(temps.alloc(1, [p, 1], false)?), 0, 8),
        _ => (None, 0, 0),
    };
    let out_data = match out {
        Some(out) => out.data,
        None => (&raw mut scalar).cast(),
    };
    // SAFETY: the caller's; the output has room for m x p elements.
    unsafe {
        lp.call(
            &[a.data, b.data, out_data],
            &[1, m, n, p],
            &[0, 0, 0, is1_m, is1_n, is2_n, is2_p, os_m, os_p],
        )
    }?;
    Some(match out {
        Some(out) => Val::Arr(out),
        None => Val::Num {
            v: scalar,
            np: true,
        },
    })
}

/// `a[key]`: a numpy scalar for every index given, else a view.
///
/// # Safety
/// Array values point to live elements.
unsafe fn index(a: Val, key: Key) -> Option<Val> {
    let Val::Arr(a) = a else {
        return None;
    };
    let wrap = |i: Intp, len: Intp| {
        let i = if i < 0 { i + len } else { i };
        (0..len).contains(&i).then_some(i)
    };
    let scalar = |offset: Intp| {
        // SAFETY: the caller's; `offset` is a checked element's.
        Some(Val::Num {
            v: unsafe { a.at(offset) },
            np: true,
        })
    };
    let view = |offset: Intp, len: Intp, stride: Intp| {
        Some(Val::Arr(Arr {
            // SAFETY: within the array (a checked row or column).
            data: unsafe { a.data.offset(offset) },
            nd: 1,
            shape: [len, 1],
            strides: [stride, 0],
            src: a.src,
            view: true,
        }))
    };
    match (a.nd, key) {
        (_, Key::All) => Some(Val::Arr(Arr { view: true, ..a })),
        (1, Key::Int(i)) => scalar(wrap(i, a.shape[0])? * a.strides[0]),
        (2, Key::Pair(i, j)) => {
            scalar(wrap(i, a.shape[0])? * a.strides[0] + wrap(j, a.shape[1])? * a.strides[1])
        }
        (2, Key::Int(i) | Key::Row(i)) => view(
            wrap(i, a.shape[0])? * a.strides[0],
            a.shape[1],
            a.strides[1],
        ),
        (2, Key::Col(j)) => view(
            wrap(j, a.shape[1])? * a.strides[1],
            a.shape[0],
            a.strides[0],
        ),
        _ => None,
    }
}

/// `a.reshape(...)` of a C-contiguous array: a view.
fn reshape(a: Val, nd: u8, dims: [Intp; 2]) -> Option<Val> {
    let Val::Arr(a) = a else {
        return None;
    };
    if !a.c_contiguous() {
        return None;
    }
    let size = a.size();
    let mut shape = [dims[0], if nd == 2 { dims[1] } else { 1 }];
    let unknown = shape[..nd as usize].iter().filter(|&&d| d == -1).count();
    let known: Intp = shape[..nd as usize].iter().filter(|&&d| d != -1).product();
    if unknown > 1 || shape[..nd as usize].iter().any(|&d| d < -1 || d == 0) {
        return None;
    }
    if unknown == 1 {
        if size % known != 0 {
            return None;
        }
        for d in &mut shape[..nd as usize] {
            if *d == -1 {
                *d = size / known;
            }
        }
    }
    if shape[0] * shape[1] != size {
        return None;
    }
    Some(Val::Arr(Arr {
        data: a.data,
        nd,
        shape,
        strides: fresh_strides(nd, shape, false),
        src: a.src,
        view: true,
    }))
}

/// `np.frombuffer(b)`: the bytes as float64s, read in place.
///
/// # Safety
/// GIL held; `b` alive.
unsafe fn from_buffer(b: Val) -> Option<Val> {
    let Val::Bytes(o) = b else {
        return None;
    };
    // SAFETY: the caller's; `o` is an exact bytes object.
    let (data, len) = unsafe { (ffi::PyBytes_AS_STRING(o), ffi::Py_SIZE(o)) };
    let n = len / 8;
    if len % 8 != 0 || n < 1 || n as usize > MAX_SIZE || !(data as usize).is_multiple_of(8) {
        return None;
    }
    Some(Val::Arr(Arr {
        data: data.cast_mut(),
        nd: 1,
        shape: [n, 1],
        strides: [8, 0],
        src: Src::Bytes(o),
        view: false,
    }))
}

/// `np.linalg.norm(a)`: sqrt(a.ravel(order='K').dot(...)), where ravel is a
/// view of a contiguous array and otherwise (a strided 1-D one) a copy.
///
/// # Safety
/// GIL held; array values point to live elements.
unsafe fn norm(np: &Numpy, a: Val, temps: &mut Temps) -> Option<Val> {
    let Val::Arr(a) = a else {
        return None;
    };
    let n = a.size();
    let data = if a.c_contiguous() || a.f_contiguous() {
        a.data
    } else if a.nd == 1 {
        let copy = temps.alloc(1, [n, 1], false)?;
        for i in 0..n {
            // SAFETY: the caller's; i is within both arrays.
            unsafe { *copy.data.cast::<f64>().offset(i) = a.at(i * a.strides[0]) };
        }
        copy.data
    } else {
        return None;
    };
    let mut sum = 0.0f64;
    let mut root = 0.0f64;
    // SAFETY: the caller's; n contiguous elements at `data`.
    unsafe {
        (np.dot)(
            data,
            8,
            data,
            8,
            (&raw mut sum).cast(),
            n,
            std::ptr::null_mut(),
        );
        let data = [(&raw mut sum).cast(), (&raw mut root).cast()];
        np.loops[SQRT as usize]
            .as_ref()?
            .call(&data, &[1], &[0, 0])?;
    }
    Some(Val::Num { v: root, np: true })
}

/// `np.trace(a)`: numpy's add reduction over the diagonal, from 0.
///
/// # Safety
/// GIL held; array values point to live elements.
unsafe fn trace(np: &Numpy, a: Val) -> Option<Val> {
    let Val::Arr(a) = a else {
        return None;
    };
    if a.nd != 2 {
        return None;
    }
    let mut sum = 0.0f64;
    let out: *mut c_char = (&raw mut sum).cast();
    let n = a.shape[0].min(a.shape[1]);
    // SAFETY: the caller's; the diagonal's n elements are within the array.
    unsafe {
        np.loops[ADD as usize].as_ref()?.call(
            &[out, a.data, out],
            &[n],
            &[0, a.strides[0] + a.strides[1], 0],
        )
    }?;
    Some(Val::Num { v: sum, np: true })
}

/// The result as a Python object (a new reference, or NULL with an error).
///
/// # Safety
/// GIL held; array values point to live elements.
unsafe fn finish(np: &Numpy, v: Val, temps: &Temps) -> *mut ffi::PyObject {
    // SAFETY: the caller's.
    unsafe {
        match v {
            Val::Num { v, np: false } => ffi::PyFloat_FromDouble(v),
            Val::Num { v, np: true } => new_float64(np, v),
            Val::Arr(a) => new_array(np, a, temps),
            Val::Packed(a) => to_bytes(a),
            // Only np.frombuffer takes bytes: never a result.
            Val::Bytes(_) => std::ptr::null_mut(),
        }
    }
}

/// A numpy float64 scalar, made as numpy makes them.
unsafe fn new_float64(np: &Numpy, v: f64) -> *mut ffi::PyObject {
    // SAFETY: GIL held; np.float64's own allocator, then its value where a
    // float's is.
    unsafe {
        let alloc = (*np.float64).tp_alloc.unwrap_or(ffi::PyType_GenericAlloc);
        let o = alloc(np.float64, 0);
        if !o.is_null() {
            (*o.cast::<ffi::PyFloatObject>()).ob_fval = v;
        }
        o
    }
}

/// The ndarray numpy would return for `a`: a fresh array for an op's whole
/// result, else a view, whose base numpy works out from the array it views.
///
/// # Safety
/// GIL held; `a` points to live elements.
unsafe fn new_array(np: &Numpy, a: Arr, temps: &Temps) -> *mut ffi::PyObject {
    // SAFETY: the caller's.
    unsafe {
        let base = match a.src {
            Src::Array(o) => {
                ffi::Py_INCREF(o);
                o
            }
            Src::Bytes(o) => {
                let buffer = from_bytes(np, o);
                if buffer.is_null() || !a.view {
                    return buffer;
                }
                buffer
            }
            Src::Temp(slot) => {
                let whole = temps.whole(slot);
                let fresh = fresh_array(np, whole);
                if fresh.is_null() || !a.view {
                    return fresh;
                }
                fresh
            }
        };
        // A view of a temporary is of the array it became.
        let mut a = a;
        if let Src::Temp(slot) = a.src {
            let offset = a.data.offset_from(temps.whole(slot).data);
            a.data = (*base.cast::<ArrayFields>()).data.offset(offset);
        }
        let view = view_of(np, base, a);
        ffi::Py_DECREF(base);
        view
    }
}

/// A fresh array holding `a` (a whole op result, in its own memory order).
unsafe fn fresh_array(np: &Numpy, a: Arr) -> *mut ffi::PyObject {
    // SAFETY: GIL held; numpy allocates the array in `a`'s order, then the
    // elements are copied in that same order.
    unsafe {
        let fortran = a.nd == 2 && !a.c_contiguous();
        ffi::Py_INCREF(np.f8);
        let o = (np.new_from_descr)(
            np.ndarray,
            np.f8,
            a.nd as c_int,
            a.shape.as_ptr(),
            std::ptr::null(),
            std::ptr::null_mut(),
            if fortran { F_CONTIGUOUS } else { 0 },
            std::ptr::null_mut(),
        );
        if !o.is_null() {
            let dst = (*o.cast::<ArrayFields>()).data;
            std::ptr::copy_nonoverlapping(a.data, dst, a.size() as usize * 8);
        }
        o
    }
}

/// The whole of a bytes object as np.frombuffer reads it.
unsafe fn bytes_array(o: *mut ffi::PyObject) -> Arr {
    // SAFETY: GIL held; an exact bytes object.
    unsafe {
        Arr {
            data: ffi::PyBytes_AS_STRING(o).cast_mut(),
            nd: 1,
            shape: [ffi::Py_SIZE(o) / 8, 1],
            strides: [8, 0],
            src: Src::Bytes(o),
            view: false,
        }
    }
}

/// np.frombuffer(o): a read-only array over the bytes, based on them.
unsafe fn from_bytes(np: &Numpy, o: *mut ffi::PyObject) -> *mut ffi::PyObject {
    // SAFETY: GIL held; `o` an exact bytes object.
    unsafe {
        let whole = bytes_array(o);
        ffi::Py_INCREF(np.f8);
        let array = (np.new_from_descr)(
            np.ndarray,
            np.f8,
            1,
            whole.shape.as_ptr(),
            std::ptr::null(),
            whole.data.cast(),
            C_CONTIGUOUS | ALIGNED,
            std::ptr::null_mut(),
        );
        if array.is_null() {
            return array;
        }
        ffi::Py_INCREF(o);
        if (np.set_base_object)(array, o) < 0 {
            ffi::Py_DECREF(array);
            return std::ptr::null_mut();
        }
        array
    }
}

/// A view of `base`'s elements described by `a`, made as numpy makes views:
/// the base's flags (but not owning the data), then numpy's own base
/// collapsing.
unsafe fn view_of(np: &Numpy, base: *mut ffi::PyObject, a: Arr) -> *mut ffi::PyObject {
    // SAFETY: GIL held; `base` an ndarray whose memory holds `a`.
    unsafe {
        let flags = (*base.cast::<ArrayFields>()).flags & !(OWNDATA | WRITEBACKIFCOPY);
        ffi::Py_INCREF(np.f8);
        let view = (np.new_from_descr)(
            np.ndarray,
            np.f8,
            a.nd as c_int,
            a.shape.as_ptr(),
            a.strides.as_ptr(),
            a.data.cast(),
            flags,
            std::ptr::null_mut(),
        );
        if view.is_null() {
            return view;
        }
        ffi::Py_INCREF(base);
        if (np.set_base_object)(view, base) < 0 {
            ffi::Py_DECREF(view);
            return std::ptr::null_mut();
        }
        view
    }
}

/// `a.tobytes()`: the elements in C order.
unsafe fn to_bytes(a: Arr) -> *mut ffi::PyObject {
    // SAFETY: GIL held; `a` points to live elements, the bytes object has
    // room for all of them.
    unsafe {
        let size = a.size();
        let o = ffi::PyBytes_FromStringAndSize(std::ptr::null(), size * 8);
        if o.is_null() {
            return o;
        }
        let out = ffi::PyBytes_AS_STRING(o).cast_mut().cast::<f64>();
        let cols = if a.nd == 2 { a.shape[1] } else { 1 };
        for i in 0..a.shape[0] {
            for j in 0..cols {
                *out.offset(i * cols + j) = a.at(i * a.strides[0] + j * a.strides[1]);
            }
        }
        o
    }
}
