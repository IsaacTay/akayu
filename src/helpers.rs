//! Python FFI helpers and utility functions.

use pyo3::prelude::*;
use std::collections::HashMap;
use std::ffi::CString;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, LazyLock, Mutex, OnceLock, PoisonError};

use crate::error::StreamError;
use crate::lock::NodeLock;

/// Defines a static Python helper reference and its getter function.
macro_rules! define_py_helper {
    ($static_name:ident, $getter_name:ident) => {
        pub(crate) static $static_name: OnceLock<Py<PyAny>> = OnceLock::new();

        pub(crate) fn $getter_name() -> PyResult<&'static Py<PyAny>> {
            $static_name
                .get()
                .ok_or_else(|| StreamError::NotInitialized.into())
        }
    };
}

// Python function references initialized at module load
define_py_helper!(BUILD_STARMAP_FUNC, get_build_starmap_func);
define_py_helper!(REJECT_ASYNC, get_reject_async);
define_py_helper!(BUILTIN_ISINSTANCE, get_builtin_isinstance);
define_py_helper!(BUILTIN_LEN, get_builtin_len);
define_py_helper!(BUILTIN_STR, get_builtin_str);
define_py_helper!(BUILTIN_INT, get_builtin_int);
define_py_helper!(BUILTIN_FLOAT, get_builtin_float);
define_py_helper!(BUILTIN_BOOL, get_builtin_bool);
define_py_helper!(BUILTIN_PRINT, get_builtin_print);
define_py_helper!(BUILTIN_TYPE, get_builtin_type);
define_py_helper!(BUILTIN_TUPLE, get_builtin_tuple);
define_py_helper!(BUILTIN_LIST, get_builtin_list);
define_py_helper!(BUILTIN_SET, get_builtin_set);
define_py_helper!(BUILTIN_FROZENSET, get_builtin_frozenset);
define_py_helper!(OPERATOR_NOT, get_operator_not);
define_py_helper!(OPERATOR_IS, get_operator_is);
define_py_helper!(OPERATOR_IS_NOT, get_operator_is_not);
define_py_helper!(OPERATOR_TRUTH, get_operator_truth);
define_py_helper!(OPERATOR_ITEMGETTER, get_operator_itemgetter);
define_py_helper!(OPERATOR_ATTRGETTER, get_operator_attrgetter);
define_py_helper!(PROFILER_CLASS, get_profiler_class);
define_py_helper!(ANALYZE_FOR_COMPILE, get_analyze_for_compile);

pub(crate) fn resolve_stream_name(
    stream_name: Option<String>,
    default_suffix: &str,
    func_name: Option<&str>,
) -> String {
    match stream_name {
        Some(name) => name,
        None => match func_name {
            Some(name) => format!("{default_suffix}({name})"),
            None => default_suffix.to_string(),
        },
    }
}

/// Extract a human-readable name from a Python callable.
///
/// Handles: named functions, lambdas (`<lambda>`), builtins (`print`),
/// methods, and `functools.partial` (extracts the wrapped function's name).
pub(crate) fn get_func_name(py: Python, func: &Py<PyAny>) -> Option<String> {
    let bound = func.bind(py);
    // Try __name__ first (cleanest for most functions)
    if let Ok(s) = bound
        .getattr(pyo3::intern!(py, "__name__"))
        .and_then(|n| n.extract::<String>())
    {
        return Some(s);
    }
    // Fallback: functools.partial — extract from .func attribute
    if let Ok(s) = bound
        .getattr(pyo3::intern!(py, "func"))
        .and_then(|inner| inner.getattr(pyo3::intern!(py, "__name__")))
        .and_then(|n| n.extract::<String>())
    {
        return Some(s);
    }
    // Fallback to __qualname__ (works for some edge cases)
    if let Ok(s) = bound
        .getattr(pyo3::intern!(py, "__qualname__"))
        .and_then(|n| n.extract::<String>())
    {
        return Some(s);
    }
    None
}

/// Interned `"file:line"` strings by file, then line.
type LocationTable = HashMap<Box<str>, HashMap<u32, Arc<str>>>;

/// `"file:line"` strings, interned: every node built on the same line shares
/// one allocation. Bounded by the number of distinct lines that build nodes.
static LOCATIONS: LazyLock<Mutex<LocationTable>> = LazyLock::new(Mutex::default);

/// akayu's own Python wrapper module (`zip()`, `union()`, MultiStream...), whose
/// frames are skipped so locations point at the user's code.
static WRAPPER_FILE: OnceLock<Option<Box<str>>> = OnceLock::new();

fn wrapper_file(py: Python) -> Option<&'static str> {
    WRAPPER_FILE
        .get_or_init(|| {
            let module = py
                .import("sys")
                .ok()?
                .getattr("modules")
                .ok()?
                .get_item("akayu")
                .ok()?;
            let file = module.getattr("__file__").ok()?;
            Some(file.extract::<String>().ok()?.into_boxed_str())
        })
        .as_deref()
}

/// The caller's source location (`file:line`): the innermost Python frame
/// outside akayu's wrapper module. Read from the frame directly (no Python
/// helper, no string formatting once the line has been seen).
pub(crate) fn capture_source_location(py: Python) -> Arc<str> {
    caller_location(py).unwrap_or_else(|| Arc::from("<unknown>"))
}

fn caller_location(py: Python) -> Option<Arc<str>> {
    let wrapper = wrapper_file(py);
    // SAFETY: GIL held. PyEval_GetFrame returns a borrowed frame (or NULL);
    // PyFrame_GetCode and PyFrame_GetBack return new references.
    unsafe {
        let top = pyo3::ffi::PyEval_GetFrame();
        if top.is_null() {
            return None;
        }
        let mut frame = Bound::from_borrowed_ptr(py, top.cast());
        let mut fallback = None;
        loop {
            let code =
                Bound::from_owned_ptr(py, pyo3::ffi::PyFrame_GetCode(frame.as_ptr().cast()).cast());
            let file = code.getattr(pyo3::intern!(py, "co_filename")).ok()?;
            let file = file.cast::<pyo3::types::PyString>().ok()?.to_str().ok()?;
            let line =
                u32::try_from(pyo3::ffi::PyFrame_GetLineNumber(frame.as_ptr().cast())).unwrap_or(0);
            if wrapper != Some(file) {
                return Some(intern_location(file, line));
            }
            if fallback.is_none() {
                fallback = Some(intern_location(file, line));
            }
            let back = pyo3::ffi::PyFrame_GetBack(frame.as_ptr().cast());
            if back.is_null() {
                return fallback;
            }
            frame = Bound::from_owned_ptr(py, back.cast());
        }
    }
}

fn intern_location(file: &str, line: u32) -> Arc<str> {
    let mut map = LOCATIONS.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(loc) = map.get(file).and_then(|lines| lines.get(&line)) {
        return Arc::clone(loc);
    }
    let loc: Arc<str> = Arc::from(format!("{file}:{line}"));
    map.entry(file.into())
        .or_default()
        .insert(line, Arc::clone(&loc));
    loc
}

/// Call a Python callable with two positional arguments, without the argument
/// tuple that `func.call1(py, (a, b))` allocates.
#[inline(always)]
pub(crate) fn vectorcall2(
    py: Python,
    func: &Py<PyAny>,
    a: &Py<PyAny>,
    b: &Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    unsafe {
        let args = [a.as_ptr(), b.as_ptr()];
        let result =
            pyo3::ffi::PyObject_Vectorcall(func.as_ptr(), args.as_ptr(), 2, std::ptr::null_mut());
        if result.is_null() {
            return Err(PyErr::fetch(py));
        }
        Ok(Py::from_owned_ptr(py, result))
    }
}

/// Call a Python callable with a single positional argument using vectorcall.
///
/// This avoids the tuple allocation that `func.call1(py, (arg,))` does via
/// `PyObject_Call`.  Falls back to `PyObject_Call` on builds where vectorcall
/// is not available.
#[inline(always)]
pub(crate) fn vectorcall1(py: Python, func: &Py<PyAny>, arg: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    unsafe {
        let args = [arg.as_ptr()];
        let result =
            pyo3::ffi::PyObject_Vectorcall(func.as_ptr(), args.as_ptr(), 1, std::ptr::null_mut());
        if result.is_null() {
            return Err(PyErr::fetch(py));
        }
        Ok(Py::from_owned_ptr(py, result))
    }
}

/// Call a Python callable using a pre-cached vectorcall function pointer.
///
/// Skips the per-call type flag check, `tp_vectorcall_offset` dereference,
/// and `_Py_CheckFunctionResult` that `PyObject_Vectorcall` performs.
/// The caller must ensure `func_ptr` was extracted from `func` and that
/// `func` has not been replaced since extraction.
#[inline(always)]
pub(crate) fn cached_vectorcall1(
    py: Python,
    func_ptr: pyo3::ffi::vectorcallfunc,
    func: &Py<PyAny>,
    arg: &Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    unsafe {
        let args = [arg.as_ptr()];
        let result = func_ptr(func.as_ptr(), args.as_ptr(), 1, std::ptr::null_mut());
        if result.is_null() {
            return Err(PyErr::fetch(py));
        }
        Ok(Py::from_owned_ptr(py, result))
    }
}

/// A callback's error as a `CallbackError` naming the node it ran in, with
/// the original as its `__cause__` and the item it was given as `item`. One
/// already wrapped (by a node further down, or a nested pipeline) goes on as
/// it is.
pub(crate) fn wrap_error<T>(
    py: Python,
    res: PyResult<T>,
    node_name: &str,
    source_location: &str,
    item: Option<&Py<PyAny>>,
) -> PyResult<T> {
    res.map_err(|e| wrap_callback_error(py, e, node_name, source_location, item))
}

#[cold]
#[inline(never)]
fn wrap_callback_error(
    py: Python,
    e: PyErr,
    node_name: &str,
    source_location: &str,
    item: Option<&Py<PyAny>>,
) -> PyErr {
    // KeyboardInterrupt, SystemExit, GeneratorExit: not a failed step but
    // a request to stop, which `except KeyboardInterrupt` and sys.exit()
    // codes must still see.
    if !e.is_instance_of::<pyo3::exceptions::PyException>(py)
        || e.is_instance_of::<crate::error::exc::CallbackError>(py)
    {
        return e;
    }
    let mut msg =
        format!("Stream operation failed at node '{node_name}' (defined at {source_location})");
    if let Some(pointer) = source_pointer(py, source_location, node_name, 0) {
        msg.push('\n');
        msg.push_str(&pointer);
    }
    let new_err = crate::error::exc::CallbackError::new_err(msg);
    let value = new_err.value(py);
    // Plain attributes on the instance: a failure to set one only loses it.
    let _ = value.setattr(pyo3::intern!(py, "node"), node_name);
    let _ = value.setattr(pyo3::intern!(py, "location"), source_location);
    // Not in the message: items can be private, and messages end up in logs.
    let _ = value.setattr(pyo3::intern!(py, "item"), item.map(|x| x.clone_ref(py)));
    new_err.set_cause(py, Some(e));
    new_err
}

/// Read a source line using Python's linecache module.
fn read_source_line(py: Python, location: &str) -> Option<String> {
    let colon_pos = location.rfind(':')?;
    let filename = &location[..colon_pos];
    let lineno: usize = location[colon_pos + 1..].parse().ok()?;

    let linecache = py.import("linecache").ok()?;
    let line = linecache.call_method1("getline", (filename, lineno)).ok()?;
    let line_str: String = line.extract().ok()?;
    let trimmed = line_str.trim_end();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Find the byte span of a method call on a source line.
///
/// `segment` is like `"map(func_name)"` — we search for `.map(` on the line.
/// `occurrence` is 0-indexed for disambiguating duplicate calls on the same line.
/// Returns `(start, end)` byte positions.
fn find_call_span(line: &str, segment: &str, occurrence: usize) -> Option<(usize, usize)> {
    let method = segment.split('(').next()?;

    // Search for ".method(" on the line
    let needle = format!(".{method}(");
    let mut count = 0;
    let mut search_from = 0;

    loop {
        let rel_pos = line[search_from..].find(&needle)?;
        let abs_pos = search_from + rel_pos;

        if count == occurrence {
            let paren_start = abs_pos + needle.len();
            let mut depth = 1;
            for (i, ch) in line[paren_start..].char_indices() {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some((abs_pos, paren_start + i + 1));
                        }
                    }
                    _ => {}
                }
            }
            return Some((abs_pos, line.len()));
        }

        count += 1;
        search_from = abs_pos + needle.len();
    }
}

/// Build a source-line pointer string like:
/// ```text
///   10 | s.map(append, 1).map(append, 2)
///                         ^^^^^^^^^^^^^^
/// ```
fn source_pointer(py: Python, location: &str, segment: &str, occurrence: usize) -> Option<String> {
    let source_line = read_source_line(py, location)?;
    let (start, end) = find_call_span(&source_line, segment, occurrence)?;
    let lineno = location.rsplit(':').next().unwrap_or("?");
    let prefix = format!("  {lineno} | ");
    let pointer_pad = " ".repeat(prefix.len() + start);
    let pointer = "^".repeat(end - start);
    Some(format!("{prefix}{source_line}\n{pointer_pad}{pointer}"))
}

/// Check that a stream has not been compiled/frozen.
/// Returns an error if the stream is frozen (compiled).
pub(crate) fn check_not_compiled(compiled: bool, name: &str) -> PyResult<()> {
    if compiled {
        return Err(StreamError::FrozenStream(name.to_string()).into());
    }
    Ok(())
}

/// Append `str(data)` and a newline to a file, creating it if needed.
///
/// Meant to be used as a sink::
///
///     source.sink(akayu.to_text_file, path="output.txt")
///
/// Args:
///     data: The item to write.
///     path: The file to append to, as a str (a `Path` raises `TypeError`).
#[pyfunction]
#[pyo3(signature = (data, path))]
pub fn to_text_file(py: Python, data: Py<PyAny>, path: String) -> PyResult<()> {
    let mut line = data.bind(py).str()?.to_string();
    line.push('\n');
    // File I/O runs with the GIL released. The line goes out as one append
    // (writeln! wrote the text and the newline separately), so concurrent
    // writers, threads or processes, never interleave within a line.
    py.detach(|| {
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        file.write_all(line.as_bytes())
    })?;
    Ok(())
}

/// Raise TypeError for async callables: akayu never awaits coroutines.
pub(crate) fn reject_async(py: Python, func: &Py<PyAny>, op: &str) -> PyResult<()> {
    if certainly_sync(py, func.bind(py))? {
        return Ok(());
    }
    // Exact check (inspect.iscoroutinefunction / isasyncgenfunction) and the error.
    get_reject_async()?.call1(py, (func, op))?;
    Ok(())
}

/// True when `func` is certainly not an async function, decided natively for
/// builtins and plain functions (the common case). The `inspect` checks cost
/// more than building the node did. Anything else is left to `inspect`.
fn certainly_sync(py: Python, func: &Bound<'_, PyAny>) -> PyResult<bool> {
    const CO_COROUTINE: u32 = 0x0080;
    const CO_ASYNC_GENERATOR: u32 = 0x0200;
    let ptr = func.as_ptr();
    // SAFETY: GIL held; type checks on a live object, and PyFunction_GetCode
    // returns a borrowed reference that `func` keeps alive.
    unsafe {
        if pyo3::ffi::PyCFunction_Check(ptr) != 0 {
            return Ok(true); // builtins and bound builtin methods are never async
        }
        if pyo3::ffi::PyFunction_Check(ptr) == 0 {
            return Ok(false);
        }
        let code = Bound::from_borrowed_ptr(py, pyo3::ffi::PyFunction_GetCode(ptr));
        let flags: u32 = code.getattr(pyo3::intern!(py, "co_flags"))?.extract()?;
        if flags & (CO_COROUTINE | CO_ASYNC_GENERATOR) != 0 {
            return Ok(false);
        }
    }
    // inspect.markcoroutinefunction() marks plain functions with this attribute.
    Ok(func
        .getattr_opt(pyo3::intern!(py, "_is_coroutine_marker"))?
        .is_none())
}

/// Initialize Python helper functions from embedded Python code.
pub fn init_helpers(py: Python, helpers_code: &str) -> PyResult<()> {
    let code = CString::new(helpers_code).unwrap();
    // Namespaced: a bare "helpers" entry in sys.modules shadows any user module of that name
    let fname = CString::new("akayu/_helpers.py").unwrap();
    let mname = CString::new("akayu._helpers").unwrap();
    let helpers_module = PyModule::from_code(py, &code, &fname, &mname)?;

    let build_starmap_func = helpers_module.getattr("_build_starmap_func")?;

    let _ = BUILD_STARMAP_FUNC.set(build_starmap_func.unbind());

    let profiler_class = helpers_module.getattr("_Profiler")?;
    let _ = PROFILER_CLASS.set(profiler_class.unbind());

    let reject_async = helpers_module.getattr("_reject_async")?;
    let _ = REJECT_ASYNC.set(reject_async.unbind());

    let analyze_for_compile = helpers_module.getattr("_analyze_for_compile")?;
    let _ = ANALYZE_FOR_COMPILE.set(analyze_for_compile.unbind());

    // Cache builtin references for fast-path detection
    let builtins = py.import("builtins")?;
    let _ = BUILTIN_ISINSTANCE.set(builtins.getattr("isinstance")?.unbind());
    let _ = BUILTIN_LEN.set(builtins.getattr("len")?.unbind());
    let _ = BUILTIN_STR.set(builtins.getattr("str")?.unbind());
    let _ = BUILTIN_INT.set(builtins.getattr("int")?.unbind());
    let _ = BUILTIN_FLOAT.set(builtins.getattr("float")?.unbind());
    let _ = BUILTIN_BOOL.set(builtins.getattr("bool")?.unbind());
    let _ = BUILTIN_PRINT.set(builtins.getattr("print")?.unbind());
    let _ = BUILTIN_TYPE.set(builtins.getattr("type")?.unbind());
    let _ = BUILTIN_TUPLE.set(builtins.getattr("tuple")?.unbind());
    let _ = BUILTIN_LIST.set(builtins.getattr("list")?.unbind());
    let _ = BUILTIN_SET.set(builtins.getattr("set")?.unbind());
    let _ = BUILTIN_FROZENSET.set(builtins.getattr("frozenset")?.unbind());

    // Cache operator module references for fast-path detection
    let operator = py.import("operator")?;
    let _ = OPERATOR_NOT.set(operator.getattr("not_")?.unbind());
    let _ = OPERATOR_IS.set(operator.getattr("is_")?.unbind());
    let _ = OPERATOR_IS_NOT.set(operator.getattr("is_not")?.unbind());
    let _ = OPERATOR_TRUTH.set(operator.getattr("truth")?.unbind());
    let _ = OPERATOR_ITEMGETTER.set(operator.getattr("itemgetter")?.unbind());
    let _ = OPERATOR_ATTRGETTER.set(operator.getattr("attrgetter")?.unbind());

    Ok(())
}

/// Hasher for pointer and address keys (compile()'s graph walks and caches):
/// a multiply instead of SipHash. Only for keys no attacker chooses.
#[derive(Default, Clone, Copy)]
pub(crate) struct PtrHasher(u64);

impl std::hash::Hasher for PtrHasher {
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.write_u64(u64::from(b));
        }
    }
    fn write_u64(&mut self, n: u64) {
        self.0 = (self.0.rotate_left(5) ^ n).wrapping_mul(0xf135_7aea_2e62_a9c5);
    }
    fn write_usize(&mut self, n: usize) {
        self.write_u64(n as u64);
    }
    fn finish(&self) -> u64 {
        // The multiply leaves the entropy high; hash tables index by low bits.
        self.0.rotate_left(26)
    }
}

pub(crate) type PtrBuild = std::hash::BuildHasherDefault<PtrHasher>;
pub(crate) type PtrMap<K, V> = std::collections::HashMap<K, V, PtrBuild>;
pub(crate) type PtrSet<K> = std::collections::HashSet<K, PtrBuild>;

/// Values made from objects compile() meets again and again, by the objects'
/// identity, so graphs built from the same code share them. Each entry keeps
/// what makes its key unambiguous alive, or can tell it died (`H`: a weak
/// reference, the object itself, the ops a run was made from).
///
/// Once the table holds `sweep_at` entries, an insert first drops the
/// entries nothing but the table still uses (`live` says which are used),
/// and beyond `max` entries new values are made but not kept. Whatever the
/// table lets go of is freed after its lock is released: freeing can run
/// Python code (a `__del__`), which may come back to the table.
pub(crate) struct InternTable<K, H, V> {
    entries: LazyLock<Mutex<PtrMap<K, (H, V)>>>,
    sweep_at: usize,
    max: usize,
}

impl<K: std::hash::Hash + Eq, H, V: Clone> InternTable<K, H, V> {
    pub(crate) const fn new(sweep_at: usize, max: usize) -> Self {
        Self {
            entries: LazyLock::new(Mutex::default),
            sweep_at,
            max,
        }
    }

    /// The value kept for `key`, if its entry is still `valid` (its object
    /// is still the one the key's address named).
    pub(crate) fn get(&self, key: &K, valid: impl Fn(&H) -> bool) -> Option<V> {
        let entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let (holder, value) = entries.get(key)?;
        valid(holder).then(|| value.clone())
    }

    /// Keep `value` for `key`, unless the table is full, and return the value
    /// kept for it (another thread's, if it got there first).
    pub(crate) fn insert(&self, key: K, holder: H, value: V, live: impl Fn(&H) -> bool) -> V {
        let mut freed: Vec<(H, V)> = Vec::new();
        let kept = {
            let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
            if entries.len() >= self.sweep_at {
                freed.extend(
                    entries
                        .extract_if(|_, (holder, _)| !live(holder))
                        .map(|(_, entry)| entry),
                );
            }
            let full = entries.len() >= self.max;
            match entries.entry(key) {
                std::collections::hash_map::Entry::Occupied(entry) => {
                    let kept = entry.get().1.clone();
                    freed.push((holder, value));
                    kept
                }
                std::collections::hash_map::Entry::Vacant(entry) if !full => {
                    entry.insert((holder, value)).1.clone()
                }
                std::collections::hash_map::Entry::Vacant(_) => {
                    let kept = value.clone();
                    freed.push((holder, value));
                    kept
                }
            }
        };
        drop(freed);
        kept
    }
}

/// `slf` for changing the graph, or a clean error (not a panic) while it is
/// processing an item: a callback building on it mid-emit.
pub(crate) fn graph_node_mut<'py>(
    slf: &Bound<'py, crate::Stream>,
) -> PyResult<PyRefMut<'py, crate::Stream>> {
    slf.try_borrow_mut()
        .map_err(|_| crate::error::StreamError::BusyNode.into())
}

/// `slf` to read while building the graph (see `graph_node_mut`).
pub(crate) fn graph_node<'py>(
    slf: &Bound<'py, crate::Stream>,
) -> PyResult<PyRef<'py, crate::Stream>> {
    slf.try_borrow()
        .map_err(|_| crate::error::StreamError::BusyNode.into())
}

thread_local! {
    /// flush()/drain() calls this thread is inside (see `with_node_mut`).
    static FLUSHING: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// The nodes that flush()/drain() calls from Python hold mutably, with their
/// lock, which the flushing thread holds throughout. A node borrowed mutably
/// can't be read, so this is how another flush of it learns which lock to
/// wait for (`with_node_mut`).
static FLUSHED: Mutex<Vec<(usize, Arc<NodeLock>)>> = Mutex::new(Vec::new());

/// Lists a node in `FLUSHED`, and counts it in `FLUSHING`, until dropped.
struct Flushed(usize);

impl Flushed {
    fn new(node: usize, lock: &Arc<NodeLock>) -> Self {
        FLUSHED
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((node, Arc::clone(lock)));
        FLUSHING.with(|n| n.set(n.get() + 1));
        Self(node)
    }
}

impl Drop for Flushed {
    fn drop(&mut self) {
        FLUSHING.with(|n| n.set(n.get() - 1));
        let mut flushed = FLUSHED.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(i) = flushed.iter().rposition(|(node, _)| *node == self.0) {
            flushed.swap_remove(i);
        }
    }
}

/// The lock of `node`, if a flush() called from Python holds it.
fn flushed_lock(node: usize) -> Option<Arc<NodeLock>> {
    FLUSHED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .find(|(n, _)| *n == node)
        .map(|(_, lock)| Arc::clone(lock))
}

/// Run `f` on `slf` for flush()/drain() called from Python on a node that
/// isn't a source (sources: `Stream::flush_source`), waiting for another
/// thread that is emitting through it or flushing it (a timer thread flushing
/// while others emit) instead of failing on its borrow.
///
/// The flush holds the node's lock throughout, as an update through a locked
/// edge does. Whether this thread already holds the lock tells a callback
/// flushing the node it runs in (an error) from another thread's emit (wait
/// for it). A node borrowed for an update can't be asked; inside one of this
/// thread's own flushes that is a re-entrant flush, otherwise another
/// thread's brief update is waited for, within a limit.
pub(crate) fn with_node_mut<R>(
    slf: &Bound<'_, crate::Stream>,
    py: Python,
    f: impl FnOnce(&mut crate::Stream) -> PyResult<R>,
) -> PyResult<R> {
    let id = slf.as_ptr() as usize;
    let deadline = std::time::Instant::now() + crate::lock::BUSY_WAIT;
    let mut pause = std::time::Duration::from_micros(10);
    loop {
        if let Ok(node) = slf.try_borrow() {
            let lock = Arc::clone(&node.lock);
            drop(node);
            // This thread may hold it already: a callback flushing a node of
            // the graph it runs in (a compiled graph has one lock). Otherwise
            // waiting for the lock lets an emit on another thread finish.
            let mine = lock.held_by_this_thread();
            let _guard = lock.acquire(py)?;
            if let Ok(mut node) = slf.try_borrow_mut() {
                let _flushed = Flushed::new(id, &lock);
                return f(&mut node);
            }
            if mine {
                // This thread's emit is in it: a callback flushing the node it
                // runs in.
                return Err(StreamError::BusyFlush.into());
            }
        } else if FLUSHING.with(std::cell::Cell::get) > 0 {
            return Err(StreamError::ReentrantFlushCall.into());
        } else if let Some(lock) = flushed_lock(id) {
            // Another thread is flushing it (this one would be in FLUSHING).
            drop(lock.acquire(py)?);
            continue;
        } else if std::time::Instant::now() > deadline {
            return Err(StreamError::BusyFlush.into());
        }
        py.detach(|| std::thread::sleep(pause));
        pause = (pause * 2).min(std::time::Duration::from_millis(1));
    }
}

/// A downstream node to hand an item to, or a clean error (not a panic) when
/// it is still processing one: an item that came back to it through a
/// callback's emit into another source.
#[inline]
pub(crate) fn enter_node<'py>(
    edge: &'py crate::lock::Downstream,
    py: Python<'py>,
) -> PyResult<PyRefMut<'py, crate::Stream>> {
    loop {
        if let Ok(node) = edge.node.try_borrow_mut(py) {
            return Ok(node);
        }
        wait_for_busy_node(edge, py)?;
    }
}

/// `enter_node` when the node is borrowed. Another thread holding the node's
/// lock is in it (flushing it, from a timer say, or updating it through a
/// merge's edge) and lets go once done: wait for the lock, then try again.
/// Anything else is an item coming back into it: this thread's own update.
#[cold]
#[inline(never)]
fn wait_for_busy_node(edge: &crate::lock::Downstream, py: Python) -> PyResult<()> {
    let owner = edge.lock.owner();
    if owner == 0 || owner == crate::lock::this_thread() {
        return Err(StreamError::Reentered.into());
    }
    drop(edge.lock.acquire(py)?);
    Ok(())
}
