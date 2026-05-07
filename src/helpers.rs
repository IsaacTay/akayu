//! Python FFI helpers and utility functions.

use pyo3::prelude::*;
use pyo3::types::PyString;
use std::ffi::CString;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

use crate::error::StreamError;
use crate::{NodeLogic, Stream};

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
define_py_helper!(GATHER, get_gather);
define_py_helper!(BUILD_MAP_FUNC, get_build_map_func);
define_py_helper!(BUILD_STARMAP_FUNC, get_build_starmap_func);
define_py_helper!(IS_SYNC_CALLABLE, get_is_sync_callable);
define_py_helper!(PARALLEL_EXECUTE, get_parallel_execute);
define_py_helper!(PREFETCH_STATE_CLASS, get_prefetch_state_class);
define_py_helper!(PREFETCH_FILTER_STATE_CLASS, get_prefetch_filter_state_class);
define_py_helper!(
    PREFETCH_BATCH_MAP_STATE_CLASS,
    get_prefetch_batch_map_state_class
);
define_py_helper!(SAFE_UPDATE, get_safe_update);
define_py_helper!(SAFE_UPDATE_BATCH, get_safe_update_batch);
define_py_helper!(STRIP_INTERNAL_FRAMES, get_strip_internal_frames);
define_py_helper!(CALLER_LOCATION, get_caller_location);
define_py_helper!(BUILTIN_ISINSTANCE, get_builtin_isinstance);
define_py_helper!(BUILTIN_LEN, get_builtin_len);
define_py_helper!(BUILTIN_STR, get_builtin_str);
define_py_helper!(BUILTIN_INT, get_builtin_int);
define_py_helper!(BUILTIN_FLOAT, get_builtin_float);
define_py_helper!(BUILTIN_BOOL, get_builtin_bool);
define_py_helper!(BUILTIN_PRINT, get_builtin_print);
define_py_helper!(BUILTIN_TYPE, get_builtin_type);
define_py_helper!(BUILTIN_HASATTR, get_builtin_hasattr);
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
define_py_helper!(ANALYZE_LAMBDA, get_analyze_lambda);

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
        .getattr("__name__")
        .and_then(|n| n.extract::<String>())
    {
        return Some(s);
    }
    // Fallback: functools.partial — extract from .func attribute
    if let Ok(s) = bound
        .getattr("func")
        .and_then(|inner| inner.getattr("__name__"))
        .and_then(|n| n.extract::<String>())
    {
        return Some(s);
    }
    // Fallback to __qualname__ (works for some edge cases)
    if let Ok(s) = bound
        .getattr("__qualname__")
        .and_then(|n| n.extract::<String>())
    {
        return Some(s);
    }
    None
}

/// Capture the caller's source location (file:line) from Python.
pub(crate) fn capture_source_location(py: Python) -> String {
    let Ok(helper) = get_caller_location() else {
        return "<unknown>".to_string();
    };
    helper
        .call0(py)
        .and_then(|r| r.extract::<String>(py))
        .unwrap_or_else(|_| "<unknown>".to_string())
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

pub(crate) fn wrap_error<T>(
    py: Python,
    res: PyResult<T>,
    node_name: &str,
    source_location: &str,
) -> PyResult<T> {
    res.map_err(|e| {
        let is_akayu_err = e.get_type(py).is_subclass_of::<pyo3::exceptions::PyRuntimeError>().unwrap_or(false)
            && e.value(py).repr().ok().map_or(false, |r| {
                r.to_string().contains("Stream operation failed at node")
            });
        if is_akayu_err {
            return e;
        }
        if let Ok(strip_fn) = get_strip_internal_frames() {
            let _ = strip_fn.call1(py, (e.value(py),));
        }
        let mut msg =
            format!("Stream operation failed at node '{node_name}' (defined at {source_location})");
        if let Some(pointer) = source_pointer(py, source_location, node_name, 0) {
            msg.push('\n');
            msg.push_str(&pointer);
        }
        let new_err = pyo3::exceptions::PyRuntimeError::new_err(msg);
        new_err.set_cause(py, Some(e));
        new_err
    })
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

#[pyfunction]
#[pyo3(signature = (data, path))]
/// Write data to a text file, appending a newline.
///
/// Converts the input data to a string and appends it as a new line to the specified file.
/// Creates the file if it doesn't exist. This is a side-effect operation intended
/// for logging or output purposes.
///
/// # Arguments
///
/// * `data` - The data to write (will be converted to string via `str()`).
/// * `path` - The file path to write to.
///
/// # Returns
///
/// None.
///
/// # Examples
///
/// ```python
/// import akayu
/// source = akayu.Stream()
/// source.sink(akayu.to_text_file, path="output.txt")
/// source.emit("Hello")
/// source.emit("World")
/// # output.txt contains:
/// # Hello
/// # World
/// ```
pub fn to_text_file(py: Python, data: Py<PyAny>, path: String) -> PyResult<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    let s = data.bind(py).str()?;
    let output = s.to_string();

    writeln!(file, "{output}")?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (path, interval=None))]
/// Create a Stream that emits lines from a text file.
///
/// Reads the file line by line in a background thread and emits each line
/// as a string to the returned stream. The file is watched and re-watched
/// if an interval is specified.
///
/// # Arguments
///
/// * `path` - The file path to read from.
/// * `interval` - Optional delay in seconds between emitting lines. If None,
///   the file is read once and the stream closes. If specified, the file
///   is watched and new lines are emitted as they appear.
///
/// # Returns
///
/// A Stream that will emit each line from the file.
///
/// # Examples
///
/// ```python
/// import akayu
/// # Read file once
/// source = akayu.from_text_file("data.txt")
/// result = []
/// source.sink(result.append)
/// # result contains all lines from data.txt
///
/// # Watch file for changes
/// source = akayu.from_text_file("log.txt", interval=0.5)
/// source.sink(print)  # prints new lines as they appear
/// ```
pub fn from_text_file(py: Python, path: String, interval: Option<f64>) -> PyResult<Py<Stream>> {
    let source_location = capture_source_location(py);
    let stream = Py::new(
        py,
        Stream {
            logic: NodeLogic::Source,
            downstreams: Vec::new(),
            parents: std::sync::Mutex::new(Vec::new()),
            name: "file_source".to_string(),
            func_is_sync: true,
            frozen: false,
            needs_lock: false,
            par_branches_marked: false,
            compiled: false,
            source_location,
            builtin_op: None,
            cached_vectorcall: None,
            compiled_chainable: false,
            compiled_filter_fast: false,
            compiled_downstream_ptrs: Vec::new(),
            flush_target: None,
            emit_target: None,
            flushing: false,
            emitting: std::sync::atomic::AtomicBool::new(false),
            emit_lock: py.import("threading").unwrap().getattr("RLock").unwrap().call0().unwrap().unbind(),
            convergence_checked: false,
        },
    )?;
    let stream_clone = stream.clone_ref(py);

    thread::spawn(move || {
        // Wrap entire thread in Python::attach so stream_clone drops with GIL held
        Python::attach(move |py| {
            // Open file with GIL released
            let file = match py.detach(|| std::fs::File::open(&path)) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Error opening file {path}: {e}");
                    return;
                }
            };
            let mut reader = BufReader::new(file);
            let mut line_buf = String::new();

            loop {
                line_buf.clear();
                // Read line with GIL released
                let bytes_read = py.detach(|| reader.read_line(&mut line_buf));

                match bytes_read {
                    Ok(0) | Err(_) => break, // EOF or error
                    Ok(_) => {
                        let text = line_buf.trim_end_matches('\n');
                        if let Ok(mut s) = stream_clone.try_borrow_mut(py) {
                            let py_str = PyString::new(py, text);
                            let _ = s.emit(py, py_str.into());
                        }

                        if let Some(delay) = interval {
                            py.detach(|| thread::sleep(Duration::from_secs_f64(delay)));
                        }
                    }
                }
            }
            // stream_clone dropped here with GIL held
        });
    });

    Ok(stream)
}

/// Initialize Python helper functions from embedded Python code.
pub fn init_helpers(py: Python, helpers_code: &str) -> PyResult<()> {
    let code = CString::new(helpers_code).unwrap();
    let fname = CString::new("helpers.py").unwrap();
    let mname = CString::new("helpers").unwrap();
    let helpers_module = PyModule::from_code(py, &code, &fname, &mname)?;

    let gather = helpers_module.getattr("_gather")?;
    let build_map_func = helpers_module.getattr("_build_map_func")?;
    let build_starmap_func = helpers_module.getattr("_build_starmap_func")?;
    let parallel_execute = helpers_module.getattr("_parallel_execute")?;
    let prefetch_state_class = helpers_module.getattr("_PrefetchState")?;
    let prefetch_filter_state_class = helpers_module.getattr("_PrefetchFilterState")?;
    let prefetch_batch_map_state_class = helpers_module.getattr("_PrefetchBatchMapState")?;
    let safe_update = helpers_module.getattr("_safe_update")?;
    let safe_update_batch = helpers_module.getattr("_safe_update_batch")?;
    let strip_internal_frames = helpers_module.getattr("_strip_internal_frames")?;

    let _ = GATHER.set(gather.unbind());
    let _ = BUILD_MAP_FUNC.set(build_map_func.unbind());
    let _ = BUILD_STARMAP_FUNC.set(build_starmap_func.unbind());
    let _ = PARALLEL_EXECUTE.set(parallel_execute.unbind());
    let _ = SAFE_UPDATE.set(safe_update.unbind());
    let _ = SAFE_UPDATE_BATCH.set(safe_update_batch.unbind());
    let _ = STRIP_INTERNAL_FRAMES.set(strip_internal_frames.unbind());
    let _ = PREFETCH_STATE_CLASS.set(prefetch_state_class.unbind());
    let _ = PREFETCH_FILTER_STATE_CLASS.set(prefetch_filter_state_class.unbind());
    let _ = PREFETCH_BATCH_MAP_STATE_CLASS.set(prefetch_batch_map_state_class.unbind());

    let caller_location = helpers_module.getattr("_get_caller_location")?;
    let _ = CALLER_LOCATION.set(caller_location.unbind());

    let profiler_class = helpers_module.getattr("_Profiler")?;
    let _ = PROFILER_CLASS.set(profiler_class.unbind());

    let analyze_lambda = helpers_module.getattr("_analyze_lambda")?;
    let _ = ANALYZE_LAMBDA.set(analyze_lambda.unbind());

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
    let _ = BUILTIN_HASATTR.set(builtins.getattr("hasattr")?.unbind());
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
