//! Python FFI helpers and utility functions.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};
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
define_py_helper!(IS_AWAITABLE, get_is_awaitable);
define_py_helper!(PROCESS_ASYNC, get_process_async);
define_py_helper!(GATHER, get_gather);
define_py_helper!(FILTER_ASYNC, get_filter_async);
define_py_helper!(BUILD_MAP_FUNC, get_build_map_func);
define_py_helper!(BUILD_STARMAP_FUNC, get_build_starmap_func);
define_py_helper!(IS_SYNC_CALLABLE, get_is_sync_callable);
define_py_helper!(COMPOSE_MAPS, get_compose_maps);
define_py_helper!(COMPOSE_FILTERS, get_compose_filters);
define_py_helper!(COMPOSE_BATCH_MAPS, get_compose_batch_maps);
define_py_helper!(COMPOSE_FILTER_MAP, get_compose_filter_map);
define_py_helper!(PARALLEL_EXECUTE, get_parallel_execute);
define_py_helper!(PREFETCH_STATE_CLASS, get_prefetch_state_class);
define_py_helper!(PREFETCH_FILTER_STATE_CLASS, get_prefetch_filter_state_class);
define_py_helper!(
    PREFETCH_BATCH_MAP_STATE_CLASS,
    get_prefetch_batch_map_state_class
);
define_py_helper!(SAFE_UPDATE, get_safe_update);
define_py_helper!(SAFE_UPDATE_BATCH, get_safe_update_batch);
define_py_helper!(ASYNC_STATE_CLASS, get_async_state_class);
define_py_helper!(CALLER_LOCATION, get_caller_location);

pub(crate) fn extract_stream_name(
    py: Python,
    kwargs: &Option<Py<PyDict>>,
    default_suffix: &str,
    func_name: Option<&str>,
) -> PyResult<String> {
    if let Some(k) = kwargs {
        let k_bound = k.bind(py);
        if let Some(item) = k_bound.get_item("stream_name")? {
            let name = item.extract::<String>()?;
            k_bound.del_item("stream_name")?;
            return Ok(name);
        }
    }
    match func_name {
        Some(name) => Ok(format!("{default_suffix}({name})")),
        None => Ok(default_suffix.to_string()),
    }
}

/// Extract a human-readable name from a Python callable.
///
/// Handles: named functions, lambdas (`<lambda>`), builtins (`print`),
/// methods, and `functools.partial` (extracts the wrapped function's name).
pub(crate) fn get_func_name(py: Python, func: &Py<PyAny>) -> Option<String> {
    let bound = func.bind(py);
    // Try __name__ first (cleanest for most functions)
    if let Ok(name) = bound.getattr("__name__") {
        if let Ok(s) = name.extract::<String>() {
            return Some(s);
        }
    }
    // Fallback: functools.partial — extract from .func attribute
    if let Ok(inner) = bound.getattr("func") {
        if let Ok(name) = inner.getattr("__name__") {
            if let Ok(s) = name.extract::<String>() {
                return Some(s);
            }
        }
    }
    // Fallback to __qualname__ (works for some edge cases)
    if let Ok(name) = bound.getattr("__qualname__") {
        if let Ok(s) = name.extract::<String>() {
            return Some(s);
        }
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

pub(crate) fn wrap_error<T>(
    py: Python,
    res: PyResult<T>,
    node_name: &str,
    source_location: &str,
) -> PyResult<T> {
    res.map_err(|e| {
        let ctx_msg = if node_name.contains('+') {
            narrow_fused_error(py, &e, node_name, source_location)
        } else {
            format!("Stream operation failed at node '{node_name}' (defined at {source_location})")
        };
        let new_err = pyo3::exceptions::PyRuntimeError::new_err(ctx_msg);
        new_err.set_cause(py, Some(e));
        new_err
    })
}

/// For fused nodes, use the `_akayu_chain_index` attribute set by composed
/// functions to identify exactly which function in the chain failed.
/// Returns a narrowed error message on success, or the full fused message as fallback.
fn narrow_fused_error(
    py: Python,
    err: &PyErr,
    node_name: &str,
    source_location: &str,
) -> String {
    let full_msg =
        format!("Stream operation failed at node '{node_name}' (defined at {source_location})");

    // Check for _akayu_chain_index on the exception
    let Ok(idx_attr) = err.value(py).getattr("_akayu_chain_index") else {
        return full_msg;
    };
    let Ok(idx) = idx_attr.extract::<usize>() else {
        return full_msg;
    };

    let name_segments: Vec<&str> = node_name.split('+').collect();
    let loc_segments: Vec<&str> = source_location.split(", ").collect();

    if let Some(segment) = name_segments.get(idx) {
        let loc = loc_segments.get(idx).unwrap_or(&source_location);
        format!(
            "Stream operation failed in {segment} \
             [fused node: {node_name}] (defined at {loc})"
        )
    } else {
        full_msg
    }
}

/// Check that a stream has not been compiled/frozen.
/// Returns an error if the stream is frozen (compiled).
pub(crate) fn check_not_compiled(compiled: bool, name: &str) -> PyResult<()> {
    if compiled {
        return Err(StreamError::FrozenStream(name.to_string()).into());
    }
    Ok(())
}

/// Write data to a text file, appending a newline.
///
/// Converts the input data to a string and appends it as a new line to the specified file.
/// Creates the file if it doesn't exist.
///
/// Args:
///     data: The data to write (will be converted to string).
///     path: The file path to write to.
#[pyfunction]
#[pyo3(signature = (data, path))]
pub fn to_text_file(py: Python, data: Py<PyAny>, path: String) -> PyResult<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    let s = data.bind(py).str()?;
    let output = s.to_string();

    writeln!(file, "{output}")?;
    Ok(())
}

/// Create a Stream that emits lines from a text file.
///
/// Reads the file line by line in a background thread and emits each line
/// as a string to the returned stream.
///
/// Args:
///     path: The file path to read from.
///     interval: Optional delay in seconds between emitting lines.
///
/// Returns:
///     A Stream that will emit each line from the file.
#[pyfunction]
#[pyo3(signature = (path, interval=None))]
pub fn from_text_file(py: Python, path: String, interval: Option<f64>) -> PyResult<Py<Stream>> {
    let source_location = capture_source_location(py);
    let stream = Py::new(
        py,
        Stream {
            logic: NodeLogic::Source,
            downstreams: Vec::new(),
            name: "file_source".to_string(),
            asynchronous: None,
            skip_async_check: false,
            func_is_sync: true,
            frozen: false,
            needs_lock: false,
            compiled: false,
            source_location,
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

    let process_async = helpers_module.getattr("_process_async")?;
    let gather = helpers_module.getattr("_gather")?;
    let filter_async = helpers_module.getattr("_filter_async")?;
    let build_map_func = helpers_module.getattr("_build_map_func")?;
    let build_starmap_func = helpers_module.getattr("_build_starmap_func")?;
    let is_sync_callable = helpers_module.getattr("_is_sync_callable")?;
    let compose_maps = helpers_module.getattr("_compose_maps")?;
    let compose_filters = helpers_module.getattr("_compose_filters")?;
    let compose_batch_maps = helpers_module.getattr("_compose_batch_maps")?;
    let compose_filter_map = helpers_module.getattr("_compose_filter_map")?;
    let parallel_execute = helpers_module.getattr("_parallel_execute")?;
    let prefetch_state_class = helpers_module.getattr("_PrefetchState")?;
    let prefetch_filter_state_class = helpers_module.getattr("_PrefetchFilterState")?;
    let prefetch_batch_map_state_class = helpers_module.getattr("_PrefetchBatchMapState")?;
    let safe_update = helpers_module.getattr("_safe_update")?;
    let safe_update_batch = helpers_module.getattr("_safe_update_batch")?;
    let async_state_class = helpers_module.getattr("_AsyncState")?;

    let _ = PROCESS_ASYNC.set(process_async.unbind());
    let _ = GATHER.set(gather.unbind());
    let _ = FILTER_ASYNC.set(filter_async.unbind());
    let _ = BUILD_MAP_FUNC.set(build_map_func.unbind());
    let _ = BUILD_STARMAP_FUNC.set(build_starmap_func.unbind());
    let _ = IS_SYNC_CALLABLE.set(is_sync_callable.unbind());
    let _ = COMPOSE_MAPS.set(compose_maps.unbind());
    let _ = COMPOSE_FILTERS.set(compose_filters.unbind());
    let _ = COMPOSE_BATCH_MAPS.set(compose_batch_maps.unbind());
    let _ = COMPOSE_FILTER_MAP.set(compose_filter_map.unbind());
    let _ = PARALLEL_EXECUTE.set(parallel_execute.unbind());
    let _ = SAFE_UPDATE.set(safe_update.unbind());
    let _ = SAFE_UPDATE_BATCH.set(safe_update_batch.unbind());
    let _ = PREFETCH_STATE_CLASS.set(prefetch_state_class.unbind());
    let _ = PREFETCH_FILTER_STATE_CLASS.set(prefetch_filter_state_class.unbind());
    let _ = PREFETCH_BATCH_MAP_STATE_CLASS.set(prefetch_batch_map_state_class.unbind());
    let _ = ASYNC_STATE_CLASS.set(async_state_class.unbind());

    let caller_location = helpers_module.getattr("_get_caller_location")?;
    let _ = CALLER_LOCATION.set(caller_location.unbind());

    let inspect = py.import("inspect")?;
    let is_awaitable = inspect.getattr("isawaitable")?;
    let _ = IS_AWAITABLE.set(is_awaitable.unbind());

    Ok(())
}
