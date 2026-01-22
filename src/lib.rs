//! # rstreamz - High-performance reactive streams for Python

// Allow clippy warnings that conflict with PyO3 patterns
#![allow(clippy::ref_option)] // PyO3 signatures use &Option<T>
#![allow(clippy::needless_pass_by_value)] // PyO3 requires owned values
#![allow(clippy::too_many_lines)] // Complex stream methods need length
#![allow(clippy::doc_markdown)] // Python docstrings use Python conventions, not Rust

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use std::collections::VecDeque;
use std::ffi::CString;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

static IS_AWAITABLE: OnceLock<Py<PyAny>> = OnceLock::new();
static PROCESS_ASYNC: OnceLock<Py<PyAny>> = OnceLock::new();
static GATHER: OnceLock<Py<PyAny>> = OnceLock::new();
static FILTER_ASYNC: OnceLock<Py<PyAny>> = OnceLock::new();
static BUILD_MAP_FUNC: OnceLock<Py<PyAny>> = OnceLock::new();
static BUILD_STARMAP_FUNC: OnceLock<Py<PyAny>> = OnceLock::new();
static IS_SYNC_CALLABLE: OnceLock<Py<PyAny>> = OnceLock::new();
static COMPOSE_MAPS: OnceLock<Py<PyAny>> = OnceLock::new();
static COMPOSE_FILTERS: OnceLock<Py<PyAny>> = OnceLock::new();
static PARALLEL_EXECUTE: OnceLock<Py<PyAny>> = OnceLock::new();
static PREFETCH_STATE_CLASS: OnceLock<Py<PyAny>> = OnceLock::new();
static PREFETCH_FILTER_STATE_CLASS: OnceLock<Py<PyAny>> = OnceLock::new();
static SAFE_UPDATE: OnceLock<Py<PyAny>> = OnceLock::new();
static SAFE_UPDATE_BATCH: OnceLock<Py<PyAny>> = OnceLock::new();

fn get_is_awaitable() -> PyResult<&'static Py<PyAny>> {
    IS_AWAITABLE.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_process_async() -> PyResult<&'static Py<PyAny>> {
    PROCESS_ASYNC.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_gather() -> PyResult<&'static Py<PyAny>> {
    GATHER.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_filter_async() -> PyResult<&'static Py<PyAny>> {
    FILTER_ASYNC.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_build_map_func() -> PyResult<&'static Py<PyAny>> {
    BUILD_MAP_FUNC.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_build_starmap_func() -> PyResult<&'static Py<PyAny>> {
    BUILD_STARMAP_FUNC.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_is_sync_callable() -> PyResult<&'static Py<PyAny>> {
    IS_SYNC_CALLABLE.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_compose_maps() -> PyResult<&'static Py<PyAny>> {
    COMPOSE_MAPS.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_compose_filters() -> PyResult<&'static Py<PyAny>> {
    COMPOSE_FILTERS.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_parallel_execute() -> PyResult<&'static Py<PyAny>> {
    PARALLEL_EXECUTE.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_prefetch_state_class() -> PyResult<&'static Py<PyAny>> {
    PREFETCH_STATE_CLASS.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_prefetch_filter_state_class() -> PyResult<&'static Py<PyAny>> {
    PREFETCH_FILTER_STATE_CLASS.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_safe_update() -> PyResult<&'static Py<PyAny>> {
    SAFE_UPDATE.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_safe_update_batch() -> PyResult<&'static Py<PyAny>> {
    SAFE_UPDATE_BATCH.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn extract_stream_name(
    py: Python,
    kwargs: &Option<Py<PyDict>>,
    default_suffix: &str,
    parent_name: &str,
) -> PyResult<String> {
    if let Some(k) = kwargs {
        let k_bound = k.bind(py);
        if let Some(item) = k_bound.get_item("stream_name")? {
            let name = item.extract::<String>()?;
            k_bound.del_item("stream_name")?;
            return Ok(name);
        }
    }
    Ok(format!("{parent_name}.{default_suffix}"))
}

fn wrap_error<T>(py: Python, res: PyResult<T>, node_name: &str) -> PyResult<T> {
    res.map_err(|e| {
        let ctx_msg = format!("Stream operation failed at node '{node_name}'");
        let new_err = pyo3::exceptions::PyRuntimeError::new_err(ctx_msg);
        new_err.set_cause(py, Some(e));
        new_err
    })
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
fn to_text_file(py: Python, data: Py<PyAny>, path: String) -> PyResult<()> {
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
fn from_text_file(py: Python, path: String, interval: Option<f64>) -> PyResult<Py<Stream>> {
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

enum NodeLogic {
    Source,
    Map {
        func: Py<PyAny>,
    },
    Starmap {
        func: Py<PyAny>,
    },
    Filter {
        predicate: Py<PyAny>,
    },
    Flatten,
    Collect {
        buffer: Vec<Py<PyAny>>,
    },
    Sink {
        func: Py<PyAny>,
    },
    Accumulate {
        func: Py<PyAny>,
        state: Py<PyAny>,
        returns_state: bool,
    },
    Tag {
        index: usize,
    },
    CombineLatest {
        state: Vec<Option<Py<PyAny>>>,
        emit_on_indices: Vec<usize>, // Empty = emit on any, non-empty = only these indices
    },
    Zip {
        buffers: Vec<VecDeque<Py<PyAny>>>,
    },
    BatchMap {
        func: Py<PyAny>,
    },
    /// Parallel node: propagates to downstreams in parallel threads
    Parallel,
    /// Prefetch marker: signals that the next map should prefetch items
    Prefetch {
        size: usize,
    },
    /// Prefetch map: processes items concurrently while preserving order
    PrefetchMap {
        #[allow(dead_code)] // Stored for debugging/inspection
        func: Py<PyAny>,
        #[allow(dead_code)] // Stored for debugging/inspection
        size: usize,
        state: Py<PyAny>, // Python _PrefetchState object
    },
    /// Prefetch filter: filters items concurrently while preserving order
    PrefetchFilter {
        #[allow(dead_code)] // Stored for debugging/inspection
        predicate: Py<PyAny>,
        #[allow(dead_code)] // Stored for debugging/inspection
        size: usize,
        state: Py<PyAny>, // Python _PrefetchState object
    },
}

/// A reactive stream for processing data through a pipeline of operations.
///
/// Stream is the core building block for creating data processing pipelines.
/// Data flows through a directed graph of stream nodes, where each node applies
/// a transformation (map, filter, etc.) before passing results downstream.
///
/// Example:
///     >>> source = `Stream()`
///     >>> result = []
///     >>> source.map(lambda x: x * 2).sink(result.append)
///     >>> source.emit(5)
///     >>> result
///     [10]
#[pyclass(subclass)]
#[allow(clippy::struct_excessive_bools)] // These flags are intentionally separate for clarity
struct Stream {
    logic: NodeLogic,
    downstreams: Vec<Py<Stream>>,
    name: String,
    asynchronous: Option<bool>,
    /// Pre-computed flag: true if async check can be skipped (asynchronous == Some(false))
    skip_async_check: bool,
    /// True if this node's function is definitely synchronous (not a coroutine function)
    func_is_sync: bool,
    /// True if topology has been frozen/optimized on first emit
    frozen: bool,
    /// True if this node needs thread-safe locking (convergence point from parallel branches)
    needs_lock: bool,
}

const HELPERS: &str = r#"
import asyncio
import inspect

async def _process_async(awaitable, downstreams):
    val = await awaitable
    results = []
    for d in downstreams:
        res = d.update(val)
        if res is not None:
            results.append(res)
    if results:
        await asyncio.gather(*results)

async def _gather(futures):
    await asyncio.gather(*futures)

async def _filter_async(coro, x, downstreams):
    allowed = await coro
    if allowed:
        results = []
        for d in downstreams:
            res = d.update(x)
            if res is not None:
                results.append(res)
        if results:
            await asyncio.gather(*results)

def _is_sync_callable(func):
    """Return True if func is definitely synchronous (not a coroutine function)."""
    if inspect.iscoroutinefunction(func):
        return False
    if inspect.isasyncgenfunction(func):
        return False
    # Assume sync for regular functions/lambdas
    return True

def _build_map_func(func, args, kwargs):
    # args is a tuple or None, kwargs is a dict or None
    if not args and not kwargs:
        return func

    if kwargs:
        if args:
            def wrapper(x):
                return func(x, *args, **kwargs)
            return wrapper
        else:
            def wrapper(x):
                return func(x, **kwargs)
            return wrapper
    else:
        # only args
        def wrapper(x):
            return func(x, *args)
        return wrapper

def _build_starmap_func(func, args, kwargs):
    # args is tuple or None, kwargs is dict or None
    if not args and not kwargs:
        def wrapper_simple(x):
            return func(*x)
        return wrapper_simple

    if kwargs:
        if args:
            def wrapper(x):
                return func(*x, *args, **kwargs)
            return wrapper
        else:
            def wrapper(x):
                return func(*x, **kwargs)
            return wrapper
    else:
        def wrapper(x):
            return func(*x, *args)
        return wrapper

def _compose_maps(f, g):
    """Compose two map functions into a flat chain."""
    # Get existing function lists or create new ones
    f_funcs = getattr(f, '_chain', [f])
    g_funcs = getattr(g, '_chain', [g])
    all_funcs = f_funcs + g_funcs

    # Create a flat composed function based on chain length
    if len(all_funcs) == 2:
        f1, f2 = all_funcs
        def composed2(x):
            return f2(f1(x))
        composed2._chain = all_funcs
        return composed2
    elif len(all_funcs) == 3:
        f1, f2, f3 = all_funcs
        def composed3(x):
            return f3(f2(f1(x)))
        composed3._chain = all_funcs
        return composed3
    elif len(all_funcs) == 4:
        f1, f2, f3, f4 = all_funcs
        def composed4(x):
            return f4(f3(f2(f1(x))))
        composed4._chain = all_funcs
        return composed4
    elif len(all_funcs) == 5:
        f1, f2, f3, f4, f5 = all_funcs
        def composed5(x):
            return f5(f4(f3(f2(f1(x)))))
        composed5._chain = all_funcs
        return composed5
    else:
        # Fallback for longer chains: use reduce-style
        def composed_long(x):
            result = x
            for fn in all_funcs:
                result = fn(result)
            return result
        composed_long._chain = all_funcs
        return composed_long

def _compose_filters(p1, p2):
    """Compose two filter predicates into a flat chain."""
    # Get existing predicate lists or create new ones
    p1_preds = getattr(p1, '_chain', [p1])
    p2_preds = getattr(p2, '_chain', [p2])
    all_preds = p1_preds + p2_preds

    # Create a flat composed predicate based on chain length
    if len(all_preds) == 2:
        pred1, pred2 = all_preds
        def composed2(x):
            return pred1(x) and pred2(x)
        composed2._chain = all_preds
        return composed2
    elif len(all_preds) == 3:
        pred1, pred2, pred3 = all_preds
        def composed3(x):
            return pred1(x) and pred2(x) and pred3(x)
        composed3._chain = all_preds
        return composed3
    elif len(all_preds) == 4:
        pred1, pred2, pred3, pred4 = all_preds
        def composed4(x):
            return pred1(x) and pred2(x) and pred3(x) and pred4(x)
        composed4._chain = all_preds
        return composed4
    else:
        # Fallback for longer chains
        def composed_long(x):
            for pred in all_preds:
                if not pred(x):
                    return False
            return True
        composed_long._chain = all_preds
        return composed_long

from concurrent.futures import ThreadPoolExecutor
import contextvars
import threading
_thread_pool = None
_node_locks = {}  # Per-node locks to prevent concurrent update calls
_node_locks_lock = threading.Lock()  # Lock for accessing _node_locks dict

def _get_node_lock(node):
    """Get or create a lock for a specific node."""
    node_id = id(node)
    if node_id not in _node_locks:
        with _node_locks_lock:
            if node_id not in _node_locks:
                _node_locks[node_id] = threading.RLock()
    return _node_locks[node_id]

def _parallel_execute(val, downstreams):
    """Execute downstream updates in parallel using a thread pool."""
    global _thread_pool
    if _thread_pool is None:
        _thread_pool = ThreadPoolExecutor(max_workers=8)

    def make_update_fn(d, v):
        # Copy context for THIS specific thread (each thread gets its own copy)
        ctx = contextvars.copy_context()
        def update_in_context():
            # Acquire per-node lock to prevent concurrent borrows
            lock = _get_node_lock(d)
            with lock:
                return ctx.run(d.update, v)
        return update_in_context

    # Submit all downstream updates to the thread pool
    futures = []
    for d in downstreams:
        futures.append(_thread_pool.submit(make_update_fn(d, val)))

    # Wait for all to complete and collect results
    results = []
    errors = []
    for f in futures:
        try:
            res = f.result()
            if res is not None:
                results.append(res)
        except Exception as e:
            errors.append(e)

    # If any errors occurred, raise the first one
    if errors:
        raise errors[0]

    # If any returned coroutines, gather them
    if results:
        return asyncio.gather(*results)
    return None

def _safe_update(node, val):
    """Call node.update(val) with per-node locking to prevent concurrent borrows."""
    lock = _get_node_lock(node)
    with lock:
        return node.update(val)

def _safe_update_batch(node, items):
    """Call node.update_batch(items) with per-node locking to prevent concurrent borrows."""
    lock = _get_node_lock(node)
    with lock:
        return node.update_batch(items)

class _PrefetchState:
    """State manager for prefetch map operations.

    Manages concurrent execution of map functions while preserving output order.
    Uses a thread pool to process items ahead while emitting results in sequence order.

    Returns results to Rust for propagation instead of calling d.update() directly,
    to avoid thread-safety issues with PyO3 borrows.
    """

    def __init__(self, func, size):
        import threading
        self.func = func
        self.size = size
        self.executor = ThreadPoolExecutor(max_workers=size)
        self.pending = {}    # {seq_num: future}
        self.next_seq = 0    # next item sequence number
        self.next_emit = 0   # next sequence to emit
        self.ready = {}      # {seq_num: result} for completed out-of-order
        self._lock = threading.Lock()  # Protect against concurrent access

    def update(self, item):
        """Process an item and return ready results in order."""
        with self._lock:
            seq = self.next_seq
            self.next_seq += 1

            # Submit to thread pool
            future = self.executor.submit(self.func, item)
            self.pending[seq] = future

            # If at capacity, block and drain
            if len(self.pending) >= self.size:
                self._drain_one()

            # Return any ready results in order
            return self._collect_ready()

    def _drain_one(self):
        """Wait for the next expected result to complete."""
        if self.next_emit in self.pending:
            # Wait for the next expected result
            result = self.pending[self.next_emit].result()
            self.ready[self.next_emit] = result
            del self.pending[self.next_emit]

    def _collect_ready(self):
        """Collect all consecutive ready results."""
        results = []
        while self.next_emit in self.ready:
            result = self.ready.pop(self.next_emit)
            results.append(result)
            self.next_emit += 1
        return results

    def flush(self):
        """Wait for all pending items and return in order."""
        with self._lock:
            # Wait for all pending futures
            for seq in sorted(self.pending.keys()):
                self.ready[seq] = self.pending[seq].result()
            self.pending.clear()

            # Return all in order
            return self._collect_ready()

class _PrefetchFilterState:
    """State manager for prefetch filter operations.

    Like _PrefetchState but for filter predicates. The wrapped predicate
    returns (keep, value) tuples, and we only emit values where keep is True.

    Returns results to Rust for propagation instead of calling d.update() directly,
    to avoid thread-safety issues with PyO3 borrows.
    """

    def __init__(self, wrapped_predicate, size):
        import threading
        self.wrapped_predicate = wrapped_predicate
        self.size = size
        self.executor = ThreadPoolExecutor(max_workers=size)
        self.pending = {}    # {seq_num: future}
        self.next_seq = 0
        self.next_emit = 0
        self.ready = {}      # {seq_num: (keep, value)}
        self._lock = threading.Lock()  # Protect against concurrent access

    def update(self, item):
        """Process an item and return ready results in order (only those that passed filter)."""
        with self._lock:
            seq = self.next_seq
            self.next_seq += 1

            # Submit to thread pool - wrapped_predicate returns (keep, value)
            future = self.executor.submit(self.wrapped_predicate, item)
            self.pending[seq] = future

            # If at capacity, block and drain
            if len(self.pending) >= self.size:
                self._drain_one()

            # Return any ready results in order
            return self._collect_ready()

    def _drain_one(self):
        """Wait for the next expected result to complete."""
        if self.next_emit in self.pending:
            result = self.pending[self.next_emit].result()
            self.ready[self.next_emit] = result
            del self.pending[self.next_emit]

    def _collect_ready(self):
        """Collect all consecutive ready results where predicate was True."""
        results = []
        while self.next_emit in self.ready:
            keep, value = self.ready.pop(self.next_emit)
            if keep:  # Only include if predicate returned True
                results.append(value)
            self.next_emit += 1
        return results

    def flush(self):
        """Wait for all pending items and return in order (only those that passed filter)."""
        with self._lock:
            for seq in sorted(self.pending.keys()):
                self.ready[seq] = self.pending[seq].result()
            self.pending.clear()
            return self._collect_ready()
"#;

#[pymethods]
impl Stream {
    /// Create a new source Stream.
    ///
    /// Args:
    ///     name: Optional name for the stream (defaults to "source").
    ///     asynchronous: If False, skip async checking for better performance.
    ///         If None or True, async coroutines are automatically awaited.
    #[new]
    #[pyo3(signature = (name=None, asynchronous=None))]
    fn new(name: Option<String>, asynchronous: Option<bool>) -> Self {
        let skip_async_check = asynchronous == Some(false);
        Self {
            logic: NodeLogic::Source,
            downstreams: Vec::new(),
            name: name.unwrap_or_else(|| "source".to_string()),
            asynchronous,
            skip_async_check,
            func_is_sync: true, // Source has no function, treat as sync
            frozen: false,
            needs_lock: false,
        }
    }

    /// Apply a function to each element in the stream.
    ///
    /// Args:
    ///     func: Function to apply to each element.
    ///     *args: Additional positional arguments passed to func.
    ///     **kwargs: Additional keyword arguments passed to func.
    ///         Use `stream_name="name`" to set a custom name for this node.
    ///
    /// Returns:
    ///     A new Stream emitting the transformed values.
    #[pyo3(signature = (func, *args, **kwargs))]
    fn map(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let name = extract_stream_name(py, &kwargs, "map", &self.name)?;

        // Check if the original function is sync (before wrapping/moving)
        let is_sync_callable = get_is_sync_callable()?;
        let func_is_sync = is_sync_callable.call1(py, (&func,))?.is_truthy(py)?;

        let builder = get_build_map_func()?;
        let args_opt = if args.bind(py).is_empty() {
            None
        } else {
            Some(args)
        };
        let wrapped_func: Py<PyAny> = builder.call1(py, (func, args_opt, kwargs))?.extract(py)?;

        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::Map { func: wrapped_func },
                downstreams: Vec::new(),
                name,
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync,
                frozen: false,
                needs_lock: false,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    /// Enable prefetching for the next map operation.
    ///
    /// When followed by a `.map()`, this causes the map to process up to `n` items
    /// concurrently while preserving output order.
    ///
    /// Args:
    ///     n: Maximum number of items to process concurrently.
    ///
    /// Returns:
    ///     A new Stream that will modify the next map to use prefetching.
    #[pyo3(name = "prefetch", signature = (n))]
    fn prefetch(&mut self, py: Python, n: usize) -> PyResult<Py<Self>> {
        if n == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "prefetch size must be at least 1",
            ));
        }
        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::Prefetch { size: n },
                downstreams: Vec::new(),
                name: format!("{}.prefetch({})", self.name, n),
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync: true,
                frozen: false,
                needs_lock: false,
            },
        )?;
        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    /// Apply a function to each element, unpacking the element as arguments.
    ///
    /// Similar to map, but each element is unpacked with *element before
    /// being passed to the function. Useful when elements are tuples.
    ///
    /// Args:
    ///     func: Function to apply (receives unpacked element).
    ///     *args: Additional positional arguments passed to func.
    ///     **kwargs: Additional keyword arguments passed to func.
    ///
    /// Returns:
    ///     A new Stream emitting the transformed values.
    #[pyo3(signature = (func, *args, **kwargs))]
    fn starmap(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let name = extract_stream_name(py, &kwargs, "starmap", &self.name)?;

        // Check if the original function is sync (before wrapping/moving)
        let is_sync_callable = get_is_sync_callable()?;
        let func_is_sync = is_sync_callable.call1(py, (&func,))?.is_truthy(py)?;

        let builder = get_build_starmap_func()?;
        let args_opt = if args.bind(py).is_empty() {
            None
        } else {
            Some(args)
        };
        let wrapped_func: Py<PyAny> = builder.call1(py, (func, args_opt, kwargs))?.extract(py)?;

        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::Starmap { func: wrapped_func },
                downstreams: Vec::new(),
                name,
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync,
                frozen: false,
                needs_lock: false,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    /// Apply a batch-aware function to process entire batches at once.
    ///
    /// Unlike `map()` which calls the function once per item, `batch_map()` calls
    /// the function once per batch with all items as a list. This is ideal for
    /// vectorized operations like `NumPy` functions that can process arrays efficiently.
    ///
    /// Args:
    ///     func: Function that receives a list of items and returns an iterable of results.
    ///     *args: Additional positional arguments passed to func.
    ///     **kwargs: Additional keyword arguments passed to func.
    ///         Use `stream_name="name`" to set a custom name for this node.
    ///
    /// Returns:
    ///     A new Stream emitting the transformed values.
    ///
    /// Example:
    ///     >>> import numpy as np
    ///     >>> s = `Stream()`
    ///     >>> `s.batch_map(np.sqrt).sink(print)`
    ///     >>> `s.emit_batch`([1, 4, 9, 16])  # prints 1.0, 2.0, 3.0, 4.0
    #[pyo3(signature = (func, *args, **kwargs))]
    fn batch_map(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let name = extract_stream_name(py, &kwargs, "batch_map", &self.name)?;

        // Check if the original function is sync (before wrapping/moving)
        let is_sync_callable = get_is_sync_callable()?;
        let func_is_sync = is_sync_callable.call1(py, (&func,))?.is_truthy(py)?;

        let builder = get_build_map_func()?;
        let args_opt = if args.bind(py).is_empty() {
            None
        } else {
            Some(args)
        };
        let wrapped_func: Py<PyAny> = builder.call1(py, (func, args_opt, kwargs))?.extract(py)?;

        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::BatchMap { func: wrapped_func },
                downstreams: Vec::new(),
                name,
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync,
                frozen: false,
                needs_lock: false,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    /// Filter elements based on a predicate function.
    ///
    /// Only elements for which the predicate returns a truthy value
    /// are passed downstream.
    ///
    /// Args:
    ///     predicate: Function that returns True for elements to keep.
    ///     *args: Additional positional arguments passed to predicate.
    ///     **kwargs: Additional keyword arguments passed to predicate.
    ///
    /// Returns:
    ///     A new Stream emitting only elements that pass the filter.
    #[pyo3(signature = (predicate, *args, **kwargs))]
    fn filter(
        &mut self,
        py: Python,
        predicate: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let name = extract_stream_name(py, &kwargs, "filter", &self.name)?;

        // Check if the original predicate is sync (before wrapping/moving)
        let is_sync_callable = get_is_sync_callable()?;
        let func_is_sync = is_sync_callable.call1(py, (&predicate,))?.is_truthy(py)?;

        let builder = get_build_map_func()?;
        let args_opt = if args.bind(py).is_empty() {
            None
        } else {
            Some(args)
        };
        let wrapped_func: Py<PyAny> = builder
            .call1(py, (predicate, args_opt, kwargs))?
            .extract(py)?;

        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::Filter {
                    predicate: wrapped_func,
                },
                downstreams: Vec::new(),
                name,
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync,
                frozen: false,
                needs_lock: false,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    /// Flatten an iterable element into individual elements.
    ///
    /// Each element is expected to be iterable. The items within each
    /// element are emitted individually downstream.
    ///
    /// Returns:
    ///     A new Stream emitting the flattened elements.
    fn flatten(&mut self, py: Python) -> PyResult<Py<Self>> {
        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::Flatten,
                downstreams: Vec::new(),
                name: format!("{}.flatten", self.name),
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync: true, // No user function
                frozen: false,
                needs_lock: false,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    /// Collect elements into a buffer until `flush()` is called.
    ///
    /// Elements are accumulated in an internal buffer. When `flush()` is
    /// called, all collected elements are emitted as a single tuple.
    ///
    /// Returns:
    ///     A new Stream that emits collected tuples on flush.
    fn collect(&mut self, py: Python) -> PyResult<Py<Self>> {
        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::Collect { buffer: Vec::new() },
                downstreams: Vec::new(),
                name: format!("{}.collect", self.name),
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync: true, // No user function
                frozen: false,
                needs_lock: false,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    /// Flush collected elements downstream.
    ///
    /// For `collect()` nodes, emits all buffered elements as a tuple
    /// and clears the buffer. Has no effect on other node types.
    ///
    /// Args:
    ///     *args: Ignored (for compatibility with streamz).
    ///     **kwargs: Ignored (for compatibility with streamz).
    ///
    /// Returns:
    ///     A coroutine if async processing is triggered, otherwise None.
    #[pyo3(signature = (*_args, **_kwargs))]
    fn flush(
        &mut self,
        py: Python,
        _args: Vec<Py<PyAny>>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let mut futures = Vec::new();

        // Handle this node's flush behavior
        match &mut self.logic {
            NodeLogic::Collect { buffer } => {
                let items = std::mem::take(buffer);
                let tuple = PyTuple::new(py, items)?;
                if let Some(f) = self.propagate(py, tuple.into())? {
                    futures.push(f);
                }
            }
            NodeLogic::PrefetchMap { state, .. } => {
                // Flush the prefetch state - wait for all pending items and propagate
                let results = wrap_error(py, state.call_method0(py, "flush"), &self.name)?;
                let results_list = results.bind(py);
                for result in results_list.try_iter()? {
                    if let Some(f) = self.propagate(py, result?.into())? {
                        futures.push(f);
                    }
                }
            }
            NodeLogic::PrefetchFilter { state, .. } => {
                // Flush the prefetch filter state - wait for all pending items and propagate
                let results = wrap_error(py, state.call_method0(py, "flush"), &self.name)?;
                let results_list = results.bind(py);
                for result in results_list.try_iter()? {
                    if let Some(f) = self.propagate(py, result?.into())? {
                        futures.push(f);
                    }
                }
            }
            _ => {}
        }

        // Propagate flush to all downstream nodes
        for downstream in &self.downstreams {
            let mut d = downstream.borrow_mut(py);
            if let Some(f) = d.flush(py, vec![], None)? {
                futures.push(f);
            }
        }

        if !futures.is_empty() {
            let gather = get_gather()?;
            let gathered = gather.call1(py, (futures,))?;
            return Ok(Some(gathered));
        }
        Ok(None)
    }

    /// Apply a function to each element for side effects.
    ///
    /// Similar to map, but the return value of func is discarded.
    /// Use this for terminal operations like printing or storing results.
    ///
    /// Args:
    ///     func: Function to call with each element.
    ///     *args: Additional positional arguments passed to func.
    ///     **kwargs: Additional keyword arguments passed to func.
    ///
    /// Returns:
    ///     A new Stream (for chaining, though sink typically ends a pipeline).
    #[pyo3(signature = (func, *args, **kwargs))]
    fn sink(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let name = extract_stream_name(py, &kwargs, "sink", &self.name)?;

        // Check if the original function is sync (before wrapping/moving)
        let is_sync_callable = get_is_sync_callable()?;
        let func_is_sync = is_sync_callable.call1(py, (&func,))?.is_truthy(py)?;

        let builder = get_build_map_func()?;
        let args_opt = if args.bind(py).is_empty() {
            None
        } else {
            Some(args)
        };
        let wrapped_func: Py<PyAny> = builder.call1(py, (func, args_opt, kwargs))?.extract(py)?;

        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::Sink { func: wrapped_func },
                downstreams: Vec::new(),
                name,
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync,
                frozen: false,
                needs_lock: false,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    /// Accumulate values using a function, emitting each intermediate state.
    ///
    /// Similar to functools.reduce, but emits every intermediate result.
    /// The function receives (`current_state`, `new_element`) and returns the new state.
    ///
    /// Args:
    ///     func: Accumulator function (state, element) -> `new_state`.
    ///           If `returns_state=True`, function should return (new_state, result).
    ///     start: Initial state value.
    ///     returns_state: If True, func returns (state, result) tuple where state
    ///           is passed to next call and result is emitted. Default False.
    ///
    /// Returns:
    ///     A new Stream emitting accumulated states (or results if `returns_state=True`).
    #[pyo3(signature = (func, start, returns_state=false))]
    fn accumulate(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        start: Py<PyAny>,
        returns_state: bool,
    ) -> PyResult<Py<Self>> {
        // Check if the accumulator function is sync
        let is_sync_callable = get_is_sync_callable()?;
        let func_is_sync = is_sync_callable.call1(py, (&func,))?.is_truthy(py)?;

        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::Accumulate {
                    func,
                    state: start,
                    returns_state,
                },
                downstreams: Vec::new(),
                name: format!("{}.accumulate", self.name),
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync,
                frozen: false,
                needs_lock: false,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    /// Create a parallel branch point that executes all downstreams concurrently.
    ///
    /// When values flow through a parallel node, all downstream branches are
    /// executed in parallel threads rather than sequentially. This is useful for
    /// I/O-bound operations where branches can overlap their waiting time.
    ///
    /// Use `seq()` to switch back to sequential execution after parallel branches.
    ///
    /// Returns:
    ///     A new Stream that will propagate values to its downstreams in parallel.
    ///
    /// Example:
    ///     >>> s = Stream()
    ///     >>> p = s.par()  # Create parallel branch point
    ///     >>> a = p.map(load_from_disk)   # Branch 1
    ///     >>> b = p.map(fetch_from_api)   # Branch 2
    ///     >>> merged = union(a, b).seq()  # Back to sequential
    ///     >>> s.emit(filename)  # a and b run in parallel
    #[pyo3(name = "par")]
    fn par(&mut self, py: Python) -> PyResult<Py<Self>> {
        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::Parallel,
                downstreams: Vec::new(),
                name: format!("{}.par", self.name),
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync: true, // No user function
                frozen: false,
                needs_lock: false,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    /// Alias for `par()`. Create a parallel branch point.
    fn parallel(&mut self, py: Python) -> PyResult<Py<Self>> {
        self.par(py)
    }

    /// Create a sequential branch point (explicit synchronization point).
    ///
    /// This is the default behavior for streams, but can be used explicitly
    /// after parallel sections to clearly indicate sequential execution resumes.
    ///
    /// Returns:
    ///     A new Stream that will propagate values to its downstreams sequentially.
    ///
    /// Example:
    ///     >>> s = Stream()
    ///     >>> p = s.par()
    ///     >>> a = p.map(slow_op1)
    ///     >>> b = p.map(slow_op2)
    ///     >>> merged = union(a, b).seq()  # Explicit sequential point
    ///     >>> merged.map(f1).sink(...)    # Sequential from here
    ///     >>> merged.map(f2).sink(...)
    fn seq(&mut self, py: Python) -> PyResult<Py<Self>> {
        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::Source,
                downstreams: Vec::new(),
                name: format!("{}.seq", self.name),
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync: true, // No user function
                frozen: false,
                needs_lock: false,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    /// Merge multiple streams into one.
    ///
    /// Creates a new stream that emits elements from this stream and all
    /// other streams. Elements are emitted in the order they arrive.
    ///
    /// Args:
    ///     *others: Other streams to merge with this one.
    ///
    /// Returns:
    ///     A new Stream emitting elements from all input streams.
    #[pyo3(signature = (*others))]
    fn union(&mut self, py: Python, others: Vec<Py<Self>>) -> PyResult<Py<Self>> {
        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::Source,
                downstreams: Vec::new(),
                name: format!("{}.union", self.name),
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync: true, // No user function
                frozen: false,
                needs_lock: false,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));

        for other in others {
            // Use try_borrow_mut to handle case where other is self (already borrowed)
            match other.try_borrow_mut(py) {
                Ok(mut other_ref) => {
                    other_ref.downstreams.push(node.clone_ref(py));
                }
                Err(_) => {
                    // Already borrowed - means it's self, skip (already added above)
                }
            }
        }

        Ok(node)
    }

    /// Combine the latest values from multiple streams.
    ///
    /// Emits a tuple of the most recent values from each stream whenever
    /// any stream emits. Only starts emitting after all streams have
    /// emitted at least one value.
    ///
    /// Args:
    ///     *others: Other streams to combine with this one.
    ///     emit_on: Stream or list of streams that trigger emission.
    ///              If None (default), emit on update from any stream.
    ///
    /// Returns:
    ///     A new Stream emitting tuples of latest values.
    #[pyo3(signature = (*others, emit_on=None))]
    fn combine_latest(
        &mut self,
        py: Python,
        others: Vec<Py<Self>>,
        emit_on: Option<Py<PyAny>>,
    ) -> PyResult<Py<Self>> {
        let total_sources = 1 + others.len();

        // Helper to find stream index. Returns Some(0) if stream is self (can't borrow),
        // Some(i+1) if stream matches others[i], or None if not found.
        let find_stream_index = |stream: &Py<Self>| -> Option<usize> {
            // Try to borrow - if it fails, it's self (already mutably borrowed)
            if stream.try_borrow(py).is_err() {
                return Some(0); // It's self
            }
            // Check against others
            for (i, other) in others.iter().enumerate() {
                if stream.as_ptr() == other.as_ptr() {
                    return Some(i + 1);
                }
            }
            None
        };

        // Resolve emit_on to indices
        let emit_on_indices: Vec<usize> = if let Some(emit_on_obj) = emit_on {
            let mut indices = Vec::new();

            // Check if it's a single stream or a list
            if let Ok(single_stream) = emit_on_obj.extract::<Py<Self>>(py) {
                if let Some(idx) = find_stream_index(&single_stream) {
                    indices.push(idx);
                }
            } else if let Ok(stream_list) = emit_on_obj.extract::<Vec<Py<Self>>>(py) {
                for stream in &stream_list {
                    if let Some(idx) = find_stream_index(stream) {
                        indices.push(idx);
                    }
                }
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "emit_on must be a Stream or list of Streams",
                ));
            }

            indices
        } else {
            Vec::new() // Empty means emit on any
        };

        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::CombineLatest {
                    state: (0..total_sources).map(|_| None).collect(),
                    emit_on_indices,
                },
                downstreams: Vec::new(),
                name: format!("{}.combine_latest", self.name),
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync: true, // No user function
                frozen: false,
                needs_lock: false,
            },
        )?;

        self.add_tag_node(py, 0, &node)?;
        for (i, other) in others.iter().enumerate() {
            // Use try_borrow_mut to handle case where other is self (already borrowed)
            match other.try_borrow_mut(py) {
                Ok(mut other_ref) => {
                    other_ref.add_tag_node(py, i + 1, &node)?;
                }
                Err(_) => {
                    // Already borrowed - means it's self, use self directly
                    self.add_tag_node(py, i + 1, &node)?;
                }
            }
        }

        Ok(node)
    }

    /// Zip multiple streams together element-by-element.
    ///
    /// Emits tuples containing one element from each stream. Waits until
    /// all streams have an element available before emitting.
    ///
    /// Args:
    ///     *others: Other streams to zip with this one.
    ///
    /// Returns:
    ///     A new Stream emitting zipped tuples.
    #[pyo3(signature = (*others))]
    fn zip(&mut self, py: Python, others: Vec<Py<Self>>) -> PyResult<Py<Self>> {
        let total_sources = 1 + others.len();
        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::Zip {
                    buffers: (0..total_sources).map(|_| VecDeque::new()).collect(),
                },
                downstreams: Vec::new(),
                name: format!("{}.zip", self.name),
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync: true, // No user function
                frozen: false,
                needs_lock: false,
            },
        )?;

        self.add_tag_node(py, 0, &node)?;
        for (i, other) in others.iter().enumerate() {
            // Use try_borrow_mut to handle case where other is self (already borrowed)
            match other.try_borrow_mut(py) {
                Ok(mut other_ref) => {
                    other_ref.add_tag_node(py, i + 1, &node)?;
                }
                Err(_) => {
                    // Already borrowed - means it's self, use self directly
                    self.add_tag_node(py, i + 1, &node)?;
                }
            }
        }

        Ok(node)
    }

    /// Emit a single value into the stream.
    ///
    /// This is the primary way to push data into a source stream.
    /// The value propagates through all downstream nodes.
    ///
    /// Args:
    ///     x: The value to emit.
    ///
    /// Returns:
    ///     A coroutine if async processing is triggered, otherwise None.
    fn emit(&mut self, py: Python, x: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        // On first emit, optimize the topology
        if !self.frozen {
            self.optimize_topology(py);
            self.frozen = true;
        }
        // Emit bypasses this node's logic and propagates directly to downstreams
        // (matching streamz behavior)
        self.propagate(py, x)
    }

    /// Emit multiple values into the stream as a batch.
    ///
    /// More efficient than calling `emit()` multiple times when processing
    /// many items. Values are processed together through the pipeline.
    ///
    /// Args:
    ///     items: List of values to emit.
    ///
    /// Returns:
    ///     A coroutine if async processing is triggered, otherwise None.
    fn emit_batch(&mut self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<Option<Py<PyAny>>> {
        // On first emit, optimize the topology
        if !self.frozen {
            self.optimize_topology(py);
            self.frozen = true;
        }
        // Emit bypasses this node's logic and propagates directly to downstreams
        // (matching streamz behavior)
        self.propagate_batch(py, items)
    }

    fn update_batch(&mut self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<Option<Py<PyAny>>> {
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

    fn propagate_batch(
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

    fn propagate(&self, py: Python, val: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
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

    fn update(&mut self, py: Python, x: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
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

impl Stream {
    fn add_tag_node(&mut self, py: Python, index: usize, target: &Py<Self>) -> PyResult<()> {
        let tag_node = Py::new(
            py,
            Self {
                logic: NodeLogic::Tag { index },
                downstreams: vec![target.clone_ref(py)],
                name: format!("{}.tag({})", self.name, index),
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync: true, // Tag has no user function
                frozen: false,
                needs_lock: false,
            },
        )?;
        self.downstreams.push(tag_node);
        Ok(())
    }

    /// Propagate value to downstreams in parallel using thread pool.
    fn propagate_parallel(&self, py: Python, val: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
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

    /// Optimize the topology on first emit.
    ///
    /// This method walks the stream graph and:
    /// - Fuses consecutive Map operations into a single composed Map
    /// - Fuses consecutive Filter operations into a single composed Filter
    /// - If all nodes have sync-only functions (`func_is_sync` = true), enables
    ///   `skip_async_check` for all nodes to avoid per-emit `is_awaitable` checks
    fn optimize_topology(&mut self, py: Python) {
        // Phase 1: Fuse consecutive maps and filters
        // We need to process nodes that might have fusable children
        self.fuse_linear_chains(py);

        // Phase 2: Collect all nodes and check sync status
        let mut all_nodes: Vec<Py<Self>> = Vec::new();
        let mut to_visit: Vec<Py<Self>> =
            self.downstreams.iter().map(|d| d.clone_ref(py)).collect();
        let mut all_sync = self.func_is_sync;

        // BFS traversal
        while let Some(node_py) = to_visit.pop() {
            let node = node_py.borrow(py);
            all_sync = all_sync && node.func_is_sync;
            for downstream in &node.downstreams {
                to_visit.push(downstream.clone_ref(py));
            }
            drop(node);
            all_nodes.push(node_py);
        }

        // If all nodes are sync and asynchronous flag wasn't explicitly set,
        // enable skip_async_check for all nodes
        if all_sync && self.asynchronous.is_none() {
            self.skip_async_check = true;
            for node_py in &all_nodes {
                let mut node = node_py.borrow_mut(py);
                if node.asynchronous.is_none() {
                    node.skip_async_check = true;
                }
            }
        }

        // Phase 3: Mark prefetch nodes inside par branches as needing locks
        // These nodes use internal threading which can conflict with par's threading
        self.mark_prefetch_in_par(py, &all_nodes);
    }

    /// Mark nodes that need locking due to being downstream of a prefetch within a par branch.
    /// When prefetch is inside par, its internal threading combined with par's threading
    /// can cause borrow conflicts at any downstream node (especially convergence points like union).
    /// We mark ALL nodes from the prefetch onwards as needing locks.
    #[allow(clippy::unused_self)] // Called as method for consistency with other topology methods
    fn mark_prefetch_in_par(&self, py: Python, all_nodes: &[Py<Self>]) {
        use std::collections::{HashMap, HashSet};

        // Find all Par nodes
        let mut par_nodes: Vec<Py<Self>> = Vec::new();
        for node_py in all_nodes {
            let node = node_py.borrow(py);
            if matches!(node.logic, NodeLogic::Parallel) {
                drop(node);
                par_nodes.push(node_py.clone_ref(py));
            }
        }

        if par_nodes.is_empty() {
            return; // No parallel branches, no prefetch locking needed
        }

        for par_py in par_nodes {
            let par = par_py.borrow(py);
            let branches: Vec<Py<Self>> = par.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            drop(par);

            if branches.len() < 2 {
                continue; // Need at least 2 branches for convergence
            }

            // Check if any branch contains a prefetch
            let mut has_prefetch = false;
            for branch_py in &branches {
                let mut visited: HashSet<usize> = HashSet::new();
                let mut stack: Vec<Py<Self>> = vec![branch_py.clone_ref(py)];

                while let Some(node_py) = stack.pop() {
                    let node_ptr = node_py.as_ptr() as usize;
                    if visited.contains(&node_ptr) {
                        continue;
                    }
                    visited.insert(node_ptr);

                    let node = node_py.borrow(py);
                    if matches!(
                        node.logic,
                        NodeLogic::PrefetchMap { .. } | NodeLogic::PrefetchFilter { .. }
                    ) {
                        has_prefetch = true;
                        break;
                    }
                    for downstream in &node.downstreams {
                        stack.push(downstream.clone_ref(py));
                    }
                }
                if has_prefetch {
                    break;
                }
            }

            if !has_prefetch {
                continue; // No prefetch in this par, no locking needed
            }

            // Find convergence points: nodes reachable from multiple branches
            // Track which branch indices can reach each node
            let mut node_branches: HashMap<usize, HashSet<usize>> = HashMap::new();
            let mut node_refs: HashMap<usize, Py<Self>> = HashMap::new();

            for (branch_idx, branch_py) in branches.iter().enumerate() {
                let mut visited: HashSet<usize> = HashSet::new();
                let mut stack: Vec<Py<Self>> = vec![branch_py.clone_ref(py)];

                while let Some(node_py) = stack.pop() {
                    let node_ptr = node_py.as_ptr() as usize;
                    if visited.contains(&node_ptr) {
                        continue;
                    }
                    visited.insert(node_ptr);

                    node_branches.entry(node_ptr).or_default().insert(branch_idx);
                    node_refs.entry(node_ptr).or_insert_with(|| node_py.clone_ref(py));

                    let node = node_py.borrow(py);
                    for downstream in &node.downstreams {
                        stack.push(downstream.clone_ref(py));
                    }
                }
            }

            // Find convergence points (nodes reachable from 2+ branches)
            let convergence_ids: HashSet<usize> = node_branches
                .iter()
                .filter(|(_, branch_set)| branch_set.len() >= 2)
                .map(|(id, _)| *id)
                .collect();

            if convergence_ids.is_empty() {
                continue; // No convergence points, no locking needed
            }

            // Build reverse graph to find nodes that can reach convergence points
            let mut can_reach_convergence: HashSet<usize> = HashSet::new();

            // For each node, check if it can reach any convergence point
            for (node_id, node_py) in &node_refs {
                if can_reach_convergence.contains(node_id) {
                    continue;
                }

                // BFS to check if this node can reach a convergence point
                let mut visited: HashSet<usize> = HashSet::new();
                let mut stack: Vec<Py<Self>> = vec![node_py.clone_ref(py)];
                let mut reaches_convergence = false;

                while let Some(n_py) = stack.pop() {
                    let n_id = n_py.as_ptr() as usize;
                    if visited.contains(&n_id) {
                        continue;
                    }
                    visited.insert(n_id);

                    if convergence_ids.contains(&n_id) {
                        reaches_convergence = true;
                        break;
                    }

                    let n = n_py.borrow(py);
                    for downstream in &n.downstreams {
                        stack.push(downstream.clone_ref(py));
                    }
                }

                if reaches_convergence {
                    // Mark all visited nodes as able to reach convergence
                    can_reach_convergence.extend(visited);
                }
            }

            // Mark all nodes that can reach convergence points (and convergence points themselves)
            for node_id in &can_reach_convergence {
                if let Some(node_py) = node_refs.get(node_id) {
                    let mut node = node_py.borrow_mut(py);
                    node.needs_lock = true;
                }
            }

            // Also mark all nodes downstream of convergence points
            let mut marked: HashSet<usize> = can_reach_convergence;
            for conv_id in &convergence_ids {
                if let Some(conv_py) = node_refs.get(conv_id) {
                    let mut to_mark: Vec<Py<Self>> = vec![conv_py.clone_ref(py)];

                    while let Some(node_py) = to_mark.pop() {
                        let node_ptr = node_py.as_ptr() as usize;
                        if marked.contains(&node_ptr) {
                            continue;
                        }
                        marked.insert(node_ptr);

                        let mut node = node_py.borrow_mut(py);
                        node.needs_lock = true;

                        for downstream in &node.downstreams {
                            to_mark.push(downstream.clone_ref(py));
                        }
                    }
                }
            }
        }
    }

    /// Recursively fuse linear chains of Map or Filter nodes.
    fn fuse_linear_chains(&mut self, py: Python) {
        // Process each downstream
        for downstream in &self.downstreams {
            let mut child = downstream.borrow_mut(py);
            child.fuse_linear_chains(py);
        }

        // Try to fuse this node with its single downstream if applicable
        if self.downstreams.len() != 1 {
            return;
        }

        let child_py = &self.downstreams[0];
        let child = child_py.borrow_mut(py);

        // Check if we can fuse Map -> Map
        if let NodeLogic::Map { func: ref my_func } = self.logic
            && let NodeLogic::Map {
                func: ref child_func,
            } = child.logic
            && let Ok(compose) = get_compose_maps()
            && let Ok(composed) = compose.call1(py, (my_func, child_func))
        {
            // Clone child's downstreams before modifying self
            let new_downstreams: Vec<Py<Self>> =
                child.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            let child_name = child.name.clone();
            let child_func_is_sync = child.func_is_sync;
            drop(child); // Release borrow before modifying self

            // Update this node's function to the composed one
            self.logic = NodeLogic::Map { func: composed };
            // Update name to show fusion
            self.name = format!("{}+{}", self.name, child_name);
            // Bypass child: point to child's downstreams
            self.downstreams = new_downstreams;
            // Inherit func_is_sync (both must be sync for composed to be sync)
            self.func_is_sync = self.func_is_sync && child_func_is_sync;
            return;
        }

        // Check if we can fuse Filter -> Filter
        if let NodeLogic::Filter {
            predicate: ref my_pred,
        } = self.logic
            && let NodeLogic::Filter {
                predicate: ref child_pred,
            } = child.logic
            && let Ok(compose) = get_compose_filters()
            && let Ok(composed) = compose.call1(py, (my_pred, child_pred))
        {
            // Clone child's downstreams before modifying self
            let new_downstreams: Vec<Py<Self>> =
                child.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            let child_name = child.name.clone();
            let child_func_is_sync = child.func_is_sync;
            drop(child); // Release borrow before modifying self

            // Update this node's predicate to the composed one
            self.logic = NodeLogic::Filter {
                predicate: composed,
            };
            // Update name to show fusion
            self.name = format!("{}+{}", self.name, child_name);
            // Bypass child: point to child's downstreams
            self.downstreams = new_downstreams;
            // Inherit func_is_sync
            self.func_is_sync = self.func_is_sync && child_func_is_sync;
            return;
        }

        // Check if we can fuse Prefetch -> Map into PrefetchMap
        if let NodeLogic::Prefetch { size } = self.logic
            && let NodeLogic::Map { ref func } = child.logic
            && let Ok(prefetch_state_class) = get_prefetch_state_class()
            && let Ok(state) = prefetch_state_class.call1(py, (func, size))
        {
            // Clone child's downstreams before modifying self
            let new_downstreams: Vec<Py<Self>> =
                child.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            let child_name = child.name.clone();
            let child_func_is_sync = child.func_is_sync;
            let child_func = func.clone_ref(py);
            drop(child); // Release borrow before modifying self

            // Replace this node with PrefetchMap
            self.logic = NodeLogic::PrefetchMap {
                func: child_func,
                size,
                state,
            };
            // Update name to show fusion
            self.name = format!("prefetch_map({size})+{child_name}");
            // Bypass child: point to child's downstreams
            self.downstreams = new_downstreams;
            // Inherit func_is_sync
            self.func_is_sync = child_func_is_sync;
            return;
        }

        // Check if we can fuse Prefetch -> Starmap into PrefetchMap
        if let NodeLogic::Prefetch { size } = self.logic
            && let NodeLogic::Starmap { ref func } = child.logic
            && let Ok(prefetch_state_class) = get_prefetch_state_class()
            && let Ok(state) = prefetch_state_class.call1(py, (func, size))
        {
            let new_downstreams: Vec<Py<Self>> =
                child.downstreams.iter().map(|d| d.clone_ref(py)).collect();
            let child_name = child.name.clone();
            let child_func_is_sync = child.func_is_sync;
            let child_func = func.clone_ref(py);
            drop(child);

            self.logic = NodeLogic::PrefetchMap {
                func: child_func,
                size,
                state,
            };
            self.name = format!("prefetch_starmap({size})+{child_name}");
            self.downstreams = new_downstreams;
            self.func_is_sync = child_func_is_sync;
            return;
        }

        // Check if we can fuse Prefetch -> Filter into PrefetchFilter
        // Note: For filter, we wrap the predicate to return (keep, value) tuples
        if let NodeLogic::Prefetch { size } = self.logic
            && let NodeLogic::Filter { ref predicate } = child.logic
            && let Ok(prefetch_filter_state_class) = get_prefetch_filter_state_class()
        {
            // Create a wrapper that returns (predicate_result, original_value)
            let pred_clone = predicate.clone_ref(py);
            let wrapper_code = "lambda p: lambda x: (p(x), x)";
            if let Ok(builtins) = py.import("builtins")
                && let Ok(eval_fn) = builtins.getattr("eval")
                && let Ok(make_wrapper) = eval_fn.call1((wrapper_code,))
                && let Ok(wrapped_pred) = make_wrapper.call1((pred_clone,))
                && let Ok(state) = prefetch_filter_state_class.call1(py, (&wrapped_pred, size))
            {
                let new_downstreams: Vec<Py<Self>> =
                    child.downstreams.iter().map(|d| d.clone_ref(py)).collect();
                let child_name = child.name.clone();
                let child_func_is_sync = child.func_is_sync;
                drop(child);

                self.logic = NodeLogic::PrefetchFilter {
                    predicate: wrapped_pred.unbind(),
                    size,
                    state,
                };
                self.name = format!("prefetch_filter({size})+{child_name}");
                self.downstreams = new_downstreams;
                self.func_is_sync = child_func_is_sync;
            }
        }
    }
}

/// Merge multiple streams into one.
///
/// Creates a new stream that emits elements from all input streams.
/// Elements are emitted in the order they arrive from any source.
///
/// Args:
///     *streams: Two or more streams to merge.
///
/// Returns:
///     A new Stream emitting elements from all input streams.
///
/// Example:
///     >>> s1 = rstreamz.Stream()
///     >>> s2 = rstreamz.Stream()
///     >>> merged = rstreamz.union(s1, s2)
#[pyfunction]
#[pyo3(signature = (*streams))]
fn union(py: Python, streams: Vec<Py<Stream>>) -> PyResult<Py<Stream>> {
    if streams.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "union requires at least one stream",
        ));
    }

    let node = Py::new(
        py,
        Stream {
            logic: NodeLogic::Source,
            downstreams: Vec::new(),
            name: "union".to_string(),
            asynchronous: None,
            skip_async_check: false,
            func_is_sync: true,
            frozen: false,
            needs_lock: false,
        },
    )?;

    for stream in streams {
        let mut stream_ref = stream.borrow_mut(py);
        stream_ref.downstreams.push(node.clone_ref(py));
    }

    Ok(node)
}

/// Combine the latest values from multiple streams.
///
/// Creates a new stream that emits a tuple of the most recent values
/// from each input stream whenever any stream emits. Only starts
/// emitting after all streams have emitted at least one value.
///
/// Args:
///     *streams: Two or more streams to combine.
///     emit_on: Stream or list of streams that trigger emission.
///              If None (default), emit on update from any stream.
///
/// Returns:
///     A new Stream emitting tuples of latest values.
///
/// Example:
///     >>> s1 = rstreamz.Stream()
///     >>> s2 = rstreamz.Stream()
///     >>> combined = rstreamz.combine_latest(s1, s2)
///     >>> combined_on_s1 = rstreamz.combine_latest(s1, s2, emit_on=s1)
#[pyfunction]
#[pyo3(signature = (*streams, emit_on=None))]
fn combine_latest(
    py: Python,
    streams: Vec<Py<Stream>>,
    emit_on: Option<Py<PyAny>>,
) -> PyResult<Py<Stream>> {
    if streams.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "combine_latest requires at least one stream",
        ));
    }

    // Resolve emit_on to indices within the streams vector
    let emit_on_indices: Vec<usize> = if let Some(emit_on_obj) = emit_on {
        let mut indices = Vec::new();

        // Check if it's a single stream or a list
        if let Ok(single_stream) = emit_on_obj.extract::<Py<Stream>>(py) {
            // Single stream - find its index by comparing Python object pointers
            for (i, s) in streams.iter().enumerate() {
                if single_stream.as_ptr() == s.as_ptr() {
                    indices.push(i);
                    break;
                }
            }
        } else if let Ok(stream_list) = emit_on_obj.extract::<Vec<Py<Stream>>>(py) {
            // List of streams
            for stream in &stream_list {
                for (i, s) in streams.iter().enumerate() {
                    if stream.as_ptr() == s.as_ptr() {
                        indices.push(i);
                        break;
                    }
                }
            }
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "emit_on must be a Stream or list of Streams",
            ));
        }

        indices
    } else {
        Vec::new() // Empty means emit on any
    };

    let total_sources = streams.len();
    let node = Py::new(
        py,
        Stream {
            logic: NodeLogic::CombineLatest {
                state: (0..total_sources).map(|_| None).collect(),
                emit_on_indices,
            },
            downstreams: Vec::new(),
            name: "combine_latest".to_string(),
            asynchronous: None,
            skip_async_check: false,
            func_is_sync: true,
            frozen: false,
            needs_lock: false,
        },
    )?;

    for (i, stream) in streams.iter().enumerate() {
        let mut stream_ref = stream.borrow_mut(py);
        stream_ref.add_tag_node(py, i, &node)?;
    }

    Ok(node)
}

/// Zip multiple streams together.
///
/// Creates a new stream that waits for one item from each input stream,
/// then emits them as a tuple. Buffers items until all streams have
/// contributed one value.
///
/// Args:
///     *streams: Two or more streams to zip together.
///
/// Returns:
///     A new stream that emits tuples of values from all input streams.
#[pyfunction]
#[pyo3(signature = (*streams))]
fn zip(py: Python, streams: Vec<Py<Stream>>) -> PyResult<Py<Stream>> {
    if streams.len() < 2 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "zip requires at least two streams",
        ));
    }

    let total_sources = streams.len();
    let node = Py::new(
        py,
        Stream {
            logic: NodeLogic::Zip {
                buffers: (0..total_sources).map(|_| VecDeque::new()).collect(),
            },
            downstreams: Vec::new(),
            name: "zip".to_string(),
            asynchronous: None,
            skip_async_check: false,
            func_is_sync: true,
            frozen: false,
            needs_lock: false,
        },
    )?;

    for (i, stream) in streams.iter().enumerate() {
        let mut stream_ref = stream.borrow_mut(py);
        stream_ref.add_tag_node(py, i, &node)?;
    }

    Ok(node)
}

#[pymodule]
fn rstreamz(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Stream>()?;
    m.add_function(wrap_pyfunction!(to_text_file, m)?)?;
    m.add_function(wrap_pyfunction!(from_text_file, m)?)?;
    m.add_function(wrap_pyfunction!(union, m)?)?;
    m.add_function(wrap_pyfunction!(combine_latest, m)?)?;
    m.add_function(wrap_pyfunction!(zip, m)?)?;

    let code = CString::new(HELPERS).unwrap();
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
    let parallel_execute = helpers_module.getattr("_parallel_execute")?;
    let prefetch_state_class = helpers_module.getattr("_PrefetchState")?;
    let prefetch_filter_state_class = helpers_module.getattr("_PrefetchFilterState")?;
    let safe_update = helpers_module.getattr("_safe_update")?;
    let safe_update_batch = helpers_module.getattr("_safe_update_batch")?;

    let _ = PROCESS_ASYNC.set(process_async.unbind());
    let _ = GATHER.set(gather.unbind());
    let _ = FILTER_ASYNC.set(filter_async.unbind());
    let _ = BUILD_MAP_FUNC.set(build_map_func.unbind());
    let _ = BUILD_STARMAP_FUNC.set(build_starmap_func.unbind());
    let _ = IS_SYNC_CALLABLE.set(is_sync_callable.unbind());
    let _ = COMPOSE_MAPS.set(compose_maps.unbind());
    let _ = COMPOSE_FILTERS.set(compose_filters.unbind());
    let _ = PARALLEL_EXECUTE.set(parallel_execute.unbind());
    let _ = SAFE_UPDATE.set(safe_update.unbind());
    let _ = SAFE_UPDATE_BATCH.set(safe_update_batch.unbind());
    let _ = PREFETCH_STATE_CLASS.set(prefetch_state_class.unbind());
    let _ = PREFETCH_FILTER_STATE_CLASS.set(prefetch_filter_state_class.unbind());

    let inspect = py.import("inspect")?;
    let is_awaitable = inspect.getattr("isawaitable")?;
    let _ = IS_AWAITABLE.set(is_awaitable.unbind());

    Ok(())
}
