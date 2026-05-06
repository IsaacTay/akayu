//! Stream transformation operations (map, filter, sink, etc.)

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PySet, PyTuple};

use crate::error::StreamError;
use crate::helpers::{
    capture_source_location, check_not_compiled, resolve_stream_name, get_async_state_class,
    get_build_map_func, get_build_starmap_func, get_func_name, get_gather, get_is_sync_callable,
    get_prefetch_batch_map_state_class, get_prefetch_filter_state_class, get_prefetch_state_class,
    get_profiler_class, wrap_error,
};
use crate::node::NodeLogic;
use crate::Stream;

struct PyLockGuard {
    lock: Py<PyAny>,
    released: bool,
}

impl PyLockGuard {
    fn acquire(py: Python, lock: &Py<PyAny>) -> PyResult<Self> {
        lock.call_method0(py, "acquire")?;
        Ok(Self {
            lock: lock.clone_ref(py),
            released: false,
        })
    }

    fn release(mut self, py: Python) -> PyResult<()> {
        if !self.released {
            self.lock.call_method1(py, "release", ())?;
            self.released = true;
        }
        Ok(())
    }
}

impl Drop for PyLockGuard {
    fn drop(&mut self) {
        if !self.released {
            Python::attach(|py| {
                let _ = self.lock.call_method1(py, "release", ());
            });
        }
    }
}

fn make_emit_lock(py: Python) -> Py<PyAny> {
    py.import("threading")
        .unwrap()
        .getattr("RLock")
        .unwrap()
        .call0()
        .unwrap()
        .unbind()
}

/// Check if a Python callable is synchronous using the cached helper.
fn check_func_is_sync(py: Python, func: &Py<PyAny>) -> PyResult<bool> {
    let is_sync_callable = get_is_sync_callable()?;
    is_sync_callable.call1(py, (func,))?.is_truthy(py)
}

/// Wrap a function with additional args/kwargs using the build_map_func helper.
fn wrap_func(
    py: Python,
    func: Py<PyAny>,
    args: Py<PyTuple>,
    kwargs: Option<Py<PyDict>>,
    builder: &Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let args_opt = if args.bind(py).is_empty() {
        None
    } else {
        Some(args)
    };
    builder.call1(py, (func, args_opt, kwargs))
}

impl Stream {
    /// Create a new Stream node with default field values.
    ///
    /// This is an associated function (not &self) to avoid borrow conflicts
    /// when the caller already holds a mutable borrow on a parent node.
    pub(crate) fn make_node(
        py: Python,
        logic: NodeLogic,
        name: String,
        source_location: String,
        asynchronous: Option<bool>,
        skip_async_check: bool,
        func_is_sync: bool,
    ) -> PyResult<Py<Self>> {
        let compiled_chainable = (logic.is_chainable()
            && !matches!(&logic, NodeLogic::Filter { .. }))
            || matches!(&logic, NodeLogic::Chain { .. });
        let compiled_filter_fast = matches!(&logic, NodeLogic::Filter { .. }) && skip_async_check;
        Py::new(
            py,
            Self {
                logic,
                downstreams: Vec::new(),
                parents: std::sync::Mutex::new(Vec::new()),
                name,
                asynchronous,
                skip_async_check,
                func_is_sync,
                frozen: false,
                needs_lock: false,
                par_branches_marked: false,
                compiled: false,
                source_location,
                builtin_op: None,
                cached_vectorcall: None,
                compiled_chainable,
                compiled_filter_fast,
                compiled_downstream_ptrs: Vec::new(),
                flush_target: None,
                emit_target: None,
                flushing: false,
                emitting: std::sync::atomic::AtomicBool::new(false),
                emit_lock: make_emit_lock(py),
                convergence_checked: false,
            },
        )
    }
}

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
    pub fn new(py: Python, name: Option<String>, asynchronous: Option<bool>) -> Self {
        let skip_async_check = asynchronous == Some(false);
        let source_location = capture_source_location(py);
        Self {
            logic: NodeLogic::Source,
            downstreams: Vec::new(),
            parents: std::sync::Mutex::new(Vec::new()),
            name: name.unwrap_or_else(|| "source".to_string()),
            asynchronous,
            skip_async_check,
            func_is_sync: true, // Source has no function, treat as sync
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
            emit_lock: make_emit_lock(py),
            convergence_checked: false,
        }
    }

    /// Returns True if the stream graph is compiled (frozen).
    #[getter]
    pub fn compiled(&self) -> bool {
        self.compiled
    }

    /// Apply a function to each element in the stream.
    ///
    /// Transforms each element by passing it through the provided function.
    /// The function can accept additional positional and keyword arguments.
    ///
    /// # Arguments
    ///
    /// * `func` - Function to apply to each element.
    /// * `*args` - Additional positional arguments passed to `func`.
    /// * `**kwargs` - Additional keyword arguments passed to `func`. Use
    ///   `stream_name="name"` to set a custom name for this node.
    ///
    /// # Returns
    ///
    /// A new Stream emitting the transformed values.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// doubled = source.map(lambda x: x * 2)
    /// result = []
    /// doubled.sink(result.append)
    /// source.emit(5)  # result contains [10]
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If the stream graph is already compiled.
    #[pyo3(signature = (func, *args, stream_name=None, **kwargs))]
    pub fn map(
        slf: &Bound<'_, Self>,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        stream_name: Option<String>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        let func_name = get_func_name(py, &func);
        let name = resolve_stream_name(stream_name, "map", func_name.as_deref());
        let source_location = capture_source_location(py);
        let func_is_sync = check_func_is_sync(py, &func)?;
        let wrapped_func = wrap_func(py, func, args, kwargs, get_build_map_func()?)?;

        if let NodeLogic::Prefetch { size } = this.logic {
            let state = get_prefetch_state_class()?.call1(py, (&wrapped_func, size))?;
            this.logic = NodeLogic::PrefetchMap {
                func: wrapped_func,
                size,
                state,
            };
            this.name = format!("prefetch_map({size})+{name}");
            this.source_location = format!("{}, {}", this.source_location, source_location);
            this.func_is_sync = func_is_sync;
            return Ok(slf.clone().unbind());
        }

        if this.asynchronous == Some(true) {
            let state = get_async_state_class()?.call0(py)?;
            let node = Self::make_node(
                py,
                NodeLogic::AsyncMap {
                    func: wrapped_func,
                    state,
                },
                name,
                source_location,
                this.asynchronous,
                true,
                false,
            )?;
            this.downstreams.push(node.clone_ref(py));
            let self_py = slf.clone().unbind();
            drop(this);
            node.borrow(py).parents.lock().unwrap().push(self_py);
            return Ok(node);
        }

        let node = Self::make_node(
            py,
            NodeLogic::Map { func: wrapped_func },
            name,
            source_location,
            this.asynchronous,
            this.skip_async_check,
            func_is_sync,
        )?;
        this.downstreams.push(node.clone_ref(py));
        let self_py = slf.clone().unbind();
        drop(this);
        node.borrow(py).parents.lock().unwrap().push(self_py);
        Ok(node)
    }

    /// Enable prefetching for the next map operation.
    ///
    /// When followed by a `.map()`, this causes the map to process up to `n` items
    /// concurrently while preserving output order. Uses a shared global thread pool,
    /// making it efficient to combine with `par()` for parallel branch execution.
    ///
    /// Eager fusion: If the next operation is `map()`, `filter()`, or `batch_map()`,
    /// the prefetch is fused into that operation at creation time, avoiding
    /// unnecessary node creation.
    ///
    /// # Arguments
    ///
    /// * `n` - Maximum number of items to process concurrently.
    ///
    /// # Returns
    ///
    /// A new Stream that will prefetch items for the next transformation.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// # Process up to 4 items concurrently
    /// result = source.prefetch(4).map(lambda x: x * 2)
    /// output = []
    /// result.sink(output.append)
    /// source.emit_batch([1, 2, 3, 4])  # processed concurrently
    /// ```
    ///
    /// # Raises
    ///
    /// * `ValueError` - If `n` is 0.
    /// * `RuntimeError` - If the stream graph is already compiled.
    #[pyo3(name = "prefetch", signature = (n, stream_name=None))]
    pub fn prefetch(
        slf: &Bound<'_, Self>,
        py: Python,
        n: usize,
        stream_name: Option<String>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        if n == 0 {
            return Err(StreamError::InvalidPrefetchSize(n).into());
        }
        let source_location = capture_source_location(py);
        let name = resolve_stream_name(stream_name, &format!("prefetch({})", n), None);
        let node = Self::make_node(
            py,
            NodeLogic::Prefetch { size: n },
            name,
            source_location,
            this.asynchronous,
            this.skip_async_check,
            true,
        )?;
        this.downstreams.push(node.clone_ref(py));
        let self_py = slf.clone().unbind();
        drop(this);
        node.borrow(py).parents.lock().unwrap().push(self_py);
        Ok(node)
    }

    /// Apply a function to each element, unpacking the element as arguments.
    ///
    /// Similar to map, but each element is unpacked with `*element` before
    /// being passed to the function. Useful when elements are tuples or
    /// iterables that should be spread as function arguments.
    ///
    /// # Arguments
    ///
    /// * `func` - Function to apply (receives unpacked element).
    /// * `*args` - Additional positional arguments passed to `func`.
    /// * `**kwargs` - Additional keyword arguments passed to `func`.
    ///
    /// # Returns
    ///
    /// A new Stream emitting the transformed values.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// summed = source.starmap(lambda a, b: a + b)
    /// result = []
    /// summed.sink(result.append)
    /// source.emit((1, 2))   # result contains [3]
    /// source.emit((3, 4))   # result contains [3, 7]
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If the stream graph is already compiled.
    #[pyo3(signature = (func, *args, stream_name=None, **kwargs))]
    pub fn starmap(
        slf: &Bound<'_, Self>,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        stream_name: Option<String>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        let func_name = get_func_name(py, &func);
        let name = resolve_stream_name(stream_name, "starmap", func_name.as_deref());
        let source_location = capture_source_location(py);
        let func_is_sync = check_func_is_sync(py, &func)?;
        let wrapped_func = wrap_func(py, func, args, kwargs, get_build_starmap_func()?)?;

        let node = Self::make_node(
            py,
            NodeLogic::Map { func: wrapped_func },
            name,
            source_location,
            this.asynchronous,
            this.skip_async_check,
            func_is_sync,
        )?;
        this.downstreams.push(node.clone_ref(py));
        let self_py = slf.clone().unbind();
        drop(this);
        node.borrow(py).parents.lock().unwrap().push(self_py);
        Ok(node)
    }

    /// Apply a batch-aware function to process entire batches at once.
    ///
    /// Unlike `map()` which calls the function once per item, `batch_map()` calls
    /// the function once per batch with all items as a list. This is ideal for
    /// vectorized operations like NumPy functions that can process arrays efficiently.
    ///
    /// # Arguments
    ///
    /// * `func` - Function that receives a list of items and returns an
    ///   iterable of results.
    /// * `*args` - Additional positional arguments passed to `func`.
    /// * `**kwargs` - Additional keyword arguments passed to `func`. Use
    ///   `stream_name="name"` to set a custom name for this node.
    ///
    /// # Returns
    ///
    /// A new Stream emitting the transformed values.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// import numpy as np
    /// source = akayu.Stream()
    /// squared = source.batch_map(np.square)
    /// result = []
    /// squared.sink(result.append)
    /// source.emit_batch([1, 4, 9, 16])  # result contains [1, 4, 9, 16]
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If the stream graph is already compiled.
    #[pyo3(signature = (func, *args, stream_name=None, **kwargs))]
    pub fn batch_map(
        slf: &Bound<'_, Self>,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        stream_name: Option<String>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        let func_name = get_func_name(py, &func);
        let name = resolve_stream_name(stream_name, "batch_map", func_name.as_deref());
        let source_location = capture_source_location(py);
        let func_is_sync = check_func_is_sync(py, &func)?;
        let wrapped_func = wrap_func(py, func, args, kwargs, get_build_map_func()?)?;

        if let NodeLogic::Prefetch { size } = this.logic {
            let state = get_prefetch_batch_map_state_class()?.call1(py, (&wrapped_func, size))?;
            this.logic = NodeLogic::PrefetchBatchMap {
                func: wrapped_func,
                size,
                state,
            };
            this.name = format!("prefetch_batch_map({size})+{name}");
            this.source_location = format!("{}, {}", this.source_location, source_location);
            this.func_is_sync = func_is_sync;
            return Ok(slf.clone().unbind());
        }

        let node = Self::make_node(
            py,
            NodeLogic::BatchMap { func: wrapped_func },
            name,
            source_location,
            this.asynchronous,
            this.skip_async_check,
            func_is_sync,
        )?;
        this.downstreams.push(node.clone_ref(py));
        let self_py = slf.clone().unbind();
        drop(this);
        node.borrow(py).parents.lock().unwrap().push(self_py);
        Ok(node)
    }

    /// Filter elements based on a predicate function.
    ///
    /// Only elements for which the predicate returns a truthy value
    /// are passed downstream.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Function that returns True for elements to keep.
    /// * `*args` - Additional positional arguments passed to `predicate`.
    /// * `**kwargs` - Additional keyword arguments passed to `predicate`.
    ///
    /// # Returns
    ///
    /// A new Stream emitting only elements that pass the filter.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// evens = source.filter(lambda x: x % 2 == 0)
    /// result = []
    /// evens.sink(result.append)
    /// source.emit(5)   # filtered out
    /// source.emit(4)   # result contains [4]
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If the stream graph is already compiled.
    #[pyo3(signature = (predicate, *args, stream_name=None, **kwargs))]
    pub fn filter(
        slf: &Bound<'_, Self>,
        py: Python,
        predicate: Py<PyAny>,
        args: Py<PyTuple>,
        stream_name: Option<String>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        let func_name = get_func_name(py, &predicate);
        let name = resolve_stream_name(stream_name, "filter", func_name.as_deref());
        let source_location = capture_source_location(py);
        let func_is_sync = check_func_is_sync(py, &predicate)?;
        let wrapped_predicate = wrap_func(py, predicate, args, kwargs, get_build_map_func()?)?;

        if let NodeLogic::Prefetch { size } = this.logic {
            let wrapper_code = "lambda p: lambda x: (p(x), x)";
            let builtins = py.import("builtins")?;
            let eval_fn = builtins.getattr("eval")?;
            let make_wrapper = eval_fn.call1((wrapper_code,))?;
            let wrapped_pred = make_wrapper.call1((&wrapped_predicate,))?;
            let state = get_prefetch_filter_state_class()?.call1(py, (&wrapped_pred, size))?;

            this.logic = NodeLogic::PrefetchFilter {
                predicate: wrapped_pred.unbind(),
                size,
                state,
            };
            this.name = format!("prefetch_filter({size})+{name}");
            this.source_location = format!("{}, {}", this.source_location, source_location);
            this.func_is_sync = func_is_sync;
            return Ok(slf.clone().unbind());
        }

        let node = Self::make_node(
            py,
            NodeLogic::Filter {
                predicate: wrapped_predicate,
            },
            name,
            source_location,
            this.asynchronous,
            this.skip_async_check,
            func_is_sync,
        )?;
        this.downstreams.push(node.clone_ref(py));
        let self_py = slf.clone().unbind();
        drop(this);
        node.borrow(py).parents.lock().unwrap().push(self_py);
        Ok(node)
    }

    /// Flatten an iterable element into individual elements.
    ///
    /// Each element is expected to be iterable. The items within each
    /// element are emitted individually downstream. Useful for
    /// processing streams where each item contains multiple sub-items.
    ///
    /// # Returns
    ///
    /// A new Stream emitting the flattened elements.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// flattened = source.flatten()
    /// result = []
    /// flattened.sink(result.append)
    /// source.emit([1, 2, 3])  # result contains [1, 2, 3]
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If the stream graph is already compiled.
    #[pyo3(signature = (stream_name=None))]
    pub fn flatten(
        slf: &Bound<'_, Self>,
        py: Python,
        stream_name: Option<String>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        let source_location = capture_source_location(py);
        let name = resolve_stream_name(stream_name, "flatten", None);
        let node = Self::make_node(
            py,
            NodeLogic::Flatten,
            name,
            source_location,
            this.asynchronous,
            this.skip_async_check,
            true,
        )?;
        this.downstreams.push(node.clone_ref(py));
        let self_py = slf.clone().unbind();
        drop(this);
        node.borrow(py).parents.lock().unwrap().push(self_py);
        Ok(node)
    }

    /// Collect elements into a buffer until `flush()` is called.
    ///
    /// Elements are accumulated in an internal buffer. **Elements are NOT passed
    /// downstream until `flush()` is called.** When flushed, all collected
    /// elements are emitted as a single tuple. This is useful for batching
    /// elements before downstream processing.
    ///
    /// # Returns
    ///
    /// A new Stream that emits collected tuples on flush.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// collected = source.collect()
    /// result = []
    /// collected.sink(result.append)
    /// source.emit(1)
    /// source.emit(2)
    /// source.emit(3)
    /// print(result)  # [] — nothing yet!
    /// source.flush()  # result now contains [(1, 2, 3)]
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If the stream graph is already compiled.
    #[pyo3(signature = (stream_name=None))]
    pub fn collect(
        slf: &Bound<'_, Self>,
        py: Python,
        stream_name: Option<String>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        let source_location = capture_source_location(py);
        let name = resolve_stream_name(stream_name, "collect", None);
        let node = Self::make_node(
            py,
            NodeLogic::Collect { buffer: Vec::new() },
            name,
            source_location,
            this.asynchronous,
            this.skip_async_check,
            true,
        )?;
        this.downstreams.push(node.clone_ref(py));
        let self_py = slf.clone().unbind();
        drop(this);
        node.borrow(py).parents.lock().unwrap().push(self_py);
        Ok(node)
    }

    /// Buffer items until a predicate matches, then emit the buffer as a tuple.
    ///
    /// Each item is buffered. When the predicate returns True for an item,
    /// the buffer (including the delimiter if `include_delimiter=True`) is
    /// emitted as a tuple downstream, and the buffer is cleared.
    /// The delimiter itself is excluded by default (set `include_delimiter=True`
    /// to include it).
    ///
    /// Any remaining items in the buffer are emitted on `flush()`.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Function that returns True for delimiter items.
    /// * `include_delimiter` - If True, the delimiter item is included
    ///   in the emitted tuple. If False (default), it is excluded.
    ///
    /// # Returns
    ///
    /// A new Stream emitting tuples of chunked items.
    ///
    /// # Examples
    ///
    /// Split a character stream on spaces:
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// words = source.chunk_by(lambda x: x == ' ')
    /// result = []
    /// words.sink(result.append)
    /// source.emit('H')
    /// source.emit('i')
    /// source.emit(' ')   # delimiter triggers emit
    /// source.emit('there')
    /// source.flush()     # flush remaining
    /// print(result)  # [('H', 'i'), ('there',)]
    /// ```
    ///
    /// Split lines including the newline delimiter:
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// lines = source.chunk_by(lambda x: x == '\n', include_delimiter=True)
    /// result = []
    /// lines.sink(result.append)
    /// for ch in 'Hello\nWorld\n':
    ///     source.emit(ch)
    /// source.flush()
    /// print(result)  # [('H', 'e', 'l', 'l', 'o', '\n'), ('W', 'o', 'r', 'l', 'd', '\n')]
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If the stream graph is already compiled.
    #[pyo3(signature = (predicate, *, include_delimiter=false, stream_name=None))]
    pub fn chunk_by(
        slf: &Bound<'_, Self>,
        py: Python,
        predicate: Py<PyAny>,
        include_delimiter: bool,
        stream_name: Option<String>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        let func_name = get_func_name(py, &predicate);
        let source_location = capture_source_location(py);
        let func_is_sync = check_func_is_sync(py, &predicate)?;

        let name = resolve_stream_name(stream_name, "chunk_by", func_name.as_deref());

        let node = Self::make_node(
            py,
            NodeLogic::ChunkBy {
                buffer: Vec::new(),
                predicate,
                include_delimiter,
                pre_buffer_chain: Vec::new(),
            },
            name,
            source_location,
            this.asynchronous,
            this.skip_async_check,
            func_is_sync,
        )?;
        this.downstreams.push(node.clone_ref(py));
        let self_py = slf.clone().unbind();
        drop(this);
        node.borrow(py).parents.lock().unwrap().push(self_py);
        Ok(node)
    }

    /// Flush collected elements downstream.
    ///
    /// For `collect()` nodes, emits all buffered elements as a tuple
    /// and clears the buffer. Has no effect on other node types.
    ///
    /// # Arguments
    ///
    /// * `*args` - Ignored (for compatibility with streamz).
    /// * `**kwargs` - Ignored (for compatibility with streamz).
    ///
    /// # Returns
    ///
    /// A coroutine if async processing is triggered, otherwise None.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// collected = source.collect()
    /// result = []
    /// collected.sink(result.append)
    /// source.emit(1)
    /// source.emit(2)
    /// source.flush()  # result now contains [(1, 2)]
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If called re-entrantly (during a flush).
    #[pyo3(signature = (*_args, **_kwargs))]
    pub fn flush(
        &mut self,
        py: Python,
        _args: Vec<Py<PyAny>>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let _guard = PyLockGuard::acquire(py, &self.emit_lock)?;
        if self.flushing {
            return Err(StreamError::ReentrantFlush(self.name.clone()).into());
        }
        self.flushing = true;
        let result = self.flush_inner(py);
        self.flushing = false;
        result
    }

    fn flush_inner(&mut self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let mut futures = Vec::new();

        // Warn if flush has no effect (non-buffering leaf that isn't a Sink)
        let has_buffer = matches!(
            self.logic,
            NodeLogic::Collect { .. }
                | NodeLogic::ChunkBy { .. }
                | NodeLogic::PrefetchMap { .. }
                | NodeLogic::PrefetchFilter { .. }
                | NodeLogic::PrefetchBatchMap { .. }
                | NodeLogic::AsyncMap { .. }
        );
        let is_sink = matches!(self.logic, NodeLogic::Sink { .. });
        if !has_buffer && self.downstreams.is_empty() && !is_sink {
            let msg = format!(
                "flush() called on '{}' which has no buffer and no downstreams — \
                 did you mean to call flush() on a collect() node?",
                self.name
            );
            let warn_type = py.get_type::<pyo3::exceptions::PyUserWarning>();
            let c_msg = std::ffi::CString::new(msg).unwrap();
            let _ = PyErr::warn(py, warn_type.as_any(), &c_msg, 1);
        }
        match &mut self.logic {
            NodeLogic::Collect { buffer } => {
                let items = std::mem::take(buffer);
                let tuple = PyTuple::new(py, items)?;
                if let Some(f) = self.propagate(py, tuple.into())? {
                    futures.push(f);
                }
            }
            NodeLogic::ChunkBy { buffer, .. } => {
                if !buffer.is_empty() {
                    let items = std::mem::take(buffer);
                    let tuple = PyTuple::new(py, items)?;
                    if let Some(f) = self.propagate(py, tuple.into())? {
                        futures.push(f);
                    }
                }
            }
            NodeLogic::PrefetchMap { state, .. }
            | NodeLogic::PrefetchFilter { state, .. }
            | NodeLogic::PrefetchBatchMap { state, .. }
            | NodeLogic::AsyncMap { state, .. } => {
                let results = wrap_error(
                    py,
                    state.call_method0(py, "flush"),
                    &self.name,
                    &self.source_location,
                )?;
                for result in results.bind(py).try_iter()? {
                    if let Some(f) = self.propagate(py, result?.into())? {
                        futures.push(f);
                    }
                }
            }
            _ => {}
        }

        // Propagate flush to downstream nodes — but NOT for Collect/ChunkBy.
        // Collect/ChunkBy emit their buffer via propagate() above; cascading
        // flush would prematurely drain downstream collects before they've had
        // a chance to accumulate items from multiple flushes.
        // Prefetch/Async nodes DO propagate so their drained results flow onward.
        let should_propagate = !matches!(
            self.logic,
            NodeLogic::Collect { .. } | NodeLogic::ChunkBy { .. }
        );
        if should_propagate {
            for downstream in &self.downstreams {
                let mut d = downstream.borrow_mut(py);
                if let Some(f) = d.flush(py, vec![], None)? {
                    futures.push(f);
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

    /// Recursively flush this node and all downstream nodes.
    ///
    /// Unlike `flush()`, which only flushes this node's buffer,
    /// `drain()` flushes this node and then recursively drains every
    /// downstream node. Use this when you want to flush an entire
    /// pipeline (e.g. at the end of processing).
    ///
    /// # Arguments
    ///
    /// * `*args` - Ignored (for compatibility with streamz).
    /// * `**kwargs` - Ignored (for compatibility with streamz).
    ///
    /// # Returns
    ///
    /// A coroutine if async processing is triggered, otherwise None.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// collected = source.collect()
    /// result = []
    /// collected.sink(result.append)
    /// source.emit(1)
    /// source.emit(2)
    /// source.drain()  # drains entire pipeline
    /// # result contains [(1, 2)]
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If called re-entrantly (during a drain).
    #[pyo3(signature = (*_args, **_kwargs))]
    pub fn drain(
        &mut self,
        py: Python,
        _args: Vec<Py<PyAny>>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<Py<PyAny>>> {
        // Re-entrancy guard: detect cycles in the flush graph
        if self.flushing {
            return Err(StreamError::ReentrantFlush(self.name.clone()).into());
        }
        self.flushing = true;
        let result = self.drain_inner(py);
        self.flushing = false;
        result
    }

    fn drain_inner(&mut self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let mut futures = Vec::new();

        // Flush this node's buffer
        match &mut self.logic {
            NodeLogic::Collect { buffer } => {
                let items = std::mem::take(buffer);
                let tuple = PyTuple::new(py, items)?;
                if let Some(f) = self.propagate(py, tuple.into())? {
                    futures.push(f);
                }
            }
            NodeLogic::ChunkBy { buffer, .. } => {
                if !buffer.is_empty() {
                    let items = std::mem::take(buffer);
                    let tuple = PyTuple::new(py, items)?;
                    if let Some(f) = self.propagate(py, tuple.into())? {
                        futures.push(f);
                    }
                }
            }
            NodeLogic::PrefetchMap { state, .. }
            | NodeLogic::PrefetchFilter { state, .. }
            | NodeLogic::PrefetchBatchMap { state, .. }
            | NodeLogic::AsyncMap { state, .. } => {
                let results = wrap_error(
                    py,
                    state.call_method0(py, "flush"),
                    &self.name,
                    &self.source_location,
                )?;
                for result in results.bind(py).try_iter()? {
                    if let Some(f) = self.propagate(py, result?.into())? {
                        futures.push(f);
                    }
                }
            }
            _ => {}
        }

        // Always recurse into all downstreams
        for downstream in &self.downstreams {
            let mut d = downstream.borrow_mut(py);
            if let Some(f) = d.drain(py, vec![], None)? {
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
    /// Unlike map which returns a transformed stream, sink typically ends
    /// a pipeline (no further operations are typically added after sink).
    ///
    /// # Arguments
    ///
    /// * `func` - Function to call with each element.
    /// * `*args` - Additional positional arguments passed to `func`.
    /// * `**kwargs` - Additional keyword arguments passed to `func`.
    ///
    /// # Returns
    ///
    /// A new Stream (for chaining, though sink typically ends a pipeline).
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// source.map(lambda x: x * 2).sink(print)  # prints doubled values
    /// source.emit(5)  # prints 10
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If the stream graph is already compiled.
    #[pyo3(signature = (func, *args, stream_name=None, **kwargs))]
    pub fn sink(
        slf: &Bound<'_, Self>,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        stream_name: Option<String>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        let func_name = get_func_name(py, &func);
        let name = resolve_stream_name(stream_name, "sink", func_name.as_deref());
        let source_location = capture_source_location(py);
        let func_is_sync = check_func_is_sync(py, &func)?;
        let wrapped_func = wrap_func(py, func, args, kwargs, get_build_map_func()?)?;

        let node = Self::make_node(
            py,
            NodeLogic::Sink { func: wrapped_func },
            name,
            source_location,
            this.asynchronous,
            this.skip_async_check,
            func_is_sync,
        )?;
        this.downstreams.push(node.clone_ref(py));
        let self_py = slf.clone().unbind();
        drop(this);
        node.borrow(py).parents.lock().unwrap().push(self_py);
        Ok(node)
    }

    /// Accumulate values using a function, emitting each intermediate state.
    ///
    /// Similar to functools.reduce, but emits every intermediate result
    /// rather than just the final state. The function receives
    /// `(current_state, new_element)` and returns the new state.
    ///
    /// # Arguments
    ///
    /// * `func` - Accumulator function `(state, element) -> new_state`. If
    ///   `returns_state=True`, function should return `(new_state, result)`.
    /// * `start` - Initial state value passed to first call of `func`.
    /// * `returns_state` - If False (default), `func` returns the new state
    ///   directly and the state is emitted. If True, `func` returns a
    ///   `(new_state, result)` tuple — `new_state` is used for the next
    ///   call and `result` is emitted. This lets you keep internal state
    ///   (e.g. a counter) separate from what gets emitted.
    ///
    /// # Returns
    ///
    /// A new Stream emitting accumulated states (or results if `returns_state=True`).
    ///
    /// # Examples
    ///
    /// Running total:
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// accumulated = source.accumulate(lambda state, x: state + x, start=0)
    /// result = []
    /// accumulated.sink(result.append)
    /// source.emit(1)  # result contains [1]
    /// source.emit(2)  # result contains [1, 3]
    /// source.emit(3)  # result contains [1, 3, 6]
    /// ```
    ///
    /// Using `returns_state` to keep a counter separate from emitted values:
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// # func returns (state, emit_value) when returns_state=True
    /// indexed = source.accumulate(
    ///     lambda count, x: (count + 1, (count, x)),
    ///     start=0,
    ///     returns_state=True,
    /// )
    /// result = []
    /// indexed.sink(result.append)
    /// source.emit('a')  # result contains [(0, 'a')]
    /// source.emit('b')  # result contains [(0, 'a'), (1, 'b')]
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If the stream graph is already compiled.
    #[pyo3(signature = (func, start, returns_state=false, stream_name=None))]
    pub fn accumulate(
        slf: &Bound<'_, Self>,
        py: Python,
        func: Py<PyAny>,
        start: Py<PyAny>,
        returns_state: bool,
        stream_name: Option<String>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        let func_name = get_func_name(py, &func);
        let source_location = capture_source_location(py);
        let func_is_sync = check_func_is_sync(py, &func)?;

        let name = resolve_stream_name(stream_name, "accumulate", func_name.as_deref());

        let node = Self::make_node(
            py,
            NodeLogic::Accumulate {
                func,
                state: start,
                returns_state,
            },
            name,
            source_location,
            this.asynchronous,
            this.skip_async_check,
            func_is_sync,
        )?;
        this.downstreams.push(node.clone_ref(py));
        let self_py = slf.clone().unbind();
        drop(this);
        node.borrow(py).parents.lock().unwrap().push(self_py);
        Ok(node)
    }

    /// Emit a single value into the stream.
    ///
    /// This is the primary way to push data into a source stream.
    /// The value propagates through all downstream nodes.
    ///
    /// **Restriction**: This method can ONLY be called on a source node.
    /// Calling it on a derived node (e.g. from `.map()`) will raise a RuntimeError.
    ///
    /// # Arguments
    ///
    /// * `x` - The value to emit.
    ///
    /// # Returns
    ///
    /// A coroutine if async processing is triggered, otherwise None.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// result = []
    /// source.map(lambda x: x * 2).sink(result.append)
    /// source.emit(5)  # result contains [10]
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If called on a non-source node or if emit is re-entrant.
    pub fn emit(&self, py: Python, x: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        let _guard = PyLockGuard::acquire(py, &self.emit_lock)?;
        if !matches!(self.logic, NodeLogic::Source) {
            return Err(StreamError::EmitOnNonSource(self.name.clone()).into());
        }
        if self.emitting.load(std::sync::atomic::Ordering::Acquire) {
            return Err(StreamError::ReentrantEmit(self.name.clone()).into());
        }
        self.emitting.store(true, std::sync::atomic::Ordering::Release);

        let profiling = crate::profiling::is_profiling();
        if profiling {
            crate::profiling::begin_emit();
        }

        let result = self.propagate(py, x);

        if profiling {
            crate::profiling::end_emit();
        }
        self.emitting.store(false, std::sync::atomic::Ordering::Release);
        result
    }

    /// Emit multiple values into the stream as a batch.
    ///
    /// More efficient than calling `emit()` multiple times when processing
    /// many items. Values are processed together through the pipeline.
    ///
    /// **Restriction**: This method can ONLY be called on a source node.
    /// Calling it on a derived node (e.g. from `.map()`) will raise a RuntimeError.
    ///
    /// # Arguments
    ///
    /// * `items` - List of values to emit.
    ///
    /// # Returns
    ///
    /// A coroutine if async processing is triggered, otherwise None.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// result = []
    /// source.map(lambda x: x * 2).sink(result.append)
    /// source.emit_batch([1, 2, 3])  # result contains [2, 4, 6]
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If called on a non-source node or if emit is re-entrant.
    pub fn emit_batch(&self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<Option<Py<PyAny>>> {
        let _guard = PyLockGuard::acquire(py, &self.emit_lock)?;
        if !matches!(self.logic, NodeLogic::Source) {
            return Err(StreamError::EmitOnNonSource(self.name.clone()).into());
        }
        if self.emitting.load(std::sync::atomic::Ordering::Acquire) {
            return Err(StreamError::ReentrantEmit(self.name.clone()).into());
        }
        self.emitting.store(true, std::sync::atomic::Ordering::Release);

        let profiling = crate::profiling::is_profiling();
        if profiling {
            crate::profiling::begin_emit();
        }

        let result = self.propagate_batch(py, items);

        if profiling {
            crate::profiling::end_emit();
        }
        self.emitting.store(false, std::sync::atomic::Ordering::Release);
        result
    }

    /// Freeze and optimize the stream graph.
    ///
    /// Triggers topology optimizations (like chain fusion) and prevents
    /// further modifications to the graph. **Once called, the entire graph
    /// becomes immutable.**
    ///
    /// This enables optimizations that are unsafe for dynamic topologies:
    /// - Consecutive map operations are fused into a single composed map
    /// - Consecutive filter operations are fused into a single composed filter
    /// - Prefetch nodes are fused with their following map/filter operations
    ///
    /// IMPORTANT: After calling compile(), attempting to add new nodes (via map,
    /// filter, sink, etc.) to any part of the graph will raise a RuntimeError.
    /// Build your complete pipeline before calling this method.
    ///
    /// # Returns
    ///
    /// self (for chaining).
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// source.map(f1).map(f2).sink(print)
    /// source.compile()  # f1 and f2 are fused into a single map
    /// source.emit(1)   # Fast path with optimized graph
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If the graph is already compiled/frozen or if
    ///   modifications are attempted after compilation.
    pub fn compile(slf: &Bound<'_, Self>, py: Python) -> PyResult<Py<Self>> {
        let source_py_ptr = slf.as_ptr() as usize;
        let mut this = slf.borrow_mut();
        if this.compiled {
            return Err(StreamError::AlreadyCompiled(this.name.clone()).into());
        }

        // Run full optimization (including chain fusion)
        this.optimize_topology_full(py, source_py_ptr);

        // Mark this node and all reachable nodes as frozen and compiled
        this.freeze_all(py);

        drop(this);
        Ok(slf.clone().unbind())
    }

    /// Start collecting profiling data for subsequent emit() calls.
    ///
    /// Call `stop_profiling()` to end the session and retrieve the report.
    /// Profiling adds minimal overhead (~80ns per node per item).
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If called on a non-source node or if profiling is already active.
    pub fn start_profiling(&self) -> PyResult<()> {
        if !matches!(self.logic, NodeLogic::Source) {
            return Err(StreamError::EmitOnNonSource(self.name.clone()).into());
        }
        crate::profiling::start_session()
    }

    /// Stop profiling and return the collected report.
    ///
    /// Returns a `ProfileReport` with per-node timing and item flow data.
    ///
    /// # Returns
    ///
    /// A `ProfileReport` with per-node timing and item flow data.
    pub fn stop_profiling(&self, py: Python) -> PyResult<Py<crate::profiling::ProfileReport>> {
        let (data, wall_time, emit_count) = crate::profiling::stop_session();
        crate::profiling::build_report(py, self, data, wall_time, emit_count)
    }

    /// Return a `Profiler` context manager for this source.
    ///
    /// Provides a convenient way to profile a block of emit calls.
    /// The profiler is a context manager that automatically starts
    /// profiling on entry and stops on exit.
    ///
    /// # Returns
    ///
    /// A `Profiler` context manager.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// source.map(lambda x: x * 2).sink(print)
    /// with source.profile() as p:
    ///     source.emit(42)
    /// print(p.report)
    /// ```
    pub fn profile(slf: &Bound<'_, Self>, py: Python) -> PyResult<Py<PyAny>> {
        {
            let this = slf.borrow();
            if !matches!(this.logic, NodeLogic::Source) {
                return Err(StreamError::EmitOnNonSource(this.name.clone()).into());
            }
        }
        let profiler_class = get_profiler_class()?;
        profiler_class.call1(py, (slf,))
    }

    /// Emit a single value and return a profiling report.
    ///
    /// Convenience method combining start_profiling + emit + stop_profiling
    /// for single-shot profiling of an emit call.
    ///
    /// # Arguments
    ///
    /// * `x` - The value to emit.
    ///
    /// # Returns
    ///
    /// A `ProfileReport` for this single emit.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// source.map(lambda x: x * 2).sink(print)
    /// report = source.emit_profiled(5)  # prints 10
    /// print(report)  # shows timing breakdown
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If called on a non-source node.
    pub fn emit_profiled(
        &mut self,
        py: Python,
        x: Py<PyAny>,
    ) -> PyResult<Py<crate::profiling::ProfileReport>> {
        if !matches!(self.logic, NodeLogic::Source) {
            return Err(StreamError::EmitOnNonSource(self.name.clone()).into());
        }
        crate::profiling::start_session()?;
        crate::profiling::begin_emit();
        let result = self.propagate(py, x);
        crate::profiling::end_emit();
        let (data, wall_time, emit_count) = crate::profiling::stop_session();
        // Propagate error after stopping profiling
        result?;
        crate::profiling::build_report(py, self, data, wall_time, emit_count)
    }

    /// Emit a batch of values and return a profiling report.
    ///
    /// Convenience method combining start_profiling + emit_batch + stop_profiling
    /// for profiling a batch emit call.
    ///
    /// # Arguments
    ///
    /// * `items` - List of values to emit.
    ///
    /// # Returns
    ///
    /// A `ProfileReport` for this batch emit.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// source.map(lambda x: x * 2).sink(print)
    /// report = source.emit_batch_profiled([1, 2, 3])  # prints 2, 4, 6
    /// print(report)  # shows timing breakdown
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If called on a non-source node.
    pub fn emit_batch_profiled(
        &mut self,
        py: Python,
        items: Vec<Py<PyAny>>,
    ) -> PyResult<Py<crate::profiling::ProfileReport>> {
        if !matches!(self.logic, NodeLogic::Source) {
            return Err(StreamError::EmitOnNonSource(self.name.clone()).into());
        }
        crate::profiling::start_session()?;
        crate::profiling::begin_emit();
        let result = self.propagate_batch(py, items);
        crate::profiling::end_emit();
        let (data, wall_time, emit_count) = crate::profiling::stop_session();
        result?;
        crate::profiling::build_report(py, self, data, wall_time, emit_count)
    }

    /// Returns a string representation of the stream for debugging.
    ///
    /// # Returns
    ///
    /// A string showing the stream type and compiled status.
    pub fn __repr__(&self) -> String {
        format!("<Stream(nodes=1, compiled={})>", self.compiled)
    }

    /// Convert the stream to a Python list.
    ///
    /// Adds a sink that appends each element to a list and returns it.
    /// This is a terminal operation that collects all emitted values.
    ///
    /// # Returns
    ///
    /// A Python list containing all emitted elements.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// result = source.map(lambda x: x * 2).to_list()
    /// source.emit(1)
    /// source.emit(2)
    /// source.emit(3)
    /// source.flush()
    /// print(result)  # [2, 4, 6]
    /// ```
    pub fn to_list(slf: &Bound<'_, Self>, py: Python) -> PyResult<Py<PyList>> {
        let list: Bound<'_, PyList> = PyList::new(py, Vec::<Py<PyAny>>::new())?;
        let append: Py<PyAny> = list.getattr("append")?.unbind();
        let args: Py<PyTuple> = PyTuple::empty(py).unbind();
        let self_py: Py<Self> = slf.clone().unbind();
        Stream::sink(&self_py.into_bound(py), py, append, args, None, None)?;
        Ok(list.unbind())
    }

    /// Convert the stream to a Python set.
    ///
    /// Adds a sink that adds each element to a set and returns it.
    /// This is a terminal operation that collects all emitted values.
    ///
    /// # Returns
    ///
    /// A Python set containing all emitted elements.
    ///
    /// # Examples
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// result = source.filter(lambda x: x > 5).to_set()
    /// source.emit(1)
    /// source.emit(10)
    /// source.emit(20)
    /// source.emit(10)  # duplicate
    /// source.flush()
    /// print(result)  # {10, 20}
    /// ```
    pub fn to_set(slf: &Bound<'_, Self>, py: Python) -> PyResult<Py<PySet>> {
        let set: Bound<'_, PySet> = PySet::new(py, Vec::<Py<PyAny>>::new())?;
        let add: Py<PyAny> = set.getattr("add")?.unbind();
        let args: Py<PyTuple> = PyTuple::empty(py).unbind();
        let self_py: Py<Self> = slf.clone().unbind();
        Stream::sink(&self_py.into_bound(py), py, add, args, None, None)?;
        Ok(set.unbind())
    }

}
