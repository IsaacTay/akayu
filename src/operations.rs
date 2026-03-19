//! Stream transformation operations (map, filter, sink, etc.)

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::error::StreamError;
use crate::helpers::{
    capture_source_location, check_not_compiled, extract_stream_name, get_async_state_class,
    get_build_map_func, get_build_starmap_func, get_func_name, get_gather, get_is_sync_callable,
    get_prefetch_batch_map_state_class, get_prefetch_filter_state_class, get_prefetch_state_class,
    get_profiler_class, wrap_error,
};
use crate::node::NodeLogic;
use crate::Stream;

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
                name,
                asynchronous,
                skip_async_check,
                func_is_sync,
                frozen: false,
                needs_lock: false,
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
            name: name.unwrap_or_else(|| "source".to_string()),
            asynchronous,
            skip_async_check,
            func_is_sync: true, // Source has no function, treat as sync
            frozen: false,
            needs_lock: false,
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
        }
    }

    /// Returns True if the stream graph is compiled (frozen).
    #[getter]
    pub fn compiled(&self) -> bool {
        self.compiled
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
    pub fn map(
        slf: &Bound<'_, Self>,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        let func_name = get_func_name(py, &func);
        let name = extract_stream_name(py, &kwargs, "map", func_name.as_deref())?;
        let source_location = capture_source_location(py);
        let func_is_sync = check_func_is_sync(py, &func)?;
        let wrapped_func = wrap_func(py, func, args, kwargs, get_build_map_func()?)?;

        // Eager Prefetch fusion: if parent is Prefetch, transform into PrefetchMap
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
            drop(this);
            return Ok(slf.clone().unbind());
        }

        // If asynchronous=True, create AsyncMap node for non-blocking emit
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
                true,  // AsyncMap handles async internally
                false, // By definition, async
            )?;
            this.downstreams.push(node.clone_ref(py));
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
        Ok(node)
    }

    /// Enable prefetching for the next map operation.
    ///
    /// When followed by a `.map()`, this causes the map to process up to `n` items
    /// concurrently while preserving output order. Uses a shared global thread pool,
    /// making it efficient to combine with `par()` for parallel branch execution.
    ///
    /// Args:
    ///     n: Maximum number of items to process concurrently.
    ///
    /// Returns:
    ///     A new Stream that will modify the next map to use prefetching.
    #[pyo3(name = "prefetch", signature = (n))]
    pub fn prefetch(&mut self, py: Python, n: usize) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
        if n == 0 {
            return Err(StreamError::InvalidPrefetchSize.into());
        }
        let source_location = capture_source_location(py);
        let node = Self::make_node(
            py,
            NodeLogic::Prefetch { size: n },
            format!("prefetch({})", n),
            source_location,
            self.asynchronous,
            self.skip_async_check,
            true,
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
    pub fn starmap(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
        let func_name = get_func_name(py, &func);
        let name = extract_stream_name(py, &kwargs, "starmap", func_name.as_deref())?;
        let source_location = capture_source_location(py);
        let func_is_sync = check_func_is_sync(py, &func)?;
        let wrapped_func = wrap_func(py, func, args, kwargs, get_build_starmap_func()?)?;

        let node = Self::make_node(
            py,
            NodeLogic::Map { func: wrapped_func },
            name,
            source_location,
            self.asynchronous,
            self.skip_async_check,
            func_is_sync,
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
    pub fn batch_map(
        slf: &Bound<'_, Self>,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        let func_name = get_func_name(py, &func);
        let name = extract_stream_name(py, &kwargs, "batch_map", func_name.as_deref())?;
        let source_location = capture_source_location(py);
        let func_is_sync = check_func_is_sync(py, &func)?;
        let wrapped_func = wrap_func(py, func, args, kwargs, get_build_map_func()?)?;

        // Eager Prefetch fusion: if parent is Prefetch, transform into PrefetchBatchMap
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
            drop(this);
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
    pub fn filter(
        slf: &Bound<'_, Self>,
        py: Python,
        predicate: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        let func_name = get_func_name(py, &predicate);
        let name = extract_stream_name(py, &kwargs, "filter", func_name.as_deref())?;
        let source_location = capture_source_location(py);
        let func_is_sync = check_func_is_sync(py, &predicate)?;
        let wrapped_predicate = wrap_func(py, predicate, args, kwargs, get_build_map_func()?)?;

        // Eager Prefetch fusion: if parent is Prefetch, transform into PrefetchFilter
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
            drop(this);
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
        Ok(node)
    }

    /// Flatten an iterable element into individual elements.
    ///
    /// Each element is expected to be iterable. The items within each
    /// element are emitted individually downstream.
    ///
    /// Returns:
    ///     A new Stream emitting the flattened elements.
    pub fn flatten(&mut self, py: Python) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
        let source_location = capture_source_location(py);
        let node = Self::make_node(
            py,
            NodeLogic::Flatten,
            "flatten".to_string(),
            source_location,
            self.asynchronous,
            self.skip_async_check,
            true,
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
    pub fn collect(&mut self, py: Python) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
        let source_location = capture_source_location(py);
        let node = Self::make_node(
            py,
            NodeLogic::Collect { buffer: Vec::new() },
            "collect".to_string(),
            source_location,
            self.asynchronous,
            self.skip_async_check,
            true,
        )?;
        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    /// Buffer items until a predicate matches, then emit the buffer as a tuple.
    ///
    /// Each item is buffered. When the predicate returns True for an item,
    /// the buffer (including the delimiter if `include_delimiter=True`) is
    /// emitted as a tuple downstream, and the buffer is cleared.
    ///
    /// Any remaining items in the buffer are emitted on `flush()`.
    ///
    /// Args:
    ///     predicate: Function that returns True for delimiter items.
    ///     include_delimiter: If True (default), the delimiter item is included
    ///         in the emitted tuple. If False, it is excluded.
    ///
    /// Returns:
    ///     A new Stream emitting tuples of chunked items.
    #[pyo3(signature = (predicate, *, include_delimiter=true))]
    pub fn chunk_by(
        &mut self,
        py: Python,
        predicate: Py<PyAny>,
        include_delimiter: bool,
    ) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
        let func_name = get_func_name(py, &predicate);
        let source_location = capture_source_location(py);
        let func_is_sync = check_func_is_sync(py, &predicate)?;

        let name = match func_name {
            Some(ref n) => format!("chunk_by({n})"),
            None => "chunk_by".to_string(),
        };

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
            self.asynchronous,
            self.skip_async_check,
            func_is_sync,
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
    pub fn flush(
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
    /// Args:
    ///     *args: Ignored (for compatibility with streamz).
    ///     **kwargs: Ignored (for compatibility with streamz).
    ///
    /// Returns:
    ///     A coroutine if async processing is triggered, otherwise None.
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
    ///
    /// Args:
    ///     func: Function to call with each element.
    ///     *args: Additional positional arguments passed to func.
    ///     **kwargs: Additional keyword arguments passed to func.
    ///
    /// Returns:
    ///     A new Stream (for chaining, though sink typically ends a pipeline).
    #[pyo3(signature = (func, *args, **kwargs))]
    pub fn sink(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
        let func_name = get_func_name(py, &func);
        let name = extract_stream_name(py, &kwargs, "sink", func_name.as_deref())?;
        let source_location = capture_source_location(py);
        let func_is_sync = check_func_is_sync(py, &func)?;
        let wrapped_func = wrap_func(py, func, args, kwargs, get_build_map_func()?)?;

        let node = Self::make_node(
            py,
            NodeLogic::Sink { func: wrapped_func },
            name,
            source_location,
            self.asynchronous,
            self.skip_async_check,
            func_is_sync,
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
    pub fn accumulate(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        start: Py<PyAny>,
        returns_state: bool,
    ) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
        let func_name = get_func_name(py, &func);
        let source_location = capture_source_location(py);
        let func_is_sync = check_func_is_sync(py, &func)?;

        let name = match func_name {
            Some(ref n) => format!("accumulate({n})"),
            None => "accumulate".to_string(),
        };

        let node = Self::make_node(
            py,
            NodeLogic::Accumulate {
                func,
                state: start,
                returns_state,
            },
            name,
            source_location,
            self.asynchronous,
            self.skip_async_check,
            func_is_sync,
        )?;
        self.downstreams.push(node.clone_ref(py));
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
    /// Args:
    ///     x: The value to emit.
    ///
    /// Returns:
    ///     A coroutine if async processing is triggered, otherwise None.
    pub fn emit(&self, py: Python, x: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        if !matches!(self.logic, NodeLogic::Source) {
            return Err(StreamError::EmitOnNonSource(self.name.clone()).into());
        }
        if self.emitting.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(StreamError::ReentrantEmit(self.name.clone()).into());
        }
        self.emitting
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let profiling = crate::profiling::is_profiling();
        if profiling {
            crate::profiling::begin_emit();
        }

        let result = self.propagate(py, x);

        if profiling {
            crate::profiling::end_emit();
        }
        self.emitting
            .store(false, std::sync::atomic::Ordering::Relaxed);
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
    /// Args:
    ///     items: List of values to emit.
    ///
    /// Returns:
    ///     A coroutine if async processing is triggered, otherwise None.
    pub fn emit_batch(&self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<Option<Py<PyAny>>> {
        if !matches!(self.logic, NodeLogic::Source) {
            return Err(StreamError::EmitOnNonSource(self.name.clone()).into());
        }
        if self.emitting.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(StreamError::ReentrantEmit(self.name.clone()).into());
        }
        self.emitting
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let profiling = crate::profiling::is_profiling();
        if profiling {
            crate::profiling::begin_emit();
        }

        let result = self.propagate_batch(py, items);

        if profiling {
            crate::profiling::end_emit();
        }
        self.emitting
            .store(false, std::sync::atomic::Ordering::Relaxed);
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
    /// Returns:
    ///     self (for chaining)
    ///
    /// Raises:
    ///     RuntimeError: If the graph is already compiled/frozen or if
    ///                  modifications are attempted after compilation.
    ///
    /// Example:
    ///     >>> s = Stream()
    ///     >>> s.map(f1).map(f2).sink(print)
    ///     >>> s.compile()  # f1 and f2 are fused into a single map
    ///     >>> s.emit(1)    # Fast path with optimized graph
    pub fn compile(slf: &Bound<'_, Self>, py: Python) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        if this.compiled {
            return Err(StreamError::AlreadyCompiled(this.name.clone()).into());
        }

        // Run full optimization (including chain fusion)
        this.optimize_topology_full(py);

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
    /// Raises:
    ///     RuntimeError: If called on a non-source node or if profiling is already active.
    pub fn start_profiling(&self) -> PyResult<()> {
        if !matches!(self.logic, NodeLogic::Source) {
            return Err(StreamError::EmitOnNonSource(self.name.clone()).into());
        }
        crate::profiling::start_session()
    }

    /// Stop profiling and return the collected report.
    ///
    /// Returns:
    ///     A `ProfileReport` with per-node timing and item flow data.
    pub fn stop_profiling(&self, py: Python) -> PyResult<Py<crate::profiling::ProfileReport>> {
        let (data, wall_time, emit_count) = crate::profiling::stop_session();
        crate::profiling::build_report(py, self, data, wall_time, emit_count)
    }

    /// Return a `Profiler` context manager for this source.
    ///
    /// Usage:
    ///     >>> with source.profile() as p:
    ///     ...     source.emit(42)
    ///     >>> print(p.report)
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
    /// Convenience method combining start_profiling + emit + stop_profiling.
    ///
    /// Args:
    ///     x: The value to emit.
    ///
    /// Returns:
    ///     A `ProfileReport` for this single emit.
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
    /// Args:
    ///     items: List of values to emit.
    ///
    /// Returns:
    ///     A `ProfileReport` for this batch emit.
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
}
