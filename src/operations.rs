//! Stream transformation operations (map, filter, sink, etc.)

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::Stream;
use crate::helpers::{
    check_not_compiled, extract_stream_name, get_async_state_class, get_build_map_func,
    get_build_starmap_func, get_gather, get_is_sync_callable, get_prefetch_batch_map_state_class,
    get_prefetch_filter_state_class, get_prefetch_state_class, wrap_error,
};
use crate::node::NodeLogic;

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
    pub fn new(name: Option<String>, asynchronous: Option<bool>) -> Self {
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
            compiled: false,
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
    pub fn map(
        slf: &Bound<'_, Self>,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        let name = extract_stream_name(py, &kwargs, "map", &this.name)?;

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

        // Eager Prefetch fusion: if parent is Prefetch, transform into PrefetchMap
        if let NodeLogic::Prefetch { size } = this.logic {
            let prefetch_state_class = get_prefetch_state_class()?;
            let state = prefetch_state_class.call1(py, (&wrapped_func, size))?;

            this.logic = NodeLogic::PrefetchMap {
                func: wrapped_func,
                size,
                state,
            };
            this.name = format!("prefetch_map({size})+{name}");
            this.func_is_sync = func_is_sync;
            drop(this);
            return Ok(slf.clone().unbind());
        }

        // If asynchronous=True, create AsyncMap node for non-blocking emit
        if this.asynchronous == Some(true) {
            let async_state_class = get_async_state_class()?;
            let state = async_state_class.call0(py)?;

            let node = Py::new(
                py,
                Self {
                    logic: NodeLogic::AsyncMap {
                        func: wrapped_func,
                        state,
                    },
                    downstreams: Vec::new(),
                    name,
                    asynchronous: this.asynchronous,
                    skip_async_check: true, // AsyncMap handles async internally
                    func_is_sync: false,    // By definition, async
                    frozen: false,
                    needs_lock: false,
                    compiled: false,
                },
            )?;

            this.downstreams.push(node.clone_ref(py));
            return Ok(node);
        }

        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::Map { func: wrapped_func },
                downstreams: Vec::new(),
                name,
                asynchronous: this.asynchronous,
                skip_async_check: this.skip_async_check,
                func_is_sync,
                frozen: false,
                needs_lock: false,
                compiled: false,
            },
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
                compiled: false,
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
    pub fn starmap(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
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
                logic: NodeLogic::Map { func: wrapped_func },
                downstreams: Vec::new(),
                name,
                asynchronous: self.asynchronous,
                skip_async_check: self.skip_async_check,
                func_is_sync,
                frozen: false,
                needs_lock: false,
                compiled: false,
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
    pub fn batch_map(
        slf: &Bound<'_, Self>,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        let mut this = slf.borrow_mut();
        check_not_compiled(this.compiled, &this.name)?;
        let name = extract_stream_name(py, &kwargs, "batch_map", &this.name)?;

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

        // Eager Prefetch fusion: if parent is Prefetch, transform into PrefetchBatchMap
        if let NodeLogic::Prefetch { size } = this.logic {
            let prefetch_batch_map_state_class = get_prefetch_batch_map_state_class()?;
            let state = prefetch_batch_map_state_class.call1(py, (&wrapped_func, size))?;

            this.logic = NodeLogic::PrefetchBatchMap {
                func: wrapped_func,
                size,
                state,
            };
            this.name = format!("prefetch_batch_map({size})+{name}");
            this.func_is_sync = func_is_sync;
            drop(this);
            return Ok(slf.clone().unbind());
        }

        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::BatchMap { func: wrapped_func },
                downstreams: Vec::new(),
                name,
                asynchronous: this.asynchronous,
                skip_async_check: this.skip_async_check,
                func_is_sync,
                frozen: false,
                needs_lock: false,
                compiled: false,
            },
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
        let name = extract_stream_name(py, &kwargs, "filter", &this.name)?;

        // Check if the original predicate is sync (before wrapping/moving)
        let is_sync_callable = get_is_sync_callable()?;
        let func_is_sync = is_sync_callable.call1(py, (&predicate,))?.is_truthy(py)?;

        let builder = get_build_map_func()?;
        let args_opt = if args.bind(py).is_empty() {
            None
        } else {
            Some(args)
        };
        let wrapped_predicate: Py<PyAny> = builder
            .call1(py, (predicate, args_opt, kwargs))?
            .extract(py)?;

        // Eager Prefetch fusion: if parent is Prefetch, transform into PrefetchFilter
        if let NodeLogic::Prefetch { size } = this.logic {
            let prefetch_filter_state_class = get_prefetch_filter_state_class()?;
            // Wrap predicate to return (predicate_result, original_value)
            let wrapper_code = "lambda p: lambda x: (p(x), x)";
            let builtins = py.import("builtins")?;
            let eval_fn = builtins.getattr("eval")?;
            let make_wrapper = eval_fn.call1((wrapper_code,))?;
            let wrapped_pred = make_wrapper.call1((&wrapped_predicate,))?;
            let state = prefetch_filter_state_class.call1(py, (&wrapped_pred, size))?;

            this.logic = NodeLogic::PrefetchFilter {
                predicate: wrapped_pred.unbind(),
                size,
                state,
            };
            this.name = format!("prefetch_filter({size})+{name}");
            this.func_is_sync = func_is_sync;
            drop(this);
            return Ok(slf.clone().unbind());
        }

        let node = Py::new(
            py,
            Self {
                logic: NodeLogic::Filter {
                    predicate: wrapped_predicate,
                },
                downstreams: Vec::new(),
                name,
                asynchronous: this.asynchronous,
                skip_async_check: this.skip_async_check,
                func_is_sync,
                frozen: false,
                needs_lock: false,
                compiled: false,
            },
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
                compiled: false,
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
    pub fn collect(&mut self, py: Python) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
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
                compiled: false,
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
    pub fn flush(
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
            NodeLogic::PrefetchBatchMap { state, .. } => {
                // Flush the prefetch batch_map state - wait for all pending items and propagate
                let results = wrap_error(py, state.call_method0(py, "flush"), &self.name)?;
                let results_list = results.bind(py);
                for result in results_list.try_iter()? {
                    if let Some(f) = self.propagate(py, result?.into())? {
                        futures.push(f);
                    }
                }
            }
            NodeLogic::AsyncMap { state, .. } => {
                // Flush the async state - wait for all pending items and propagate
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
    pub fn sink(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
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
                compiled: false,
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
    pub fn accumulate(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        start: Py<PyAny>,
        returns_state: bool,
    ) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
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
                compiled: false,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
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
    pub fn emit(&mut self, py: Python, x: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
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
    pub fn emit_batch(&mut self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<Option<Py<PyAny>>> {
        // Emit bypasses this node's logic and propagates directly to downstreams
        // (matching streamz behavior)
        self.propagate_batch(py, items)
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
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Stream '{}' is already compiled",
                this.name
            )));
        }

        // Run full optimization (including chain fusion)
        this.optimize_topology_full(py);

        // Mark this node and all reachable nodes as frozen and compiled
        this.freeze_all(py);

        drop(this);
        Ok(slf.clone().unbind())
    }
}
