//! Stream transformation operations (map, filter, sink, etc.)

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PySet, PyTuple};

use crate::Stream;
use crate::error::StreamError;
use crate::helpers::{
    capture_source_location, check_not_compiled, get_build_starmap_func, get_func_name,
    get_profiler_class, reject_async, resolve_stream_name, wrap_error,
};
use crate::lock::{Downstream, NodeLock};
use crate::node::{ChunkByState, ChunkOrder, ExtractBetweenState, NodeLogic};
use crate::prefetch::{PrefetchKind, Prefetcher};

/// Wrap a function with additional args/kwargs using the build_map_func helper.
/// Bind extra `args`/`kwargs` after the item (see `crate::call`), or return
/// `func` itself when there are none.
fn wrap_func(
    py: Python,
    func: Py<PyAny>,
    args: Py<PyTuple>,
    kwargs: Option<Py<PyDict>>,
) -> PyResult<Py<PyAny>> {
    let kwargs = kwargs
        .as_ref()
        .map(|k| k.bind(py))
        .filter(|k| !k.is_empty());
    if args.bind(py).is_empty() && kwargs.is_none() {
        return Ok(func);
    }
    crate::call::bind(py, func, args.bind(py), kwargs)
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
        source_location: std::sync::Arc<str>,
    ) -> PyResult<Py<Self>> {
        let compiled_chainable = (logic.is_chainable()
            && !matches!(&logic, NodeLogic::Filter { .. }))
            || matches!(&logic, NodeLogic::Chain { .. });
        let compiled_filter_fast = matches!(&logic, NodeLogic::Filter { .. });
        let builtin_op = crate::builtin::eager_op(py, &logic);
        Py::new(
            py,
            Self {
                logic,
                downstreams: crate::Downstreams::new(),
                parents: std::sync::Mutex::new(crate::Parents::new()),
                name: name.into(),
                frozen: false,
                needs_lock: false,
                compiled: false,
                source_location,
                builtin_op,
                cached_vectorcall: None,
                compiled_chainable,
                compiled_filter_fast,
                batch_fanout: false,
                flush_target: None,
                emit_target: None,
                flushing: false,
                emitting: std::sync::atomic::AtomicU8::new(crate::lock::IDLE),
                busy: false,
                guard_reentry: false,
                lock: NodeLock::new(),
            },
        )
    }

    /// `slf` borrowed shared, if it is a source (see `flush_source`).
    fn as_source<'py>(slf: &Bound<'py, Self>) -> Option<PyRef<'py, Self>> {
        slf.try_borrow()
            .ok()
            .filter(|node| matches!(node.logic, NodeLogic::Source))
    }

    /// flush()/drain() called on a source. A source buffers nothing, so this
    /// takes only what emit() takes, its lock and a shared borrow: an emit()
    /// on another thread waits for the lock instead of failing on a borrow,
    /// and the flush waits for a running emit. A callback of the flush that
    /// emits into the source or flushes it again is refused.
    fn flush_source(&self, py: Python, drain: bool) -> PyResult<()> {
        let _guard = self.lock.acquire(py);
        let _flushing = crate::lock::Emitting::enter_flush(&self.emitting)?;
        if !drain && self.downstreams.is_empty() {
            warn_noop_flush(py, &self.name);
        }
        let mut visited = crate::helpers::PtrSet::default();
        visited.insert(std::ptr::from_ref(self) as usize);
        for downstream in &self.downstreams {
            let mut d = downstream
                .try_borrow_mut(py)
                .map_err(|_| PyErr::from(StreamError::BusyFlush))?;
            if drain {
                d.drain_visit(py, &mut visited)?;
            } else {
                d.flush_visit(py, false, &mut visited)?;
            }
        }
        Ok(())
    }

    /// Flush with the re-entrancy guard. `warn_if_noop` is true only for the
    /// node the user called `flush()` on: nodes reached by propagation are
    /// expected to be leaves and must not warn.
    pub(crate) fn flush_guarded(&mut self, py: Python, warn_if_noop: bool) -> PyResult<()> {
        self.flush_visit(py, warn_if_noop, &mut crate::helpers::PtrSet::default())
    }

    /// One flush() call reaches each node once: below a merge (union, zip,
    /// ...) a node is reachable along several paths, and a collect() there
    /// used to emit once per path. `visited` holds the nodes already flushed.
    fn flush_visit(
        &mut self,
        py: Python,
        warn_if_noop: bool,
        visited: &mut crate::helpers::PtrSet<usize>,
    ) -> PyResult<()> {
        crate::stack::check()?;
        if !visited.insert(std::ptr::from_ref(self) as usize) {
            return Ok(());
        }
        let lock = std::sync::Arc::clone(&self.lock);
        let _guard = lock.acquire(py);
        if self.flushing {
            return Err(StreamError::ReentrantFlush(self.name.to_string()).into());
        }
        self.flushing = true;
        let result = self.flush_inner(py, warn_if_noop, visited);
        self.flushing = false;
        result
    }

    fn flush_inner(
        &mut self,
        py: Python,
        warn_if_noop: bool,
        visited: &mut crate::helpers::PtrSet<usize>,
    ) -> PyResult<()> {
        // Warn if flush has no effect (non-buffering leaf that isn't a Sink)
        let has_buffer = matches!(
            self.logic,
            NodeLogic::Collect { .. }
                | NodeLogic::ChunkBy(_)
                | NodeLogic::ExtractBetween(_)
                | NodeLogic::Prefetched { .. }
        );
        let is_sink = match &self.logic {
            NodeLogic::Sink { .. } => true,
            // A fused chain ending in a sink is a sink
            NodeLogic::Chain { steps } => {
                matches!(
                    steps.last().map(|step| &step.logic),
                    Some(NodeLogic::Sink { .. })
                )
            }
            NodeLogic::ChainMember { was_sink } => *was_sink,
            _ => false,
        };
        if warn_if_noop && !has_buffer && self.downstreams.is_empty() && !is_sink {
            warn_noop_flush(py, &self.name);
        }
        match &mut self.logic {
            NodeLogic::Collect { buffer } => {
                let items = std::mem::take(buffer);
                let tuple = PyTuple::new(py, items)?;
                self.propagate(py, tuple.into())?;
            }
            NodeLogic::ChunkBy(boxed) => {
                let ChunkByState { buffer, order, .. } = &mut **boxed;
                let emit_empty = matches!(order, ChunkOrder::Fused { .. });
                if let Some(chunk) = crate::execution::chunk_take(py, buffer, emit_empty)? {
                    self.propagate(py, chunk)?;
                }
            }
            NodeLogic::ExtractBetween(boxed) => {
                let ExtractBetweenState { buffer, inside, .. } = &mut **boxed;
                // Discard the incomplete chunk and return to the outside state
                buffer.clear();
                *inside = false;
            }
            NodeLogic::Prefetched { prefetcher } => {
                // The results before a failed item still go on, then its error.
                let (results, error) = prefetcher.flush(py);
                for result in results {
                    self.propagate(py, result)?;
                }
                if let Some(e) = error {
                    return wrap_error(py, Err(e), &self.name, &self.source_location);
                }
            }
            _ => {}
        }

        // Propagate flush to downstream nodes — but NOT for Collect/ChunkBy.
        // Collect/ChunkBy emit their buffer via propagate() above; cascading
        // flush would prematurely drain downstream collects before they've had
        // a chance to accumulate items from multiple flushes.
        // Prefetch nodes DO propagate so their drained results flow onward.
        // ExtractBetween also propagates: it discards rather than emits on
        // flush, so stopping here would silently swallow the flush.
        let should_propagate = !matches!(
            self.logic,
            NodeLogic::Collect { .. } | NodeLogic::ChunkBy(_)
        );
        if should_propagate {
            for downstream in &self.downstreams {
                let mut d = downstream
                    .try_borrow_mut(py)
                    .map_err(|_| PyErr::from(StreamError::BusyFlush))?;
                d.flush_visit(py, false, visited)?;
            }
        }
        Ok(())
    }

    /// drain(), reaching each node once (see `flush_visit`).
    fn drain_visit(
        &mut self,
        py: Python,
        visited: &mut crate::helpers::PtrSet<usize>,
    ) -> PyResult<()> {
        crate::stack::check()?;
        if !visited.insert(std::ptr::from_ref(self) as usize) {
            return Ok(());
        }
        // Re-entrancy guard: detect cycles in the flush graph
        if self.flushing {
            return Err(StreamError::ReentrantFlush(self.name.to_string()).into());
        }
        self.flushing = true;
        let result = self.drain_inner(py, visited);
        self.flushing = false;
        result
    }

    fn drain_inner(
        &mut self,
        py: Python,
        visited: &mut crate::helpers::PtrSet<usize>,
    ) -> PyResult<()> {
        // Flush this node's buffer
        match &mut self.logic {
            NodeLogic::Collect { buffer } => {
                let items = std::mem::take(buffer);
                let tuple = PyTuple::new(py, items)?;
                self.propagate(py, tuple.into())?;
            }
            NodeLogic::ChunkBy(boxed) => {
                let ChunkByState { buffer, order, .. } = &mut **boxed;
                let emit_empty = matches!(order, ChunkOrder::Fused { .. });
                if let Some(chunk) = crate::execution::chunk_take(py, buffer, emit_empty)? {
                    self.propagate(py, chunk)?;
                }
            }
            NodeLogic::ExtractBetween(boxed) => {
                let ExtractBetweenState { buffer, inside, .. } = &mut **boxed;
                // Discard the incomplete chunk and return to the outside state
                buffer.clear();
                *inside = false;
            }
            NodeLogic::Prefetched { prefetcher } => {
                // The results before a failed item still go on, then its error.
                let (results, error) = prefetcher.flush(py);
                for result in results {
                    self.propagate(py, result)?;
                }
                if let Some(e) = error {
                    return wrap_error(py, Err(e), &self.name, &self.source_location);
                }
            }
            _ => {}
        }

        // Always recurse into all downstreams
        for downstream in &self.downstreams {
            let mut d = downstream
                .try_borrow_mut(py)
                .map_err(|_| PyErr::from(StreamError::BusyFlush))?;
            d.drain_visit(py, visited)?;
        }
        Ok(())
    }
}

#[pymethods]
impl Stream {
    /// Create a new source Stream.
    ///
    /// Args:
    ///     name: Optional name for the stream (defaults to "source").
    #[new]
    #[pyo3(signature = (name=None))]
    pub fn new(py: Python, name: Option<String>) -> Self {
        let source_location = capture_source_location(py);
        Self {
            logic: NodeLogic::Source,
            downstreams: crate::Downstreams::new(),
            parents: std::sync::Mutex::new(crate::Parents::new()),
            name: name.map_or_else(|| "source".into(), Into::into),
            frozen: false,
            needs_lock: false,
            compiled: false,
            source_location,
            builtin_op: None,
            cached_vectorcall: None,
            compiled_chainable: false,
            compiled_filter_fast: false,
            batch_fanout: false,
            flush_target: None,
            emit_target: None,
            flushing: false,
            emitting: std::sync::atomic::AtomicU8::new(crate::lock::IDLE),
            busy: false,
            guard_reentry: false,
            lock: NodeLock::new(),
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
        let mut this = crate::helpers::graph_node_mut(slf)?;
        check_not_compiled(this.compiled, &this.name)?;
        reject_async(py, &func, "map")?;
        let func_name = get_func_name(py, &func);
        let name = resolve_stream_name(stream_name, "map", func_name.as_deref());
        let source_location = capture_source_location(py);
        let wrapped_func = wrap_func(py, func, args, kwargs)?;

        if let NodeLogic::Prefetch { size } = this.logic {
            this.logic = NodeLogic::Prefetched {
                prefetcher: Box::new(Prefetcher::new(wrapped_func, PrefetchKind::Map, size)),
            };
            this.name = format!("prefetch_map({size})+{name}").into();
            this.source_location = format!("{}, {}", this.source_location, source_location).into();
            return Ok(slf.clone().unbind());
        }

        let node = Self::make_node(
            py,
            NodeLogic::Map { func: wrapped_func },
            name,
            source_location,
        )?;
        this.downstreams.push(Downstream::new(py, &node));
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
    #[pyo3(name = "prefetch", signature = (n, *, stream_name=None))]
    pub fn prefetch(
        slf: &Bound<'_, Self>,
        py: Python,
        n: i64,
        stream_name: Option<String>,
    ) -> PyResult<Py<Self>> {
        let mut this = crate::helpers::graph_node_mut(slf)?;
        check_not_compiled(this.compiled, &this.name)?;
        // Signed, so prefetch(-1) gets the same ValueError as prefetch(0).
        let Ok(n @ 1..) = usize::try_from(n) else {
            return Err(StreamError::InvalidPrefetchSize(n).into());
        };
        let source_location = capture_source_location(py);
        let name = resolve_stream_name(stream_name, &format!("prefetch({})", n), None);
        let node = Self::make_node(py, NodeLogic::Prefetch { size: n }, name, source_location)?;
        this.downstreams.push(Downstream::new(py, &node));
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
        let mut this = crate::helpers::graph_node_mut(slf)?;
        check_not_compiled(this.compiled, &this.name)?;
        reject_async(py, &func, "starmap")?;
        let func_name = get_func_name(py, &func);
        let name = resolve_stream_name(stream_name, "starmap", func_name.as_deref());
        let source_location = capture_source_location(py);
        // Without positional extras, star-call natively (keyword arguments
        // bound with functools.partial); otherwise wrap in Python.
        let logic = if args.bind(py).is_empty() {
            let func = match kwargs.as_ref().filter(|k| !k.bind(py).is_empty()) {
                None => func,
                Some(k) => py
                    .import(pyo3::intern!(py, "functools"))?
                    .getattr(pyo3::intern!(py, "partial"))?
                    .call((func,), Some(k.bind(py)))?
                    .unbind(),
            };
            NodeLogic::StarMap { func }
        } else {
            let wrapped_func = get_build_starmap_func()?.call1(py, (func, args, kwargs))?;
            NodeLogic::Map { func: wrapped_func }
        };
        let node = Self::make_node(py, logic, name, source_location)?;
        this.downstreams.push(Downstream::new(py, &node));
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
        let mut this = crate::helpers::graph_node_mut(slf)?;
        check_not_compiled(this.compiled, &this.name)?;
        reject_async(py, &func, "batch_map")?;
        let func_name = get_func_name(py, &func);
        let name = resolve_stream_name(stream_name, "batch_map", func_name.as_deref());
        let source_location = capture_source_location(py);
        let wrapped_func = wrap_func(py, func, args, kwargs)?;

        if let NodeLogic::Prefetch { size } = this.logic {
            this.logic = NodeLogic::Prefetched {
                prefetcher: Box::new(Prefetcher::new(wrapped_func, PrefetchKind::BatchMap, size)),
            };
            this.name = format!("prefetch_batch_map({size})+{name}").into();
            this.source_location = format!("{}, {}", this.source_location, source_location).into();
            return Ok(slf.clone().unbind());
        }

        let node = Self::make_node(
            py,
            NodeLogic::BatchMap { func: wrapped_func },
            name,
            source_location,
        )?;
        this.downstreams.push(Downstream::new(py, &node));
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
        let mut this = crate::helpers::graph_node_mut(slf)?;
        check_not_compiled(this.compiled, &this.name)?;
        reject_async(py, &predicate, "filter")?;
        let func_name = get_func_name(py, &predicate);
        let name = resolve_stream_name(stream_name, "filter", func_name.as_deref());
        let source_location = capture_source_location(py);
        let wrapped_predicate = wrap_func(py, predicate, args, kwargs)?;

        if let NodeLogic::Prefetch { size } = this.logic {
            this.logic = NodeLogic::Prefetched {
                prefetcher: Box::new(Prefetcher::new(
                    wrapped_predicate,
                    PrefetchKind::Filter,
                    size,
                )),
            };
            this.name = format!("prefetch_filter({size})+{name}").into();
            this.source_location = format!("{}, {}", this.source_location, source_location).into();
            return Ok(slf.clone().unbind());
        }

        let node = Self::make_node(
            py,
            NodeLogic::Filter {
                predicate: wrapped_predicate,
            },
            name,
            source_location,
        )?;
        this.downstreams.push(Downstream::new(py, &node));
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
    #[pyo3(signature = (*, stream_name=None))]
    pub fn flatten(
        slf: &Bound<'_, Self>,
        py: Python,
        stream_name: Option<String>,
    ) -> PyResult<Py<Self>> {
        let mut this = crate::helpers::graph_node_mut(slf)?;
        check_not_compiled(this.compiled, &this.name)?;
        let source_location = capture_source_location(py);
        let name = resolve_stream_name(stream_name, "flatten", None);
        let node = Self::make_node(py, NodeLogic::Flatten, name, source_location)?;
        this.downstreams.push(Downstream::new(py, &node));
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
    #[pyo3(signature = (*, stream_name=None))]
    pub fn collect(
        slf: &Bound<'_, Self>,
        py: Python,
        stream_name: Option<String>,
    ) -> PyResult<Py<Self>> {
        let mut this = crate::helpers::graph_node_mut(slf)?;
        check_not_compiled(this.compiled, &this.name)?;
        let source_location = capture_source_location(py);
        let name = resolve_stream_name(stream_name, "collect", None);
        let node = Self::make_node(
            py,
            NodeLogic::Collect { buffer: Vec::new() },
            name,
            source_location,
        )?;
        this.downstreams.push(Downstream::new(py, &node));
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
        let mut this = crate::helpers::graph_node_mut(slf)?;
        check_not_compiled(this.compiled, &this.name)?;
        reject_async(py, &predicate, "chunk_by")?;
        let func_name = get_func_name(py, &predicate);
        let source_location = capture_source_location(py);

        let name = resolve_stream_name(stream_name, "chunk_by", func_name.as_deref());

        let node = Self::make_node(
            py,
            NodeLogic::ChunkBy(Box::new(ChunkByState {
                buffer: Vec::new(),
                predicate,
                order: ChunkOrder::Operator { include_delimiter },
                pre_buffer_chain: Vec::new(),
            })),
            name,
            source_location,
        )?;
        this.downstreams.push(Downstream::new(py, &node));
        let self_py = slf.clone().unbind();
        drop(this);
        node.borrow(py).parents.lock().unwrap().push(self_py);
        Ok(node)
    }

    /// Extract items between two delimiter predicates.
    ///
    /// State machine that buffers items between `open` and `close` delimiters.
    /// When `open` returns True, transitions to Inside state and starts buffering.
    /// When `close` returns True in Inside state, emits the buffer as a tuple
    /// and transitions back to Outside state. The delimiters themselves are
    /// NOT included in the emitted tuple.
    ///
    /// Only `open` is called while Outside and only `close` while Inside, so
    /// a stray `close` Outside or a stray `open` Inside is ignored, and the
    /// same predicate can serve as both delimiters (e.g. quotes).
    ///
    /// `flush()`/`drain()` discard an incomplete chunk, return to the Outside
    /// state, and continue on to downstream nodes.
    ///
    /// # Arguments
    ///
    /// * `open` - Predicate that returns True for the opening delimiter.
    /// * `close` - Predicate that returns True for the closing delimiter.
    ///
    /// # Returns
    ///
    /// A new Stream emitting tuples of extracted items.
    ///
    /// # Examples
    ///
    /// Extract content between `[` and `]`:
    ///
    /// ```python
    /// import akayu
    /// source = akayu.Stream()
    /// content = source.chunks_between(lambda c: c == '[', lambda c: c == ']')
    /// results = []
    /// content.sink(results.append)
    /// for ch in "abc[def]ghi[jkl]mno":
    ///     source.emit(ch)
    /// source.flush()
    /// # results == [('d', 'e', 'f'), ('j', 'k', 'l')]
    /// ```
    ///
    /// # Raises
    ///
    /// * `RuntimeError` - If the stream graph is already compiled.
    #[pyo3(signature = (open, close, *, stream_name=None))]
    pub fn chunks_between(
        slf: &Bound<'_, Self>,
        py: Python,
        open: Py<PyAny>,
        close: Py<PyAny>,
        stream_name: Option<String>,
    ) -> PyResult<Py<Self>> {
        let mut this = crate::helpers::graph_node_mut(slf)?;
        check_not_compiled(this.compiled, &this.name)?;
        reject_async(py, &open, "chunks_between")?;
        reject_async(py, &close, "chunks_between")?;
        let open_name = get_func_name(py, &open);
        let close_name = get_func_name(py, &close);
        let source_location = capture_source_location(py);

        let name = resolve_stream_name(
            stream_name,
            "chunks_between",
            Some(&format!(
                "{}, {}",
                open_name.as_deref().unwrap_or("?"),
                close_name.as_deref().unwrap_or("?")
            )),
        );

        let node = Self::make_node(
            py,
            NodeLogic::ExtractBetween(Box::new(ExtractBetweenState {
                buffer: Vec::new(),
                open,
                close,
                inside: false,
                close_vectorcall: None,
            })),
            name,
            source_location,
        )?;
        this.downstreams.push(Downstream::new(py, &node));
        let self_py = slf.clone().unbind();
        drop(this);
        node.borrow(py).parents.lock().unwrap().push(self_py);
        Ok(node)
    }

    /// Flush buffered elements downstream.
    ///
    /// `collect()` and `chunk_by()` nodes emit their buffer as a tuple and
    /// stop there; `chunks_between()` discards an incomplete chunk; prefetch
    /// nodes wait for items still in flight. Other nodes pass the flush on,
    /// so `source.flush()` reaches the first buffering node on every path.
    /// Use `drain()` to flush every node in the graph.
    ///
    /// # Arguments
    ///
    /// * `*args` - Ignored (for compatibility with streamz).
    /// * `**kwargs` - Ignored (for compatibility with streamz).
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
        slf: &Bound<'_, Self>,
        py: Python,
        _args: Vec<Py<PyAny>>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        if let Some(source) = Self::as_source(slf) {
            return source.flush_source(py, false);
        }
        crate::helpers::with_node_mut(slf, py, |node| node.flush_guarded(py, true))
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
        slf: &Bound<'_, Self>,
        py: Python,
        _args: Vec<Py<PyAny>>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        if let Some(source) = Self::as_source(slf) {
            return source.flush_source(py, true);
        }
        crate::helpers::with_node_mut(slf, py, |node| {
            node.drain_visit(py, &mut crate::helpers::PtrSet::default())
        })
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
        let mut this = crate::helpers::graph_node_mut(slf)?;
        check_not_compiled(this.compiled, &this.name)?;
        reject_async(py, &func, "sink")?;
        let func_name = get_func_name(py, &func);
        let name = resolve_stream_name(stream_name, "sink", func_name.as_deref());
        let source_location = capture_source_location(py);
        let wrapped_func = wrap_func(py, func, args, kwargs)?;

        let node = Self::make_node(
            py,
            NodeLogic::Sink { func: wrapped_func },
            name,
            source_location,
        )?;
        this.downstreams.push(Downstream::new(py, &node));
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
    #[pyo3(signature = (func, start, *, returns_state=false, stream_name=None))]
    pub fn accumulate(
        slf: &Bound<'_, Self>,
        py: Python,
        func: Py<PyAny>,
        start: Py<PyAny>,
        returns_state: bool,
        stream_name: Option<String>,
    ) -> PyResult<Py<Self>> {
        let mut this = crate::helpers::graph_node_mut(slf)?;
        check_not_compiled(this.compiled, &this.name)?;
        reject_async(py, &func, "accumulate")?;
        let func_name = get_func_name(py, &func);
        let source_location = capture_source_location(py);

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
        )?;
        this.downstreams.push(Downstream::new(py, &node));
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
    pub fn emit(&self, py: Python, x: Py<PyAny>) -> PyResult<()> {
        let _guard = self.lock.acquire(py);
        if !matches!(self.logic, NodeLogic::Source) {
            return Err(StreamError::EmitOnNonSource(self.name.to_string()).into());
        }
        let _emitting = crate::lock::Emitting::enter(&self.emitting, &self.name)?;

        let profiling = crate::profiling::is_profiling();
        if profiling {
            crate::profiling::begin_emit();
        }

        let result = self.propagate(py, x);

        if profiling {
            crate::profiling::end_emit();
        }
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
    pub fn emit_batch(&self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<()> {
        let _guard = self.lock.acquire(py);
        if !matches!(self.logic, NodeLogic::Source) {
            return Err(StreamError::EmitOnNonSource(self.name.to_string()).into());
        }
        let _emitting = crate::lock::Emitting::enter(&self.emitting, &self.name)?;

        let profiling = crate::profiling::is_profiling();
        if profiling {
            crate::profiling::begin_emit();
        }

        let result = self.propagate_batch(py, items);

        if profiling {
            crate::profiling::end_emit();
        }
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
        let mut this = crate::helpers::graph_node_mut(slf)?;
        if this.compiled {
            return Err(StreamError::AlreadyCompiled(this.name.to_string()).into());
        }

        let all_nodes = this.optimize_topology_full(py, source_py_ptr);

        this.freeze_all(py, &all_nodes);
        this.mark_independent_fanouts(py, &all_nodes);
        if this.has_other_sources(py, source_py_ptr) {
            for node_py in &all_nodes {
                if let Ok(mut node) = node_py.try_borrow_mut(py) {
                    node.guard_reentry = true;
                }
            }
        }

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
            return Err(StreamError::EmitOnNonSource(self.name.to_string()).into());
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
            let this = crate::helpers::graph_node(slf)?;
            if !matches!(this.logic, NodeLogic::Source) {
                return Err(StreamError::EmitOnNonSource(this.name.to_string()).into());
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
        &self,
        py: Python,
        x: Py<PyAny>,
    ) -> PyResult<Py<crate::profiling::ProfileReport>> {
        let _guard = self.lock.acquire(py);
        if !matches!(self.logic, NodeLogic::Source) {
            return Err(StreamError::EmitOnNonSource(self.name.to_string()).into());
        }
        let _emitting = crate::lock::Emitting::enter(&self.emitting, &self.name)?;
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
        &self,
        py: Python,
        items: Vec<Py<PyAny>>,
    ) -> PyResult<Py<crate::profiling::ProfileReport>> {
        let _guard = self.lock.acquire(py);
        if !matches!(self.logic, NodeLogic::Source) {
            return Err(StreamError::EmitOnNonSource(self.name.to_string()).into());
        }
        let _emitting = crate::lock::Emitting::enter(&self.emitting, &self.name)?;
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

/// Warn about a flush() that can't do anything: a node with no buffer and no
/// downstreams, that isn't a sink.
fn warn_noop_flush(py: Python, name: &str) {
    let msg = format!(
        "flush() called on '{name}' which has no buffer and no downstreams — \
         did you mean to call flush() on a collect() node?"
    );
    let warn_type = py.get_type::<pyo3::exceptions::PyUserWarning>();
    let c_msg = std::ffi::CString::new(msg).unwrap();
    let _ = PyErr::warn(py, warn_type.as_any(), &c_msg, 1);
}
