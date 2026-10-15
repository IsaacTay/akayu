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
        let builtin_op = crate::builtin::eager_op(py, &logic);
        if let Some(interrupt) = crate::builtin::take_interrupt() {
            return Err(interrupt);
        }
        let mut node = Self::blank(logic, name.into(), source_location);
        node.builtin_op = builtin_op;
        Py::new(py, node)
    }

    /// A node with no edges, not compiled, every flag clear.
    fn blank(
        logic: NodeLogic,
        name: std::sync::Arc<str>,
        source_location: std::sync::Arc<str>,
    ) -> Self {
        Self {
            chainable: logic.is_step(),
            logic,
            downstreams: crate::Downstreams::new(),
            parents: std::sync::Mutex::new(crate::Parents::new()),
            name,
            locks_children: false,
            compiled: false,
            source_location,
            builtin_op: None,
            cached_vectorcall: None,
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

    /// Build a node below `slf` (`this`, borrowed by the caller) and link
    /// the two: `slf` gets the edge down, the node its parent.
    pub(crate) fn attach(
        slf: &Bound<'_, Self>,
        this: &mut Self,
        logic: NodeLogic,
        name: String,
        source_location: std::sync::Arc<str>,
    ) -> PyResult<Py<Self>> {
        let py = slf.py();
        let node = Self::make_node(py, logic, name, source_location)?;
        this.downstreams.push(Downstream::new(py, &node));
        node.borrow(py)
            .parents
            .lock()
            .unwrap()
            .push(slf.clone().unbind());
        Ok(node)
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
    fn flush_source(&self, py: Python, mode: FlushMode) -> PyResult<()> {
        let _entered = self.enter_source(py, true)?;
        if mode == FlushMode::Flush && self.downstreams.is_empty() {
            warn_noop_flush(py, &self.name);
        }
        let mut visited = crate::helpers::PtrSet::default();
        visited.insert(std::ptr::from_ref(self) as usize);
        for downstream in &self.downstreams {
            let (mut d, _guard) = downstream.locked_for_flush(py)?;
            d.flush_visit(py, mode, false, &mut visited)?;
        }
        Ok(())
    }

    /// flush() or drain() of this node, which the caller holds the lock of
    /// (`helpers::with_node_mut`). `warn_if_noop`: warn if flushing it does
    /// nothing (only for the node the user called flush() on: nodes reached
    /// by propagation are expected to be leaves).
    pub(crate) fn flush_node(
        &mut self,
        py: Python,
        mode: FlushMode,
        warn_if_noop: bool,
    ) -> PyResult<()> {
        self.flush_visit(
            py,
            mode,
            warn_if_noop,
            &mut crate::helpers::PtrSet::default(),
        )
    }

    /// One flush() or drain() call reaches each node once: below a merge
    /// (union, zip, ...) a node is reachable along several paths, and a
    /// collect() there used to emit once per path. `visited` holds the nodes
    /// already flushed. The caller holds this node's lock (its edge's, see
    /// `Downstream::locked_for_flush`).
    fn flush_visit(
        &mut self,
        py: Python,
        mode: FlushMode,
        warn_if_noop: bool,
        visited: &mut crate::helpers::PtrSet<usize>,
    ) -> PyResult<()> {
        crate::stack::check()?;
        if self.busy {
            // Compiled, mid-update: a callback it ran is flushing it.
            return Err(StreamError::BusyFlush.into());
        }
        if !visited.insert(std::ptr::from_ref(self) as usize) {
            return Ok(());
        }
        if self.flushing {
            return Err(StreamError::ReentrantFlush(self.name.to_string()).into());
        }
        self.flushing = true;
        let result = self.flush_inner(py, mode, warn_if_noop, visited);
        self.flushing = false;
        result
    }

    fn flush_inner(
        &mut self,
        py: Python,
        mode: FlushMode,
        warn_if_noop: bool,
        visited: &mut crate::helpers::PtrSet<usize>,
    ) -> PyResult<()> {
        if warn_if_noop && self.flush_does_nothing() {
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
                if let Some(chunk) = crate::steps::chunk_take(py, buffer, emit_empty)? {
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

        // drain() goes on to every node below. flush() stops at a collect()
        // or chunk_by(): their buffer went downstream as an item, and flushing
        // further would empty the collects below before they have gathered
        // the items of several flushes. Prefetch nodes go on (their results
        // went downstream), and so does chunks_between(), which discards
        // rather than emits: stopping there would swallow the flush.
        let goes_on = mode == FlushMode::Drain
            || !matches!(
                self.logic,
                NodeLogic::Collect { .. } | NodeLogic::ChunkBy(_)
            );
        if goes_on {
            for downstream in &self.downstreams {
                let (mut d, _guard) = downstream.locked_for_flush(py)?;
                d.flush_visit(py, mode, false, visited)?;
            }
        }
        Ok(())
    }

    /// A node with no buffer and no downstreams that isn't a sink: flushing
    /// it can't do anything.
    fn flush_does_nothing(&self) -> bool {
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
        !has_buffer && self.downstreams.is_empty() && !is_sink
    }
}

/// flush(): buffers emit and stop; drain(): every node below is flushed too.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlushMode {
    Flush,
    Drain,
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
        let name = name.map_or_else(|| "source".into(), Into::into);
        Self::blank(NodeLogic::Source, name, capture_source_location(py))
    }

    /// True if the stream graph is compiled (frozen).
    #[getter]
    pub fn compiled(&self) -> bool {
        self.compiled
    }

    /// Apply a function to each element in the stream.
    ///
    /// Args:
    ///     func: Function to apply to each element.
    ///     *args: Additional positional arguments passed to func.
    ///     stream_name: Optional custom name for this node.
    ///     **kwargs: Additional keyword arguments passed to func.
    ///
    /// Returns:
    ///     A new Stream emitting the transformed values.
    ///
    /// Raises:
    ///     TypeError: If `func` is an async function.
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

        Self::attach(
            slf,
            &mut this,
            NodeLogic::Map { func: wrapped_func },
            name,
            source_location,
        )
    }

    /// Run the next `map()`, `filter()` or `batch_map()` on up to `n` items at once.
    ///
    /// The function of the `map()`, `filter()` or `batch_map()` called on the
    /// returned node runs on the thread pool shared with `par()`, up to `n`
    /// items at a time, and its results go on in input order. A result is
    /// passed on only once later items have come in: the last `n - 1` stay in
    /// flight until more items arrive or `flush()`/`drain()` is called, so
    /// flush at the end of the input. Under `emit_batch()`, a `batch_map()`
    /// gets each whole batch as one task.
    ///
    /// That `map()`/`filter()`/`batch_map()` call returns this same node, now
    /// running the function, so a second `.map()` on it is an ordinary map
    /// after the first. Any other operation after `prefetch()` (`sink()`,
    /// `starmap()`, ...) runs on the calling thread, with no concurrency.
    ///
    /// Only work that releases the GIL overlaps (I/O, `time.sleep()`, C code
    /// that releases it), and the pool (one thread per CPU) also caps how many
    /// functions run at once.
    ///
    /// Args:
    ///     n: Maximum number of items in flight.
    ///     stream_name: Optional custom name for this node.
    ///
    /// Returns:
    ///     A prefetch node: call `map()`, `filter()` or `batch_map()` on it.
    ///
    /// Raises:
    ///     ValueError: If `n` is less than 1 or doesn't fit in 64 bits.
    ///
    /// Example:
    ///     ```python
    ///     source = Stream()
    ///     pages = source.prefetch(8).map(fetch).to_list()
    ///     source.emit_batch(urls)
    ///     source.flush()  # wait for the last results
    ///     ```
    #[pyo3(name = "prefetch", signature = (n, *, stream_name=None))]
    pub fn prefetch(
        slf: &Bound<'_, Self>,
        py: Python,
        n: &Bound<'_, pyo3::types::PyInt>,
        stream_name: Option<String>,
    ) -> PyResult<Py<Self>> {
        let mut this = crate::helpers::graph_node_mut(slf)?;
        check_not_compiled(this.compiled, &this.name)?;
        // Any int, so prefetch(-1) and prefetch(2**64) get the same
        // ValueError as prefetch(0).
        let Ok(n @ 1..) = n.extract::<usize>() else {
            return Err(StreamError::InvalidPrefetchSize(n.to_string()).into());
        };
        let source_location = capture_source_location(py);
        let name = resolve_stream_name(stream_name, &format!("prefetch({})", n), None);
        Self::attach(
            slf,
            &mut this,
            NodeLogic::Prefetch { size: n },
            name,
            source_location,
        )
    }

    /// Apply a function to each element, unpacking the element as arguments.
    ///
    /// Similar to map, but each element is unpacked with *element before
    /// being passed to the function. Useful when elements are tuples.
    ///
    /// Args:
    ///     func: Function to apply (receives unpacked element).
    ///     *args: Additional positional arguments passed to func.
    ///     stream_name: Optional custom name for this node.
    ///     **kwargs: Additional keyword arguments passed to func.
    ///
    /// Returns:
    ///     A new Stream emitting the transformed values.
    ///
    /// Raises:
    ///     TypeError: If `func` is an async function.
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
        Self::attach(slf, &mut this, logic, name, source_location)
    }

    /// Apply a batch-aware function to process entire batches at once.
    ///
    /// Unlike `map()` which calls the function once per item, `batch_map()` calls
    /// the function once per batch with all items as a list. This is ideal for
    /// vectorized operations like NumPy functions that can process arrays efficiently.
    /// Each item of the returned iterable is passed downstream separately.
    ///
    /// The function gets the whole batch only from `emit_batch()` in a linear
    /// part of the graph. Under `emit()`, and below a branch point even under
    /// `emit_batch()`, it is called once per item with a one-item list.
    ///
    /// Args:
    ///     func: Function that receives a list of items and returns an iterable of results.
    ///     *args: Additional positional arguments passed to func.
    ///     stream_name: Optional custom name for this node.
    ///     **kwargs: Additional keyword arguments passed to func.
    ///
    /// Returns:
    ///     A new Stream emitting the transformed values.
    ///
    /// Raises:
    ///     TypeError: If `func` is an async function.
    ///
    /// Example:
    ///     ```python
    ///     import numpy as np
    ///     s = Stream()
    ///     s.batch_map(np.sqrt).sink(print)
    ///     s.emit_batch([1, 4, 9, 16])  # prints 1.0, 2.0, 3.0, 4.0
    ///     ```
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

        Self::attach(
            slf,
            &mut this,
            NodeLogic::BatchMap { func: wrapped_func },
            name,
            source_location,
        )
    }

    /// Filter elements based on a predicate function.
    ///
    /// Only elements for which the predicate returns a truthy value
    /// are passed downstream.
    ///
    /// Args:
    ///     predicate: Function that returns True for elements to keep.
    ///     *args: Additional positional arguments passed to predicate.
    ///     stream_name: Optional custom name for this node.
    ///     **kwargs: Additional keyword arguments passed to predicate.
    ///
    /// Returns:
    ///     A new Stream emitting only elements that pass the filter.
    ///
    /// Raises:
    ///     TypeError: If `predicate` is an async function.
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

        Self::attach(
            slf,
            &mut this,
            NodeLogic::Filter {
                predicate: wrapped_predicate,
            },
            name,
            source_location,
        )
    }

    /// Flatten an iterable element into individual elements.
    ///
    /// Each element is expected to be iterable. The items within each
    /// element are emitted individually downstream.
    ///
    /// Returns:
    ///     A new Stream emitting the flattened elements.
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
        Self::attach(slf, &mut this, NodeLogic::Flatten, name, source_location)
    }

    /// Collect elements into a buffer until `flush()` is called.
    ///
    /// Elements are accumulated in an internal buffer. When `flush()` is
    /// called, all collected elements are emitted as a single tuple.
    ///
    /// Returns:
    ///     A new Stream that emits collected tuples on flush.
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
        Self::attach(
            slf,
            &mut this,
            NodeLogic::Collect { buffer: Vec::new() },
            name,
            source_location,
        )
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
    ///     include_delimiter: If True, the delimiter item is included
    ///         in the emitted tuple. If False, it is excluded.
    ///
    /// Returns:
    ///     A new Stream emitting tuples of chunked items.
    ///
    /// Raises:
    ///     TypeError: If `predicate` is an async function.
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

        Self::attach(
            slf,
            &mut this,
            NodeLogic::ChunkBy(Box::new(ChunkByState {
                buffer: Vec::new(),
                predicate,
                order: ChunkOrder::Operator { include_delimiter },
                pre_buffer_chain: Vec::new(),
            })),
            name,
            source_location,
        )
    }

    /// Emit the items between an opening and a closing delimiter as a tuple.
    ///
    /// Outside a chunk only `open` is called; inside one only `close` is.
    /// Items are buffered until `close` matches, then emitted as a tuple.
    /// The delimiters themselves are not included, and empty chunks are not
    /// emitted. The same predicate can serve as both delimiters.
    ///
    /// `flush()`/`drain()` discard an incomplete chunk, return to the
    /// outside state, and continue on to downstream nodes.
    ///
    /// Args:
    ///     open: Predicate that returns True for the opening delimiter.
    ///     close: Predicate that returns True for the closing delimiter.
    ///     stream_name: Optional custom name for this node.
    ///
    /// Returns:
    ///     A new Stream emitting tuples of the items between delimiters.
    ///
    /// Raises:
    ///     TypeError: If `open` or `close` is an async function.
    ///
    /// Example:
    ///     ```python
    ///     source = Stream()
    ///     out = []
    ///     source.chunks_between(lambda c: c == "[", lambda c: c == "]").sink(out.append)
    ///     for ch in "a[bc]d[e]":
    ///         source.emit(ch)
    ///     out  # [('b', 'c'), ('e',)]
    ///     ```
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

        Self::attach(
            slf,
            &mut this,
            NodeLogic::ExtractBetween(Box::new(ExtractBetweenState {
                buffer: Vec::new(),
                open,
                close,
                inside: false,
                close_vectorcall: None,
            })),
            name,
            source_location,
        )
    }

    /// Flush buffered elements downstream.
    ///
    /// `collect()` and `chunk_by()` nodes emit their buffer as a tuple and
    /// stop there; `chunks_between()` discards an incomplete chunk; prefetch
    /// nodes wait for items still in flight. Other nodes pass the flush on,
    /// so `source.flush()` reaches the first buffering node on every path.
    /// Use `drain()` to flush every node in the graph.
    ///
    /// Called from another thread, flushing a source (or any node of a
    /// compiled graph) waits for a running `emit()` to finish, and an `emit()`
    /// waits for the flush. See the Concurrency guide for the limits.
    ///
    /// Args:
    ///     *args: Ignored (for compatibility with streamz).
    ///     **kwargs: Ignored (for compatibility with streamz).
    ///
    /// Raises:
    ///     CallbackError: If a callback raises (as for `emit()`).
    ///     BusyError: If a node is flushed from one of its own downstream
    ///         callbacks mid-update.
    ///     ReentrantError: If a callback of the flush flushes a node that is
    ///         being flushed.
    ///     DeadlockError: If waiting for another thread would never end (see
    ///         the Concurrency guide).
    ///     RecursionError: If the graph is too deep for the thread's stack.
    #[pyo3(signature = (*_args, **_kwargs))]
    pub fn flush(
        slf: &Bound<'_, Self>,
        py: Python,
        _args: Vec<Py<PyAny>>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        if let Some(source) = Self::as_source(slf) {
            return source.flush_source(py, FlushMode::Flush);
        }
        crate::helpers::with_node_mut(slf, py, |node| node.flush_node(py, FlushMode::Flush, true))
    }

    /// Recursively flush this node and all downstream nodes.
    ///
    /// Unlike `flush()`, which stops at the first buffering node on each
    /// path, `drain()` flushes this node and then recursively drains every
    /// downstream node, so every buffer's contents are emitted. Use this when
    /// you want to flush an entire pipeline (e.g. at the end of processing).
    ///
    /// Args:
    ///     *args: Ignored (for compatibility with streamz).
    ///     **kwargs: Ignored (for compatibility with streamz).
    #[pyo3(signature = (*_args, **_kwargs))]
    pub fn drain(
        slf: &Bound<'_, Self>,
        py: Python,
        _args: Vec<Py<PyAny>>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        if let Some(source) = Self::as_source(slf) {
            return source.flush_source(py, FlushMode::Drain);
        }
        crate::helpers::with_node_mut(slf, py, |node| node.flush_node(py, FlushMode::Drain, false))
    }

    /// Apply a function to each element for side effects.
    ///
    /// Similar to map, but the return value of func is discarded.
    /// Use this for terminal operations like printing or storing results.
    ///
    /// Args:
    ///     func: Function to call with each element.
    ///     *args: Additional positional arguments passed to func.
    ///     stream_name: Optional custom name for this node.
    ///     **kwargs: Additional keyword arguments passed to func.
    ///
    /// Returns:
    ///     A new Stream (for chaining, though sink typically ends a pipeline).
    ///
    /// Raises:
    ///     TypeError: If `func` is an async function.
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

        Self::attach(
            slf,
            &mut this,
            NodeLogic::Sink { func: wrapped_func },
            name,
            source_location,
        )
    }

    /// Accumulate values using a function, emitting each intermediate state.
    ///
    /// Similar to functools.reduce, but emits every intermediate result.
    /// The function receives (current_state, new_element) and returns the new state.
    ///
    /// Args:
    ///     func: Accumulator function (state, element) -> new_state.
    ///         If returns_state=True, function should return (new_state, result).
    ///     start: Initial state value.
    ///     returns_state: If True, func returns (state, result) tuple where state
    ///         is passed to next call and result is emitted. Default False.
    ///
    /// Returns:
    ///     A new Stream emitting accumulated states (or results if returns_state=True).
    ///
    /// Raises:
    ///     TypeError: If `func` is an async function.
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

        Self::attach(
            slf,
            &mut this,
            NodeLogic::Accumulate {
                func,
                state: start,
                returns_state,
            },
            name,
            source_location,
        )
    }

    /// Emit a single value into the stream.
    ///
    /// This is the primary way to push data into a source stream. The value
    /// goes through every downstream node before `emit()` returns, except
    /// results still in flight in a `prefetch()` (see `flush()`).
    ///
    /// Emits and flushes of the same graph from different threads wait for
    /// each other; see the Concurrency guide for the limits. Exceptions that
    /// aren't `Exception` subclasses (`KeyboardInterrupt`, `SystemExit`, ...)
    /// raised in a callback pass through unchanged.
    ///
    /// Args:
    ///     x: The value to emit.
    ///
    /// Raises:
    ///     CallbackError: If a callback raises: it names the node (`node`,
    ///         `location`), and the original exception is its `__cause__`.
    ///         akayu errors a callback causes arrive this way too: a callback
    ///         emitting into its own source (`ReentrantError`), or changing
    ///         the graph mid-emit (`BusyError`).
    ///     NotASourceError: If this isn't a source node.
    ///     ReentrantError: If an item comes back into a node still processing
    ///         another (a loop through sources).
    ///     DeadlockError: If waiting for another thread would never end:
    ///         threads emitting into each other's sources (see the
    ///         Concurrency guide).
    ///     RecursionError: If the graph is too deep for the thread's stack
    ///         (thousands of nodes, or long chains of sources feeding each other).
    pub fn emit(&self, py: Python, x: Py<PyAny>) -> PyResult<()> {
        let _entered = self.enter_source(py, false)?;

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
    /// Cheaper per item than calling `emit()` for each. The batch goes through
    /// the pipeline step by step: each step processes every item before the
    /// next step starts (below a branch point, items go through the branches
    /// one at a time unless `compile()` proves the branches independent). If
    /// a step raises, the steps after it see none of the batch.
    ///
    /// Args:
    ///     items: The values, as a sequence (list, tuple, range, ...).
    ///
    /// Raises:
    ///     CallbackError: As for `emit()`.
    ///     NotASourceError: As for `emit()`.
    ///     ReentrantError: As for `emit()`.
    ///     DeadlockError: As for `emit()`.
    ///     RecursionError: As for `emit()`.
    ///     TypeError: If `items` isn't a sequence (a generator or a set), or is a str.
    pub fn emit_batch(&self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<()> {
        let _entered = self.enter_source(py, false)?;

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

    /// Freeze and optimize the graph below this stream.
    ///
    /// Call this after building the complete pipeline, before emitting data.
    /// Each linear run of steps (map, filter, starmap, flatten, accumulate,
    /// batch_map, sink) becomes one node that calls its functions in turn;
    /// errors and profiles still name each step. Steps compile() recognizes
    /// (simple lambdas, builtins such as `len`) run natively, and a graph
    /// without `par()` or `prefetch()` gets a single lock.
    ///
    /// compile() freezes every node reachable downstream of this stream:
    /// adding a node below any of them raises `FrozenGraphError`. Streams that
    /// only feed into the graph (another input of a `union()`, or the source
    /// above a derived node you compile) are neither frozen nor optimized, so
    /// call compile() on every source. State such as an `accumulate()`'s
    /// total carries across compile().
    ///
    /// Returns:
    ///     self (for chaining)
    ///
    /// Raises:
    ///     FrozenGraphError: If this stream is already compiled.
    ///     BusyError: If a node of the graph is processing an item (compile()
    ///         called from a callback, or while another thread emits).
    ///
    /// Warns:
    ///     UserWarning: For a `par()` with fewer than two branches.
    ///
    /// Example:
    ///     ```python
    ///     s = Stream()
    ///     s.map(f1).map(f2).sink(print)
    ///     s.compile()  # map, map and sink become one node
    ///     s.emit(1)
    ///     ```
    pub fn compile(slf: &Bound<'_, Self>, py: Python) -> PyResult<Py<Self>> {
        let source_py_ptr = slf.as_ptr() as usize;
        let mut this = crate::helpers::graph_node_mut(slf)?;
        if this.compiled {
            return Err(StreamError::AlreadyCompiled(this.name.to_string()).into());
        }

        let all_nodes = this.optimize_topology_full(py, slf);

        this.freeze_all(py, &all_nodes);
        this.mark_independent_fanouts(py, &all_nodes);
        let other_sources = this.has_other_sources(py, source_py_ptr);
        for node_py in &all_nodes {
            if let Ok(mut node) = node_py.try_borrow_mut(py) {
                // A node that runs callbacks mid-update (it emits a chunk or
                // a result as an item arrives) can be flushed by one of them:
                // that is refused while it is busy, as the borrow refuses it
                // uncompiled.
                let calls_out_mid_update = matches!(
                    node.logic,
                    NodeLogic::ChunkBy(_)
                        | NodeLogic::ExtractBetween(_)
                        | NodeLogic::Prefetched { .. }
                );
                if other_sources || calls_out_mid_update {
                    node.guard_reentry = true;
                }
            }
        }

        #[cfg(debug_assertions)]
        crate::optimization::assert_graph_consistent(py, slf.as_unbound(), &this);
        drop(this);
        // A Ctrl+C during the analysis: the graph is compiled (what wasn't
        // analysed is called as is), and the interrupt goes on.
        if let Some(interrupt) = crate::builtin::take_interrupt() {
            return Err(interrupt);
        }
        Ok(slf.clone().unbind())
    }

    /// Start collecting profiling data for subsequent emit() calls.
    ///
    /// Call `stop_profiling()` to end the session and retrieve the report.
    /// Profiling adds minimal overhead (~80ns per node per item). It records
    /// the emits of the thread that started it; emits from other threads
    /// meanwhile are not counted.
    ///
    /// Raises:
    ///     NotASourceError: If called on a non-source node.
    ///     StreamError: If profiling is already active.
    pub fn start_profiling(&self) -> PyResult<()> {
        if !matches!(self.logic, NodeLogic::Source) {
            return Err(StreamError::EmitOnNonSource(self.name.to_string()).into());
        }
        crate::profiling::start_session()
    }

    /// Stop profiling and return the collected report.
    ///
    /// Returns:
    ///     A ProfileReport with per-node timing and item flow data.
    pub fn stop_profiling(&self, py: Python) -> PyResult<Py<crate::profiling::ProfileReport>> {
        let (data, wall_time, emit_count) = crate::profiling::stop_session();
        crate::profiling::build_report(py, self, data, wall_time, emit_count)
    }

    /// Return a Profiler context manager for this source.
    ///
    /// Usage:
    ///     >>> with source.profile() as p:
    ///     ...     source.emit(42)
    ///     >>> print(p.report)
    ///
    /// Returns:
    ///     A context manager that profiles all emits within the block made
    ///     by the thread that entered it.
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
    /// Convenience method combining start_profiling + emit + stop_profiling.
    ///
    /// Args:
    ///     x: The value to emit.
    ///
    /// Returns:
    ///     A ProfileReport for this single emit.
    pub fn emit_profiled(
        &self,
        py: Python,
        x: Py<PyAny>,
    ) -> PyResult<Py<crate::profiling::ProfileReport>> {
        let _entered = self.enter_source(py, false)?;
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
    ///     items: The values, as a sequence (see `emit_batch()`).
    ///
    /// Returns:
    ///     A ProfileReport for this batch emit.
    pub fn emit_batch_profiled(
        &self,
        py: Python,
        items: Vec<Py<PyAny>>,
    ) -> PyResult<Py<crate::profiling::ProfileReport>> {
        let _entered = self.enter_source(py, false)?;
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

    /// Collect all emitted items into a Python list.
    ///
    /// Creates a sink that appends each item to a list, then returns the list.
    /// Items are collected as they flow through the stream.
    ///
    /// Returns:
    ///     A Python list containing all emitted items.
    ///
    /// Example:
    ///     >>> s = Stream()
    ///     >>> results = s.map(lambda x: x * 2).to_list()
    ///     >>> s.emit_batch([1, 2, 3])
    ///     >>> results
    ///     [2, 4, 6]
    pub fn to_list(slf: &Bound<'_, Self>, py: Python) -> PyResult<Py<PyList>> {
        let list: Bound<'_, PyList> = PyList::new(py, Vec::<Py<PyAny>>::new())?;
        let append: Py<PyAny> = list.getattr("append")?.unbind();
        let args: Py<PyTuple> = PyTuple::empty(py).unbind();
        let self_py: Py<Self> = slf.clone().unbind();
        Stream::sink(&self_py.into_bound(py), py, append, args, None, None)?;
        Ok(list.unbind())
    }

    /// Collect all emitted items into a Python set (deduplicates).
    ///
    /// Creates a sink that adds each item to a set, then returns the set.
    /// Items are collected as they flow through the stream.
    ///
    /// Returns:
    ///     A Python set containing all emitted items.
    ///
    /// Example:
    ///     >>> s = Stream()
    ///     >>> results = s.map(lambda x: x % 3).to_set()
    ///     >>> s.emit_batch([1, 2, 3, 4, 5])
    ///     >>> results
    ///     {0, 1, 2}
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
