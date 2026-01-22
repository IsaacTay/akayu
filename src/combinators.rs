//! Multi-stream combinators (union, zip, combine_latest, par)

use pyo3::prelude::*;
use std::collections::VecDeque;

use crate::Stream;
use crate::helpers::check_not_compiled;
use crate::node::NodeLogic;

// Stream methods for combinators
#[pymethods]
impl Stream {
    /// Create a parallel branch point that executes all downstreams concurrently.
    ///
    /// When values flow through a parallel node, all downstream branches are
    /// executed in parallel threads rather than sequentially. This is useful for
    /// I/O-bound operations where branches can overlap their waiting time.
    ///
    /// Uses a shared global thread pool with `prefetch()`, so combining them has
    /// minimal overhead. Locks are only applied at convergence points (union/zip)
    /// for efficient parallel execution.
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
    pub fn par(&mut self, py: Python) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
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
                compiled: false,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    /// Alias for `par()`. Create a parallel branch point.
    pub fn parallel(&mut self, py: Python) -> PyResult<Py<Self>> {
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
    pub fn seq(&mut self, py: Python) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
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
                compiled: false,
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
    pub fn union(&mut self, py: Python, others: Vec<Py<Self>>) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
        // Check if any of the other streams are frozen
        for other in &others {
            if let Ok(other_ref) = other.try_borrow(py) {
                check_not_compiled(other_ref.compiled, &other_ref.name)?;
            }
        }
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
                compiled: false,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));

        for other in others {
            // Use try_borrow_mut to handle case where other is self (already borrowed)
            if let Ok(mut other_ref) = other.try_borrow_mut(py) {
                other_ref.downstreams.push(node.clone_ref(py));
            }
            // If borrow fails, it means it's self (already added above), so skip
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
    pub fn combine_latest(
        &mut self,
        py: Python,
        others: Vec<Py<Self>>,
        emit_on: Option<Py<PyAny>>,
    ) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
        // Check if any of the other streams are frozen
        for other in &others {
            if let Ok(other_ref) = other.try_borrow(py) {
                check_not_compiled(other_ref.compiled, &other_ref.name)?;
            }
        }
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
                compiled: false,
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
    pub fn zip(&mut self, py: Python, others: Vec<Py<Self>>) -> PyResult<Py<Self>> {
        check_not_compiled(self.compiled, &self.name)?;
        // Check if any of the other streams are frozen
        for other in &others {
            if let Ok(other_ref) = other.try_borrow(py) {
                check_not_compiled(other_ref.compiled, &other_ref.name)?;
            }
        }
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
                compiled: false,
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
}

// Helper method for tagging (used by combinators)
impl Stream {
    pub(crate) fn add_tag_node(
        &mut self,
        py: Python,
        index: usize,
        target: &Py<Self>,
    ) -> PyResult<()> {
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
                compiled: false,
            },
        )?;
        self.downstreams.push(tag_node);
        Ok(())
    }
}

// Module-level combinator functions

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
///     >>> s1 = akayu.Stream()
///     >>> s2 = akayu.Stream()
///     >>> merged = akayu.union(s1, s2)
#[pyfunction]
#[pyo3(signature = (*streams))]
pub fn union(py: Python, streams: Vec<Py<Stream>>) -> PyResult<Py<Stream>> {
    if streams.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "union requires at least one stream",
        ));
    }

    // Check if any of the streams are frozen
    for stream in &streams {
        let stream_ref = stream.borrow(py);
        check_not_compiled(stream_ref.compiled, &stream_ref.name)?;
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
            compiled: false,
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
///     >>> s1 = akayu.Stream()
///     >>> s2 = akayu.Stream()
///     >>> combined = akayu.combine_latest(s1, s2)
///     >>> combined_on_s1 = akayu.combine_latest(s1, s2, emit_on=s1)
#[pyfunction]
#[pyo3(signature = (*streams, emit_on=None))]
pub fn combine_latest(
    py: Python,
    streams: Vec<Py<Stream>>,
    emit_on: Option<Py<PyAny>>,
) -> PyResult<Py<Stream>> {
    if streams.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "combine_latest requires at least one stream",
        ));
    }

    // Check if any of the streams are frozen
    for stream in &streams {
        let stream_ref = stream.borrow(py);
        check_not_compiled(stream_ref.compiled, &stream_ref.name)?;
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
            compiled: false,
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
pub fn zip(py: Python, streams: Vec<Py<Stream>>) -> PyResult<Py<Stream>> {
    if streams.len() < 2 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "zip requires at least two streams",
        ));
    }

    // Check if any of the streams are frozen
    for stream in &streams {
        let stream_ref = stream.borrow(py);
        check_not_compiled(stream_ref.compiled, &stream_ref.name)?;
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
            compiled: false,
        },
    )?;

    for (i, stream) in streams.iter().enumerate() {
        let mut stream_ref = stream.borrow_mut(py);
        stream_ref.add_tag_node(py, i, &node)?;
    }

    Ok(node)
}
