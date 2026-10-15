//! Multi-stream combinators (union, zip, combine_latest, par)
//!
//! Each merge has one builder, `merge()`: the methods (`a.zip(b)`) and the
//! module functions (`akayu.zip(a, b)`) only collect its inputs.

use pyo3::prelude::*;
use std::collections::VecDeque;

use crate::Stream;
use crate::error::StreamError;
use crate::helpers::{capture_source_location, check_not_compiled, graph_node_mut};
use crate::lock::Downstream;
use crate::node::{CombineLatestState, NodeLogic};

// Stream methods for combinators
#[pymethods]
impl Stream {
    /// Create a branch point whose downstream branches run concurrently.
    ///
    /// Each item goes to every branch at once: the branches run on the thread
    /// pool shared with `prefetch()` (the last one on the calling thread), and
    /// `emit()` returns once all of them have finished. If branches raise, the
    /// first failing branch's error (in the order the branches were added) is
    /// raised.
    ///
    /// Only work that releases the GIL overlaps: I/O, `time.sleep()`, C code
    /// that releases it. Pure-Python CPU work gains nothing. Branches that
    /// converge (`union()`, `zip()`) take turns at the merge.
    ///
    /// Use `seq()` directly below a `par()` to run several steps as one
    /// sequential branch.
    ///
    /// Returns:
    ///     A new Stream that will propagate values to its downstreams in parallel.
    ///
    /// Example:
    ///     ```python
    ///     s = Stream()
    ///     p = s.par()  # Create parallel branch point
    ///     a = p.map(load_from_disk)   # Branch 1
    ///     b = p.map(fetch_from_api)   # Branch 2
    ///     merged = union(a, b)
    ///     s.emit(filename)  # a and b run in parallel
    ///     ```
    #[pyo3(name = "par")]
    pub fn par(slf: &Bound<'_, Self>, py: Python) -> PyResult<Py<Self>> {
        let mut this = graph_node_mut(slf)?;
        check_not_compiled(this.compiled, &this.name)?;
        let source_location = capture_source_location(py);
        Self::attach(
            slf,
            &mut this,
            NodeLogic::Parallel,
            "par".to_string(),
            source_location,
        )
    }

    /// Alias for `par()`. Create a parallel branch point.
    pub fn parallel(slf: &Bound<'_, Self>, py: Python) -> PyResult<Py<Self>> {
        Stream::par(slf, py)
    }

    /// Create a node whose downstreams run one after another (the default).
    ///
    /// Directly below a `par()`, `seq()` groups the nodes built on it into one
    /// branch whose steps run in order on one thread. Anywhere else it is a
    /// pass-through: it doesn't move work back to the calling thread (nodes
    /// after `union(a, b).seq()` still run on the thread that delivered the item).
    ///
    /// Returns:
    ///     A new Stream that will propagate values to its downstreams sequentially.
    ///
    /// Example:
    ///     ```python
    ///     s = Stream()
    ///     p = s.par()
    ///     q = p.seq()               # one branch...
    ///     q.map(parse).sink(store)  # ...whose steps run in order
    ///     q.map(index).sink(search)
    ///     p.map(make_thumbnail).sink(save)  # a second, concurrent branch
    ///     ```
    pub fn seq(slf: &Bound<'_, Self>, py: Python) -> PyResult<Py<Self>> {
        let mut this = graph_node_mut(slf)?;
        check_not_compiled(this.compiled, &this.name)?;
        let source_location = capture_source_location(py);
        Self::attach(
            slf,
            &mut this,
            NodeLogic::Source,
            "seq".to_string(),
            source_location,
        )
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
    pub fn union(slf: &Bound<'_, Self>, py: Python, others: Vec<Py<Self>>) -> PyResult<Py<Self>> {
        merge(py, Merge::Union, &with_self(slf, others))
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
    ///         If None (default), emit on update from any stream.
    ///
    /// Returns:
    ///     A new Stream emitting tuples of latest values.
    ///
    /// Raises:
    ///     ValueError: If `emit_on` names a stream that isn't an input, or is
    ///         an empty list.
    #[pyo3(signature = (*others, emit_on=None))]
    pub fn combine_latest(
        slf: &Bound<'_, Self>,
        py: Python,
        others: Vec<Py<Self>>,
        emit_on: Option<Py<PyAny>>,
    ) -> PyResult<Py<Self>> {
        merge(py, Merge::CombineLatest(emit_on), &with_self(slf, others))
    }

    /// Zip multiple streams together element-by-element.
    ///
    /// Emits tuples containing one element from each stream. Waits until
    /// all streams have an element available before emitting. Items wait in
    /// an unbounded buffer: an input that runs ahead of the others keeps
    /// its items in memory until they are paired.
    ///
    /// Args:
    ///     *others: Other streams to zip with this one.
    ///
    /// Returns:
    ///     A new Stream emitting zipped tuples.
    #[pyo3(signature = (*others))]
    pub fn zip(slf: &Bound<'_, Self>, py: Python, others: Vec<Py<Self>>) -> PyResult<Py<Self>> {
        merge(py, Merge::Zip, &with_self(slf, others))
    }
}

/// `slf` followed by `others`: a method's inputs.
fn with_self(slf: &Bound<'_, Stream>, others: Vec<Py<Stream>>) -> Vec<Py<Stream>> {
    std::iter::once(slf.clone().unbind())
        .chain(others)
        .collect()
}

/// The kind of node `merge()` builds.
enum Merge {
    Union,
    Zip,
    /// With the streams `emit_on` names, if any.
    CombineLatest(Option<Py<PyAny>>),
}

/// Merge `inputs`, in order, into a new node. Every input is borrowed and
/// checked before the graph changes, so the merge is built whole or not at
/// all: an input that is processing an item (a callback building on it
/// mid-emit) raises `BusyError`, a compiled one `FrozenGraphError`.
///
/// Every input may be emitted from a different thread, so the edges into
/// the merge lock it (compile() removes those locks when it can).
fn merge(py: Python, kind: Merge, inputs: &[Py<Stream>]) -> PyResult<Py<Stream>> {
    let count = inputs.len();
    // emit_on first: reading a list of streams can run Python code.
    let (logic, name) = match kind {
        Merge::Union => (NodeLogic::Source, "union"),
        Merge::Zip => (
            NodeLogic::Zip {
                buffers: (0..count).map(|_| VecDeque::new()).collect(),
            },
            "zip",
        ),
        Merge::CombineLatest(emit_on) => {
            let ptrs: Vec<_> = inputs.iter().map(Py::as_ptr).collect();
            let emit_on_indices = emit_on_positions(py, emit_on, &ptrs)?;
            let state = CombineLatestState {
                state: (0..count).map(|_| None).collect(),
                emit_on_indices,
            };
            (NodeLogic::CombineLatest(Box::new(state)), "combine_latest")
        }
    };
    let is_union = matches!(logic, NodeLogic::Source);

    // Each distinct input borrowed once (`a.zip(a)` names one twice).
    let mut distinct: Vec<&Py<Stream>> = Vec::with_capacity(count);
    for input in inputs {
        if !distinct.iter().any(|d| d.is(input)) {
            distinct.push(input);
        }
    }
    let mut held = distinct
        .iter()
        .map(|input| graph_node_mut(input.bind(py)))
        .collect::<PyResult<Vec<_>>>()?;
    for node in &held {
        check_not_compiled(node.compiled, &node.name)?;
    }

    let source_location = capture_source_location(py);
    let node = Stream::make_node(py, logic, name.to_string(), source_location.clone())?;
    for (index, input) in inputs.iter().enumerate() {
        let at = distinct
            .iter()
            .position(|d| d.is(input))
            .expect("every input is among the distinct ones");
        let held = &mut held[at];
        if is_union {
            held.locks_children = true;
            held.downstreams.push(Downstream::new(py, &node));
            node.borrow(py)
                .parents
                .lock()
                .unwrap()
                .push(input.clone_ref(py));
        } else {
            held.add_tag_node(py, index, &node, input.clone_ref(py), &source_location)?;
        }
    }
    if is_union {
        node.borrow_mut(py).locks_children = true;
    }
    Ok(node)
}

// Helper method for tagging (used by combinators)
impl Stream {
    pub(crate) fn add_tag_node(
        &mut self,
        py: Python,
        index: usize,
        target: &Py<Self>,
        source_py: Py<Self>,
        source_location: &std::sync::Arc<str>,
    ) -> PyResult<()> {
        let tag_node = Self::make_node(
            py,
            NodeLogic::Tag { index },
            format!("tag({})", index),
            std::sync::Arc::clone(source_location),
        )?;
        {
            let mut tag = tag_node.borrow_mut(py);
            tag.downstreams.push(Downstream::new(py, target));
            // The combinator is shared by every input, which may be emitted
            // from different threads (compile() can remove this lock).
            tag.locks_children = true;
        }
        target
            .borrow(py)
            .parents
            .lock()
            .unwrap()
            .push(tag_node.clone_ref(py));
        self.downstreams.push(Downstream::new(py, &tag_node));
        tag_node.borrow(py).parents.lock().unwrap().push(source_py);
        Ok(())
    }
}

// Module-level combinator functions

/// Merge multiple streams into one.
///
/// Creates a new stream that emits elements from all input streams.
/// Elements are emitted in the order they arrive from any source.
///
/// # Arguments
///
/// * `*streams` - Two or more streams to merge.
///
/// # Returns
///
/// A new Stream emitting elements from all input streams.
///
/// # Examples
///
/// ```python
/// import akayu
/// s1 = akayu.Stream()
/// s2 = akayu.Stream()
/// merged = akayu.union(s1, s2)
/// result = []
/// merged.sink(result.append)
/// s1.emit(1)  # result contains [1]
/// s2.emit(2)  # result contains [1, 2]
/// ```
///
/// # Raises
///
/// * `ValueError` - If fewer than one stream is provided.
#[pyfunction]
#[pyo3(signature = (*streams))]
pub fn union(py: Python, streams: Vec<Py<Stream>>) -> PyResult<Py<Stream>> {
    if streams.is_empty() {
        return Err(StreamError::UnionEmpty(0).into());
    }
    merge(py, Merge::Union, &streams)
}

/// Combine the latest values from multiple streams.
///
/// Creates a new stream that emits a tuple of the most recent values
/// from each input stream whenever any stream emits. Only starts
/// emitting after all streams have emitted at least one value.
///
/// # Arguments
///
/// * `*streams` - Two or more streams to combine.
/// * `emit_on` - Stream or list of streams that trigger emission.
///   If None (default), emit on update from any stream.
///
/// # Returns
///
/// A new Stream emitting tuples of latest values.
///
/// # Examples
///
/// ```python
/// import akayu
/// s1 = akayu.Stream()
/// s2 = akayu.Stream()
/// combined = akayu.combine_latest(s1, s2)
/// result = []
/// combined.sink(result.append)
/// s1.emit('a')
/// s2.emit(1)
/// s1.emit('b')  # result contains [('a', 1), ('b', 1)]
/// s2.emit(2)    # result contains [('a', 1), ('b', 1), ('b', 2)]
///
/// # With emit_on trigger
/// combined_on_s1 = akayu.combine_latest(s1, s2, emit_on=s1)
/// ```
///
/// # Raises
///
/// * `ValueError` - If fewer than one stream is provided.
/// * `TypeError` - If `emit_on` is not a Stream or list of Streams.
#[pyfunction]
#[pyo3(signature = (*streams, emit_on=None))]
pub fn combine_latest(
    py: Python,
    streams: Vec<Py<Stream>>,
    emit_on: Option<Py<PyAny>>,
) -> PyResult<Py<Stream>> {
    if streams.is_empty() {
        return Err(StreamError::CombineLatestEmpty(0).into());
    }
    merge(py, Merge::CombineLatest(emit_on), &streams)
}

/// Zip multiple streams together.
///
/// Creates a new stream that waits for one item from each input stream,
/// then emits them as a tuple. Buffers items until all streams have
/// contributed one value; the buffer is unbounded, so an input that runs
/// ahead of the others keeps its items in memory until they are paired.
///
/// # Arguments
///
/// * `*streams` - Two or more streams to zip together.
///
/// # Returns
///
/// A new stream that emits tuples of values from all input streams.
///
/// # Examples
///
/// ```python
/// import akayu
/// s1 = akayu.Stream()
/// s2 = akayu.Stream()
/// zipped = akayu.zip(s1, s2)
/// result = []
/// zipped.sink(result.append)
/// s1.emit(1)
/// s1.emit(2)  # waiting for s2...
/// s2.emit('a')  # result contains [(1, 'a'), (2, 'a')]
/// ```
///
/// # Raises
///
/// * `ValueError` - If fewer than two streams are provided.
#[pyfunction]
#[pyo3(signature = (*streams))]
pub fn zip(py: Python, streams: Vec<Py<Stream>>) -> PyResult<Py<Stream>> {
    if streams.is_empty() {
        return Err(StreamError::ZipEmpty(0).into());
    }
    merge(py, Merge::Zip, &streams)
}

/// The positions among `inputs` of the streams `emit_on` names: a stream or
/// a list of streams, each one of the inputs. None names no stream: emit on
/// every input.
fn emit_on_positions(
    py: Python,
    emit_on: Option<Py<PyAny>>,
    inputs: &[*mut pyo3::ffi::PyObject],
) -> PyResult<Vec<usize>> {
    let Some(emit_on) = emit_on else {
        return Ok(Vec::new());
    };
    let streams: Vec<Py<Stream>> = if let Ok(one) = emit_on.extract::<Py<Stream>>(py) {
        vec![one]
    } else if let Ok(list) = emit_on.extract::<Vec<Py<Stream>>>(py) {
        list
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "emit_on must be a Stream or list of Streams",
        ));
    };
    if streams.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "emit_on is empty: name the inputs that trigger output, or leave it out to emit \
             on every input",
        ));
    }
    streams
        .iter()
        .map(|stream| {
            inputs
                .iter()
                .position(|input| *input == stream.as_ptr())
                .ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(
                        "emit_on names a stream that isn't one of combine_latest()'s inputs",
                    )
                })
        })
        .collect()
}
