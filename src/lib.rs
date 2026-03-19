//! # akayu - High-performance reactive streams for Python

// Allow clippy warnings that conflict with PyO3 patterns
#![allow(clippy::ref_option)] // PyO3 signatures use &Option<T>
#![allow(clippy::needless_pass_by_value)] // PyO3 requires owned values
#![allow(clippy::too_many_lines)] // Complex stream methods need length
#![allow(clippy::doc_markdown)] // Python docstrings use Python conventions, not Rust
#![allow(clippy::missing_errors_doc)] // Errors documented in Python docstrings
#![allow(clippy::missing_panics_doc)] // Internal panics on invariant violations
#![allow(clippy::must_use_candidate)] // PyO3 handles return value usage
#![allow(dead_code)] // Optimizations temporarily disabled
#![allow(unused_variables)] // Optimizations temporarily disabled

use pyo3::prelude::*;

// Module declarations
mod combinators;
mod error;
mod execution;
mod helpers;
mod node;
mod operations;
mod optimization;
mod profiling;

// Re-exports
pub use combinators::{combine_latest, union, zip};
pub use helpers::{from_text_file, to_text_file};
pub use node::{BuiltinOp, NodeLogic};

/// A raw pointer to a downstream `Stream`, cached at freeze time to bypass
/// PyCell's atomic borrow checks in the propagation hot path.
///
/// # Safety
/// Valid only while the parent `Py<Stream>` in `downstreams` keeps the Python object alive.
/// Must only be dereferenced while holding the GIL, with no other `&mut` to the same `Stream`.
/// These invariants hold because: the graph is a DAG (no cycles), processing is sequential
/// (one subtree completes before the next starts), and topology is frozen after compile().
#[derive(Clone, Copy)]
pub(crate) struct StreamPtr(*mut Stream);
unsafe impl Send for StreamPtr {}
unsafe impl Sync for StreamPtr {}

impl StreamPtr {
    #[inline(always)]
    pub(crate) unsafe fn get_mut(self) -> &'static mut Stream {
        unsafe { &mut *self.0 }
    }
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
pub struct Stream {
    pub(crate) logic: NodeLogic,
    pub(crate) downstreams: Vec<Py<Stream>>,
    pub(crate) name: String,
    pub(crate) asynchronous: Option<bool>,
    /// Pre-computed flag: true if async check can be skipped (asynchronous == Some(false))
    pub(crate) skip_async_check: bool,
    /// True if this node's function is definitely synchronous (not a coroutine function)
    pub(crate) func_is_sync: bool,
    /// True if topology has been frozen/optimized on first emit
    pub(crate) frozen: bool,
    /// True if this node needs thread-safe locking (convergence point from parallel branches)
    pub(crate) needs_lock: bool,
    /// True if compile() was explicitly called (prevents modifications)
    pub(crate) compiled: bool,
    /// Source location where this node was created (e.g., "pipeline.py:42")
    pub(crate) source_location: String,
    /// Optional builtin fast-path hint. When set, execution bypasses Python
    /// function dispatch for known builtins. Cleared by fusion.
    pub(crate) builtin_op: Option<BuiltinOp>,
    /// Cached vectorcall function pointer, extracted at compile time.
    /// Bypasses per-call type flag check + offset dereference in the hot loop.
    pub(crate) cached_vectorcall: Option<pyo3::ffi::vectorcallfunc>,
    /// Set at freeze time. True if update() should route through apply_logic
    /// (non-filter chainable: Map/Flatten/Accumulate/Sink/BatchMap, or compiled Chain).
    pub(crate) compiled_chainable: bool,
    /// Set at freeze time. True if this is a Filter whose predicate can skip the
    /// async check (has builtin_op or skip_async_check), so update() routes through apply_logic.
    pub(crate) compiled_filter_fast: bool,
    /// Cached raw pointers to downstream `Stream` objects, set at freeze time.
    /// Bypasses PyCell's atomic borrow check (~10ns/downstream/item) in propagate().
    /// Empty before freeze — propagate falls back to normal `borrow_mut()`.
    pub(crate) compiled_downstream_ptrs: Vec<StreamPtr>,
    /// If this Sink node's function calls .flush() on a Collect node in the same graph,
    /// store a direct reference to skip Python dispatch at runtime.
    /// Detected at compile time via bound method check or lambda bytecode analysis.
    pub(crate) flush_target: Option<Py<Stream>>,
    /// If this Sink node's function calls .emit() on a Source node in the same graph,
    /// store a direct reference to skip Python dispatch at runtime.
    /// Detected at compile time via bound method check or lambda bytecode analysis.
    pub(crate) emit_target: Option<Py<Stream>>,
    /// Guard against re-entrant flush/drain calls. True while this node is
    /// actively being flushed/drained. Prevents cycles and undefined behavior
    /// in the compiled raw-pointer path.
    pub(crate) flushing: bool,
    /// Guard against re-entrant emit calls. True while this node is
    /// actively propagating from an emit(). Prevents cycles in the data graph.
    pub(crate) emitting: std::sync::atomic::AtomicBool,
}

const HELPERS: &str = include_str!("akayu.py");

#[pymodule]
fn akayu(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Stream>()?;
    m.add_class::<profiling::ProfileReport>()?;
    m.add_function(wrap_pyfunction!(to_text_file, m)?)?;
    m.add_function(wrap_pyfunction!(from_text_file, m)?)?;
    m.add_function(wrap_pyfunction!(union, m)?)?;
    m.add_function(wrap_pyfunction!(combine_latest, m)?)?;
    m.add_function(wrap_pyfunction!(zip, m)?)?;

    // Initialize Python helpers
    helpers::init_helpers(py, HELPERS)?;

    Ok(())
}
