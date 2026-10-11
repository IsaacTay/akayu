//! # akayu - High-performance reactive streams for Python

// Allow clippy warnings that conflict with PyO3 patterns
#![allow(clippy::ref_option)] // PyO3 signatures use &Option<T>
#![allow(clippy::needless_pass_by_value)] // PyO3 requires owned values
#![allow(clippy::too_many_lines)] // Complex stream methods need length
#![allow(clippy::doc_markdown)] // Python docstrings use Python conventions, not Rust
#![allow(clippy::missing_errors_doc)] // Errors documented in Python docstrings
#![allow(clippy::missing_panics_doc)] // Internal panics on invariant violations
#![allow(clippy::must_use_candidate)] // PyO3 handles return value usage

use pyo3::prelude::*;

// Module declarations
mod builtin;
mod call;
mod combinators;
mod error;
mod execution;
mod gc;
mod helpers;
mod lock;
mod node;
mod numeric;
mod operations;
mod optimization;
mod pool;
mod prefetch;
mod profiling;
mod stack;

// Re-exports
pub use combinators::{combine_latest, union, zip};
pub use helpers::to_text_file;
pub use node::{BuiltinOp, NodeLogic};

/// Most nodes have one parent and one downstream, so one of each is stored
/// inline: no heap allocation per edge.
pub(crate) type Downstreams = smallvec::SmallVec<[lock::Downstream; 1]>;
pub(crate) type Parents = smallvec::SmallVec<[Py<Stream>; 1]>;

/// A raw pointer to a downstream `Stream`, cached at freeze time to bypass
/// PyCell's atomic borrow checks in the propagation hot path.
///
/// # Safety
/// Valid only while the parent `Py<Stream>` in `downstreams` keeps the Python object alive.
/// Must only be dereferenced while holding the GIL, with no other `&mut` to the same `Stream`.
/// These invariants hold because: the graph is a DAG (no cycles), processing is sequential
/// (one subtree completes before the next starts), and topology is frozen after compile().
#[derive(Clone, Copy)]
pub(crate) struct StreamPtr(std::ptr::NonNull<Stream>);
unsafe impl Send for StreamPtr {}
unsafe impl Sync for StreamPtr {}

impl StreamPtr {
    pub(crate) fn new(stream: &mut Stream) -> Self {
        Self(std::ptr::NonNull::from(stream))
    }

    #[inline(always)]
    pub(crate) unsafe fn get_mut(self) -> &'static mut Stream {
        unsafe { &mut *self.0.as_ptr() }
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
#[pyclass(subclass, weakref)]
#[allow(clippy::struct_excessive_bools)] // These flags are intentionally separate for clarity
pub struct Stream {
    pub(crate) logic: NodeLogic,
    pub(crate) downstreams: Downstreams,
    pub(crate) parents: std::sync::Mutex<Parents>,
    pub(crate) name: std::sync::Arc<str>,
    /// True if topology has been frozen/optimized on first emit
    pub(crate) frozen: bool,
    /// True if this node needs thread-safe locking (convergence point from parallel branches)
    pub(crate) needs_lock: bool,
    /// True if compile() was explicitly called (prevents modifications)
    pub(crate) compiled: bool,
    /// Source location where this node was created (e.g., "pipeline.py:42")
    pub(crate) source_location: std::sync::Arc<str>,
    /// Optional builtin fast-path hint. When set, execution bypasses Python
    /// function dispatch for known builtins. Cleared by fusion.
    pub(crate) builtin_op: Option<std::sync::Arc<BuiltinOp>>,
    /// Cached vectorcall function pointer, extracted at compile time.
    /// Bypasses per-call type flag check + offset dereference in the hot loop.
    pub(crate) cached_vectorcall: Option<pyo3::ffi::vectorcallfunc>,
    /// Set at freeze time. True if update() should route through apply_logic
    /// (non-filter chainable: Map/Flatten/Accumulate/Sink/BatchMap, or compiled Chain).
    pub(crate) compiled_chainable: bool,
    /// Set at freeze time. True if this is a Filter whose predicate can skip the
    /// async check (has builtin_op), so update() routes through apply_logic.
    pub(crate) compiled_filter_fast: bool,
    /// Set by compile() on a branch point whose branches it proved can't
    /// observe each other: emit_batch hands each branch the whole batch.
    pub(crate) batch_fanout: bool,
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
    /// Set while the compiled fast path updates this node (see
    /// `update_compiled`), when `guard_reentry`.
    pub(crate) busy: bool,
    /// Set by compile() when more than one source feeds this node's graph:
    /// only then can an item come back into a node mid-update.
    pub(crate) guard_reentry: bool,
    /// Serializes emit()/flush() on this node and updates through `needs_lock` edges.
    pub(crate) lock: std::sync::Arc<lock::NodeLock>,
}

const HELPERS: &str = include_str!("akayu.py");

#[pymodule]
fn akayu(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Stream>()?;
    m.add_class::<profiling::ProfileReport>()?;
    m.add_function(wrap_pyfunction!(to_text_file, m)?)?;
    m.add_function(wrap_pyfunction!(union, m)?)?;
    m.add_function(wrap_pyfunction!(combine_latest, m)?)?;
    m.add_function(wrap_pyfunction!(zip, m)?)?;

    // Initialize Python helpers
    helpers::init_helpers(py, HELPERS)?;

    // At exit, let running pool tasks finish (ThreadPoolExecutor did).
    let threading = py.import("threading")?;
    if let Some(register) = threading.getattr_opt("_register_atexit")? {
        register.call1((wrap_pyfunction!(pool::_shutdown_pool, m)?,))?;
    }

    // A forked child has none of the parent's pool threads: start a new pool.
    let os = py.import("os")?;
    if let Some(register) = os.getattr_opt("register_at_fork")? {
        let reset = wrap_pyfunction!(pool::_reset_pool_after_fork, m)?;
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("after_in_child", reset)?;
        register.call((), Some(&kwargs))?;
    }

    Ok(())
}
