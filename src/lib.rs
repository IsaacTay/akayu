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
mod execution;
mod helpers;
mod node;
mod operations;
mod optimization;

// Re-exports
pub use combinators::{combine_latest, union, zip};
pub use helpers::{from_text_file, to_text_file};
pub use node::NodeLogic;

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
}

const HELPERS: &str = include_str!("akayu.py");

#[pymodule]
fn akayu(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Stream>()?;
    m.add_function(wrap_pyfunction!(to_text_file, m)?)?;
    m.add_function(wrap_pyfunction!(from_text_file, m)?)?;
    m.add_function(wrap_pyfunction!(union, m)?)?;
    m.add_function(wrap_pyfunction!(combine_latest, m)?)?;
    m.add_function(wrap_pyfunction!(zip, m)?)?;

    // Initialize Python helpers
    helpers::init_helpers(py, HELPERS)?;

    Ok(())
}
