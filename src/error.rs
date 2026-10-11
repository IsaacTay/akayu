//! Structured error types for stream operations.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

/// Errors that can occur during stream graph construction and execution.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error(
        "Cannot modify frozen stream '{0}'. Call compile() only after building the complete graph."
    )]
    FrozenStream(String),

    #[error("Emit called on non-source node '{0}'. Data can only be emitted into source nodes.")]
    EmitOnNonSource(String),

    #[error("Stream '{0}' is already compiled")]
    AlreadyCompiled(String),

    #[error("prefetch size must be at least 1, got {0}")]
    InvalidPrefetchSize(i64),

    #[error("union() requires at least one stream, got {0}")]
    UnionEmpty(usize),

    #[error("combine_latest() requires at least two streams, got {0}")]
    CombineLatestEmpty(usize),

    #[error("zip() requires at least two streams, got {0}")]
    ZipTooFew(usize),

    #[error(
        "Re-entrant flush detected on node '{0}'. \
         A downstream callback is calling flush() on a node that is already being flushed. \
         This creates a cycle in the flush graph — restructure your pipeline so that \
         flush targets are not ancestors of the flushing sink."
    )]
    ReentrantFlush(String),

    #[error(
        "Re-entrant emit detected on node '{0}'. \
         A downstream callback is calling emit() on a node that is already emitting. \
         This creates a cycle in the data graph — restructure your pipeline to break the loop."
    )]
    ReentrantEmit(String),

    #[error(
        "Can't change the graph at a node that is processing an item: a callback is adding \
         to the graph (or compiling it) mid-emit. Build the graph before emitting."
    )]
    BusyNode,

    #[error(
        "flush()/drain() reached a node that is still processing an item (a callback \
         flushed mid-emit, or a profile was taken mid-emit). Call it after emit() returns."
    )]
    BusyFlush,

    #[error(
        "Re-entrant flush: flush() or drain() was called, from a callback, on a node this \
         thread is already flushing. Restructure the pipeline so that flush targets are not \
         ancestors of the flushing sink."
    )]
    ReentrantFlushCall,

    #[error(
        "An item re-entered a node that is still processing another: a callback emitted \
         into a source whose graph leads back to it. Restructure the pipeline to break the loop."
    )]
    Reentered,

    #[error(
        "akayu module not properly initialized. \
         This is an internal error — please report this issue at https://github.com/user/akayu/issues"
    )]
    NotInitialized,
}

impl From<StreamError> for PyErr {
    fn from(err: StreamError) -> Self {
        match &err {
            StreamError::InvalidPrefetchSize(_)
            | StreamError::UnionEmpty(_)
            | StreamError::CombineLatestEmpty(_)
            | StreamError::ZipTooFew(_) => PyValueError::new_err(err.to_string()),
            _ => PyRuntimeError::new_err(err.to_string()),
        }
    }
}
