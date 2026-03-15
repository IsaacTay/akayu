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

    #[error("prefetch size must be at least 1")]
    InvalidPrefetchSize,

    #[error("union requires at least one stream")]
    UnionEmpty,

    #[error("combine_latest requires at least one stream")]
    CombineLatestEmpty,

    #[error("zip requires at least two streams")]
    ZipTooFew,

    #[error("akayu not initialized")]
    NotInitialized,
}

impl From<StreamError> for PyErr {
    fn from(err: StreamError) -> Self {
        match &err {
            StreamError::InvalidPrefetchSize
            | StreamError::UnionEmpty
            | StreamError::CombineLatestEmpty
            | StreamError::ZipTooFew => PyValueError::new_err(err.to_string()),
            _ => PyRuntimeError::new_err(err.to_string()),
        }
    }
}
