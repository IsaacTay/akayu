//! Structured error types for stream operations.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// The exception classes `akayu` exports. Every error about a graph's state
/// is a `StreamError`, itself a `RuntimeError`, so `except RuntimeError` code
/// keeps working; invalid arguments stay `ValueError`/`TypeError`.
pub(crate) mod exc {
    use pyo3::exceptions::PyRuntimeError;

    pyo3::create_exception!(
        akayu,
        StreamError,
        PyRuntimeError,
        "Base class of the errors akayu raises about a stream graph.\n\n\
         A subclass of RuntimeError. Invalid arguments raise ValueError or\n\
         TypeError instead."
    );
    pyo3::create_exception!(
        akayu,
        CallbackError,
        StreamError,
        "A function given to a node (map, filter, sink, ...) raised.\n\n\
         The function's exception is `__cause__`. `node` is the node's name\n\
         and `location` the file and line that built it."
    );
    pyo3::create_exception!(
        akayu,
        FrozenGraphError,
        StreamError,
        "The graph was changed after compile() froze it, or compiled twice."
    );
    pyo3::create_exception!(
        akayu,
        NotASourceError,
        StreamError,
        "emit(), emit_batch() or profiling was used on a node that isn't a source."
    );
    pyo3::create_exception!(
        akayu,
        ReentrantError,
        StreamError,
        "A callback re-entered the graph it runs in.\n\n\
         It emitted into a source its own item came from, or flushed a node\n\
         that is mid-flush: the graph has a loop. Restructure the pipeline."
    );
    pyo3::create_exception!(
        akayu,
        BusyError,
        StreamError,
        "A node was changed or flushed while it was processing an item.\n\n\
         Build the graph before emitting, and flush after emit() returns."
    );
    pyo3::create_exception!(
        akayu,
        DeadlockError,
        StreamError,
        "Waiting would never end: whoever holds what this thread needs is\n\
         waiting for this thread."
    );
}

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

    #[error("prefetch size must be at least 1, and fit in a machine word; got {0}")]
    InvalidPrefetchSize(String),

    #[error("union() requires at least one stream, got {0}")]
    UnionEmpty(usize),

    #[error("combine_latest() requires at least one stream, got {0}")]
    CombineLatestEmpty(usize),

    #[error("zip() requires at least one stream, got {0}")]
    ZipEmpty(usize),

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
         to the graph (or compiling it) mid-emit, or another thread is emitting through it. \
         Build the graph before emitting."
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
        "emit() was called on '{0}' from a callback of its own flush() or drain(). \
         Emit after flush() returns."
    )]
    EmitDuringFlush(String),

    #[error(
        "emit() found its source busy: it is processing an item (a callback emitted into a \
         source its own graph leads back to), or another thread is changing or compiling the \
         graph."
    )]
    BusySource,

    #[error(
        "Waiting here would never end: the thread holding what this thread needs waits, \
         directly or through other threads, for this one. A prefetch() or par() call that emits \
         into a graph its caller is emitting through, or threads whose callbacks emit into each \
         other's graphs, do this. Emit after the other emit returns, or pass items between the \
         threads through a queue."
    )]
    WaitCycle,

    #[error(
        "An item reached '{0}', which compile() folded into a chain of steps: emit into \
         the compiled graph's source instead"
    )]
    ChainMemberReached(String),

    #[error(
        "akayu module not properly initialized. \
         This is an internal error — please report this issue at https://github.com/IsaacTay/akayu/issues"
    )]
    NotInitialized,
}

impl From<StreamError> for PyErr {
    fn from(err: StreamError) -> Self {
        let msg = err.to_string();
        match err {
            StreamError::InvalidPrefetchSize(_)
            | StreamError::UnionEmpty(_)
            | StreamError::CombineLatestEmpty(_)
            | StreamError::ZipEmpty(_) => PyValueError::new_err(msg),
            StreamError::FrozenStream(_) | StreamError::AlreadyCompiled(_) => {
                exc::FrozenGraphError::new_err(msg)
            }
            StreamError::EmitOnNonSource(_) | StreamError::ChainMemberReached(_) => {
                exc::NotASourceError::new_err(msg)
            }
            StreamError::ReentrantFlush(_)
            | StreamError::ReentrantEmit(_)
            | StreamError::ReentrantFlushCall
            | StreamError::Reentered
            | StreamError::EmitDuringFlush(_) => exc::ReentrantError::new_err(msg),
            StreamError::BusyNode | StreamError::BusyFlush | StreamError::BusySource => {
                exc::BusyError::new_err(msg)
            }
            StreamError::WaitCycle => exc::DeadlockError::new_err(msg),
            StreamError::NotInitialized => exc::StreamError::new_err(msg),
        }
    }
}

/// Add the exception classes to the native module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("StreamError", py.get_type::<exc::StreamError>())?;
    m.add("CallbackError", py.get_type::<exc::CallbackError>())?;
    m.add("FrozenGraphError", py.get_type::<exc::FrozenGraphError>())?;
    m.add("NotASourceError", py.get_type::<exc::NotASourceError>())?;
    m.add("ReentrantError", py.get_type::<exc::ReentrantError>())?;
    m.add("BusyError", py.get_type::<exc::BusyError>())?;
    m.add("DeadlockError", py.get_type::<exc::DeadlockError>())?;
    Ok(())
}
