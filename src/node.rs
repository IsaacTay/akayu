//! Node logic variants for stream processing.

use pyo3::prelude::*;
use std::collections::VecDeque;

/// Defines the type of operation a stream node performs.
pub enum NodeLogic {
    Source,
    Map {
        func: Py<PyAny>,
    },
    Starmap {
        func: Py<PyAny>,
    },
    Filter {
        predicate: Py<PyAny>,
    },
    Flatten,
    Collect {
        buffer: Vec<Py<PyAny>>,
    },
    Sink {
        func: Py<PyAny>,
    },
    Accumulate {
        func: Py<PyAny>,
        state: Py<PyAny>,
        returns_state: bool,
    },
    Tag {
        index: usize,
    },
    CombineLatest {
        state: Vec<Option<Py<PyAny>>>,
        emit_on_indices: Vec<usize>, // Empty = emit on any, non-empty = only these indices
    },
    Zip {
        buffers: Vec<VecDeque<Py<PyAny>>>,
    },
    BatchMap {
        func: Py<PyAny>,
    },
    /// Parallel node: propagates to downstreams in parallel threads
    Parallel,
    /// Prefetch marker: signals that the next map should prefetch items
    Prefetch {
        size: usize,
    },
    /// Prefetch map: processes items concurrently while preserving order
    PrefetchMap {
        #[allow(dead_code)] // Stored for debugging/inspection
        func: Py<PyAny>,
        #[allow(dead_code)] // Stored for debugging/inspection
        size: usize,
        state: Py<PyAny>, // Python _PrefetchState object
    },
    /// Prefetch filter: filters items concurrently while preserving order
    PrefetchFilter {
        #[allow(dead_code)] // Stored for debugging/inspection
        predicate: Py<PyAny>,
        #[allow(dead_code)] // Stored for debugging/inspection
        size: usize,
        state: Py<PyAny>, // Python _PrefetchState object
    },
}
