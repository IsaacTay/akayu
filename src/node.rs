//! Node logic variants for stream processing.

use pyo3::prelude::*;
use std::collections::VecDeque;

/// Defines the type of operation a stream node performs.
#[derive(strum::Display)]
pub enum NodeLogic {
    Source,
    Map {
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
    /// Prefetch batch_map: processes batches concurrently while preserving order
    PrefetchBatchMap {
        #[allow(dead_code)] // Stored for debugging/inspection
        func: Py<PyAny>,
        #[allow(dead_code)] // Stored for debugging/inspection
        size: usize,
        state: Py<PyAny>, // Python _PrefetchBatchMapState object
    },
    /// Fused filter + map: if predicate(x) then yield func(x)
    /// More efficient than separate filter->map because it skips map for filtered items
    FilterMap {
        predicate: Py<PyAny>,
        func: Py<PyAny>,
        /// Number of predicate segments in the fused node name, used to offset
        /// `_akayu_chain_index` from composed map functions so narrowing points
        /// to the correct segment.
        pred_segment_count: usize,
    },
    /// Fused map + sink: sink(func(x))
    /// Eliminates propagation overhead for terminal map->sink chains
    MapSink {
        map_func: Py<PyAny>,
        sink_func: Py<PyAny>,
    },
    /// Fused filter + sink: if predicate(x) then sink(x)
    /// Skips sink call for filtered items
    FilterSink {
        predicate: Py<PyAny>,
        sink_func: Py<PyAny>,
    },
    /// Fused filter + map + sink: if predicate(x) then sink(func(x))
    /// Combines filter, map, and sink into single operation
    FilterMapSink {
        predicate: Py<PyAny>,
        map_func: Py<PyAny>,
        sink_func: Py<PyAny>,
        /// Number of predicate segments in the fused node name, used to offset
        /// `_akayu_chain_index` from composed map functions so narrowing points
        /// to the correct segment.
        pred_segment_count: usize,
    },
    /// Async map: non-blocking emit with buffered results for async functions
    /// When asynchronous=True is set on a map, coroutines are submitted to a background
    /// event loop and results are returned in order when ready or on flush()
    AsyncMap {
        #[allow(dead_code)] // Stored for debugging/inspection
        func: Py<PyAny>,
        state: Py<PyAny>, // Python _AsyncState object
    },
}

/// Hint for builtin function fast-paths.
///
/// Stored on `Stream` alongside the normal `NodeLogic`. When set, the execution
/// engine uses a direct Rust/C-API call instead of Python function dispatch.
/// The regular `func`/`predicate` in `NodeLogic` is still present as fallback
/// (e.g., after fusion clears the hint).
pub enum BuiltinOp {
    /// isinstance(x, type_obj) — uses PyO3's is_instance()
    IsInstance(Py<PyAny>),
    /// len(x) — uses PyO3's len()
    Len,
    /// str(x) — uses PyO3's str()
    Str,
    /// int(x) — uses ffi::PyNumber_Long
    Int,
    /// float(x) — uses ffi::PyNumber_Float
    Float,
    /// bool(x) — uses ffi::PyObject_IsTrue
    Bool,
    /// print(x) — uses the cached builtin print reference
    Print,
}
