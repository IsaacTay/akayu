//! Node logic variants for stream processing.

use pyo3::prelude::*;
use pyo3::types::{PyString, PyType};
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
    /// starmap(func) without extra arguments: func(*item), with no wrapper.
    StarMap {
        func: Py<PyAny>,
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
    CombineLatest(Box<CombineLatestState>),
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
    /// A map, filter or batch_map after prefetch(n): runs its function ahead
    /// on the thread pool while preserving order.
    Prefetched {
        prefetcher: Box<crate::prefetch::Prefetcher>,
    },
    /// Compiled chain: a linear segment of chainable nodes extracted at compile time.
    /// Eliminates per-node borrow_mut/drop/propagate overhead by processing all steps
    /// in a flat loop via apply_logic().
    Chain {
        steps: Vec<ChainNode>,
    },
    /// Sentinel replacing logic in nodes absorbed into a Chain.
    ChainMember {
        /// The member was a Sink, so flushing it directly is not a mistake.
        was_sink: bool,
    },
    /// Buffers items until a predicate matches, then emits the buffer as a tuple.
    /// Fused from `collect() + filter(pred).sink(flush)` patterns at compile time,
    /// or created directly via `chunk_by(pred)`.
    ChunkBy(Box<ChunkByState>),
    /// Extracts items between two delimiter predicates.
    /// State machine: Outside state waits for `open` predicate, Inside state
    /// buffers items until `close` predicate, then emits buffer as a tuple.
    /// Flush discards an incomplete chunk and returns to the Outside state.
    ExtractBetween(Box<ExtractBetweenState>),
}

// Rarely used variants keep their state behind a Box, so NodeLogic (inline in
// every node) is 32 bytes instead of 64.

pub struct CombineLatestState {
    pub state: Vec<Option<Py<PyAny>>>,
    /// Empty = emit on any input, non-empty = only these inputs.
    pub emit_on_indices: Vec<usize>,
}

/// Buffers items until a predicate matches, then emits the buffer as a tuple.
/// Fused from `collect() + filter(pred).sink(flush)` patterns at compile time,
/// or created directly via `chunk_by(pred)`.
pub struct ChunkByState {
    pub buffer: Vec<Py<PyAny>>,
    pub predicate: Py<PyAny>,
    pub order: ChunkOrder,
    /// Transforms applied to each item before buffering.
    /// Extracted from nodes between branch point and the original Collect.
    /// Empty when Collect was a direct downstream of the branch point.
    pub pre_buffer_chain: Vec<ChainNode>,
}

/// When a chunk_by node buffers an item relative to testing it.
#[derive(Clone, Copy)]
pub enum ChunkOrder {
    /// `chunk_by()`: test, then buffer. A delimiter ends the chunk (kept in
    /// it with include_delimiter), empty chunks are not emitted, and an item
    /// whose test raises is not buffered.
    Operator { include_delimiter: bool },
    /// compile()'s fusion of `collect()` plus `filter(p).sink(collect.flush)`,
    /// keeping the graph's branch order: with the collect branch first an
    /// item is buffered, then tested (a delimiter ends its chunk); with the
    /// filter first it is tested, the chunk goes downstream, then it is
    /// buffered (a delimiter starts the next chunk). Like collect's flush,
    /// every delimiter and flush() emits, even an empty tuple.
    Fused { buffer_first: bool },
}

/// Extracts items between two delimiter predicates.
/// State machine: Outside state waits for `open` predicate, Inside state
/// buffers items until `close` predicate, then emits buffer as a tuple.
/// Flush discards an incomplete chunk and returns to the Outside state.
pub struct ExtractBetweenState {
    pub buffer: Vec<Py<PyAny>>,
    pub open: Py<PyAny>,
    pub close: Py<PyAny>,
    pub inside: bool,
    /// Vectorcall pointer for `close`. The node's `cached_vectorcall`
    /// belongs to `open`, and the two may be different callable types.
    pub close_vectorcall: Option<pyo3::ffi::vectorcallfunc>,
}

/// A single step in a compiled chain, holding the extracted NodeLogic
/// and per-node metadata needed for execution and profiling.
/// An uncompiled node's native op (see `builtin::eager_op`): used while its
/// function still runs the code it was made from (IPython's autoreload, for
/// one, replaces a function's code in place); otherwise the function is
/// called.
pub struct EagerOp {
    pub func: Py<PyAny>,
    pub code: Py<PyAny>,
    pub op: std::sync::Arc<BuiltinOp>,
}

impl EagerOp {
    /// Whether the function still runs the code the op was made from.
    #[inline]
    pub fn current(&self) -> bool {
        // SAFETY: GIL held by every caller; an EagerOp is only made for a
        // plain function, and a function's type can't change.
        let code = unsafe { pyo3::ffi::PyFunction_GetCode(self.func.as_ptr()) };
        std::ptr::eq(code, self.code.as_ptr())
    }
}

/// `x.name(*args)` with constant arguments (at most two).
pub struct MethodCall {
    pub name: Py<PyString>,
    pub args: Box<[Py<PyAny>]>,
    /// The method on each builtin type that defines it (str, dict, ...),
    /// resolved by compile(). Builtin types can't be changed, so an item of
    /// exactly one of them calls its C method directly, without a lookup.
    pub direct: Box<[(Py<PyType>, Py<PyAny>)]>,
}

pub struct ChainNode {
    pub logic: NodeLogic,
    pub name: std::sync::Arc<str>,
    pub source_location: std::sync::Arc<str>,
    pub builtin_op: Option<std::sync::Arc<BuiltinOp>>,
    pub node_id: usize,
    /// Cached vectorcall function pointer, extracted at compile time.
    /// Avoids per-call type flag check + offset dereference in the hot loop.
    pub cached_vectorcall: Option<pyo3::ffi::vectorcallfunc>,
    /// If this is a Sink that calls .flush() on a Collect node, store the target
    /// for direct Rust dispatch (bypasses Python vectorcall).
    pub flush_target: Option<Py<crate::Stream>>,
    /// If this is a Sink that calls .emit() on a Source node, store the target
    /// for direct Rust dispatch (bypasses Python vectorcall).
    pub emit_target: Option<Py<crate::Stream>>,
    /// Set by compile() on the first step of a run of numeric steps, which
    /// then execute together on unboxed values (see `numeric`).
    pub numeric_run: Option<std::sync::Arc<crate::numeric::NumRun>>,
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
    /// operator.not_(x) — uses ffi::PyObject_IsTrue + invert
    Not,
    /// operator.is_(x, sentinel) — pointer comparison (e.g., `x is None`)
    Is(Py<PyAny>),
    /// operator.is_not(x, sentinel) — pointer comparison + invert
    IsNot(Py<PyAny>),
    /// operator.truth(x) — same as bool(x)
    Truth,
    /// operator.itemgetter(key)(x) — uses x.get_item(key)
    ItemGetter(Py<PyAny>),
    /// operator.attrgetter(name)(x) — uses x.getattr(name)
    AttrGetter(Py<PyString>),
    /// print(x) — uses the cached builtin print reference
    Print,
    /// type(x) — returns the type object
    Type,
    /// hasattr(x, name) — uses ffi::PyObject_HasAttr
    HasAttr(Py<PyString>),
    /// tuple(x) — converts iterable to tuple via ffi::PySequence_Tuple
    Tuple,
    /// list(x) — converts iterable to list via ffi::PySequence_List
    List,
    /// set(x) — converts iterable to set via ffi::PySet_New
    Set,
    /// frozenset(x) — converts iterable to frozenset via ffi::PyFrozenSet_New
    FrozenSet,

    // --- Phase 2: Lambda-decompiled variants ---
    /// x <binop> rhs — uses PyNumber_* FFI calls
    BinOp(BinaryOp, Py<PyAny>),
    /// x <cmpop> rhs — uses PyObject_RichCompareBool
    Compare(CompareOp, Py<PyAny>),
    /// x in collection — uses PySequence_Contains
    Contains(Py<PyAny>),
    /// x not in collection — uses PySequence_Contains + invert
    NotContains(Py<PyAny>),
    /// x.method(), x.method(arg), x.method(arg1, arg2)
    MethodCall(Box<MethodCall>),
    /// An uncompiled node's op, guarded by its function's code (`EagerOp`)
    Eager(EagerOp),
    /// x[start:stop] — uses PySlice_New + PyObject_GetItem
    Slice(Py<PyAny>, Py<PyAny>),
    /// (op1(x), op2(x), ...) — apply each op to x, collect into tuple
    TupleConstruct(Vec<BuiltinOp>),
    /// [op1(x), op2(x), ...] — apply each op to x, collect into list
    ListConstruct(Vec<BuiltinOp>),
    /// Apply first op to x, then apply second op to result
    Chain(Box<BuiltinOp>, Box<BuiltinOp>),
    /// not <expr>
    Negate(Box<BuiltinOp>),
    /// <A> and <B> — short-circuit evaluation
    And(Box<BuiltinOp>, Box<BuiltinOp>),
    /// <A> or <B> — short-circuit evaluation
    Or(Box<BuiltinOp>, Box<BuiltinOp>),

    // --- accumulate(func) patterns, applied to (state, x) ---
    /// state <binop> x
    AccumBinOp(BinaryOp),
    /// state <binop> const
    AccumBinOpConst(BinaryOp, Py<PyAny>),
    /// max(state, x)
    AccumMax,
    /// min(state, x)
    AccumMin,

    /// Arithmetic/comparison chain with an unboxed fast path for exact ints
    /// and floats; the boxed op is the fallback (see `numeric`).
    Numeric(Box<crate::numeric::NumProgram>, Box<BuiltinOp>),

    // --- sink(func) patterns ---
    /// sink(lst.append) on an exact list: PyList_Append, no method call
    ListAppend(Py<PyAny>),
}

/// Binary arithmetic operations for `BuiltinOp::BinOp`.
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    TrueDiv,
    FloorDiv,
    Mod,
    Pow,
}

/// Comparison operations for `BuiltinOp::Compare`.
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

impl NodeLogic {
    /// Returns true if this logic variant can be absorbed into a compiled Chain.
    pub fn is_chainable(&self) -> bool {
        matches!(
            self,
            NodeLogic::Map { .. }
                | NodeLogic::StarMap { .. }
                | NodeLogic::Filter { .. }
                | NodeLogic::Flatten
                | NodeLogic::Accumulate { .. }
                | NodeLogic::Sink { .. }
                | NodeLogic::BatchMap { .. }
        )
    }
}

impl CompareOp {
    /// Convert to the C-API `Py_CompareOp` constant for `PyObject_RichCompareBool`.
    pub fn to_ffi(&self) -> std::ffi::c_int {
        match self {
            Self::Lt => pyo3::ffi::Py_LT,
            Self::Le => pyo3::ffi::Py_LE,
            Self::Eq => pyo3::ffi::Py_EQ,
            Self::Ne => pyo3::ffi::Py_NE,
            Self::Gt => pyo3::ffi::Py_GT,
            Self::Ge => pyo3::ffi::Py_GE,
        }
    }
}
