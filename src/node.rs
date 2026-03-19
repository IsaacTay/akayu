//! Node logic variants for stream processing.

use pyo3::prelude::*;
use pyo3::types::PyString;
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
    /// Async map: non-blocking emit with buffered results for async functions
    /// When asynchronous=True is set on a map, coroutines are submitted to a background
    /// event loop and results are returned in order when ready or on flush()
    AsyncMap {
        #[allow(dead_code)] // Stored for debugging/inspection
        func: Py<PyAny>,
        state: Py<PyAny>, // Python _AsyncState object
    },
    /// Compiled chain: a linear segment of chainable nodes extracted at compile time.
    /// Eliminates per-node borrow_mut/drop/propagate overhead by processing all steps
    /// in a flat loop via apply_logic().
    Chain {
        steps: Vec<ChainNode>,
    },
    /// Sentinel replacing logic in nodes absorbed into a Chain.
    ChainMember,
}

/// A single step in a compiled chain, holding the extracted NodeLogic
/// and per-node metadata needed for execution and profiling.
pub struct ChainNode {
    pub logic: NodeLogic,
    pub name: String,
    pub source_location: String,
    pub builtin_op: Option<BuiltinOp>,
    pub node_id: usize,
    /// Cached vectorcall function pointer, extracted at compile time.
    /// Avoids per-call type flag check + offset dereference in the hot loop.
    pub cached_vectorcall: Option<pyo3::ffi::vectorcallfunc>,
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
    /// x.method() — zero-arg method call
    MethodCall(Py<PyString>),
    /// x.method(arg) — single-arg method call
    MethodCall1(Py<PyString>, Py<PyAny>),
    /// x.method(arg1, arg2) — two-arg method call
    MethodCall2(Py<PyString>, Py<PyAny>, Py<PyAny>),
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
