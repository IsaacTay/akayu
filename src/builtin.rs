//! Builtin callable analysis and fast-path detection.

use pyo3::prelude::*;
use pyo3::types::{
    PyBytes, PyDict, PyFloat, PyFrozenSet, PyInt, PyList, PySet, PyString, PyTuple, PyType,
    PyWeakrefMethods, PyWeakrefReference,
};
use std::sync::Arc;

use crate::Stream;
use crate::arrays::{ArrayOp, Fallback, Program};
use crate::helpers::{
    InternTable, get_analyze_for_compile, get_analyze_star, get_builtin_abs, get_builtin_bool,
    get_builtin_float, get_builtin_frozenset, get_builtin_int, get_builtin_isinstance,
    get_builtin_len, get_builtin_list, get_builtin_print, get_builtin_set, get_builtin_str,
    get_builtin_tuple, get_builtin_type, get_operator_attrgetter, get_operator_is,
    get_operator_is_not, get_operator_itemgetter, get_operator_not, get_operator_truth,
    get_why_python,
};
use crate::node::{BinaryOp, BuiltinOp, CompareOp, EagerOp, MethodCall, NodeLogic, UnaryOp};

/// Parse a comparison op string from the Python analyzer into a `CompareOp`.
fn parse_compare_op(s: &str) -> Option<CompareOp> {
    match s {
        "==" => Some(CompareOp::Eq),
        "!=" => Some(CompareOp::Ne),
        "<" => Some(CompareOp::Lt),
        ">" => Some(CompareOp::Gt),
        "<=" => Some(CompareOp::Le),
        ">=" => Some(CompareOp::Ge),
        _ => None,
    }
}

/// Parse a binary op string from the Python analyzer into a `BinaryOp`.
fn parse_binary_op(s: &str) -> Option<BinaryOp> {
    match s {
        "+" => Some(BinaryOp::Add),
        "-" => Some(BinaryOp::Sub),
        "*" => Some(BinaryOp::Mul),
        "/" => Some(BinaryOp::TrueDiv),
        "//" => Some(BinaryOp::FloorDiv),
        "%" => Some(BinaryOp::Mod),
        "**" => Some(BinaryOp::Pow),
        "<<" => Some(BinaryOp::LShift),
        ">>" => Some(BinaryOp::RShift),
        "&" => Some(BinaryOp::And),
        "|" => Some(BinaryOp::Or),
        "^" => Some(BinaryOp::Xor),
        _ => None,
    }
}

/// Parse a unary op from the Python analyzer into a `UnaryOp`.
fn parse_unary_op(s: &str) -> Option<UnaryOp> {
    match s {
        "-" => Some(UnaryOp::Neg),
        "+" => Some(UnaryOp::Pos),
        "~" => Some(UnaryOp::Invert),
        "abs" => Some(UnaryOp::Abs),
        _ => None,
    }
}

/// Methods `x.name(...)` that leave `x` and everything else as they were on
/// every builtin type that has them (`MethodCall::pure`). They may still run
/// the item's own dunder methods, as every native op may.
const PURE_METHODS: &[&str] = &[
    "capitalize",
    "casefold",
    "center",
    "count",
    "decode",
    "encode",
    "endswith",
    "expandtabs",
    "find",
    "format",
    "get",
    "hex",
    "index",
    "isalnum",
    "isalpha",
    "isascii",
    "isdecimal",
    "isdigit",
    "isidentifier",
    "islower",
    "isnumeric",
    "isprintable",
    "isspace",
    "istitle",
    "isupper",
    "is_integer",
    "items",
    "join",
    "keys",
    "ljust",
    "lower",
    "lstrip",
    "partition",
    "removeprefix",
    "removesuffix",
    "replace",
    "rfind",
    "rindex",
    "rjust",
    "rpartition",
    "rsplit",
    "rstrip",
    "split",
    "splitlines",
    "startswith",
    "strip",
    "swapcase",
    "title",
    "translate",
    "upper",
    "values",
    "zfill",
];

/// The C methods named `name` that builtin types define themselves (see
/// `MethodCall::direct`), most common receivers first.
fn builtin_methods(py: Python, name: &Bound<'_, PyString>) -> Box<[(Py<PyType>, Py<PyAny>)]> {
    let types = [
        py.get_type::<PyString>(),
        py.get_type::<PyDict>(),
        py.get_type::<PyBytes>(),
        py.get_type::<PyList>(),
        py.get_type::<PyTuple>(),
        py.get_type::<PySet>(),
        py.get_type::<PyFrozenSet>(),
        py.get_type::<PyInt>(),
        py.get_type::<PyFloat>(),
    ];
    types
        .into_iter()
        .filter_map(|tp| {
            // The type's own namespace: inherited methods take the lookup.
            let method = tp
                .getattr(pyo3::intern!(py, "__dict__"))
                .ok()?
                .get_item(name)
                .ok()?;
            // SAFETY: GIL held; a type check on a live object.
            let is_c_method = unsafe {
                std::ptr::eq(
                    pyo3::ffi::Py_TYPE(method.as_ptr()),
                    &raw mut pyo3::ffi::PyMethodDescr_Type,
                )
            };
            is_c_method.then(|| (tp.unbind(), method.unbind()))
        })
        .collect()
}

/// Recursively parse the tuple returned by `_analyze_lambda()` into a `BuiltinOp`.
///
/// The Python analyzer returns nested tuples like `("chain", ("attrgetter", "name"), ("compare", "==", "John"))`.
#[allow(clippy::only_used_in_recursion)]
fn parse_analysis_tuple(py: Python, obj: &Bound<'_, PyAny>) -> Option<BuiltinOp> {
    let tuple = obj.cast::<PyTuple>().ok()?;
    let tag: String = tuple.get_item(0).ok()?.extract().ok()?;

    match tag.as_str() {
        "len" => Some(BuiltinOp::Len),
        "str" => Some(BuiltinOp::Str),
        "int" => Some(BuiltinOp::Int),
        "float" => Some(BuiltinOp::Float),
        "bool" => Some(BuiltinOp::Bool),
        "not" => Some(BuiltinOp::Not),
        "itemgetter" => {
            let key = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::ItemGetter(key))
        }
        "attrgetter" => {
            let name_str: Py<PyString> = tuple.get_item(1).ok()?.extract().ok()?;
            Some(BuiltinOp::AttrGetter(name_str))
        }
        "isinstance" => {
            let type_obj = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::IsInstance(type_obj))
        }
        "is" => {
            let val = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::Is(val))
        }
        "is_not" => {
            let val = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::IsNot(val))
        }
        "compare" => {
            let op_str: String = tuple.get_item(1).ok()?.extract().ok()?;
            let cmp_op = parse_compare_op(&op_str)?;
            let rhs = tuple.get_item(2).ok()?.unbind();
            Some(BuiltinOp::Compare(cmp_op, rhs))
        }
        "binop" => {
            let op_str: String = tuple.get_item(1).ok()?.extract().ok()?;
            let bin_op = parse_binary_op(&op_str)?;
            let rhs = tuple.get_item(2).ok()?.unbind();
            Some(BuiltinOp::BinOp(bin_op, rhs))
        }
        "rcompare" => {
            let op_str: String = tuple.get_item(1).ok()?.extract().ok()?;
            let cmp_op = parse_compare_op(&op_str)?;
            let lhs = tuple.get_item(2).ok()?.unbind();
            Some(BuiltinOp::RCompare(cmp_op, lhs))
        }
        "rbinop" => {
            let op_str: String = tuple.get_item(1).ok()?.extract().ok()?;
            let bin_op = parse_binary_op(&op_str)?;
            let lhs = tuple.get_item(2).ok()?.unbind();
            Some(BuiltinOp::RBinOp(bin_op, lhs))
        }
        "binop2" | "compare2" => {
            let op_str: String = tuple.get_item(1).ok()?.extract().ok()?;
            let left = Box::new(parse_analysis_tuple(py, &tuple.get_item(2).ok()?)?);
            let right = Box::new(parse_analysis_tuple(py, &tuple.get_item(3).ok()?)?);
            if tag == "binop2" {
                Some(BuiltinOp::BinOpOf(parse_binary_op(&op_str)?, left, right))
            } else {
                Some(BuiltinOp::CompareOf(
                    parse_compare_op(&op_str)?,
                    left,
                    right,
                ))
            }
        }
        "item" => Some(BuiltinOp::Item),
        "unary" => {
            let op_str: String = tuple.get_item(1).ok()?.extract().ok()?;
            Some(BuiltinOp::Unary(parse_unary_op(&op_str)?))
        }
        "getitem_from" => {
            let container = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::GetItemFrom(container))
        }
        "contains" => {
            let coll = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::Contains(coll))
        }
        "not_contains" => {
            let coll = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::NotContains(coll))
        }
        "method0" | "method1" | "method2" => {
            let name = tuple.get_item(1).ok()?.cast_into::<PyString>().ok()?;
            let args = (2..tuple.len())
                .map(|i| tuple.get_item(i).map(Bound::unbind))
                .collect::<PyResult<Box<[_]>>>()
                .ok()?;
            if args.len() > 2 {
                return None;
            }
            let direct = builtin_methods(py, &name);
            let pure = name.to_str().is_ok_and(|name| PURE_METHODS.contains(&name));
            Some(BuiltinOp::MethodCall(Box::new(MethodCall {
                name: name.unbind(),
                args,
                direct,
                pure,
            })))
        }
        "type" => Some(BuiltinOp::Type),
        "tuple" => Some(BuiltinOp::Tuple),
        "list" => Some(BuiltinOp::List),
        "set" => Some(BuiltinOp::Set),
        "frozenset" => Some(BuiltinOp::FrozenSet),
        "hasattr" => {
            let name_str: Py<PyString> = tuple.get_item(1).ok()?.extract().ok()?;
            Some(BuiltinOp::HasAttr(name_str))
        }
        "slice" => {
            let start = tuple.get_item(1).ok()?.unbind();
            let stop = tuple.get_item(2).ok()?.unbind();
            Some(BuiltinOp::Slice(start, stop))
        }
        "build_tuple" | "build_list" => {
            let mut ops = Vec::new();
            for i in 1..tuple.len() {
                let sub = parse_analysis_tuple(py, &tuple.get_item(i).ok()?)?;
                ops.push(sub);
            }
            if tag == "build_tuple" {
                Some(BuiltinOp::TupleConstruct(ops))
            } else {
                Some(BuiltinOp::ListConstruct(ops))
            }
        }
        "chain" => {
            let first = parse_analysis_tuple(py, &tuple.get_item(1).ok()?)?;
            let second = parse_analysis_tuple(py, &tuple.get_item(2).ok()?)?;
            Some(BuiltinOp::Chain(Box::new(first), Box::new(second)))
        }
        "negate" => {
            let inner = parse_analysis_tuple(py, &tuple.get_item(1).ok()?)?;
            Some(BuiltinOp::Negate(Box::new(inner)))
        }
        "and" => {
            let left = parse_analysis_tuple(py, &tuple.get_item(1).ok()?)?;
            let right = parse_analysis_tuple(py, &tuple.get_item(2).ok()?)?;
            Some(BuiltinOp::And(Box::new(left), Box::new(right)))
        }
        "or" => {
            let left = parse_analysis_tuple(py, &tuple.get_item(1).ok()?)?;
            let right = parse_analysis_tuple(py, &tuple.get_item(2).ok()?)?;
            Some(BuiltinOp::Or(Box::new(left), Box::new(right)))
        }
        _ => None,
    }
}

/// Run `_analyze_lambda()` on a plain function's bytecode.
fn analyze_bytecode(py: Python, func: &Py<PyAny>) -> Option<Py<PyAny>> {
    analyze_function(py, func, false)
}

/// compile()'s analysis results that depend only on a function's code object
/// (no closure, default or global value), by (code, accumulate): reused for
/// every function with that code without calling into Python. Entries hold a
/// weak reference to their code, so the code still dies with its functions,
/// and a dead one means its address may belong to another object now.
/// Values: (result, is_lambda).
type CodeOnlyKey = (usize, bool);
type CodeOnlyValue = Arc<(Option<Py<PyAny>>, bool)>;
static CODE_ONLY: InternTable<CodeOnlyKey, Py<PyWeakrefReference>, CodeOnlyValue> =
    InternTable::new(256, 1024);

/// `_analyze_lambda(func)` (or `_analyze_accumulate` with `accumulate`) for a
/// plain Python function. None for anything else: builtins have no bytecode,
/// and a bound method's code takes `self` first, not the item.
fn analyze_function(py: Python, func: &Py<PyAny>, accumulate: bool) -> Option<Py<PyAny>> {
    analysis(py, func, accumulate)?.result
}

/// A function's analysis parsed into an op (None if nothing was recognised).
fn analyze_to_op(py: Python, func: &Py<PyAny>) -> Option<Arc<BuiltinOp>> {
    let analysis = analysis(py, func, false)?;
    parse_cached(py, analysis.result.as_ref()?, analysis.kept)
}

/// The native op an uncompiled map or filter node can use from the start:
/// for a lambda whose analysis depends only on its code, there is nothing
/// compile() would snapshot. Only lambdas: a `def` function's body would no
/// longer run, which coverage and breakpoints would notice; a lambda's line
/// runs anyway, when the graph is built.
pub(crate) fn eager_op(py: Python, logic: &NodeLogic) -> Option<Arc<BuiltinOp>> {
    let (NodeLogic::Map { func } | NodeLogic::Filter { predicate: func }) = logic else {
        return None;
    };
    let Analysis {
        result: Some(result),
        code_only: true,
        is_lambda: true,
        code,
        kept,
    } = analysis(py, func, false)?
    else {
        return None;
    };
    // Without its numpy program: before compile(), arrays run as Python, and
    // nothing else pays for telling them apart.
    let (result, _) = split_arrays(result.bind(py));
    Some(Arc::new(BuiltinOp::Eager(EagerOp {
        func: func.clone_ref(py),
        code,
        op: parse_cached(py, &result.unbind(), kept)?,
    })))
}

/// Why a map, filter or chunk_by node's function runs as Python, for
/// explain(): "until compile()" in a graph not compiled yet whose function
/// compile() would run natively; otherwise `_why_python`'s phrase. A starmap
/// says "until compile()" or nothing; "" for other nodes, and when there's
/// nothing to add.
pub(crate) fn why_python(
    py: Python,
    logic: &NodeLogic,
    name: &str,
    compiled: bool,
) -> PyResult<String> {
    let why = find_why_python(py, logic, name, compiled);
    // The analyzer keeps a Ctrl+C for compile() (see `INTERRUPTED`): this
    // was explain()'s.
    match take_interrupt() {
        Some(interrupt) => Err(interrupt),
        None => why,
    }
}

fn find_why_python(py: Python, logic: &NodeLogic, name: &str, compiled: bool) -> PyResult<String> {
    let func = match logic {
        NodeLogic::Map { func } | NodeLogic::Filter { predicate: func } => func,
        NodeLogic::ChunkBy(chunk) => &chunk.predicate,
        // Its only native form is a numpy program, which compile() adds.
        NodeLogic::StarMap { func } => {
            let later = !compiled && detect_starmap_builtin(py, func).is_some();
            return Ok(if later {
                "until compile()".to_string()
            } else {
                String::new()
            });
        }
        _ => return Ok(String::new()),
    };
    if !compiled {
        let mut copy = match logic {
            NodeLogic::Map { func } => NodeLogic::Map {
                func: func.clone_ref(py),
            },
            _ => NodeLogic::Filter {
                predicate: func.clone_ref(py),
            },
        };
        if detect_builtin_for_logic(py, &mut copy).is_some() {
            return Ok("until compile()".to_string());
        }
    }
    // `map(round, 2)`: no function of the item alone to explain.
    if crate::call::bound_parts(py, func).is_some() {
        return Ok(String::new());
    }
    let native = pyo3::wrap_pyfunction!(has_native_form, py)?;
    get_why_python()?
        .call1(py, (func, native, name))?
        .extract(py)
}

/// Whether compile() runs the function `func` of a map or filter natively
/// (for `_why_python`).
#[pyfunction]
fn has_native_form(func: Bound<'_, PyAny>) -> bool {
    let py = func.py();
    let func = func.unbind();
    if analyze_to_op(py, &func).is_some() {
        return true;
    }
    // A map that only has a numpy program.
    let Some(result) = analysis(py, &func, false).and_then(|a| a.result) else {
        return false;
    };
    split_arrays(result.bind(py))
        .1
        .is_some_and(|ops| Program::parse(py, 1, &ops).is_some())
}

/// What `analyze_function` found for a function.
struct Analysis {
    result: Option<Py<PyAny>>,
    /// The result depends only on the code object.
    code_only: bool,
    is_lambda: bool,
    code: Py<PyAny>,
    /// The analyzer keeps `result`: the same object comes back for this
    /// function, so ops parsed from it can be shared (`PARSED`).
    kept: bool,
}

thread_local! {
    /// A KeyboardInterrupt (or another exception that isn't an `Exception`)
    /// raised while the analyzer ran. Analysis answers "nothing to optimize"
    /// from then on, so compile() finishes quickly with the functions it
    /// hasn't analysed called as they are, and then raises it.
    static INTERRUPTED: std::cell::RefCell<Option<PyErr>> =
        const { std::cell::RefCell::new(None) };
}

/// The interrupt the analyzer was stopped by, if any (see `INTERRUPTED`).
pub(crate) fn take_interrupt() -> Option<PyErr> {
    INTERRUPTED.with(|i| i.borrow_mut().take())
}

fn analysis(py: Python, func: &Py<PyAny>, accumulate: bool) -> Option<Analysis> {
    if INTERRUPTED.with(|i| i.borrow().is_some()) {
        return None;
    }
    // SAFETY: GIL held; a type check, then a borrowed field of a live
    // function, made a strong reference before any Python code runs.
    let code = unsafe {
        if pyo3::ffi::PyFunction_Check(func.as_ptr()) == 0 {
            return None;
        }
        Bound::from_borrowed_ptr(py, pyo3::ffi::PyFunction_GetCode(func.as_ptr()))
    };
    let key = (code.as_ptr() as usize, accumulate);
    let alive = |weak: &Py<PyWeakrefReference>| weak.bind(py).upgrade().is_some();
    if let Some(found) = CODE_ONLY.get(&key, alive) {
        let (result, is_lambda) = &*found;
        return Some(Analysis {
            result: result.as_ref().map(|r| r.clone_ref(py)),
            code_only: true,
            is_lambda: *is_lambda,
            code: code.unbind(),
            kept: true,
        });
    }
    let answer = match get_analyze_for_compile()
        .ok()?
        .call1(py, (func, accumulate))
    {
        Ok(answer) => answer,
        Err(e) => {
            // An analysis that fails is no analysis; a Ctrl+C is the user's.
            if !e.is_instance_of::<pyo3::exceptions::PyException>(py) {
                INTERRUPTED.with(|i| *i.borrow_mut() = Some(e));
            }
            return None;
        }
    };
    let (result, code_only, is_lambda, kept): (Py<PyAny>, bool, bool, bool) =
        answer.extract(py).ok()?;
    let result = (!result.is_none(py)).then_some(result);
    if code_only && let Ok(weak) = PyWeakrefReference::new(&code) {
        let value = Arc::new((result.as_ref().map(|r| r.clone_ref(py)), is_lambda));
        CODE_ONLY.insert(key, weak.unbind(), value, alive);
    }
    Some(Analysis {
        result,
        code_only,
        is_lambda,
        code: code.unbind(),
        kept,
    })
}

/// Parsed ops by the analysis result they came from (`_analyze_lambda`
/// caches results per code object), so graphs built from the same code
/// share one immutable op tree instead of allocating their own. Entries hold
/// their result, so a key can't be reused by another object; entries only
/// this table still references are dropped when it grows.
static PARSED: InternTable<usize, Py<PyAny>, Arc<BuiltinOp>> = InternTable::new(256, 4096);

/// The op for an analysis `result`, shared through `PARSED` when the
/// analyzer `kept` it (a result it doesn't keep is made afresh each time: an
/// entry would keep it, and whatever it captured, alive).
fn parse_cached(py: Python, result: &Py<PyAny>, kept: bool) -> Option<Arc<BuiltinOp>> {
    let key = result.as_ptr() as usize;
    if kept && let Some(op) = PARSED.get(&key, |_| true) {
        return Some(op);
    }
    // Parsed without the lock: parsing calls into Python, which could need it.
    let (tree, arrays) = split_arrays(result.bind(py));
    let op = parse_analysis_tuple(py, &tree)?;
    let numeric = crate::numeric::NumProgram::from_op(py, &op);
    // A numpy program runs for arrays, behind the fast path for numbers:
    // which then runs even for one step, so that numbers don't pay for the
    // check for arrays.
    let (op, numbers_first) = match arrays.and_then(|ops| Program::parse(py, 1, &ops)) {
        Some(program) => (
            BuiltinOp::Arrays(Box::new(ArrayOp {
                program,
                fallback: Fallback::Op(Box::new(op)),
            })),
            true,
        ),
        None => (op, false),
    };
    let op = Arc::new(match numeric {
        Some(program) if program.worth_alone() || numbers_first => {
            BuiltinOp::Numeric(Box::new(program), Box::new(op))
        }
        _ => op,
    });
    if !kept {
        return Some(op);
    }
    let in_use = |held: &Py<PyAny>| held.get_refcnt(py) > 1;
    Some(PARSED.insert(key, result.clone_ref(py), op, in_use))
}

/// An analysis result and the numpy program the analyzer translated the
/// function into, if any: `("arrays", result, program)` (see
/// `_analyze_function`).
fn split_arrays<'py>(result: &Bound<'py, PyAny>) -> (Bound<'py, PyAny>, Option<Bound<'py, PyAny>>) {
    // The tag's bytes, not a Python comparison: this runs for every analysis.
    let is_arrays = |tag: Bound<'_, PyAny>| {
        tag.cast::<PyString>()
            .is_ok_and(|tag| tag.to_str().is_ok_and(|tag| tag == "arrays"))
    };
    if let Ok(tuple) = result.cast::<PyTuple>()
        && tuple.len() == 3
        && tuple.get_item(0).is_ok_and(is_arrays)
        && let (Ok(tree), Ok(program)) = (tuple.get_item(1), tuple.get_item(2))
    {
        return (tree, Some(program));
    }
    (result.clone(), None)
}

/// A map's numpy program with the function itself as the fallback: when the
/// function has no other native form (not shared between graphs, as the
/// function is this node's).
fn arrays_calling(
    py: Python,
    arrays: Option<Bound<'_, PyAny>>,
    func: &Py<PyAny>,
) -> Option<Arc<BuiltinOp>> {
    let program = Program::parse(py, 1, &arrays?)?;
    Some(Arc::new(BuiltinOp::Arrays(Box::new(ArrayOp {
        program,
        fallback: Fallback::Call(func.clone_ref(py)),
    }))))
}

/// Detect builtin fast-path for a Map function via identity checks only.
fn detect_map_builtin_identity(py: Python, func: &Py<PyAny>) -> Option<Arc<BuiltinOp>> {
    // Simple identity checks: func is exactly the builtin
    macro_rules! check_identity {
        ($getter:ident, $op:expr) => {
            if let Ok(b) = $getter() {
                if func.is(b) {
                    return Some(Arc::new($op));
                }
            }
        };
    }
    check_identity!(get_builtin_len, BuiltinOp::Len);
    check_identity!(get_builtin_str, BuiltinOp::Str);
    check_identity!(get_builtin_int, BuiltinOp::Int);
    check_identity!(get_builtin_float, BuiltinOp::Float);
    check_identity!(get_builtin_bool, BuiltinOp::Bool);
    check_identity!(get_operator_not, BuiltinOp::Not);
    check_identity!(get_operator_truth, BuiltinOp::Truth);
    check_identity!(get_builtin_type, BuiltinOp::Type);
    check_identity!(get_builtin_tuple, BuiltinOp::Tuple);
    check_identity!(get_builtin_list, BuiltinOp::List);
    check_identity!(get_builtin_set, BuiltinOp::Set);
    check_identity!(get_builtin_frozenset, BuiltinOp::FrozenSet);
    check_identity!(get_builtin_abs, BuiltinOp::Unary(UnaryOp::Abs));

    // Detect operator.itemgetter instances (e.g., operator.itemgetter("name"))
    if get_operator_itemgetter()
        .is_ok_and(|ig| func.bind(py).is_instance(ig.bind(py)).unwrap_or(false))
    {
        let key = func
            .call_method0(py, "__reduce__")
            .ok()
            .and_then(|r| r.bind(py).get_item(1).ok().map(|a| a.unbind()))
            .and_then(|a| a.bind(py).cast::<PyTuple>().ok().map(|t| t.to_owned()))
            .filter(|t| t.len() == 1)
            .and_then(|t| t.get_item(0).ok().map(|k| k.unbind()));
        if let Some(k) = key {
            return Some(Arc::new(BuiltinOp::ItemGetter(k)));
        }
    }
    // Detect operator.attrgetter instances (e.g., operator.attrgetter("name"))
    if get_operator_attrgetter()
        .is_ok_and(|ag| func.bind(py).is_instance(ag.bind(py)).unwrap_or(false))
    {
        let name = func
            .call_method0(py, "__reduce__")
            .ok()
            .and_then(|r| r.bind(py).get_item(1).ok().map(|a| a.unbind()))
            .and_then(|a| a.bind(py).cast::<PyTuple>().ok().map(|t| t.to_owned()))
            .filter(|t| t.len() == 1)
            .and_then(|t| {
                t.get_item(0)
                    .ok()?
                    .cast_into::<pyo3::types::PyString>()
                    .ok()
            });
        if let Some(name_str) = name {
            // attrgetter("a.b") follows the dots: one step per name.
            let path = name_str.to_cow().ok()?;
            let mut steps = path
                .rsplit('.')
                .map(|part| BuiltinOp::AttrGetter(PyString::intern(py, part).unbind()));
            let last = steps.next()?;
            let op = steps.fold(last, |rest, step| {
                BuiltinOp::Chain(Box::new(step), Box::new(rest))
            });
            return Some(Arc::new(op));
        }
    }
    // Bytecode analysis is the caller's (once: it is a Python call).
    None
}

/// Detect builtin fast-path for a Map function, also handling `("unwrap", callable)`.
///
/// When a lambda wraps a simple callable (e.g., `lambda x: MyClass(x)`), the analyzer
/// returns `("unwrap", resolved_callable)`. We replace the func in the Map logic directly
/// and return `None` — the callable gets cached vectorcall automatically.
fn detect_map_builtin_with_unwrap(py: Python, func: &mut Py<PyAny>) -> Option<Arc<BuiltinOp>> {
    // First try the normal detection
    if let Some(op) = detect_map_builtin_identity(py, func) {
        return Some(op);
    }
    // Fallback: analyze lambda bytecode
    let analysis = analysis(py, func, false)?;
    let result = analysis.result?;
    let (tree, arrays) = split_arrays(result.bind(py));
    if tree.is_none() {
        return arrays_calling(py, arrays, func);
    }
    let tuple = tree.cast::<PyTuple>().ok()?;
    let tag: String = tuple.get_item(0).ok()?.extract().ok()?;
    if tag == "unwrap" {
        // Replace func with the resolved callable — no BuiltinOp needed,
        // unless numpy can run it (then the callable is the fallback)
        let resolved = tuple.get_item(1).ok()?.unbind();
        *func = resolved;
        return arrays_calling(py, arrays, func);
    }
    // A result with no op of its own (a call on a module, `np.degrees(x)`)
    // still has its numpy program.
    parse_cached(py, &result, analysis.kept).or_else(|| arrays_calling(py, arrays, func))
}

/// Detect builtin fast-path for a Filter predicate.
///
/// `filter(isinstance, int)` binds `int` after the item (`crate::call::bind`);
/// a bound call with exactly one extra positional argument is checked against
/// isinstance / operator.is_ / operator.is_not.
fn detect_filter_builtin(py: Python, predicate: &Py<PyAny>) -> Option<Arc<BuiltinOp>> {
    let Some(bound) = crate::call::bound_parts(py, predicate) else {
        // No extra arguments: analyse the predicate's own bytecode
        return analyze_to_op(py, predicate);
    };
    let call = bound.get();
    let ([arg], false) = (call.args(), call.has_kwargs()) else {
        return None;
    };
    let (func, arg) = (&call.func, arg.clone_ref(py));

    if get_builtin_isinstance().is_ok_and(|b| func.is(b)) {
        return Some(Arc::new(BuiltinOp::IsInstance(arg)));
    }
    if get_operator_is().is_ok_and(|b| func.is(b)) {
        return Some(Arc::new(BuiltinOp::Is(arg)));
    }
    if get_operator_is_not().is_ok_and(|b| func.is(b)) {
        return Some(Arc::new(BuiltinOp::IsNot(arg)));
    }
    None
}

/// Detect builtin fast-path for a Sink function (no extra args).
fn detect_sink_builtin(py: Python, func: &Py<PyAny>) -> Option<BuiltinOp> {
    if get_builtin_print().is_ok_and(|b| func.is(b)) {
        return Some(BuiltinOp::Print);
    }
    // lst.append on an exact list (a subclass may override append)
    // SAFETY: GIL held; a type check on a live object.
    if unsafe { pyo3::ffi::PyCFunction_Check(func.as_ptr()) } != 0 {
        let func = func.bind(py);
        let owner = func.getattr_opt(pyo3::intern!(py, "__self__")).ok()??;
        let name = func.getattr_opt(pyo3::intern!(py, "__name__")).ok()??;
        if owner.is_exact_instance_of::<pyo3::types::PyList>() && name.eq("append").ok()? {
            return Some(BuiltinOp::ListAppend(owner.unbind()));
        }
    }
    None
}

/// Detect the appropriate builtin fast-path for a given NodeLogic variant.
///
/// For Map nodes, also handles `("unwrap", callable)` — replacing the func in-place.
pub(crate) fn detect_builtin_for_logic(
    py: Python,
    logic: &mut NodeLogic,
) -> Option<Arc<BuiltinOp>> {
    match logic {
        NodeLogic::Map { func } => detect_map_builtin_with_unwrap(py, func),
        NodeLogic::StarMap { func } => detect_starmap_builtin(py, func),
        NodeLogic::Filter { predicate } => detect_filter_builtin(py, predicate),
        NodeLogic::ChunkBy(chunk) => detect_filter_builtin(py, &chunk.predicate),
        NodeLogic::ExtractBetween(_) => None,
        NodeLogic::Sink { func } => detect_sink_builtin(py, func).map(Arc::new),
        NodeLogic::Accumulate {
            func,
            returns_state: false,
            ..
        } => detect_accumulate_builtin(py, func).map(Arc::new),
        _ => None,
    }
}

/// A starmap function's native form: a numpy program of its arguments (see
/// `_analyze_star_for_compile`), with the function for the items it doesn't
/// fit.
fn detect_starmap_builtin(py: Python, func: &Py<PyAny>) -> Option<Arc<BuiltinOp>> {
    if INTERRUPTED.with(|i| i.borrow().is_some()) {
        return None;
    }
    let answer = match get_analyze_star().ok()?.call1(py, (func,)) {
        Ok(answer) => answer,
        Err(e) => {
            // As in `analysis`: a Ctrl+C is the user's.
            if !e.is_instance_of::<pyo3::exceptions::PyException>(py) {
                INTERRUPTED.with(|i| *i.borrow_mut() = Some(e));
            }
            return None;
        }
    };
    let (nargs, ops): (usize, Bound<'_, PyAny>) = answer.bind(py).extract().ok()?;
    let program = Program::parse(py, nargs, &ops)?;
    Some(Arc::new(BuiltinOp::Arrays(Box::new(ArrayOp {
        program,
        fallback: Fallback::StarCall(func.clone_ref(py)),
    }))))
}

/// Native form of an accumulate function: `acc <op> x`, `acc <op> const`,
/// `max(acc, x)` or `min(acc, x)` (see `_analyze_accumulate`).
fn detect_accumulate_builtin(py: Python, func: &Py<PyAny>) -> Option<BuiltinOp> {
    let result = analyze_function(py, func, true)?;
    let tuple = result.bind(py).cast::<PyTuple>().ok()?;
    let tag: String = tuple.get_item(0).ok()?.extract().ok()?;
    match tag.as_str() {
        "acc_binop" => {
            let op: String = tuple.get_item(1).ok()?.extract().ok()?;
            Some(BuiltinOp::AccumBinOp(parse_binary_op(&op)?))
        }
        "acc_binop_const" => {
            let op: String = tuple.get_item(1).ok()?.extract().ok()?;
            let value = tuple.get_item(2).ok()?.unbind();
            Some(BuiltinOp::AccumBinOpConst(parse_binary_op(&op)?, value))
        }
        "acc_max" => Some(BuiltinOp::AccumMax),
        "acc_min" => Some(BuiltinOp::AccumMin),
        _ => None,
    }
}

/// Detect if a Sink's function calls .flush() on a Stream (Collect node).
///
/// Handles two forms:
/// - Bound method: `collected.flush` — check `__self__` is Stream, `__func__.__name__` is "flush"
/// - Lambda: `lambda _: collected.flush()` — bytecode analysis returns `("call_method0", obj, "flush")`
pub(crate) fn detect_flush_target_from_func(py: Python, func: &Py<PyAny>) -> Option<Py<Stream>> {
    // Method A: Bound method check (e.g., .sink(collected.flush))
    let bound = func.bind(py);
    // getattr_opt: plain functions have no __self__, and a raised-and-discarded
    // AttributeError per sink made this a large part of compile().
    if let (Ok(Some(self_obj)), Ok(Some(func_obj))) = (
        bound.getattr_opt(pyo3::intern!(py, "__self__")),
        bound.getattr_opt(pyo3::intern!(py, "__func__")),
    ) && let Ok(name) = func_obj.getattr("__name__")
        && let Ok(name_str) = name.extract::<String>()
        && name_str == "flush"
        && self_obj.is_instance_of::<Stream>()
        && let Ok(target) = self_obj.extract::<Py<Stream>>()
    {
        // Verify target is a Collect node
        let target_node = target.try_borrow(py).ok()?;
        if matches!(target_node.logic, NodeLogic::Collect { .. }) {
            drop(target_node);
            return Some(target);
        }
    }

    // Method B: Lambda bytecode analysis (e.g., .sink(lambda _: collected.flush()))
    let result = analyze_bytecode(py, func)?;
    let bound_result = result.bind(py);
    let tuple = bound_result.cast::<PyTuple>().ok()?;
    if tuple.len() != 3 {
        return None;
    }
    let tag: String = tuple.get_item(0).ok()?.extract().ok()?;
    if tag != "call_method0" {
        return None;
    }
    let method_name: String = tuple.get_item(2).ok()?.extract().ok()?;
    if method_name != "flush" {
        return None;
    }
    let obj = tuple.get_item(1).ok()?;
    if !obj.is_instance_of::<Stream>() {
        return None;
    }
    let target: Py<Stream> = obj.extract().ok()?;
    // Verify target is a Collect node
    let target_node = target.try_borrow(py).ok()?;
    if matches!(target_node.logic, NodeLogic::Collect { .. }) {
        drop(target_node);
        return Some(target);
    }
    None
}

/// Detect if a Sink's function calls .emit() on a Stream (Source node).
///
/// Handles two forms:
/// - Bound method: `source.emit` — check `__self__` is Stream, `__func__.__name__` is "emit"
/// - Lambda: `lambda x: source.emit(x)` — bytecode analysis returns `("call_method1", obj, "emit")`
pub(crate) fn detect_emit_target_from_func(py: Python, func: &Py<PyAny>) -> Option<Py<Stream>> {
    // Method A: Bound method check (e.g., .sink(source.emit))
    let bound = func.bind(py);
    // getattr_opt: plain functions have no __self__, and a raised-and-discarded
    // AttributeError per sink made this a large part of compile().
    // Only an exact Stream's own (builtin) emit: a subclass may override
    // emit(), and a Python function bound to a stream has a __func__.
    if let (Ok(Some(self_obj)), Ok(None)) = (
        bound.getattr_opt(pyo3::intern!(py, "__self__")),
        bound.getattr_opt(pyo3::intern!(py, "__func__")),
    ) && let Ok(name) = bound.getattr(pyo3::intern!(py, "__name__"))
        && let Ok(name_str) = name.extract::<String>()
        && name_str == "emit"
        && self_obj.is_exact_instance_of::<Stream>()
        && let Ok(target) = self_obj.extract::<Py<Stream>>()
    {
        // Verify target is a Source node
        let target_node = target.try_borrow(py).ok()?;
        if matches!(target_node.logic, NodeLogic::Source) {
            drop(target_node);
            return Some(target);
        }
    }

    // Method B: Lambda bytecode analysis (e.g., .sink(lambda x: source.emit(x)))
    let result = analyze_bytecode(py, func)?;
    let bound_result = result.bind(py);
    let tuple = bound_result.cast::<PyTuple>().ok()?;
    if tuple.len() != 3 {
        return None;
    }
    let tag: String = tuple.get_item(0).ok()?.extract().ok()?;
    if tag != "call_method1" {
        return None;
    }
    let method_name: String = tuple.get_item(2).ok()?.extract().ok()?;
    if method_name != "emit" {
        return None;
    }
    let obj = tuple.get_item(1).ok()?;
    // A subclass may override emit().
    if !obj.is_exact_instance_of::<Stream>() {
        return None;
    }
    let target: Py<Stream> = obj.extract().ok()?;
    // Verify target is a Source node
    let target_node = target.try_borrow(py).ok()?;
    if matches!(target_node.logic, NodeLogic::Source) {
        drop(target_node);
        return Some(target);
    }
    None
}

/// Extract the vectorcall function pointer from a Python callable.
///
/// Returns `None` if the callable doesn't support vectorcall.
/// The pointer is valid as long as the callable object is alive.
unsafe fn extract_vectorcall(
    callable: *mut pyo3::ffi::PyObject,
) -> Option<pyo3::ffi::vectorcallfunc> {
    unsafe {
        let tp = pyo3::ffi::Py_TYPE(callable);
        // PyType_GetFlags, not the field: free-threaded builds make it atomic.
        if pyo3::ffi::PyType_GetFlags(tp) & pyo3::ffi::Py_TPFLAGS_HAVE_VECTORCALL == 0 {
            return None;
        }
        let offset = (*tp).tp_vectorcall_offset;
        if offset <= 0 {
            return None;
        }
        // Unaligned: CPython itself copies the slot with memcpy.
        let ptr = (callable as *const u8).offset(offset as isize)
            as *const Option<pyo3::ffi::vectorcallfunc>;
        ptr.read_unaligned()
    }
}

/// Extract the vectorcall function pointer for the callable in a NodeLogic.
pub(crate) fn extract_vectorcall_for_logic(
    _py: Python,
    logic: &NodeLogic,
) -> Option<pyo3::ffi::vectorcallfunc> {
    let callable_ptr = match logic {
        NodeLogic::Map { func } => func.as_ptr(),
        NodeLogic::Filter { predicate } => predicate.as_ptr(),
        NodeLogic::ChunkBy(chunk) => chunk.predicate.as_ptr(),
        NodeLogic::ExtractBetween(between) => between.open.as_ptr(),
        NodeLogic::Sink { func } => func.as_ptr(),
        NodeLogic::Accumulate { func, .. } => func.as_ptr(),
        NodeLogic::BatchMap { func } => func.as_ptr(),
        _ => return None,
    };
    unsafe { extract_vectorcall(callable_ptr) }
}

/// Cache vectorcall pointers for callables beyond the primary one that
/// `extract_vectorcall_for_logic` caches on the node.
pub(crate) fn cache_secondary_vectorcalls(logic: &mut NodeLogic) {
    if let NodeLogic::ExtractBetween(between) = logic {
        between.close_vectorcall = unsafe { extract_vectorcall(between.close.as_ptr()) };
    }
}
