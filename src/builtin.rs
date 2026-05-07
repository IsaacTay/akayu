//! Builtin callable analysis and fast-path detection.

use pyo3::prelude::*;
use pyo3::types::{PyString, PyTuple};

use crate::helpers::{
    get_analyze_lambda, get_builtin_bool, get_builtin_float, get_builtin_frozenset,
    get_builtin_int, get_builtin_isinstance, get_builtin_len, get_builtin_list, get_builtin_print,
    get_builtin_set, get_builtin_str, get_builtin_tuple, get_builtin_type, get_operator_attrgetter,
    get_operator_is, get_operator_is_not, get_operator_itemgetter, get_operator_not,
    get_operator_truth,
};
use crate::node::{BinaryOp, BuiltinOp, CompareOp, NodeLogic};
use crate::Stream;

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
        _ => None,
    }
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
        "contains" => {
            let coll = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::Contains(coll))
        }
        "not_contains" => {
            let coll = tuple.get_item(1).ok()?.unbind();
            Some(BuiltinOp::NotContains(coll))
        }
        "method0" => {
            let name_str: Py<PyString> = tuple.get_item(1).ok()?.extract().ok()?;
            Some(BuiltinOp::MethodCall(name_str))
        }
        "method1" => {
            let name_str: Py<PyString> = tuple.get_item(1).ok()?.extract().ok()?;
            let arg = tuple.get_item(2).ok()?.unbind();
            Some(BuiltinOp::MethodCall1(name_str, arg))
        }
        "method2" => {
            let name_str: Py<PyString> = tuple.get_item(1).ok()?.extract().ok()?;
            let arg1 = tuple.get_item(2).ok()?.unbind();
            let arg2 = tuple.get_item(3).ok()?.unbind();
            Some(BuiltinOp::MethodCall2(name_str, arg1, arg2))
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

/// Analyze a Python callable's bytecode via `_analyze_lambda()` and convert to `BuiltinOp`.
fn analyze_lambda_fallback(py: Python, func: &Py<PyAny>) -> Option<BuiltinOp> {
    let analyzer = get_analyze_lambda().ok()?;
    let result = analyzer.call1(py, (func,)).ok()?;
    if result.is_none(py) {
        return None;
    }
    parse_analysis_tuple(py, result.bind(py))
}

/// Detect builtin fast-path for a Map function via identity checks only.
fn detect_map_builtin_identity(py: Python, func: &Py<PyAny>) -> Option<BuiltinOp> {
    // Simple identity checks: func is exactly the builtin
    macro_rules! check_identity {
        ($getter:ident, $op:expr) => {
            if let Ok(b) = $getter() {
                if func.is(b) {
                    return Some($op);
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
            return Some(BuiltinOp::ItemGetter(k));
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
            return Some(BuiltinOp::AttrGetter(name_str.unbind()));
        }
    }
    // Fallback: analyze lambda bytecode
    analyze_lambda_fallback(py, func)
}

/// Detect builtin fast-path for a Map function, also handling `("unwrap", callable)`.
///
/// When a lambda wraps a simple callable (e.g., `lambda x: MyClass(x)`), the analyzer
/// returns `("unwrap", resolved_callable)`. We replace the func in the Map logic directly
/// and return `None` — the callable gets cached vectorcall automatically.
fn detect_map_builtin_with_unwrap(py: Python, func: &mut Py<PyAny>) -> Option<BuiltinOp> {
    // First try the normal detection
    if let Some(op) = detect_map_builtin_identity(py, func) {
        return Some(op);
    }
    // Fallback: analyze lambda bytecode
    let analyzer = get_analyze_lambda().ok()?;
    let result = analyzer.call1(py, (func.clone_ref(py),)).ok()?;
    if result.is_none(py) {
        return None;
    }
    let bound = result.bind(py);
    let tuple = bound.cast::<PyTuple>().ok()?;
    let tag: String = tuple.get_item(0).ok()?.extract().ok()?;
    if tag == "unwrap" {
        // Replace func with the resolved callable — no BuiltinOp needed
        let resolved = tuple.get_item(1).ok()?.unbind();
        *func = resolved;
        return None;
    }
    parse_analysis_tuple(py, bound)
}

/// Detect builtin fast-path for a Filter predicate.
///
/// Handles wrapped predicates by checking `_akayu_inner` / `_akayu_args` attributes
/// set by `_build_map_func` when extra args were provided (e.g., `filter(isinstance, int)`).
fn detect_filter_builtin(py: Python, predicate: &Py<PyAny>) -> Option<BuiltinOp> {
    // Try to recover the original function and args from the wrapper
    let (func, args) = if let Ok(inner) = predicate.getattr(py, "_akayu_inner") {
        let args = predicate.getattr(py, "_akayu_args").ok();
        (inner, args)
    } else {
        // No wrapper — predicate is the original function (no extra args)
        // Fall through to lambda bytecode analysis
        return analyze_lambda_fallback(py, predicate);
    };

    // Filter builtins require exactly one extra arg
    let arg = if let Some(ref args_obj) = args {
        let args_bound = args_obj.bind(py);
        if let Ok(tuple) = args_bound.cast::<PyTuple>() {
            if tuple.len() == 1 {
                Some(tuple.get_item(0).ok()?.unbind())
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };
    let arg = arg?;

    if get_builtin_isinstance().is_ok_and(|b| func.is(b)) {
        return Some(BuiltinOp::IsInstance(arg));
    }
    if get_operator_is().is_ok_and(|b| func.is(b)) {
        return Some(BuiltinOp::Is(arg));
    }
    if get_operator_is_not().is_ok_and(|b| func.is(b)) {
        return Some(BuiltinOp::IsNot(arg));
    }
    None
}

/// Detect builtin fast-path for a Sink function (no extra args).
fn detect_sink_builtin(func: &Py<PyAny>) -> Option<BuiltinOp> {
    if get_builtin_print().is_ok_and(|b| func.is(b)) {
        return Some(BuiltinOp::Print);
    }
    None
}

/// Detect the appropriate builtin fast-path for a given NodeLogic variant.
///
/// For Map nodes, also handles `("unwrap", callable)` — replacing the func in-place.
pub(crate) fn detect_builtin_for_logic(py: Python, logic: &mut NodeLogic) -> Option<BuiltinOp> {
    match logic {
        NodeLogic::Map { func } => detect_map_builtin_with_unwrap(py, func),
        NodeLogic::Filter { predicate } => detect_filter_builtin(py, predicate),
        NodeLogic::ChunkBy { predicate, .. } => detect_filter_builtin(py, predicate),
        NodeLogic::ExtractBetween { .. } => None,
        NodeLogic::Sink { func } => detect_sink_builtin(func),
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
    if let (Ok(self_obj), Ok(func_obj)) = (bound.getattr("__self__"), bound.getattr("__func__"))
        && let Ok(name) = func_obj.getattr("__name__")
        && let Ok(name_str) = name.extract::<String>()
        && name_str == "flush"
        && self_obj.is_instance_of::<Stream>()
        && let Ok(target) = self_obj.extract::<Py<Stream>>()
    {
        // Verify target is a Collect node
        let target_node = target.borrow(py);
        if matches!(target_node.logic, NodeLogic::Collect { .. }) {
            drop(target_node);
            return Some(target);
        }
    }

    // Method B: Lambda bytecode analysis (e.g., .sink(lambda _: collected.flush()))
    let analyzer = get_analyze_lambda().ok()?;
    let result = analyzer.call1(py, (func,)).ok()?;
    if result.is_none(py) {
        return None;
    }
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
    let target_node = target.borrow(py);
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
    if let (Ok(self_obj), Ok(func_obj)) = (bound.getattr("__self__"), bound.getattr("__func__"))
        && let Ok(name) = func_obj.getattr("__name__")
        && let Ok(name_str) = name.extract::<String>()
        && name_str == "emit"
        && self_obj.is_instance_of::<Stream>()
        && let Ok(target) = self_obj.extract::<Py<Stream>>()
    {
        // Verify target is a Source node
        let target_node = target.borrow(py);
        if matches!(target_node.logic, NodeLogic::Source) {
            drop(target_node);
            return Some(target);
        }
    }

    // Method B: Lambda bytecode analysis (e.g., .sink(lambda x: source.emit(x)))
    let analyzer = get_analyze_lambda().ok()?;
    let result = analyzer.call1(py, (func,)).ok()?;
    if result.is_none(py) {
        return None;
    }
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
    if !obj.is_instance_of::<Stream>() {
        return None;
    }
    let target: Py<Stream> = obj.extract().ok()?;
    // Verify target is a Source node
    let target_node = target.borrow(py);
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
        if (*tp).tp_flags & pyo3::ffi::Py_TPFLAGS_HAVE_VECTORCALL == 0 {
            return None;
        }
        let offset = (*tp).tp_vectorcall_offset;
        if offset <= 0 {
            return None;
        }
        let ptr = (callable as *const u8).offset(offset as isize)
            as *const Option<pyo3::ffi::vectorcallfunc>;
        *ptr
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
        NodeLogic::ChunkBy { predicate, .. } => predicate.as_ptr(),
        NodeLogic::ExtractBetween { open, .. } => open.as_ptr(),
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
    if let NodeLogic::ExtractBetween {
        close,
        close_vectorcall,
        ..
    } = logic
    {
        *close_vectorcall = unsafe { extract_vectorcall(close.as_ptr()) };
    }
}
