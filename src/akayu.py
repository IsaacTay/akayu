import builtins as _builtins
import dis
import inspect
import os
import sys
import warnings
import weakref
from types import BuiltinFunctionType as _BuiltinFunctionType
from types import FunctionType as _FunctionType

_NOISE_OPCODES = frozenset(
    {
        "RESUME",
        "COPY_FREE_VARS",
        "NOP",
        "CACHE",
        "PUSH_NULL",
        "PRECALL",
        "NOT_TAKEN",  # 3.14: branch-prediction marker after conditional jumps
    }
)

# --- Fast bytecode extractor ---
# Pre-built lookup tables for custom bytecode parsing.
# Replaces dis.get_instructions() (~35µs) with direct byte reading (~2µs)
# by avoiding Instruction namedtuple creation and inline cache traversal overhead.

_OP_NAME = {v: k for k, v in dis.opmap.items()}  # opcode int -> name string


def _inline_cache_counts():
    """opcode int -> inline cache entry count, or None when this Python
    doesn't say (a private `dis` table): bytecode can't be read then."""
    try:
        entries = dis._inline_cache_entries.items()
        return {dis.opmap[name]: count for name, count in entries if name in dis.opmap}
    except (AttributeError, TypeError, ValueError):
        return None


_OP_CACHE = _inline_cache_counts()

# The Python versions the bytecode patterns below were written and tested
# for. On any other, a new bytecode spelling could make a pattern match
# something it shouldn't, so lambdas aren't analysed (they run as Python)
# unless AKAYU_ANALYZER=1. AKAYU_ANALYZER=0 turns it off everywhere.
_TESTED_VERSIONS = ((3, 13), (3, 14))


def _analyzer_enabled():
    setting = os.environ.get("AKAYU_ANALYZER", "")
    if setting in ("0", "1"):
        return setting == "1" and _OP_CACHE is not None
    return sys.version_info[:2] in _TESTED_VERSIONS and _OP_CACHE is not None


_ANALYZER_ON = _analyzer_enabled()
_warned_off = False


def _warn_analyzer_off():
    """Say once why compile() runs lambdas as Python on this version."""
    global _warned_off  # noqa: PLW0603 - a once-per-process flag
    if _warned_off or os.environ.get("AKAYU_ANALYZER") == "0":
        return
    _warned_off = True
    version = ".".join(map(str, sys.version_info[:2]))
    if _OP_CACHE is None:
        why, hint = f"can't read Python {version}'s bytecode (no dis._inline_cache_entries)", ""
    else:
        why = f"hasn't been tested on Python {version}"
        hint = " Set AKAYU_ANALYZER=1 to try the native forms anyway."
    warnings.warn(
        f"akayu {why}, so it runs lambdas as plain Python calls instead of natively: "
        f"results are the same, only slower.{hint}",
        RuntimeWarning,
        stacklevel=3,
    )


try:
    import _opcode as _opcode_mod

    _NB_OPS = _opcode_mod.get_nb_ops()  # [(name, symbol), ...]
except (ImportError, AttributeError):
    _NB_OPS = []

_CMP_OP = dis.cmp_op  # ('<', '<=', '==', '!=', '>', '>=')


def _fast_extract(co):
    """Extract (opname, argval) pairs from a code object, ~19x faster than dis.

    Reads raw co_code bytes directly, resolves argvals inline, filters noise
    opcodes and RETURN_VALUE. Returns a list of (opname_str, argval) tuples
    suitable for _analyze_simple/_analyze_compound pattern matching.
    """
    raw = co.co_code
    consts = co.co_consts
    names = co.co_names
    varnames = co.co_varnames
    result = []
    offsets = []
    j = 0
    n = len(raw)
    while j < n:
        op = raw[j]
        arg = raw[j + 1]
        offset = j  # byte offset of this instruction
        j += 2
        name = _OP_NAME.get(op)
        caches = _OP_CACHE.get(op, 0)
        j += caches * 2  # skip inline cache entries
        if name is None or name in _NOISE_OPCODES or name == "RETURN_VALUE":
            continue
        # Python 3.14 spellings of 3.13 opcodes, so the patterns below match
        if name == "LOAD_FAST_BORROW":
            name = "LOAD_FAST"
        elif name == "LOAD_FAST_BORROW_LOAD_FAST_BORROW":
            name = "LOAD_FAST_LOAD_FAST"
        elif name == "LOAD_SMALL_INT":  # the int is the oparg itself
            result.append(("LOAD_CONST", arg))
            offsets.append(offset)
            continue
        # Resolve argval based on opcode
        if name == "LOAD_CONST":
            av = consts[arg]
        elif name in ("LOAD_ATTR", "LOAD_GLOBAL"):
            av = names[arg >> 1]
        elif name == "COMPARE_OP":
            av = _CMP_OP[arg >> 5]
        elif name == "BINARY_OP":
            av = _NB_OPS[arg][1] if arg < len(_NB_OPS) else arg
            if av == "[]":  # 3.14 spells BINARY_SUBSCR as BINARY_OP []
                name, av = "BINARY_SUBSCR", 0
        elif name in ("CONTAINS_OP", "IS_OP"):
            av = arg
        elif name == "LOAD_FAST":
            av = varnames[arg]
        elif name == "LOAD_DEREF":
            # Free variable — resolve name from combined locals table
            # In 3.13, LOAD_DEREF arg indexes into co_varnames + co_cellvars + co_freevars
            all_names = co.co_varnames + co.co_cellvars + co.co_freevars
            av = all_names[arg] if arg < len(all_names) else arg
        elif name == "POP_JUMP_IF_FALSE" or name == "POP_JUMP_IF_TRUE":
            # Jump target resolution: offset + 2 + arg*2 + cache_size*2
            av = offset + 2 + arg * 2 + caches * 2
        else:
            av = arg
        result.append((name, av))
        offsets.append(offset)
    return _finish_instructions(result, offsets)


_JUMPS = ("POP_JUMP_IF_FALSE", "POP_JUMP_IF_TRUE")


def _finish_instructions(instrs, offsets):
    """Drop TO_BOOL where it only feeds a jump or `not` (elsewhere it is the
    conversion `not not x` compiles to), and turn each jump's target byte
    offset into a distance in instructions, which survives slicing."""
    keep = [
        i
        for i, (op, _) in enumerate(instrs)
        if not (op == "TO_BOOL" and i + 1 < len(instrs) and instrs[i + 1][0] in (*_JUMPS, "UNARY_NOT"))
    ]
    kept_offsets = [offsets[i] for i in keep]
    out = []
    for new, i in enumerate(keep):
        op, av = instrs[i]
        if op in _JUMPS:
            # the first remaining instruction at or after the target
            target = next((k for k, off in enumerate(kept_offsets) if off >= av), len(keep))
            av = target - new
        out.append((op, av))
    return out


_EXCEPTION_OPCODES = frozenset(
    {
        "SETUP_FINALLY",
        "POP_EXCEPT",
        "SETUP_WITH",
        "BEFORE_WITH",
        "SETUP_ASYNC_WITH",
        "RERAISE",
        "PUSH_EXC_INFO",
    }
)

# Known builtins we allow resolving from LOAD_GLOBAL
_KNOWN_BUILTINS = {
    "len": _builtins.len,
    "str": _builtins.str,
    "int": _builtins.int,
    "float": _builtins.float,
    "bool": _builtins.bool,
    "isinstance": _builtins.isinstance,
    "type": _builtins.type,
    "hasattr": _builtins.hasattr,
    "tuple": _builtins.tuple,
    "list": _builtins.list,
    "set": _builtins.set,
    "frozenset": _builtins.frozenset,
    "max": _builtins.max,
    "min": _builtins.min,
}

_UNRESOLVABLE = object()  # sentinel


class _Bindings:
    """The function under analysis, plus every value resolved from it.

    The analyzer passes this where it used to pass the function; the recorded
    values decide whether a cached result applies to another function with
    the same code (see _analyze_lambda).
    """

    __slots__ = ("deps", "func")

    def __init__(self, func):
        self.func = func
        self.deps = []


def _resolve_value(instr, ctx):
    """Resolve a value from an instruction + the function under analysis,
    recording it as a dependency of the result."""
    value = _resolve(instr, ctx.func)
    # Only defaults, closure cells and globals differ between functions with
    # the same code; a constant is part of the code, and anything else
    # resolves the same (to _UNRESOLVABLE) for every function.
    if instr[0] in ("LOAD_DEFAULT", "LOAD_DEREF", "LOAD_GLOBAL"):
        ctx.deps.append((instr, value))
    return value


def _resolve(instr, func):
    """Resolve a value from an instruction + function context.

    instr is a (opname, argval) tuple from _fast_extract.
    Returns the resolved Python object, or _UNRESOLVABLE sentinel.
    """
    op = instr[0]
    if op == "LOAD_CONST":
        return instr[1]
    if op == "LOAD_DEFAULT":
        # A defaulted parameter (`lambda x, k=k: ...`): nodes call with one
        # argument, so it always holds its default.
        code = func.__code__
        params = code.co_varnames[: code.co_argcount]
        defaults = func.__defaults__ or ()
        if instr[1] in params[1:]:
            i = params.index(instr[1]) - (len(params) - len(defaults))
            if i >= 0:
                return defaults[i]
        return _UNRESOLVABLE
    if op == "LOAD_DEREF":
        # Closure variable — snapshot at compile time
        closure = getattr(func, "__closure__", None)
        if closure is None:
            return _UNRESOLVABLE
        # Find the index of this free var
        code = func.__code__
        free = code.co_cellvars + code.co_freevars
        name = instr[1]
        if name in free:
            idx = free.index(name)
            if idx < len(closure):
                try:
                    return closure[idx].cell_contents
                except ValueError:
                    return _UNRESOLVABLE
        return _UNRESOLVABLE
    if op == "LOAD_GLOBAL":
        # As the function sees the name: its module's globals first (a module
        # may define its own `len`), then its builtins, of which only the
        # known ones resolve.
        name = instr[1]
        globs = getattr(func, "__globals__", None)
        if globs is not None and name in globs:
            return globs[name]
        known = _KNOWN_BUILTINS.get(name, _UNRESOLVABLE)
        builtins = getattr(func, "__builtins__", None)
        if isinstance(builtins, dict) and builtins.get(name) is not known:
            return _UNRESOLVABLE
        return known
    return _UNRESOLVABLE


def _clean_instructions(func):
    """Get cleaned instruction list: no noise opcodes, no RETURN_VALUE suffix.

    Returns a list of (opname, argval) tuples (see _fast_extract), or None
    for bytecode that can't be read: then nothing is recognised.
    """
    code = getattr(func, "__code__", None)
    if code is None or _OP_CACHE is None:
        return None
    try:
        return _fast_extract(code)
    except (IndexError, KeyError, TypeError):
        return None


def _analyze_simple(instrs, func):
    """Analyze a simple (non-compound) expression from cleaned instructions.

    Returns a tuple describing the pattern, or None.
    """
    if not instrs:
        return None

    n = len(instrs)

    # --- Single-step patterns ---

    # lambda x: x.attr  (AttrGetter)
    # LOAD_FAST, LOAD_ATTR(name), where LOAD_ATTR is NOT a method load
    if n == 2 and instrs[0][0] == "LOAD_FAST" and instrs[1][0] == "LOAD_ATTR":
        attr_name = instrs[1][1]
        # In 3.12+, LOAD_ATTR can be used for method lookup (argval may have extra info)
        # If it's a method load, the next instruction would be CALL — but we only have 2 instrs
        # so this is a plain attribute access
        return ("attrgetter", attr_name)

    # lambda x: x[k]  (ItemGetter)
    # LOAD_FAST, LOAD_CONST/LOAD_DEREF(k), BINARY_SUBSCR
    if n == 3 and instrs[0][0] == "LOAD_FAST" and instrs[2][0] == "BINARY_SUBSCR":
        key = _resolve_value(instrs[1], func)
        if key is not _UNRESOLVABLE:
            return ("itemgetter", key)

    # lambda x: not x
    # LOAD_FAST, UNARY_NOT
    if n == 2 and instrs[0][0] == "LOAD_FAST" and instrs[1][0] == "UNARY_NOT":
        return ("not",)

    # lambda x: not not x (compiled to a bare TO_BOOL)
    if n == 2 and instrs[0][0] == "LOAD_FAST" and instrs[1][0] == "TO_BOOL":
        return ("bool",)

    # lambda x: <builtin>(x) — len, str, int, float, bool
    # LOAD_GLOBAL(name), LOAD_FAST, CALL 1
    if n == 3 and instrs[0][0] == "LOAD_GLOBAL" and instrs[1][0] == "LOAD_FAST" and instrs[2][0] == "CALL":
        resolved = _resolve_value(instrs[0], func)
        if resolved is _builtins.len:
            return ("len",)
        if resolved is _builtins.str:
            return ("str",)
        if resolved is _builtins.int:
            return ("int",)
        if resolved is _builtins.float:
            return ("float",)
        if resolved is _builtins.bool:
            return ("bool",)
        if resolved is _builtins.type:
            return ("type",)
        if resolved is _builtins.tuple:
            return ("tuple",)
        if resolved is _builtins.list:
            return ("list",)
        if resolved is _builtins.set:
            return ("set",)
        if resolved is _builtins.frozenset:
            return ("frozenset",)
        # Arbitrary callable: lambda x: MyClass(x) → unwrap
        if resolved is not _UNRESOLVABLE and callable(resolved):
            return ("unwrap", resolved)

    # lambda x: isinstance(x, T)
    # LOAD_GLOBAL(isinstance), LOAD_FAST, LOAD_GLOBAL/LOAD_DEREF/LOAD_CONST(T), CALL 2
    if n == 4 and instrs[0][0] == "LOAD_GLOBAL" and instrs[1][0] == "LOAD_FAST" and instrs[3][0] == "CALL":
        resolved_func = _resolve_value(instrs[0], func)
        if resolved_func is _builtins.isinstance:
            type_obj = _resolve_value(instrs[2], func)
            if type_obj is not _UNRESOLVABLE:
                return ("isinstance", type_obj)
        if resolved_func is _builtins.hasattr:
            attr_name = _resolve_value(instrs[2], func)
            if attr_name is not _UNRESOLVABLE:
                return ("hasattr", attr_name)

    # lambda x: x is <value>
    # LOAD_FAST, LOAD_CONST/LOAD_DEREF(val), IS_OP(0)
    if n == 3 and instrs[0][0] == "LOAD_FAST" and instrs[2][0] == "IS_OP":
        val = _resolve_value(instrs[1], func)
        if val is not _UNRESOLVABLE:
            if instrs[2][1] == 0:  # is
                return ("is", val)
            else:  # is not
                return ("is_not", val)

    # --- Comparison: lambda x: x > 0 ---
    # LOAD_FAST, LOAD_CONST/LOAD_DEREF(val), COMPARE_OP(op)
    if n == 3 and instrs[0][0] == "LOAD_FAST" and instrs[2][0] == "COMPARE_OP":
        val = _resolve_value(instrs[1], func)
        if val is not _UNRESOLVABLE:
            op_name = instrs[2][1]
            if op_name in ("==", "!=", "<", ">", "<=", ">="):
                return ("compare", op_name, val)

    # --- BinOp: lambda x: x + 1 ---
    # LOAD_FAST, LOAD_CONST/LOAD_DEREF(val), BINARY_OP(op)
    if n == 3 and instrs[0][0] == "LOAD_FAST" and instrs[2][0] == "BINARY_OP":
        val = _resolve_value(instrs[1], func)
        if val is not _UNRESOLVABLE:
            op_name = instrs[2][1]
            if op_name in ("+", "-", "*", "/", "//", "%", "**"):
                return ("binop", op_name, val)

    # --- Contains: lambda x: x in collection ---
    # LOAD_FAST, LOAD_DEREF/LOAD_CONST/LOAD_GLOBAL(coll), CONTAINS_OP
    if n == 3 and instrs[0][0] == "LOAD_FAST" and instrs[2][0] == "CONTAINS_OP":
        coll = _resolve_value(instrs[1], func)
        if coll is not _UNRESOLVABLE:
            if instrs[2][1] == 0:
                return ("contains", coll)
            else:
                return ("not_contains", coll)

    # --- Method call with 0 args: lambda x: x.strip() ---
    # LOAD_FAST, LOAD_ATTR(method), CALL 0
    if n == 3 and instrs[0][0] == "LOAD_FAST" and instrs[1][0] == "LOAD_ATTR" and instrs[2][0] == "CALL":
        method_name = instrs[1][1]
        return ("method0", method_name)

    # --- Method call with 1 arg: lambda x: x.startswith('foo') ---
    # LOAD_FAST, LOAD_ATTR(method), LOAD_CONST/LOAD_DEREF(arg), CALL 1
    if n == 4 and instrs[0][0] == "LOAD_FAST" and instrs[1][0] == "LOAD_ATTR" and instrs[3][0] == "CALL":
        method_name = instrs[1][1]
        arg = _resolve_value(instrs[2], func)
        if arg is not _UNRESOLVABLE:
            return ("method1", method_name, arg)

    # --- Method call with 2 args: lambda x: x.replace('a', 'b') ---
    # LOAD_FAST, LOAD_ATTR(method), LOAD_CONST/LOAD_DEREF(arg1), LOAD_CONST/LOAD_DEREF(arg2), CALL 2
    if n == 5 and instrs[0][0] == "LOAD_FAST" and instrs[1][0] == "LOAD_ATTR" and instrs[4][0] == "CALL":
        method_name = instrs[1][1]
        arg1 = _resolve_value(instrs[2], func)
        arg2 = _resolve_value(instrs[3], func)
        if arg1 is not _UNRESOLVABLE and arg2 is not _UNRESOLVABLE:
            return ("method2", method_name, arg1, arg2)

    # --- Side-effect method call on closure/global: lambda _: obj.method() ---
    # LOAD_DEREF/LOAD_GLOBAL(obj), LOAD_ATTR(method), CALL 0
    # x (LOAD_FAST) is never referenced — the lambda ignores its argument.
    if (
        n == 3
        and instrs[0][0] in ("LOAD_DEREF", "LOAD_GLOBAL")
        and instrs[1][0] == "LOAD_ATTR"
        and instrs[2][0] == "CALL"
    ):
        obj = _resolve_value(instrs[0], func)
        if obj is not _UNRESOLVABLE:
            return ("call_method0", obj, instrs[1][1])

    # --- Side-effect method call passing x: lambda x: obj.method(x) ---
    # LOAD_DEREF/LOAD_GLOBAL(obj), LOAD_ATTR(method), LOAD_FAST(x), CALL 1
    # The lambda's argument is passed directly to the method.
    if (
        n == 4
        and instrs[0][0] in ("LOAD_DEREF", "LOAD_GLOBAL")
        and instrs[1][0] == "LOAD_ATTR"
        and instrs[2][0] == "LOAD_FAST"
        and instrs[3][0] == "CALL"
    ):
        obj = _resolve_value(instrs[0], func)
        if obj is not _UNRESOLVABLE:
            return ("call_method1", obj, instrs[1][1])

    # --- Slice: lambda x: x[start:stop] ---
    # LOAD_FAST, LOAD_CONST(start), LOAD_CONST(stop), BINARY_SLICE
    if n == 4 and instrs[0][0] == "LOAD_FAST" and instrs[3][0] == "BINARY_SLICE":
        start = _resolve_value(instrs[1], func)
        stop = _resolve_value(instrs[2], func)
        if start is not _UNRESOLVABLE and stop is not _UNRESOLVABLE:
            return ("slice", start, stop)

    # --- Chained patterns ---
    # These reference x once at the start and build a pipeline

    # lambda x: x.attr <op> val  (Chain: AttrGetter → Compare/BinOp/Contains)
    # LOAD_FAST, LOAD_ATTR(name), LOAD_CONST/LOAD_DEREF(val), COMPARE_OP/BINARY_OP/CONTAINS_OP/IS_OP
    if n == 4 and instrs[0][0] == "LOAD_FAST" and instrs[1][0] == "LOAD_ATTR":
        attr_name = instrs[1][1]
        val = _resolve_value(instrs[2], func)
        if val is not _UNRESOLVABLE:
            if instrs[3][0] == "COMPARE_OP":
                op_name = instrs[3][1]
                if op_name in ("==", "!=", "<", ">", "<=", ">="):
                    return ("chain", ("attrgetter", attr_name), ("compare", op_name, val))
            if instrs[3][0] == "BINARY_OP":
                op_name = instrs[3][1]
                if op_name in ("+", "-", "*", "/", "//", "%", "**"):
                    return ("chain", ("attrgetter", attr_name), ("binop", op_name, val))
            if instrs[3][0] == "CONTAINS_OP":
                if instrs[3][1] == 0:
                    return ("chain", ("attrgetter", attr_name), ("contains", val))
                else:
                    return ("chain", ("attrgetter", attr_name), ("not_contains", val))
            if instrs[3][0] == "IS_OP":
                if instrs[3][1] == 0:
                    return ("chain", ("attrgetter", attr_name), ("is", val))
                else:
                    return ("chain", ("attrgetter", attr_name), ("is_not", val))

    # lambda x: x[k] <op> val  (Chain: ItemGetter → Compare/etc.)
    # LOAD_FAST, LOAD_CONST/LOAD_DEREF(k), BINARY_SUBSCR, LOAD_CONST/LOAD_DEREF(val), COMPARE_OP/etc.
    if n == 5 and instrs[0][0] == "LOAD_FAST" and instrs[2][0] == "BINARY_SUBSCR":
        key = _resolve_value(instrs[1], func)
        val = _resolve_value(instrs[3], func)
        if key is not _UNRESOLVABLE and val is not _UNRESOLVABLE:
            if instrs[4][0] == "COMPARE_OP":
                op_name = instrs[4][1]
                if op_name in ("==", "!=", "<", ">", "<=", ">="):
                    return ("chain", ("itemgetter", key), ("compare", op_name, val))
            if instrs[4][0] == "BINARY_OP":
                op_name = instrs[4][1]
                if op_name in ("+", "-", "*", "/", "//", "%", "**"):
                    return ("chain", ("itemgetter", key), ("binop", op_name, val))
            if instrs[4][0] == "CONTAINS_OP":
                if instrs[4][1] == 0:
                    return ("chain", ("itemgetter", key), ("contains", val))
                else:
                    return ("chain", ("itemgetter", key), ("not_contains", val))
            if instrs[4][0] == "IS_OP":
                if instrs[4][1] == 0:
                    return ("chain", ("itemgetter", key), ("is", val))
                else:
                    return ("chain", ("itemgetter", key), ("is_not", val))

    # lambda x: len(x) > 0 etc.  (Chain: Len → Compare)
    # LOAD_GLOBAL(len), LOAD_FAST, CALL 1, LOAD_CONST(val), COMPARE_OP
    if n == 5 and instrs[0][0] == "LOAD_GLOBAL" and instrs[1][0] == "LOAD_FAST" and instrs[2][0] == "CALL":
        resolved_func = _resolve_value(instrs[0], func)
        if resolved_func in (
            _builtins.len,
            _builtins.str,
            _builtins.int,
            _builtins.float,
            _builtins.bool,
            _builtins.type,
        ):
            val = _resolve_value(instrs[3], func)
            if val is not _UNRESOLVABLE:
                first_op = {
                    _builtins.len: "len",
                    _builtins.str: "str",
                    _builtins.int: "int",
                    _builtins.float: "float",
                    _builtins.bool: "bool",
                    _builtins.type: "type",
                }[resolved_func]
                if instrs[4][0] == "COMPARE_OP":
                    op_name = instrs[4][1]
                    if op_name in ("==", "!=", "<", ">", "<=", ">="):
                        return ("chain", (first_op,), ("compare", op_name, val))
                if instrs[4][0] == "BINARY_OP":
                    op_name = instrs[4][1]
                    if op_name in ("+", "-", "*", "/", "//", "%", "**"):
                        return ("chain", (first_op,), ("binop", op_name, val))

    # --- Negate: lambda x: not <expr> ---
    # <expr instrs> + UNARY_NOT
    if n >= 2 and instrs[-1][0] == "UNARY_NOT":
        inner = _analyze_simple(instrs[:-1], func)
        if inner is not None:
            return ("negate", inner)

    # --- Tuple/list construction: (x["a"], x["b"]) or [x.name, x.age] ---
    # Multiple LOAD_FAST + sub-op groups ending with BUILD_TUPLE/BUILD_LIST
    if n >= 3 and instrs[-1][0] in ("BUILD_TUPLE", "BUILD_LIST"):
        count = instrs[-1][1]
        remaining = instrs[:-1]
        groups = []
        pos = 0
        for _ in range(count):
            if pos >= len(remaining) or remaining[pos][0] != "LOAD_FAST":
                break
            end = pos + 1
            while end < len(remaining) and remaining[end][0] != "LOAD_FAST":
                end += 1
            sub = _analyze_simple(remaining[pos:end], func)
            if sub is None:
                break
            groups.append(sub)
            pos = end
        if len(groups) == count and pos == len(remaining):
            tag = "build_tuple" if instrs[-1][0] == "BUILD_TUPLE" else "build_list"
            return (tag, *groups)

    return None


def _find_jump_split(instrs):
    """Find the outermost boolean operator's jump in instructions.

    For nested compounds like ``(A and B) or C``, the bytecode is flat:
    all operators become COPY + POP_JUMP_IF_FALSE/TRUE sequences. The only
    structural difference is the **jump target offset**: the outermost
    operator always jumps the farthest (to the end of the full expression),
    while inner operators jump to intermediate positions.

    Returns (jump_idx, is_and) or None.
    """
    best = None
    best_target = -1
    for i, instr in enumerate(instrs):
        # Jump arguments are distances (see _finish_instructions).
        if instr[0] == "POP_JUMP_IF_FALSE" and i + instr[1] > best_target:
            best_target = i + instr[1]
            best = (i, True)  # `and` short-circuits on False
        elif instr[0] == "POP_JUMP_IF_TRUE" and i + instr[1] > best_target:
            best_target = i + instr[1]
            best = (i, False)  # `or` short-circuits on True
    return best


def _analyze_compound(instrs, func):
    """Analyze compound (and/or) expressions.

    The bytecode for `A and B` is:
      <eval A>, COPY, [TO_BOOL], POP_JUMP_IF_FALSE, POP_TOP, <eval B>
    For `A or B`:
      <eval A>, COPY, [TO_BOOL], POP_JUMP_IF_TRUE, POP_TOP, <eval B>
    """
    split = _find_jump_split(instrs)
    if split is None:
        return None

    jump_idx, is_and = split

    # Left side: everything before COPY (which is 1 or 2 before the jump)
    # Find COPY before the jump
    copy_idx = None
    for i in range(jump_idx - 1, -1, -1):
        if instrs[i][0] == "COPY":
            copy_idx = i
            break
    if copy_idx is None:
        return None

    left_instrs = instrs[:copy_idx]

    # Right side: after POP_TOP (which follows the jump) up to the jump's
    # target, where the and/or expression ends. Whatever follows applies to
    # its result: `(a or b) * 10`, `not (a or b)`.
    pop_idx = jump_idx + 1
    if pop_idx >= len(instrs) or instrs[pop_idx][0] != "POP_TOP":
        return None
    end = jump_idx + instrs[jump_idx][1]
    if end <= pop_idx:
        return None
    right_instrs = instrs[pop_idx + 1 : end]

    # Recursively analyze each side
    left = _analyze_expr(left_instrs, func)
    right = _analyze_expr(right_instrs, func)

    if left is None or right is None:
        return None

    result = ("and" if is_and else "or", left, right)
    if end < len(instrs):
        steps = _postfix_steps(instrs[end:], func)
        if steps is None:
            return None
        for step in steps:
            result = ("chain", result, step)
    return result


def _analyze_expr(instrs, func):
    """Analyze an expression — tries compound first, then simple, then a
    longer chain of single steps."""
    # Try compound (and/or) first
    compound = _analyze_compound(instrs, func)
    if compound is not None:
        return compound
    simple = _analyze_simple(instrs, func)
    if simple is not None:
        return simple
    return _analyze_postfix(instrs, func)


_ITEM = ("LOAD_FAST", None)  # stands for the previous step's result


def _analyze_postfix(instrs, func):
    """The item, or builtin(item), followed by a sequence of steps that each
    take the previous result as their only input: `x % 2 == 0`,
    `x["a"]["b"]`, `x.a.b`, `str(x).upper()`, `x.strip().lower()`.

    Each step is matched by the single-step patterns (`_analyze_simple` on
    the step's instructions after a load of the item), longest first so a
    method call isn't read as an attribute. Returns nested ("chain", a, b)
    tuples, applied left to right as Python evaluates them, or None.
    """
    if len(instrs) >= 3 and instrs[0][0] == "LOAD_GLOBAL" and instrs[1][0] == "LOAD_FAST" and instrs[2] == ("CALL", 1):
        head = _analyze_simple(instrs[:3], func)  # len(x), str(x), ...
        if head is None:
            # A call that doesn't resolve (abs(x) from builtins) can't be
            # left out: the steps after it apply to its result.
            return None
        pos = 3
    elif instrs and instrs[0][0] == "LOAD_FAST":
        head, pos = None, 1
    else:
        return None
    rest = _postfix_steps(instrs[pos:], func)
    if rest is None:
        return None
    steps = ([head] if head is not None else []) + rest
    if len(steps) < 2:
        return None
    result = steps[-1]
    for step in reversed(steps[:-1]):
        result = ("chain", step, result)
    return result


def _postfix_steps(instrs, func):
    """`instrs` as single steps, each applied to the previous result, or None.

    A step that loads the item again (`(x.a, x.b)` after `x.a`) is not one:
    it would read the previous result instead of the item.
    """
    steps, pos = [], 0
    while pos < len(instrs):
        for size in range(min(5, len(instrs) - pos), 0, -1):
            chunk = instrs[pos : pos + size]
            if any(op == "LOAD_FAST" for op, _ in chunk):
                continue
            step = _analyze_simple([_ITEM, *chunk], func)
            if step is not None:
                steps.append(step)
                pos += size
                break
        else:
            return None
    return steps


# Flags for bail-out checks
_CO_GENERATOR = 0x20
_CO_COROUTINE = 0x100
_CO_ASYNC_GENERATOR = 0x200
_CO_VARARGS = 0x04
_CO_VARKEYWORDS = 0x08

# _analyze_lambda results by code object (_analyze_accumulate's are in
# _accumulate_cache), each with the values the analysis resolved from closures,
# defaults and globals. Another function with the same code (a
# request handler's fresh lambdas) reuses a result only if those resolve to the
# very same objects. Keyed by id(code) for speed (a WeakKeyDictionary hashes and
# compares code objects by value); each entry holds a weak reference that must
# still point at the same code, since a freed object's id can be reused, and
# whose callback removes the entry when the code dies.
_analysis_cache = {}
_accumulate_cache = {}
# Results kept per code object: a closure over per-call values (a threshold, a
# lookup table) adds one per distinct binding, so the oldest are dropped.
_MAX_RESULTS_PER_CODE = 8

# A cached result keeps the values it was resolved from alive. Only values
# that are cheap to keep and don't hold user data are cached: a result that
# captured a Stream (an emit target), a list or an instance is analysed again
# on every compile() instead of pinning its graph or its data.
_PINNABLE_TYPES = frozenset((type(None), bool, int, float, complex, str, bytes, range, _BuiltinFunctionType))
_MAX_PINNED_ITEMS = 64


def _pinnable(value):
    """Whether a cached result may keep `value` alive (see above)."""
    kind = type(value)
    if kind in _PINNABLE_TYPES or isinstance(value, type):
        return True
    if kind is _FunctionType:  # a module-level function, not a closure over data
        return value.__closure__ is None
    if kind is tuple or kind is frozenset:
        return len(value) <= _MAX_PINNED_ITEMS and all(_pinnable(v) for v in value)
    return False


def _remember(results, deps, result):
    """Cache `result` for `deps` if keeping them alive is harmless."""
    if all(_pinnable(value) for _, value in deps):
        results.append((deps, result))
        del results[:-_MAX_RESULTS_PER_CODE]


# Maximum bytecode length for analyzable patterns.
# The most complex analyzable pattern (compound and/or with chained sub-expressions)
# is ~80 bytes. Anything longer is definitely too complex for our pattern matcher.
_MAX_BYTECODE_LEN = 80


def _analyze_lambda(func):
    """Analyze bytecode of a single-arg function to detect optimizable patterns.

    Returns a tuple describing the pattern, or None:
      ("itemgetter", key), ("attrgetter", "name"), ("len",), ("str",),
      ("int",), ("float",), ("bool",), ("not",), ("is", value),
      ("is_not", value), ("isinstance", type_obj),
      ("compare", ">", value), ("binop", "+", value),
      ("contains", collection), ("not_contains", collection),
      ("method0", "strip"), ("method1", "startswith", "foo"),
      ("chain", first_tuple, second_tuple),
      ("negate", inner_tuple),
      ("and", left_tuple, right_tuple), ("or", left_tuple, right_tuple),
      ("unwrap", resolved_callable)
    """
    # Only plain Python functions: a bound method's code also has __code__,
    # but its first parameter is `self`, not the item.
    if type(func) is not _FunctionType:
        return None
    code = func.__code__
    results = _cached_results(code)
    for deps, result in results:
        if _bindings_match(deps, func):
            return result
    ctx = _Bindings(func)
    result = _analyze_code(func, code, ctx)
    _remember(results, tuple(ctx.deps), result)
    return result


def _analyze_for_compile(func, accumulate):
    """compile()'s analysis of a plain function: (result, code_only, is_lambda, kept).

    code_only: the result depends on nothing but func.__code__ (no closure,
    default or global value was read), so compile() reuses it for every
    function with that code without calling here again.
    kept: the result is cached here, so the same object comes back for the
    same function: compile() may share what it builds from it.
    """
    code = func.__code__
    is_lambda = code.co_name == "<lambda>"
    if not _ANALYZER_ON:
        _warn_analyzer_off()
        return None, True, is_lambda, True
    if accumulate:
        result, cache = _analyze_accumulate(func), _accumulate_cache
    else:
        result, cache = _analyze_lambda(func), _analysis_cache
    kept = [deps for deps, r in _cached_results(code, cache) if r is result]
    code_only = any(not deps for deps in kept)
    return result, code_only, is_lambda, bool(kept)


def _cached_results(code, cache=_analysis_cache):
    """The cached (deps, result) list for `code`, created empty if needed."""
    key = id(code)
    entry = cache.get(key)
    if entry is None or entry[0]() is not code:
        entry = (weakref.ref(code, lambda _, key=key: cache.pop(key, None)), [])
        cache[key] = entry
    return entry[1]


def _bindings_match(deps, func):
    # A loop, not all(<genexpr>): this runs on every compile() and the
    # generator costs more than the (usually empty) check.
    for instr, value in deps:  # noqa: SIM110
        if _resolve(instr, func) is not value:
            return False
    return True


def _bind_defaults(instrs, code):
    """Rewrite loads of defaulted parameters as LOAD_DEFAULT, so only the item
    is LOAD_FAST; split LOAD_FAST_LOAD_FAST (two locals) into two loads."""
    item = code.co_varnames[0]
    out = []
    for op, av in instrs:
        if op == "LOAD_FAST_LOAD_FAST":
            names = (code.co_varnames[av >> 4], code.co_varnames[av & 15])
        elif op == "LOAD_FAST":
            names = (av,)
        else:
            out.append((op, av))
            continue
        out.extend(("LOAD_FAST", n) if n == item else ("LOAD_DEFAULT", n) for n in names)
    return out


def _analyze_code(func, code, ctx):
    """Uncached body of _analyze_lambda."""
    # One item parameter. Further positional parameters are fine if they all
    # have defaults (the `lambda x, k=k:` idiom for capturing a loop variable):
    # a node calls with one argument, so they are constants.
    extra = code.co_argcount - 1
    if extra < 0 or extra > len(getattr(func, "__defaults__", None) or ()) or code.co_kwonlyargcount:
        return None
    flags = code.co_flags
    if flags & (_CO_GENERATOR | _CO_COROUTINE | _CO_ASYNC_GENERATOR | _CO_VARARGS | _CO_VARKEYWORDS):
        return None

    # Pre-filter: reject obviously complex bytecodes before expensive disassembly
    if len(code.co_code) > _MAX_BYTECODE_LEN:
        return None

    # Get cleaned instructions
    clean = _clean_instructions(func)
    if clean is None:
        return None

    # Complexity guard
    if len(clean) > 25:
        return None

    # Reject multi-statement functions
    if any(i[0] == "STORE_FAST" for i in clean):
        return None

    # Reject exception handling
    if any(i[0] in _EXCEPTION_OPCODES for i in clean):
        return None

    # Reject multiple LOAD_FAST (references x more than once in non-compound)
    # For compound expressions, each sub-expression can reference x once
    # We'll let the recursive analyzer handle this naturally

    return _analyze_expr(_bind_defaults(clean, code), ctx)


_ACCUMULATE_OPS = frozenset(("+", "-", "*", "/", "//", "%", "**"))


def _analyze_accumulate(func):
    """Recognise an accumulate function `f(acc, x)` that has a native form.

    Returns ("acc_binop", op) for `acc <op> x`, ("acc_binop_const", op, value)
    for `acc <op> value`, ("acc_max",) / ("acc_min",) for `max(acc, x)` /
    `min(acc, x)`, or None. Like the map/filter analysis this runs at compile()
    time, snapshots closure and global values, and is cached per code object.
    """
    if type(func) is not _FunctionType:
        return None
    code = func.__code__
    results = _cached_results(code, _accumulate_cache)
    for deps, result in results:
        if _bindings_match(deps, func):
            return result
    ctx = _Bindings(func)
    result = _analyze_accumulate_code(func, code, ctx)
    _remember(results, tuple(ctx.deps), result)
    return result


def _analyze_accumulate_code(func, code, ctx):
    """Uncached body of _analyze_accumulate."""
    if code.co_argcount != 2 or code.co_kwonlyargcount:
        return None
    if code.co_flags & (_CO_GENERATOR | _CO_COROUTINE | _CO_ASYNC_GENERATOR | _CO_VARARGS | _CO_VARKEYWORDS):
        return None
    if len(code.co_code) > _MAX_BYTECODE_LEN:
        return None
    clean = _clean_instructions(func)
    if not clean or len(clean) > 6:
        return None
    acc, item = code.co_varnames[:2]
    instrs = []
    for op, av in clean:
        if op == "LOAD_FAST_LOAD_FAST":
            instrs += [("LOAD_FAST", code.co_varnames[av >> 4]), ("LOAD_FAST", code.co_varnames[av & 15])]
        else:
            instrs.append((op, av))
    ops = [op for op, _ in instrs]
    if ops == ["LOAD_FAST", "LOAD_FAST", "BINARY_OP"] and instrs[2][1] in _ACCUMULATE_OPS:
        if (instrs[0][1], instrs[1][1]) == (acc, item):
            return ("acc_binop", instrs[2][1])
        return None
    if ops[0::2] == ["LOAD_FAST", "BINARY_OP"] and len(ops) == 3 and instrs[0][1] == acc:
        value = _resolve_value(instrs[1], ctx)
        if value is not _UNRESOLVABLE and instrs[2][1] in _ACCUMULATE_OPS:
            return ("acc_binop_const", instrs[2][1], value)
        return None
    if ops == ["LOAD_GLOBAL", "LOAD_FAST", "LOAD_FAST", "CALL"] and instrs[3][1] == 2:
        if (instrs[1][1], instrs[2][1]) != (acc, item):
            return None
        fn = _resolve_value(instrs[0], ctx)
        if fn is _builtins.max:
            return ("acc_max",)
        if fn is _builtins.min:
            return ("acc_min",)
    return None


def _reject_async(func, op):
    """Raise TypeError for async callables: akayu never awaits coroutines."""
    call = type(func).__call__ if callable(func) else None  # an object with async def __call__
    if (
        inspect.iscoroutinefunction(func)
        or inspect.isasyncgenfunction(func)
        or (not inspect.isroutine(func) and (inspect.iscoroutinefunction(call) or inspect.isasyncgenfunction(call)))
    ):
        name = getattr(func, "__qualname__", None) or repr(func)
        raise TypeError(
            f"{op}() got async function {name!r}, but akayu does not run coroutines. "
            "Pass a regular function; use prefetch() or par() for concurrent I/O."
        )


def _build_starmap_func(func, args, kwargs):
    # args is tuple or None, kwargs is dict or None
    if not args and not kwargs:

        def wrapper_simple(x):
            return func(*x)

        return wrapper_simple

    if kwargs:
        if args:

            def wrapper(x):
                return func(*x, *args, **kwargs)

            return wrapper
        else:

            def wrapper(x):
                return func(*x, **kwargs)

            return wrapper
    else:

        def wrapper(x):
            return func(*x, *args)

        return wrapper


class _Profiler:
    """Context manager for profiling a pipeline.

    Usage:
        with source.profile() as p:
            source.emit(42)
        print(p.report)
    """

    def __init__(self, source):
        self._source = source
        self.report = None

    def __enter__(self):
        self._source.start_profiling()
        return self

    def __exit__(self, *exc):
        self.report = self._source.stop_profiling()
        return False
