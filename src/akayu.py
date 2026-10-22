import ast
import builtins as _builtins
import dis
import inspect
import linecache
import os
import sys
import textwrap
import warnings
import weakref
from types import BuiltinFunctionType as _BuiltinFunctionType
from types import FunctionType as _FunctionType
from types import ModuleType as _ModuleType

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
# COMPARE_OP's flag (3.13+) for a comparison that converts its result to bool.
_COMPARE_TO_BOOL = 16

# Opcodes a newer Python spells differently, as the patterns below spell
# them. (3.14 also has LOAD_SMALL_INT, whose oparg is the int itself, and
# spells BINARY_SUBSCR as BINARY_OP []: see _fast_extract.)
_RESPELLED = {
    "LOAD_FAST_BORROW": "LOAD_FAST",  # 3.14
    "LOAD_FAST_BORROW_LOAD_FAST_BORROW": "LOAD_FAST_LOAD_FAST",  # 3.14
}

# The operators the native ops support (BinaryOp and CompareOp in Rust).
_BINARY_OPS = frozenset(("+", "-", "*", "/", "//", "%", "**", "<<", ">>", "&", "|", "^"))
_COMPARE_OPS = frozenset(("==", "!=", "<", ">", "<=", ">="))


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
        name = _RESPELLED.get(name, name)
        if name == "LOAD_SMALL_INT":  # 3.14: the int is the oparg itself
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
        if name == "COMPARE_OP" and arg & _COMPARE_TO_BOOL:
            # The result converted to bool, as a TO_BOOL after it would:
            # `not not (x > c)` compiles to this alone.
            result.append(("TO_BOOL", 0))
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
    "abs": _builtins.abs,
}

_UNRESOLVABLE = object()  # sentinel


class _Bindings:
    """The function under analysis, plus every value resolved from it.

    The analyzer passes this where it used to pass the function; the recorded
    values decide whether a cached result applies to another function with
    the same code (see _analyze_lambda).
    """

    __slots__ = ("deps", "func", "memo")

    def __init__(self, func):
        self.func = func
        self.deps = []
        self.memo = {}  # expression analyses by instructions (see _analyze_expr)


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

    # lambda x: -x, ~x, +x
    # LOAD_FAST, UNARY_NEGATIVE / UNARY_INVERT / CALL_INTRINSIC_1 (positive)
    if n == 2 and instrs[0][0] == "LOAD_FAST":
        if instrs[1][0] in _UNARY_OPCODES:
            return ("unary", _UNARY_OPCODES[instrs[1][0]])
        if instrs[1] == ("CALL_INTRINSIC_1", _INTRINSIC_UNARY_POSITIVE):
            return ("unary", "+")

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
        if resolved is _builtins.abs:
            return ("unary", "abs")
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
            if op_name in _COMPARE_OPS:
                return ("compare", op_name, val)

    # --- BinOp: lambda x: x + 1 ---
    # LOAD_FAST, LOAD_CONST/LOAD_DEREF(val), BINARY_OP(op)
    if n == 3 and instrs[0][0] == "LOAD_FAST" and instrs[2][0] == "BINARY_OP":
        val = _resolve_value(instrs[1], func)
        if val is not _UNRESOLVABLE:
            op_name = instrs[2][1]
            if op_name in _BINARY_OPS:
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

    # --- Negate: lambda x: not <expr> ---
    # <expr instrs> + UNARY_NOT
    if n >= 2 and instrs[-1][0] == "UNARY_NOT":
        inner = _analyze_steps(instrs[:-1], func)
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
            sub = _analyze_steps(remaining[pos:end], func)
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
    longer chain of single steps, then operators on other expressions.

    Results are remembered for the analysis (`func` is its _Bindings): the
    operator patterns analyse parts of the instructions, and the same parts
    again for each way they might split."""
    try:
        key = tuple(instrs)
        return func.memo[key]
    except KeyError:
        pass
    except TypeError:  # an unhashable constant: not remembered
        key = None
    result = (
        _analyze_compound(instrs, func)
        or _analyze_simple(instrs, func)
        or _analyze_postfix(instrs, func)
        or _analyze_operators(instrs, func)
    )
    if key is not None:
        func.memo[key] = result
    return result


_ITEM = ("LOAD_FAST", None)  # stands for the previous step's result


def _analyze_steps(instrs, func):
    """One step on the item, or a chain of them (no and/or)."""
    return _analyze_simple(instrs, func) or _analyze_postfix(instrs, func)


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


# How many values each instruction of an analysable expression leaves on the
# stack, counting a call's callable as one value (its NULL or self slot is
# left out): an expression leaves one. CALL and BUILD_* depend on their count.
_STACK_EFFECT = {
    **dict.fromkeys(("LOAD_FAST", "LOAD_CONST", "LOAD_GLOBAL", "LOAD_DEREF", "LOAD_DEFAULT", "COPY"), 1),
    **dict.fromkeys(("LOAD_ATTR", "UNARY_NEGATIVE", "UNARY_INVERT", "UNARY_NOT", "TO_BOOL", "CALL_INTRINSIC_1"), 0),
    **dict.fromkeys(("BINARY_OP", "BINARY_SUBSCR", "COMPARE_OP", "CONTAINS_OP", "IS_OP", "POP_TOP"), -1),
    **dict.fromkeys(("POP_JUMP_IF_FALSE", "POP_JUMP_IF_TRUE"), -1),
    "BINARY_SLICE": -2,
}
_UNARY_OPCODES = {"UNARY_NEGATIVE": "-", "UNARY_INVERT": "~"}
_INTRINSIC_UNARY_POSITIVE = 5  # CALL_INTRINSIC_1's argument for `+x`
# Builtins taking the item alone that have native forms (see _analyze_simple).
_ONE_ARG_BUILTINS = frozenset(
    ("len", "str", "int", "float", "bool", "type", "tuple", "list", "set", "frozenset", "abs")
)


def _depths(instrs):
    """The stack depth after each instruction (falling through any jump),
    or None for an instruction no pattern handles."""
    depth, depths = 0, []
    for op, arg in instrs:
        if op == "CALL":
            effect = -arg
        elif op in ("BUILD_TUPLE", "BUILD_LIST"):
            effect = 1 - arg
        else:
            effect = _STACK_EFFECT.get(op)
            if effect is None:
                return None
        depth += effect
        depths.append(depth)
    return depths


def _operand(instrs, func):
    """One operand of an operator: ("item",) for the item itself, ("const",
    value) for a value that doesn't depend on it, an expression of it (its
    analysis), or None."""
    if len(instrs) == 1:
        op = instrs[0][0]
        if op == "LOAD_FAST":
            return ("item",)
        if op in ("LOAD_CONST", "LOAD_GLOBAL", "LOAD_DEREF", "LOAD_DEFAULT"):
            value = _resolve_value(instrs[0], func)
            return None if value is _UNRESOLVABLE else ("const", value)
        return None
    return _analyze_expr(instrs, func)


def _analyze_operators(instrs, func):
    """Operators on expressions of the item, beyond the item and a constant:
    `2 * x`, `0 < x`, `x * x`, `x.a + x.b`, `TABLE[x]`, `TABLE[x.key]`,
    `-(x + 1)`, `abs(x - 1)`, `not 2 * x`, and steps after any of these
    (`TABLE[x]["a"]`, `(2 * x).bit_length()`).

    An operator's operands are found by the stack depth: the left one ends
    where one value is on the stack. That point is unique for a valid
    split (an expression never empties the stack it started on), but and/or
    inside an operand can bring the depth back to one, so each such point is
    tried; only the right one analyses on both sides."""
    depths = _depths(instrs)
    if depths is None or len(instrs) < 2 or depths[-1] != 1:
        return None
    op, arg = instrs[-1]
    unary = _UNARY_OPCODES.get(op) or ("+" if (op, arg) == ("CALL_INTRINSIC_1", _INTRINSIC_UNARY_POSITIVE) else None)
    if unary is not None:
        inner = _analyze_expr(instrs[:-1], func)
        return None if inner is None else ("chain", inner, ("unary", unary))
    if op == "UNARY_NOT":
        inner = _analyze_expr(instrs[:-1], func)
        return None if inner is None else ("negate", inner)
    if op == "CALL" and arg == 1 and instrs[0][0] == "LOAD_GLOBAL" and instrs[0][1] in _ONE_ARG_BUILTINS:
        inner = _analyze_expr(instrs[1:-1], func)
        call = _analyze_simple([instrs[0], _ITEM, instrs[-1]], func)
        if inner is not None and call is not None and call[0] != "unwrap":
            return ("chain", inner, call)
    if (
        (op == "BINARY_OP" and arg in _BINARY_OPS)
        or (op == "COMPARE_OP" and arg in _COMPARE_OPS)
        or op == "BINARY_SUBSCR"
    ):
        for k in (i + 1 for i, depth in enumerate(depths[:-2]) if depth == 1):
            left = _operand(instrs[:k], func)
            right = _operand(instrs[k:-1], func) if left is not None else None
            if right is not None:
                return _operator(op, arg, left, right)
    # Steps after an operator: `TABLE[x]["a"]` is TABLE[x], then ["a"].
    for k in (i + 1 for i, depth in enumerate(depths[:-1]) if depth == 1):
        if instrs[k - 1][0] in ("LOAD_FAST", "LOAD_CONST", "LOAD_GLOBAL", "LOAD_DEREF", "LOAD_DEFAULT"):
            continue  # the item or a value: the postfix patterns' heads
        head = _analyze_expr(instrs[:k], func)
        steps = _postfix_steps(instrs[k:], func) if head is not None else None
        if steps:
            for step in steps:
                head = ("chain", head, step)
            return head
    return None


def _operator(op, arg, left, right):
    """The analysis of `left <op> right` (operands from _operand), or None."""
    left_const, right_const = left[0] == "const", right[0] == "const"
    if left_const and right_const:
        return None  # nothing depends on the item
    if right_const or left_const:
        # An expression of the item, then the operator with the constant on
        # its side (the constant's load has no effect to keep in order).
        c = right[1] if right_const else left[1]
        expr = left if right_const else right
        if op == "BINARY_SUBSCR":
            step = ("itemgetter", c) if right_const else ("getitem_from", c)
        elif op == "COMPARE_OP":
            step = ("compare" if right_const else "rcompare", arg, c)
        else:
            step = ("binop" if right_const else "rbinop", arg, c)
        return step if expr == ("item",) else ("chain", expr, step)
    if op == "BINARY_SUBSCR":
        return None  # x[x.key]: rare enough to leave as Python
    return ("binop2" if op == "BINARY_OP" else "compare2", arg, left, right)


# --- numpy expressions (src/arrays.rs) ---
# A lambda made of numpy operations on its arguments also translates into a
# program that src/arrays.rs runs with numpy's own loops on small float64
# arrays: a tuple of ops, each (name, *parameters, *operands). An operand is
# ("s",), the value an earlier op left on the stack, ("arg", i), ("num", f)
# for a float or int constant (a Python scalar), or ("obj", value) for a
# constant ndarray or numpy float64. The ops: "+", "-", "*", "/" and "@" (a,
# b), "neg", "T", "norm", "trace", "frombuffer" and "tobytes" (the last op)
# (a), "clip" (a, lo, hi), ("index", key, a), ("reshape", shape, a) and
# ("ufunc", name, *operands) for any ufunc src/arrays.rs has a loop for.

_MAX_ARRAY_INSTRUCTIONS = 48
_ARRAY_OPERATORS = frozenset(("+", "-", "*", "/", "@"))
# numpy's functions with ops, by their number of arguments.
_ARRAY_FUNCTION_ARGS = {"norm": 1, "trace": 1, "clip": 3, "frombuffer": 1}
_array_functions = (None, {})  # (the numpy module, {function: op name})


def _numpy_functions(np):
    """numpy's functions (not ufuncs) that have ops, by object."""
    global _array_functions  # noqa: PLW0603 - a cache for the numpy module
    if _array_functions[0] is not np:
        try:
            table = {np.linalg.norm: "norm", np.trace: "trace", np.clip: "clip", np.frombuffer: "frombuffer"}
        except AttributeError:
            table = {}
        _array_functions = (np, table)
    return _array_functions[1]


def _array_operand(entry, np):
    """A stack entry of _analyze_arrays as an operand (see above), or None."""
    if entry[0] in ("s", "arg"):
        return entry
    if entry[0] != "ct":
        return None
    value = entry[1]
    kind = type(value)
    if kind is float:
        return ("num", value)
    if kind is int and abs(value) <= 2**53:
        return ("num", float(value))
    if kind is np.float64 or kind is np.ndarray:
        return ("obj", value)
    return None


def _full_slice(key):
    return type(key) is slice and key.start is None and key.stop is None and key.step is None


def _array_key(key):
    """An index the ops take: a[i], a[i, j], a[:, j], a[i, :] or a[:]."""
    if type(key) is int:
        return ("int", key)
    if _full_slice(key):
        return ("all",)
    if type(key) is tuple and len(key) == 2:
        i, j = key
        if type(i) is int and type(j) is int:
            return ("pair", i, j)
        if _full_slice(i) and type(j) is int:
            return ("col", j)
        if type(i) is int and _full_slice(j):
            return ("row", i)
    return None


def _array_call(callee, args, np):
    """The op for a call of `callee` with `args` (stack entries), or None."""
    if callee[0] == "meth":
        _, name, receiver = callee
        if name == "reshape":
            if len(args) == 1 and args[0][0] == "ct" and type(args[0][1]) is tuple:
                shape = args[0][1]
            else:
                shape = tuple(entry[1] if entry[0] == "ct" else None for entry in args)
            if 1 <= len(shape) <= 2 and all(type(d) is int for d in shape):
                return ("reshape", shape, receiver)
            return None
        return ("tobytes", receiver) if name == "tobytes" and not args else None
    if callee[0] != "fn":
        return None
    if all(entry[0] == "ct" and type(entry[1]) is int for entry in args):
        return None  # ints alone: numpy's int loop, not the float64 one
    operands = [_array_operand(entry, np) for entry in args]
    if None in operands:
        return None
    func = callee[1]
    if type(func) is np.ufunc:
        return ("ufunc", func.__name__, *operands) if func.nout == 1 and func.nin == len(args) else None
    try:
        name = _numpy_functions(np).get(func)
    except TypeError:  # unhashable
        return None
    if name is None or _ARRAY_FUNCTION_ARGS[name] != len(args):
        return None
    return (name, *operands)


def _analyze_arrays(func, code, ctx, nargs):
    """The numpy expression of its first `nargs` arguments `func` returns, as
    a program for src/arrays.rs (see above), or None.

    The bytecode runs abstractly: each stack entry is a value an op computes
    ("s",), an argument ("arg", i), a constant ("ct", value), a module or
    function ("fn", value), or a method of a value ("meth", name, operand),
    and each instruction becomes an op or is resolved now."""
    np = sys.modules.get("numpy")
    if np is None:
        return None  # nothing can be an ndarray
    instrs = _clean_instructions(func)
    if instrs is None or len(instrs) > _MAX_ARRAY_INSTRUCTIONS:
        return None
    found = _Bindings(func)
    program = _translate_arrays(code, instrs, nargs, np, found)
    if program is not None:
        # The values it was translated from decide whether a cached result
        # applies to another function. Without a program they don't: a
        # cache that keeps them (a user's function) would keep them alive.
        ctx.deps.extend(found.deps)
    return program


def _translate_arrays(code, instrs, nargs, np, ctx):
    """_analyze_arrays' translation of `instrs` (values resolved into `ctx`)."""
    params = code.co_varnames[: code.co_argcount]
    stack, ops = [], []
    for op, av in instrs:
        if op in ("LOAD_FAST", "LOAD_FAST_LOAD_FAST"):
            names = (av,) if op == "LOAD_FAST" else (code.co_varnames[av >> 4], code.co_varnames[av & 15])
            for name in names:
                if name not in params:
                    return None
                i = params.index(name)
                if i < nargs:
                    stack.append(("arg", i))
                    continue
                value = _resolve_value(("LOAD_DEFAULT", name), ctx)  # a defaulted parameter
                if value is _UNRESOLVABLE:
                    return None
                stack.append(("ct", value))
        elif op == "LOAD_CONST":
            stack.append(("ct", av))
        elif op in ("LOAD_GLOBAL", "LOAD_DEREF"):
            value = _resolve_value((op, av), ctx)
            if value is _UNRESOLVABLE:
                return None
            stack.append(("fn" if isinstance(value, _ModuleType) or callable(value) else "ct", value))
        elif op == "LOAD_ATTR" and stack:
            entry = stack.pop()
            if entry[0] == "fn" and isinstance(entry[1], _ModuleType):
                # numpy's function or submodule (np.linalg.norm); no other
                # module's attributes are looked up here.
                name = getattr(entry[1], "__name__", None)
                if type(name) is not str or not (name == "numpy" or name.startswith("numpy.")):
                    return None
                value = getattr(entry[1], av, _UNRESOLVABLE)
                if value is _UNRESOLVABLE:
                    return None
                stack.append(("fn", value))
                continue
            operand = _array_operand(entry, np)
            if operand is None or operand[0] == "num":
                return None
            if av == "T":
                ops.append(("T", operand))
                stack.append(("s",))
            else:
                stack.append(("meth", av, operand))
        elif op in ("BINARY_OP", "UNARY_NEGATIVE", "BINARY_SUBSCR"):
            arity = 1 if op == "UNARY_NEGATIVE" else 2
            if len(stack) < arity or (op == "BINARY_OP" and av not in _ARRAY_OPERATORS):
                return None
            last = stack.pop()
            a = _array_operand(stack.pop(), np) if arity == 2 else _array_operand(last, np)
            if op == "BINARY_SUBSCR":
                key = _array_key(last[1]) if last[0] == "ct" else None
                if key is None or a is None or a[0] == "num":
                    return None
                ops.append(("index", key, a))
            elif op == "UNARY_NEGATIVE":
                if a is None or a[0] == "num":
                    return None
                ops.append(("neg", a))
            else:
                b = _array_operand(last, np)
                if a is None or b is None or (a[0] == "num" and b[0] == "num"):
                    return None
                ops.append((av, a, b))
            stack.append(("s",))
        elif op in ("BUILD_SLICE", "BUILD_TUPLE"):
            parts = stack[len(stack) - av :]
            if not 1 <= av <= len(stack) or any(entry[0] != "ct" for entry in parts):
                return None
            del stack[len(stack) - av :]
            values = tuple(entry[1] for entry in parts)
            stack.append(("ct", slice(*values) if op == "BUILD_SLICE" else values))
        elif op == "CALL" and av < len(stack):
            args = stack[len(stack) - av :]
            del stack[len(stack) - av :]
            new = _array_call(stack.pop(), args, np)
            if new is None:
                return None
            ops.append(new)
            stack.append(("s",))
        else:
            return None
    if stack != [("s",)] or any(op[0] == "tobytes" for op in ops[:-1]):
        return None
    return tuple(ops)


def _analyze_star_for_compile(func):
    """compile()'s analysis of a starmap function: (number of arguments,
    program) for a numpy expression of them (see _analyze_arrays), or None."""
    if not _ANALYZER_ON or type(func) is not _FunctionType or "numpy" not in sys.modules:
        return None
    code = func.__code__
    nargs = code.co_argcount
    if nargs < 1 or not _takes(func, code, nargs):
        return None
    program = _analyze_arrays(func, code, _Bindings(func), nargs)
    return None if program is None else (nargs, program)


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
    if kind in _PINNABLE_TYPES or isinstance(value, (type, _ModuleType)):
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
# And of instructions, without the noise (see _clean_instructions).
_MAX_INSTRUCTIONS = 25


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

    compile() also asks for a numpy program (see _analyze_function).
    """
    result = _analyze_function(func)
    if type(result) is tuple and result[0] == "arrays":
        return result[1]
    return result


def _analyze_function(func):
    """_analyze_lambda's result, or ("arrays", that result, program) for a
    function that also has a numpy program (see _analyze_arrays): the
    program runs for ndarrays, the result for anything else."""
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
    if _takes(func, code, 1):
        # For ndarrays, a numpy program too (the result stays for anything else).
        program = _analyze_arrays(func, code, ctx, 1)
        if program is not None:
            result = ("arrays", result, program)
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
        result, cache = _analyze_function(func), _analysis_cache
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


def _takes(func, code, nargs):
    """Whether a node calling `func` with `nargs` arguments calls its plain
    body: further positional parameters are fine if they all have defaults
    (the `lambda x, k=k:` idiom for capturing a loop variable), as a node
    calls with `nargs` arguments, so they are constants."""
    extra = code.co_argcount - nargs
    if extra < 0 or extra > len(getattr(func, "__defaults__", None) or ()) or code.co_kwonlyargcount:
        return False
    flags = code.co_flags
    return not flags & (_CO_GENERATOR | _CO_COROUTINE | _CO_ASYNC_GENERATOR | _CO_VARARGS | _CO_VARKEYWORDS)


def _analyze_code(func, code, ctx):
    """Uncached body of _analyze_lambda."""
    # One item parameter (see _takes).
    if not _takes(func, code, 1):
        return None

    # Pre-filter: reject obviously complex bytecodes before expensive disassembly
    if len(code.co_code) > _MAX_BYTECODE_LEN:
        return None

    # Get cleaned instructions
    clean = _clean_instructions(func)
    if clean is None:
        return None

    # Complexity guard
    if len(clean) > _MAX_INSTRUCTIONS:
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
    if ops == ["LOAD_FAST", "LOAD_FAST", "BINARY_OP"] and instrs[2][1] in _BINARY_OPS:
        if (instrs[0][1], instrs[1][1]) == (acc, item):
            return ("acc_binop", instrs[2][1])
        return None
    if ops[0::2] == ["LOAD_FAST", "BINARY_OP"] and len(ops) == 3 and instrs[0][1] == acc:
        value = _resolve_value(instrs[1], ctx)
        if value is not _UNRESOLVABLE and instrs[2][1] in _BINARY_OPS:
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


# --- Why a function stays Python (for Stream.explain()) ---

# Expressions explain() names by their kind instead of showing them: the kind
# is what to change (`x if x > 0 else -x` is Python for its if/else, whatever
# `-x` is).
_KINDS = {
    ast.IfExp: "if/else",
    ast.JoinedStr: "an f-string",
    ast.ListComp: "a comprehension",
    ast.SetComp: "a comprehension",
    ast.DictComp: "a comprehension",
    ast.GeneratorExp: "a generator expression",
    ast.Lambda: "a lambda",
    ast.NamedExpr: ":=",
    ast.Await: "await",
}
_STATEMENTS = {
    ast.If: "an if statement",
    ast.For: "a loop",
    ast.While: "a loop",
    ast.Try: "try/except",
    ast.With: "with",
}
_MAX_SHOWN = 28  # characters of an expression shown


def _why_python(func, native, shown):
    """Why compile() runs `func`, the function of the map or filter node
    `shown` (its name), as Python: a short phrase for explain(), or "" when
    there's nothing to add (a class, a C function, source that can't be read).

    `native(f)` tells whether compile() would run the function `f` natively.
    The part of the function to blame is found by asking it about smaller
    and smaller parts, as `lambda x: <part>`, so the answer follows the
    analyzer as it learns new forms. Only explain() calls this.
    """
    if not _ANALYZER_ON:
        if os.environ.get("AKAYU_ANALYZER") == "0":
            return "AKAYU_ANALYZER=0"
        return f"not analyzed on Python {sys.version_info[0]}.{sys.version_info[1]}"
    name = getattr(func, "__name__", None)
    if "<lambda>" in shown and isinstance(name, str) and name != "<lambda>":
        # compile() replaced `lambda x: f(x)` with f: the node calls f itself.
        return _shorten(f"calls {name}()")
    if type(func) is not _FunctionType:
        return ""
    code = func.__code__
    if code.co_flags & (_CO_GENERATOR | _CO_ASYNC_GENERATOR):
        return "a generator"
    if code.co_flags & _CO_VARARGS:
        return "takes *args"
    if code.co_flags & _CO_VARKEYWORDS:
        return "takes **kwargs"
    if code.co_kwonlyargcount or code.co_argcount - 1 > len(func.__defaults__ or ()):
        return "takes more than the item"
    body = _returned_expression(func)
    if not isinstance(body, ast.expr):
        return body
    item = code.co_varnames[0]
    # Names as the function sees them: its globals, closure and defaults.
    namespace = dict(func.__globals__)
    if func.__closure__:
        try:
            namespace.update((n, c.cell_contents) for n, c in zip(code.co_freevars, func.__closure__, strict=True))
        except ValueError:  # an empty cell: a variable not yet assigned
            return ""
    # The defaults are the last parameters' (the item's, too, if it has one).
    params = code.co_varnames[1 : code.co_argcount]
    namespace.update(zip(reversed(params), reversed(func.__defaults__ or ()), strict=False))
    found = _blame(body, item, namespace, native)
    if found is None or isinstance(found, str):
        return found or ""
    if isinstance(found, ast.Call) and not _uses(found.func, item):
        return _KINDS.get(type(found.func)) or _shorten(f"calls {ast.unparse(found.func)}()")
    return _shorten(ast.unparse(found))


def _shorten(text):
    return text if len(text) <= _MAX_SHOWN else text[: _MAX_SHOWN - 1] + "…"


def _uses(expr, name):
    """Whether the ast node `expr` reads the variable `name`."""
    return any(isinstance(n, ast.Name) and n.id == name for n in ast.walk(expr))


def _returned_expression(func):
    """The expression `func` returns, as an ast node; or why there isn't
    one, a str ("" if its source can't be read)."""
    code = func.__code__
    if code.co_name == "<lambda>":
        # The body's span, from where its instructions are (the first,
        # RESUME, is at the start of the line and spans nothing).
        spots = [p for p in code.co_positions() if None not in p and (p[1], p[3]) > (p[0], p[2])]
        if not spots:
            return ""
        (line, col), (end_line, end_col) = min((p[0], p[2]) for p in spots), max((p[1], p[3]) for p in spots)
        lines = linecache.getlines(code.co_filename)[line - 1 : end_line]
        if len(lines) != end_line - line + 1:
            return ""
        lines[-1] = lines[-1][:end_col]
        lines[0] = lines[0][col:]
        try:
            return ast.parse("(" + "".join(lines) + ")", mode="eval").body
        except (SyntaxError, RecursionError, MemoryError):
            return ""
    try:
        tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
    except (OSError, TypeError, SyntaxError, RecursionError, MemoryError):
        return ""
    if not tree.body or not isinstance(tree.body[0], ast.FunctionDef):
        return ""
    body = tree.body[0].body
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        body = body[1:]  # its docstring
    if len(body) > 1:
        return f"{len(body)} statements"
    if body and isinstance(body[0], ast.Return) and body[0].value is not None:
        return body[0].value
    return _STATEMENTS.get(type(body[0]), "no return value") if body else ""


def _blame(expr, item, namespace, native):
    """The smallest part of `expr` that has no native form (an ast node, or
    a phrase: "too long", or one of _KINDS), or None if `lambda item: expr`
    has one."""
    func = _lambda_of(expr, item, namespace)
    if func is None or native(func):
        return None
    if _too_long(func):
        return "too long"  # its parts would be too: no need to look at them
    if type(expr) in _KINDS:
        return _KINDS[type(expr)]
    for part in ast.iter_child_nodes(expr):
        # Parts without the item are values (`TABLE` in `TABLE[x]`), and the
        # item itself is the start of every native form.
        if isinstance(part, ast.expr) and not isinstance(part, ast.Name) and _uses(part, item):
            found = _blame(part, item, namespace, native)
            if found is not None:
                return found
    return expr


def _lambda_of(expr, item, namespace):
    """`lambda item: expr`, with `namespace` as its globals, or None."""
    args = ast.arguments(posonlyargs=[], args=[ast.arg(item)], kwonlyargs=[], kw_defaults=[], defaults=[])
    tree = ast.fix_missing_locations(ast.Expression(ast.Lambda(args, expr)))
    try:
        # Defines the function, runs none of it.
        return eval(compile(tree, "<explain>", "eval"), namespace)
    except (SyntaxError, ValueError, TypeError, RecursionError, MemoryError):
        return None


def _too_long(func):
    """Whether the analyzer turns `func` down for its length alone."""
    clean = _clean_instructions(func)
    return len(func.__code__.co_code) > _MAX_BYTECODE_LEN or (clean is not None and len(clean) > _MAX_INSTRUCTIONS)


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
