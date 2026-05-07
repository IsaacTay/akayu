import contextvars
import dis
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor


import builtins as _builtins

_NOISE_OPCODES = frozenset({
    'RESUME', 'COPY_FREE_VARS', 'NOP', 'CACHE', 'PUSH_NULL', 'PRECALL', 'TO_BOOL',
})

_EXCEPTION_OPCODES = frozenset({
    'SETUP_FINALLY', 'POP_EXCEPT', 'SETUP_WITH', 'BEFORE_WITH',
    'SETUP_ASYNC_WITH', 'RERAISE', 'PUSH_EXC_INFO',
})

# Known builtins we allow resolving from LOAD_GLOBAL
_KNOWN_BUILTINS = {
    'len': _builtins.len, 'str': _builtins.str, 'int': _builtins.int,
    'float': _builtins.float, 'bool': _builtins.bool, 'isinstance': _builtins.isinstance,
    'type': _builtins.type, 'hasattr': _builtins.hasattr,
    'tuple': _builtins.tuple, 'list': _builtins.list, 'set': _builtins.set,
    'frozenset': _builtins.frozenset,
}

_UNRESOLVABLE = object()  # sentinel


def _resolve_value(instr, func):
    """Resolve a value from an instruction + function context.

    Returns the resolved Python object, or _UNRESOLVABLE sentinel.
    """
    op = instr.opname
    if op == 'LOAD_CONST':
        return instr.argval
    if op == 'LOAD_DEREF':
        # Closure variable — snapshot at compile time
        closure = getattr(func, '__closure__', None)
        if closure is None:
            return _UNRESOLVABLE
        # Find the index of this free var
        code = func.__code__
        free = code.co_cellvars + code.co_freevars
        name = instr.argval
        if name in free:
            idx = free.index(name)
            if idx < len(closure):
                try:
                    return closure[idx].cell_contents
                except ValueError:
                    return _UNRESOLVABLE
        return _UNRESOLVABLE
    if op == 'LOAD_GLOBAL':
        name = instr.argval
        if name in _KNOWN_BUILTINS:
            return _KNOWN_BUILTINS[name]
        # Try resolving from func's globals for non-builtins (arbitrary callables)
        globs = getattr(func, '__globals__', None)
        if globs is not None and name in globs:
            return globs[name]
        return _UNRESOLVABLE
    return _UNRESOLVABLE


def _clean_instructions(func):
    """Get cleaned instruction list: no noise opcodes, no RETURN_VALUE suffix."""
    try:
        instrs = list(dis.get_instructions(func))
    except TypeError:
        return None
    clean = [i for i in instrs if i.opname not in _NOISE_OPCODES]
    # Strip trailing RETURN_VALUE(s)
    while clean and clean[-1].opname == 'RETURN_VALUE':
        clean.pop()
    return clean


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
    if n == 2 and instrs[0].opname == 'LOAD_FAST' and instrs[1].opname == 'LOAD_ATTR':
        attr_name = instrs[1].argval
        # In 3.12+, LOAD_ATTR can be used for method lookup (argval may have extra info)
        # If it's a method load, the next instruction would be CALL — but we only have 2 instrs
        # so this is a plain attribute access
        return ("attrgetter", attr_name)

    # lambda x: x[k]  (ItemGetter)
    # LOAD_FAST, LOAD_CONST/LOAD_DEREF(k), BINARY_SUBSCR
    if n == 3 and instrs[0].opname == 'LOAD_FAST' and instrs[2].opname == 'BINARY_SUBSCR':
        key = _resolve_value(instrs[1], func)
        if key is not _UNRESOLVABLE:
            return ("itemgetter", key)

    # lambda x: not x
    # LOAD_FAST, UNARY_NOT
    if n == 2 and instrs[0].opname == 'LOAD_FAST' and instrs[1].opname == 'UNARY_NOT':
        return ("not",)

    # lambda x: <builtin>(x) — len, str, int, float, bool
    # LOAD_GLOBAL(name), LOAD_FAST, CALL 1
    if (n == 3 and instrs[0].opname == 'LOAD_GLOBAL' and instrs[1].opname == 'LOAD_FAST'
            and instrs[2].opname == 'CALL'):
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
    if (n == 4 and instrs[0].opname == 'LOAD_GLOBAL' and instrs[1].opname == 'LOAD_FAST'
            and instrs[3].opname == 'CALL'):
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
    if n == 3 and instrs[0].opname == 'LOAD_FAST' and instrs[2].opname == 'IS_OP':
        val = _resolve_value(instrs[1], func)
        if val is not _UNRESOLVABLE:
            if instrs[2].argval == 0:  # is
                return ("is", val)
            else:  # is not
                return ("is_not", val)

    # --- Comparison: lambda x: x > 0 ---
    # LOAD_FAST, LOAD_CONST/LOAD_DEREF(val), COMPARE_OP(op)
    if n == 3 and instrs[0].opname == 'LOAD_FAST' and instrs[2].opname == 'COMPARE_OP':
        val = _resolve_value(instrs[1], func)
        if val is not _UNRESOLVABLE:
            op_name = instrs[2].argval
            if op_name in ('==', '!=', '<', '>', '<=', '>='):
                return ("compare", op_name, val)

    # --- BinOp: lambda x: x + 1 ---
    # LOAD_FAST, LOAD_CONST/LOAD_DEREF(val), BINARY_OP(op)
    if n == 3 and instrs[0].opname == 'LOAD_FAST' and instrs[2].opname == 'BINARY_OP':
        val = _resolve_value(instrs[1], func)
        if val is not _UNRESOLVABLE:
            op_name = instrs[2].argval
            if op_name in ('+', '-', '*', '/', '//', '%', '**'):
                return ("binop", op_name, val)

    # --- Contains: lambda x: x in collection ---
    # LOAD_FAST, LOAD_DEREF/LOAD_CONST/LOAD_GLOBAL(coll), CONTAINS_OP
    if n == 3 and instrs[0].opname == 'LOAD_FAST' and instrs[2].opname == 'CONTAINS_OP':
        coll = _resolve_value(instrs[1], func)
        if coll is not _UNRESOLVABLE:
            if instrs[2].argval == 0:
                return ("contains", coll)
            else:
                return ("not_contains", coll)

    # --- Method call with 0 args: lambda x: x.strip() ---
    # LOAD_FAST, LOAD_ATTR(method), CALL 0
    if (n == 3 and instrs[0].opname == 'LOAD_FAST' and instrs[1].opname == 'LOAD_ATTR'
            and instrs[2].opname == 'CALL'):
        method_name = instrs[1].argval
        return ("method0", method_name)

    # --- Method call with 1 arg: lambda x: x.startswith('foo') ---
    # LOAD_FAST, LOAD_ATTR(method), LOAD_CONST/LOAD_DEREF(arg), CALL 1
    if (n == 4 and instrs[0].opname == 'LOAD_FAST' and instrs[1].opname == 'LOAD_ATTR'
            and instrs[3].opname == 'CALL'):
        method_name = instrs[1].argval
        arg = _resolve_value(instrs[2], func)
        if arg is not _UNRESOLVABLE:
            return ("method1", method_name, arg)

    # --- Method call with 2 args: lambda x: x.replace('a', 'b') ---
    # LOAD_FAST, LOAD_ATTR(method), LOAD_CONST/LOAD_DEREF(arg1), LOAD_CONST/LOAD_DEREF(arg2), CALL 2
    if (n == 5 and instrs[0].opname == 'LOAD_FAST' and instrs[1].opname == 'LOAD_ATTR'
            and instrs[4].opname == 'CALL'):
        method_name = instrs[1].argval
        arg1 = _resolve_value(instrs[2], func)
        arg2 = _resolve_value(instrs[3], func)
        if arg1 is not _UNRESOLVABLE and arg2 is not _UNRESOLVABLE:
            return ("method2", method_name, arg1, arg2)

    # --- Side-effect method call on closure/global: lambda _: obj.method() ---
    # LOAD_DEREF/LOAD_GLOBAL(obj), LOAD_ATTR(method), CALL 0
    # x (LOAD_FAST) is never referenced — the lambda ignores its argument.
    if (n == 3 and instrs[0].opname in ('LOAD_DEREF', 'LOAD_GLOBAL')
            and instrs[1].opname == 'LOAD_ATTR' and instrs[2].opname == 'CALL'):
        obj = _resolve_value(instrs[0], func)
        if obj is not _UNRESOLVABLE:
            return ("call_method0", obj, instrs[1].argval)

    # --- Side-effect method call passing x: lambda x: obj.method(x) ---
    # LOAD_DEREF/LOAD_GLOBAL(obj), LOAD_ATTR(method), LOAD_FAST(x), CALL 1
    # The lambda's argument is passed directly to the method.
    if (n == 4 and instrs[0].opname in ('LOAD_DEREF', 'LOAD_GLOBAL')
            and instrs[1].opname == 'LOAD_ATTR'
            and instrs[2].opname == 'LOAD_FAST'
            and instrs[3].opname == 'CALL'):
        obj = _resolve_value(instrs[0], func)
        if obj is not _UNRESOLVABLE:
            return ("call_method1", obj, instrs[1].argval)

    # --- Slice: lambda x: x[start:stop] ---
    # LOAD_FAST, LOAD_CONST(start), LOAD_CONST(stop), BINARY_SLICE
    if n == 4 and instrs[0].opname == 'LOAD_FAST' and instrs[3].opname == 'BINARY_SLICE':
        start = _resolve_value(instrs[1], func)
        stop = _resolve_value(instrs[2], func)
        if start is not _UNRESOLVABLE and stop is not _UNRESOLVABLE:
            return ("slice", start, stop)

    # --- Chained patterns ---
    # These reference x once at the start and build a pipeline

    # lambda x: x.attr <op> val  (Chain: AttrGetter → Compare/BinOp/Contains)
    # LOAD_FAST, LOAD_ATTR(name), LOAD_CONST/LOAD_DEREF(val), COMPARE_OP/BINARY_OP/CONTAINS_OP/IS_OP
    if n == 4 and instrs[0].opname == 'LOAD_FAST' and instrs[1].opname == 'LOAD_ATTR':
        attr_name = instrs[1].argval
        val = _resolve_value(instrs[2], func)
        if val is not _UNRESOLVABLE:
            if instrs[3].opname == 'COMPARE_OP':
                op_name = instrs[3].argval
                if op_name in ('==', '!=', '<', '>', '<=', '>='):
                    return ("chain", ("attrgetter", attr_name), ("compare", op_name, val))
            if instrs[3].opname == 'BINARY_OP':
                op_name = instrs[3].argval
                if op_name in ('+', '-', '*', '/', '//', '%', '**'):
                    return ("chain", ("attrgetter", attr_name), ("binop", op_name, val))
            if instrs[3].opname == 'CONTAINS_OP':
                if instrs[3].argval == 0:
                    return ("chain", ("attrgetter", attr_name), ("contains", val))
                else:
                    return ("chain", ("attrgetter", attr_name), ("not_contains", val))
            if instrs[3].opname == 'IS_OP':
                if instrs[3].argval == 0:
                    return ("chain", ("attrgetter", attr_name), ("is", val))
                else:
                    return ("chain", ("attrgetter", attr_name), ("is_not", val))

    # lambda x: x[k] <op> val  (Chain: ItemGetter → Compare/etc.)
    # LOAD_FAST, LOAD_CONST/LOAD_DEREF(k), BINARY_SUBSCR, LOAD_CONST/LOAD_DEREF(val), COMPARE_OP/etc.
    if n == 5 and instrs[0].opname == 'LOAD_FAST' and instrs[2].opname == 'BINARY_SUBSCR':
        key = _resolve_value(instrs[1], func)
        val = _resolve_value(instrs[3], func)
        if key is not _UNRESOLVABLE and val is not _UNRESOLVABLE:
            if instrs[4].opname == 'COMPARE_OP':
                op_name = instrs[4].argval
                if op_name in ('==', '!=', '<', '>', '<=', '>='):
                    return ("chain", ("itemgetter", key), ("compare", op_name, val))
            if instrs[4].opname == 'BINARY_OP':
                op_name = instrs[4].argval
                if op_name in ('+', '-', '*', '/', '//', '%', '**'):
                    return ("chain", ("itemgetter", key), ("binop", op_name, val))
            if instrs[4].opname == 'CONTAINS_OP':
                if instrs[4].argval == 0:
                    return ("chain", ("itemgetter", key), ("contains", val))
                else:
                    return ("chain", ("itemgetter", key), ("not_contains", val))
            if instrs[4].opname == 'IS_OP':
                if instrs[4].argval == 0:
                    return ("chain", ("itemgetter", key), ("is", val))
                else:
                    return ("chain", ("itemgetter", key), ("is_not", val))

    # lambda x: len(x) > 0 etc.  (Chain: Len → Compare)
    # LOAD_GLOBAL(len), LOAD_FAST, CALL 1, LOAD_CONST(val), COMPARE_OP
    if (n == 5 and instrs[0].opname == 'LOAD_GLOBAL' and instrs[1].opname == 'LOAD_FAST'
            and instrs[2].opname == 'CALL'):
        resolved_func = _resolve_value(instrs[0], func)
        if resolved_func in (
            _builtins.len, _builtins.str, _builtins.int, _builtins.float, _builtins.bool,
            _builtins.type,
        ):
            val = _resolve_value(instrs[3], func)
            if val is not _UNRESOLVABLE:
                first_op = {
                    _builtins.len: "len", _builtins.str: "str", _builtins.int: "int",
                    _builtins.float: "float", _builtins.bool: "bool",
                    _builtins.type: "type",
                }[resolved_func]
                if instrs[4].opname == 'COMPARE_OP':
                    op_name = instrs[4].argval
                    if op_name in ('==', '!=', '<', '>', '<=', '>='):
                        return ("chain", (first_op,), ("compare", op_name, val))
                if instrs[4].opname == 'BINARY_OP':
                    op_name = instrs[4].argval
                    if op_name in ('+', '-', '*', '/', '//', '%', '**'):
                        return ("chain", (first_op,), ("binop", op_name, val))

    # --- Negate: lambda x: not <expr> ---
    # <expr instrs> + UNARY_NOT
    if n >= 2 and instrs[-1].opname == 'UNARY_NOT':
        inner = _analyze_simple(instrs[:-1], func)
        if inner is not None:
            return ("negate", inner)

    # --- Tuple/list construction: (x["a"], x["b"]) or [x.name, x.age] ---
    # Multiple LOAD_FAST + sub-op groups ending with BUILD_TUPLE/BUILD_LIST
    if n >= 3 and instrs[-1].opname in ('BUILD_TUPLE', 'BUILD_LIST'):
        count = instrs[-1].argval
        remaining = instrs[:-1]
        groups = []
        pos = 0
        for _ in range(count):
            if pos >= len(remaining) or remaining[pos].opname != 'LOAD_FAST':
                break
            end = pos + 1
            while end < len(remaining) and remaining[end].opname != 'LOAD_FAST':
                end += 1
            sub = _analyze_simple(remaining[pos:end], func)
            if sub is None:
                break
            groups.append(sub)
            pos = end
        if len(groups) == count and pos == len(remaining):
            tag = "build_tuple" if instrs[-1].opname == 'BUILD_TUPLE' else "build_list"
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
        if instr.opname == 'POP_JUMP_IF_FALSE' and instr.argval > best_target:
            best_target = instr.argval
            best = (i, True)  # `and` short-circuits on False
        elif instr.opname == 'POP_JUMP_IF_TRUE' and instr.argval > best_target:
            best_target = instr.argval
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
        if instrs[i].opname == 'COPY':
            copy_idx = i
            break
    if copy_idx is None:
        return None

    left_instrs = instrs[:copy_idx]

    # Right side: everything after POP_TOP (which follows the jump)
    pop_idx = jump_idx + 1
    if pop_idx >= len(instrs) or instrs[pop_idx].opname != 'POP_TOP':
        return None
    right_instrs = instrs[pop_idx + 1:]

    # Recursively analyze each side
    left = _analyze_expr(left_instrs, func)
    right = _analyze_expr(right_instrs, func)

    if left is None or right is None:
        return None

    if is_and:
        return ("and", left, right)
    else:
        return ("or", left, right)


def _analyze_expr(instrs, func):
    """Analyze an expression — tries compound first, then simple."""
    # Try compound (and/or) first
    compound = _analyze_compound(instrs, func)
    if compound is not None:
        return compound
    # Fall back to simple
    return _analyze_simple(instrs, func)


# Flags for bail-out checks
_CO_GENERATOR = 0x20
_CO_COROUTINE = 0x100
_CO_ASYNC_GENERATOR = 0x200
_CO_VARARGS = 0x04
_CO_VARKEYWORDS = 0x08


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
    # Early bail-out: must have __code__
    code = getattr(func, '__code__', None)
    if code is None:
        return None

    # Must be single-arg, no *args/**kwargs
    if code.co_argcount != 1:
        return None
    flags = code.co_flags
    if flags & (_CO_GENERATOR | _CO_COROUTINE | _CO_ASYNC_GENERATOR | _CO_VARARGS | _CO_VARKEYWORDS):
        return None

    # Get cleaned instructions
    clean = _clean_instructions(func)
    if clean is None:
        return None

    # Complexity guard
    if len(clean) > 25:
        return None

    # Reject multi-statement functions
    if any(i.opname == 'STORE_FAST' for i in clean):
        return None

    # Reject exception handling
    if any(i.opname in _EXCEPTION_OPCODES for i in clean):
        return None

    # Reject multiple LOAD_FAST (references x more than once in non-compound)
    # For compound expressions, each sub-expression can reference x once
    # We'll let the recursive analyzer handle this naturally

    return _analyze_expr(clean, func)


def _get_caller_location():
    """Return 'filename:lineno' of the caller's caller.

    Frame 0 = this function, frame 1 = the user's code.
    PyO3 Rust methods don't create Python frames, so frame 1
    is the Python code that called the Rust method which called us.
    """
    try:
        frame = sys._getframe(1)
        return f"{frame.f_code.co_filename}:{frame.f_lineno}"
    except ValueError:
        return "<unknown>"

def _build_map_func(func, args, kwargs):
    # args is a tuple or None, kwargs is a dict or None
    if not args and not kwargs:
        return func

    if kwargs:
        if args:
            def wrapper(x):
                return func(x, *args, **kwargs)
            wrapper._akayu_inner = func
            wrapper._akayu_args = args
            return wrapper
        else:
            def wrapper(x):
                return func(x, **kwargs)
            wrapper._akayu_inner = func
            return wrapper
    else:
        # only args
        def wrapper(x):
            return func(x, *args)
        wrapper._akayu_inner = func
        wrapper._akayu_args = args
        return wrapper

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

_thread_pool = None
_node_locks = {}  # Per-node locks to prevent concurrent update calls
_node_locks_lock = threading.Lock()  # Lock for accessing _node_locks dict

def _get_global_pool():
    """Get or create the shared global thread pool for par() and prefetch().

    Using a shared pool eliminates the overhead of creating separate pools
    for each prefetch() call, making par() + prefetch() combinations efficient.
    The pool size defaults to the number of CPUs available.
    """
    global _thread_pool
    if _thread_pool is None:
        _thread_pool = ThreadPoolExecutor(max_workers=os.cpu_count() or 4)
    return _thread_pool

def _get_node_lock(node):
    """Get or create a lock for a specific node."""
    node_id = id(node)
    if node_id not in _node_locks:
        with _node_locks_lock:
            if node_id not in _node_locks:
                _node_locks[node_id] = threading.RLock()
    return _node_locks[node_id]

def _parallel_execute(val, downstreams):
    """Execute downstream updates in parallel using a thread pool."""
    pool = _get_global_pool()

    def make_update_fn(d, v):
        ctx = contextvars.copy_context()
        def update_in_context():
            lock = _get_node_lock(d)
            with lock:
                return ctx.run(d.update, v)
        return update_in_context

    futures = []
    for d in downstreams:
        futures.append(pool.submit(make_update_fn(d, val)))

    results = []
    errors = []
    for f in futures:
        try:
            res = f.result()
            if res is not None:
                results.append(res)
        except Exception as e:
            errors.append(e)

    if errors:
        raise errors[0]

    return None

def _gather(futures):
    pass

def _safe_update(node, val):
    """Call node.update(val) with per-node locking to prevent concurrent borrows."""
    lock = _get_node_lock(node)
    with lock:
        return node.update(val)

def _safe_update_batch(node, items):
    """Call node.update_batch(items) with per-node locking to prevent concurrent borrows."""
    lock = _get_node_lock(node)
    with lock:
        return node.update_batch(items)

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


class _PrefetchState:
    """State manager for prefetch map operations.

    Manages concurrent execution of map functions while preserving output order.
    Uses a shared global thread pool to process items ahead while emitting results
    in sequence order. The shared pool minimizes overhead when combining with par().

    Returns results to Rust for propagation instead of calling d.update() directly,
    to avoid thread-safety issues with PyO3 borrows.
    """

    def __init__(self, func, size):
        import threading
        self.func = func
        self.size = size
        self.executor = _get_global_pool()  # Use shared pool
        self.pending = {}    # {seq_num: future}
        self.next_seq = 0    # next item sequence number
        self.next_emit = 0   # next sequence to emit
        self.ready = {}      # {seq_num: result} for completed out-of-order
        self._lock = threading.Lock()  # Protect against concurrent access

    def update(self, item):
        """Process an item and return ready results in order."""
        with self._lock:
            seq = self.next_seq
            self.next_seq += 1

            # Submit to thread pool
            future = self.executor.submit(self.func, item)
            self.pending[seq] = future

            # If at capacity, block and drain
            if len(self.pending) >= self.size:
                self._drain_one()

            # Return any ready results in order
            return self._collect_ready()

    def _drain_one(self):
        """Wait for the next expected result to complete."""
        if self.next_emit in self.pending:
            # Wait for the next expected result
            result = self.pending[self.next_emit].result()
            self.ready[self.next_emit] = result
            del self.pending[self.next_emit]

    def _collect_ready(self):
        """Collect all consecutive ready results."""
        results = []
        while self.next_emit in self.ready:
            result = self.ready.pop(self.next_emit)
            results.append(result)
            self.next_emit += 1
        return results

    def flush(self):
        """Wait for all pending items and return in order."""
        with self._lock:
            # Wait for all pending futures
            for seq in sorted(self.pending.keys()):
                self.ready[seq] = self.pending[seq].result()
            self.pending.clear()

            # Return all in order
            return self._collect_ready()

class _PrefetchFilterState:
    """State manager for prefetch filter operations.

    Like _PrefetchState but for filter predicates. The wrapped predicate
    returns (keep, value) tuples, and we only emit values where keep is True.

    Returns results to Rust for propagation instead of calling d.update() directly,
    to avoid thread-safety issues with PyO3 borrows.
    """

    def __init__(self, wrapped_predicate, size):
        import threading
        self.wrapped_predicate = wrapped_predicate
        self.size = size
        self.executor = _get_global_pool()  # Use shared pool
        self.pending = {}    # {seq_num: future}
        self.next_seq = 0
        self.next_emit = 0
        self.ready = {}      # {seq_num: (keep, value)}
        self._lock = threading.Lock()  # Protect against concurrent access

    def update(self, item):
        """Process an item and return ready results in order (only those that passed filter)."""
        with self._lock:
            seq = self.next_seq
            self.next_seq += 1

            # Submit to thread pool - wrapped_predicate returns (keep, value)
            future = self.executor.submit(self.wrapped_predicate, item)
            self.pending[seq] = future

            # If at capacity, block and drain
            if len(self.pending) >= self.size:
                self._drain_one()

            # Return any ready results in order
            return self._collect_ready()

    def _drain_one(self):
        """Wait for the next expected result to complete."""
        if self.next_emit in self.pending:
            result = self.pending[self.next_emit].result()
            self.ready[self.next_emit] = result
            del self.pending[self.next_emit]

    def _collect_ready(self):
        """Collect all consecutive ready results where predicate was True."""
        results = []
        while self.next_emit in self.ready:
            keep, value = self.ready.pop(self.next_emit)
            if keep:  # Only include if predicate returned True
                results.append(value)
            self.next_emit += 1
        return results

    def flush(self):
        """Wait for all pending items and return in order (only those that passed filter)."""
        with self._lock:
            for seq in sorted(self.pending.keys()):
                self.ready[seq] = self.pending[seq].result()
            self.pending.clear()
            return self._collect_ready()


class _PrefetchBatchMapState:
    """State manager for prefetch batch_map operations.

    Like _PrefetchState but for batch_map. Each item is wrapped in a list before
    being submitted to the thread pool, and the single result is extracted.

    For update_batch, the entire batch is submitted as one unit.
    """

    def __init__(self, func, size):
        import threading
        self.func = func
        self.size = size
        self.executor = _get_global_pool()
        self.pending = {}    # {seq_num: future}
        self.next_seq = 0
        self.next_emit = 0
        self.ready = {}      # {seq_num: result}
        self._lock = threading.Lock()

    def _run_single(self, item):
        """Run func on single item wrapped in list, extract single result."""
        result = self.func([item])
        # batch_map returns iterable, extract first element
        return next(iter(result))

    def update(self, item):
        """Process a single item and return ready results in order."""
        with self._lock:
            seq = self.next_seq
            self.next_seq += 1

            # Submit single item (wrapped in list internally by _run_single)
            future = self.executor.submit(self._run_single, item)
            self.pending[seq] = future

            if len(self.pending) >= self.size:
                self._drain_one()

            return self._collect_ready()

    def update_batch(self, items):
        """Process a batch and return ready results in order.

        Each item in the batch is submitted separately to allow prefetching.
        """
        with self._lock:
            # Submit each item separately for prefetching
            for item in items:
                seq = self.next_seq
                self.next_seq += 1
                future = self.executor.submit(self._run_single, item)
                self.pending[seq] = future

                if len(self.pending) >= self.size:
                    self._drain_one()

            return self._collect_ready()

    def _drain_one(self):
        """Wait for the next expected result to complete."""
        if self.next_emit in self.pending:
            result = self.pending[self.next_emit].result()
            self.ready[self.next_emit] = result
            del self.pending[self.next_emit]

    def _collect_ready(self):
        """Collect all consecutive ready results."""
        results = []
        while self.next_emit in self.ready:
            result = self.ready.pop(self.next_emit)
            results.append(result)
            self.next_emit += 1
        return results

    def flush(self):
        """Wait for all pending items and return in order."""
        with self._lock:
            for seq in sorted(self.pending.keys()):
                self.ready[seq] = self.pending[seq].result()
            self.pending.clear()
            return self._collect_ready()
