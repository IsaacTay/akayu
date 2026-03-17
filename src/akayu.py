import asyncio
import contextvars
import inspect
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor


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

async def _process_async(awaitable, downstreams):
    val = await awaitable
    results = []
    for d in downstreams:
        res = d.update(val)
        if res is not None:
            results.append(res)
    if results:
        await asyncio.gather(*results)

async def _gather(futures):
    await asyncio.gather(*futures)

async def _filter_async(coro, x, downstreams):
    allowed = await coro
    if allowed:
        results = []
        for d in downstreams:
            res = d.update(x)
            if res is not None:
                results.append(res)
        if results:
            await asyncio.gather(*results)

def _is_sync_callable(func):
    """Return True if func is definitely synchronous (not a coroutine function)."""
    if inspect.iscoroutinefunction(func):
        return False
    if inspect.isasyncgenfunction(func):
        return False
    # Assume sync for regular functions/lambdas
    return True

def _build_map_func(func, args, kwargs):
    # args is a tuple or None, kwargs is a dict or None
    if not args and not kwargs:
        return func

    if kwargs:
        if args:
            def wrapper(x):
                return func(x, *args, **kwargs)
            return wrapper
        else:
            def wrapper(x):
                return func(x, **kwargs)
            return wrapper
    else:
        # only args
        def wrapper(x):
            return func(x, *args)
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

# Codegen for composed functions: generates unrolled try/except chains for any
# chain length.  Each function call is individually wrapped so _akayu_chain_index
# tags the correct function on error.  try/except has zero overhead on the happy
# path in CPython.  Generated source is registered in linecache so tracebacks
# show the actual generated code.

import linecache as _linecache

_COMPOSED_FACTORIES = {}

def _gen_composed(n):
    """Generate and cache an unrolled compose-N factory.

    Returns a factory(f0, f1, ..., fN-1) -> composed(x) where each fi is
    called in sequence with error index tagging.
    """
    if n not in _COMPOSED_FACTORIES:
        args = ', '.join(f'f{i}' for i in range(n))
        lines = [f'def _mk({args}):', '  def _c(x):']
        for i in range(n):
            src = 'x' if i == 0 else 'r'
            lines += [
                f'    try:',
                f'      r = f{i}({src})',
                f'    except BaseException as e:',
                f'      e._akayu_chain_index = {i}',
                f'      raise',
            ]
        lines += ['    return r', '  return _c']
        source = '\n'.join(lines)
        filename = f'<akayu:composed{n}>'
        _linecache.cache[filename] = (
            len(source), None, source.splitlines(True), filename,
        )
        ns = {}
        exec(compile(source, filename, 'exec'), ns)  # noqa: S102
        _COMPOSED_FACTORIES[n] = ns['_mk']
    return _COMPOSED_FACTORIES[n]


def _compose_maps(f, g):
    """Compose two map functions into a flat chain.

    Exceptions are tagged with _akayu_chain_index for programmatic narrowing.
    """
    f_funcs = getattr(f, '_chain', [f])
    g_funcs = getattr(g, '_chain', [g])
    all_funcs = f_funcs + g_funcs
    factory = _gen_composed(len(all_funcs))
    composed = factory(*all_funcs)
    composed._chain = all_funcs
    return composed


def _compose_batch_maps(f, g):
    """Compose two batch_map functions: batch_map(f).batch_map(g) -> batch_map(composed).

    Each function takes a list and returns a list. The composed function
    passes the output of f directly to g, avoiding intermediate list allocation.
    Exceptions are tagged with _akayu_chain_index for programmatic narrowing.
    """
    f_funcs = getattr(f, '_chain', [f])
    g_funcs = getattr(g, '_chain', [g])
    all_funcs = f_funcs + g_funcs
    factory = _gen_composed(len(all_funcs))
    composed = factory(*all_funcs)
    composed._chain = all_funcs
    return composed

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
        # Copy context for THIS specific thread (each thread gets its own copy)
        ctx = contextvars.copy_context()
        def update_in_context():
            # Acquire per-node lock to prevent concurrent borrows
            lock = _get_node_lock(d)
            with lock:
                return ctx.run(d.update, v)
        return update_in_context

    # Submit all downstream updates to the thread pool
    futures = []
    for d in downstreams:
        futures.append(pool.submit(make_update_fn(d, val)))

    # Wait for all to complete and collect results
    results = []
    errors = []
    for f in futures:
        try:
            res = f.result()
            if res is not None:
                results.append(res)
        except Exception as e:
            errors.append(e)

    # If any errors occurred, raise the first one
    if errors:
        raise errors[0]

    # If any returned coroutines, gather them
    if results:
        return asyncio.gather(*results)
    return None

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


class _AsyncState:
    """State manager for async map operations with asynchronous=True.

    Manages concurrent execution of async coroutines while preserving output order.
    Uses a dedicated event loop running in a background thread.

    Returns results to Rust for propagation instead of calling d.update() directly,
    to avoid thread-safety issues with PyO3 borrows.
    """

    def __init__(self):
        import threading
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.loop_thread.start()
        self.pending = {}    # {seq_num: Future}
        self.next_seq = 0    # next item sequence number
        self.next_emit = 0   # next sequence to emit
        self.ready = {}      # {seq_num: result} for completed out-of-order
        self._lock = threading.Lock()

    def submit(self, coro):
        """Submit coroutine, return ready results in order."""
        with self._lock:
            seq = self.next_seq
            self.next_seq += 1
            future = asyncio.run_coroutine_threadsafe(coro, self.loop)
            self.pending[seq] = future

        # Collect ready results in order
        results = []
        with self._lock:
            # First, check if any completed pending futures can be moved to ready
            for s in list(self.pending.keys()):
                if self.pending[s].done():
                    res = self.pending.pop(s).result()
                    if s == self.next_emit:
                        results.append(res)
                        self.next_emit += 1
                    else:
                        self.ready[s] = res

            # Then emit any consecutive ready results
            while self.next_emit in self.ready:
                results.append(self.ready.pop(self.next_emit))
                self.next_emit += 1

        return results

    def flush(self):
        """Wait for all pending and return in order."""
        results = []
        with self._lock:
            pending_items = list(self.pending.items())

        # Wait for all pending futures (outside lock to avoid deadlock)
        for seq, future in pending_items:
            res = future.result()
            with self._lock:
                if seq == self.next_emit:
                    results.append(res)
                    self.next_emit += 1
                else:
                    self.ready[seq] = res
                self.pending.pop(seq, None)

        # Collect any remaining ready results in order
        with self._lock:
            while self.next_emit in self.ready:
                results.append(self.ready.pop(self.next_emit))
                self.next_emit += 1

        return results
