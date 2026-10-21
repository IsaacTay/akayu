"""Unusual flows people bend akayu into, which used to crash, hang, panic or
silently lose data. Each test pins the behaviour it was fixed to; the ones
that used to crash the interpreter run in a subprocess.

Sections are by topic. A new regression test goes in its feature's own file
(CONTRIBUTING.md has the table); this one holds the probing rounds' findings
that fit no feature.
"""

import gc
import operator
import signal
import sys
import textwrap
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from support import innermost, run_script

import akayu

# --- Deep graphs: a clean RecursionError, never a native stack overflow ---

# The main thread's stack is as big as the OS makes it (ulimit -s: 8 MB on
# most Linux machines, 16 MB on GitHub's runners, unlimited on some), so a
# deep graph may fit there and simply run. A thread gets the stack it asks
# for: 4 MiB, which each graph here is over three times too deep for.
DEEP_SCRIPT = """
import threading
import akayu
{build}
def attempt():
    try:
        print(run())
    except BaseException as e:
        while e.__cause__ is not None:
            e = e.__cause__
        print(type(e).__name__)
if {on_thread}:
    threading.stack_size(4 * 1024 * 1024)
    t = threading.Thread(target=attempt)
    t.start()
    t.join()
else:
    attempt()
"""

WHERE = pytest.mark.parametrize("where", ["main", "thread"])


def run_deep(build, where, result):
    """Run the `run()` that `build` defines on the main thread or a 4 MiB
    thread, in a subprocess. On the thread it must raise RecursionError; on
    the main thread it may instead fit and print `result`."""
    script = DEEP_SCRIPT.format(build=textwrap.dedent(build), on_thread=where == "thread")
    code, last = run_script(script)
    assert code == 0
    assert last == "RecursionError" or (where == "main" and last == result)


@WHERE
@pytest.mark.parametrize("entry", ["emit", "emit_batch"])
def test_a_very_deep_graph_raises_recursion_error(entry, where):
    build = f"""
        def inc(x):
            y = x + 1
            return y
        def run():
            s = akayu.Stream(); node = s
            for _ in range(30000):
                node = node.map(inc)
            out = []
            node.sink(out.append)
            s.emit(0) if "{entry}" == "emit" else s.emit_batch([0])
            return out
    """
    run_deep(build, where, "[30000]")


def test_compiling_a_very_deep_graph():
    code, last = run_script("""
        import akayu
        s = akayu.Stream(); node = s
        for _ in range(120000):
            node = node.map(lambda x: x + 1)
        out = []
        node.sink(out.append)
        s.compile()
        s.emit(0)
        print(out)
    """)
    assert (code, last) == (0, "[120000]")


def test_a_thread_with_a_small_stack():
    code, last = run_script("""
        import akayu, threading
        threading.stack_size(128 * 1024)  # musl's default; aarch64 glibc's minimum
        result = []
        def work():
            s = akayu.Stream(); node = s
            for _ in range(400):
                node = node.map(lambda x: x + 1)
            node.sink(result.append)
            try:
                s.emit(0)
            except RuntimeError as e:
                while e.__cause__ is not None:
                    e = e.__cause__
                result.append(type(e).__name__)
        t = threading.Thread(target=work); t.start(); t.join()
        print(result)
    """)
    assert code == 0
    assert last in ("[400]", "['RecursionError']")


@WHERE
def test_a_long_chain_of_sources(where):
    build = """
        def run():
            sources = [akayu.Stream() for _ in range(8000)]
            for a, b in zip(sources, sources[1:]):
                a.sink(b.emit)
            out = []
            sources[-1].sink(out.append)
            sources[0].emit(1)
            return out
    """
    run_deep(build, where, "[1]")


def test_compiling_stacked_diamonds_is_linear():
    s = akayu.Stream()
    node = s
    for _ in range(40):  # 2**40 paths
        node = akayu.union(node.map(lambda x: x), node.map(lambda x: x))
    out = []
    node.sink(out.append)
    s.compile()  # used to walk every path


# --- Re-entrancy: clean errors, and the graph keeps working ---


def test_building_on_a_node_mid_emit_is_a_clean_error():
    s, out = akayu.Stream(), []
    s.sink(lambda x: s.map(lambda y: y) if x == 1 else out.append(x))
    with pytest.raises(RuntimeError) as info:
        s.emit(1)
    assert "processing an item" in str(innermost(info.value))
    s.emit(2)  # the source isn't left marked as emitting
    assert out == [2]


def test_profiling_mid_emit_is_a_clean_error():
    s = akayu.Stream()
    s.sink(lambda x: s.stop_profiling())
    s.start_profiling()
    with pytest.raises(RuntimeError):
        s.emit(1)
    s.stop_profiling()


def test_compile_with_a_sink_emitting_into_its_own_source():
    s, out = akayu.Stream(), []
    s.filter(lambda x: x > 0).sink(lambda x: s.emit(x - 1))
    s.sink(out.append)
    s.compile()  # used to panic
    with pytest.raises(RuntimeError) as info:
        s.emit(3)
    assert "Re-entrant emit" in str(innermost(info.value))


def test_a_feedback_loop_through_another_source_is_refused(compiled):
    """compile() turns `lambda x: fb.emit(x)` into a direct emit, which used
    to skip the re-entrancy check and recurse until the stack overflowed."""
    a, fb = akayu.Stream(), akayu.Stream()
    loop = akayu.union(a, fb).map(lambda x: x - 1)
    loop.filter(lambda x: x > 0).sink(lambda x: fb.emit(x))
    if compiled:
        a.compile()
        fb.compile()
    with pytest.raises(RuntimeError):
        a.emit(5000)


def test_an_item_coming_back_into_a_busy_node(compiled):
    """A callback below a union emits into the union's other source: the
    union is re-entered mid-update. Refused in both modes (compiled mode
    used to run it through aliased mutable references)."""
    a, b = akayu.Stream(), akayu.Stream()
    seen = []

    def echo(x):
        seen.append(x)
        if x == "from-a":
            b.emit("echo")

    akayu.union(a, b).sink(echo)
    if compiled:
        a.compile()
        b.compile()
    with pytest.raises(RuntimeError) as info:
        a.emit("from-a")
    assert "re-entered" in str(innermost(info.value))
    b.emit("later")
    assert seen == ["from-a", "later"]


# --- KeyboardInterrupt, SystemExit, GeneratorExit pass through unwrapped ---


@pytest.mark.parametrize("exc", [KeyboardInterrupt, SystemExit, GeneratorExit])
@pytest.mark.parametrize("batch", [False, True])
def test_stop_requests_are_not_wrapped(exc, compiled, batch):
    s, out = akayu.Stream(), []
    fired = []

    def sink(x):
        if not fired:
            fired.append(1)
            raise exc("stop")
        out.append(x)

    s.map(lambda x: x + 1).sink(sink)
    if compiled:
        s.compile()
    with pytest.raises(exc):
        s.emit_batch([1]) if batch else s.emit(1)
    s.emit(2)
    assert out == [3]


def test_sys_exit_in_a_callback_keeps_its_code():
    code, _ = run_script("""
        import akayu, sys
        s = akayu.Stream()
        s.sink(lambda x: sys.exit(3))
        s.emit(1)
    """)
    assert code == 3


# --- flush() ---


def test_one_flush_reaches_a_node_below_a_merge_once(compiled):
    s, out = akayu.Stream(), []
    akayu.union(s.map(lambda x: x), s.map(lambda x: x * 10)).collect().sink(out.append)
    if compiled:
        s.compile()
    s.emit(1)
    s.flush()
    assert out == [(1, 10)]  # used to add an empty () per extra path


@pytest.mark.parametrize("kind", ["source", "collect"])
def test_flush_from_another_thread_waits_for_emits(compiled, kind, fast_switching):
    s, out = akayu.Stream(), []
    collected = s.map(lambda x: x).collect()
    collected.sink(out.extend)
    if compiled:
        s.compile()
    target = s if kind == "source" else collected
    errors = []
    stop = threading.Event()

    def flusher():
        while not stop.is_set():
            try:
                target.flush()
            except Exception as e:
                errors.append(e)

    t = threading.Thread(target=flusher)
    t.start()
    emitters = [threading.Thread(target=lambda k=k: [s.emit(k * 300 + i) for i in range(300)]) for k in range(3)]
    for e in emitters:
        e.start()
    for e in emitters:
        e.join()
    stop.set()
    t.join()
    s.flush()
    assert errors == []
    assert sorted(out) == list(range(900))


def test_flush_while_another_thread_emits_nonstop(compiled):
    # The callback releases the GIL mid-item. flush() waited for the emit's
    # lock, let go of it, and found the next emit had taken it: forever.
    code, last = run_script(
        f"""
        import akayu, threading, time
        s, out = akayu.Stream(), []
        s.map(lambda x: (time.sleep(0), x)[1]).collect().sink(out.extend)
        if {compiled}:
            s.compile()
        stop = threading.Event()
        def forever():
            i = 0
            while not stop.is_set():
                s.emit(i)
                i += 1
        t = threading.Thread(target=forever, daemon=True)
        t.start()
        time.sleep(0.05)
        s.flush()
        stop.set()
        t.join()
        s.flush()
        print(len(out) > 0 and out == list(range(len(out))))
    """,
        timeout=60,
    )
    assert (code, last) == (0, "True")


def test_flush_at_exit_while_a_daemon_thread_emits():
    code, last = run_script(
        """
        import akayu, atexit, threading, time
        s = akayu.Stream()
        s.map(lambda x: (time.sleep(0.001), x)[1]).collect().sink(lambda t: None)
        def forever():
            i = 0
            while True:
                s.emit(i)
                i += 1
        threading.Thread(target=forever, daemon=True).start()
        atexit.register(lambda: (s.flush(), print("flushed")))
        time.sleep(0.05)
    """,
        timeout=60,
    )
    assert (code, last) == (0, "flushed")


def test_emit_from_another_thread_waits_for_a_flush(compiled):
    s, got = akayu.Stream(), []
    flushing = threading.Event()

    def slow(t):
        flushing.set()
        time.sleep(0.2)
        got.append(t)

    s.map(lambda x: x).collect().sink(slow)
    if compiled:
        s.compile()
    s.emit_batch([1, 2])
    errors = []

    def emitter():
        flushing.wait()
        try:
            s.emit(3)  # used to raise "Already mutably borrowed"
        except Exception as e:
            errors.append(e)

    t = threading.Thread(target=emitter)
    t.start()
    s.flush()
    t.join()
    s.flush()
    assert errors == []
    assert got == [(1, 2), (3,)]


def test_a_timer_flushes_a_collect_while_a_thread_emits(compiled):
    # Uncompiled, an item reaching the collect mid-flush failed as having
    # "re-entered" it: nearly every item was lost.
    s, out = akayu.Stream(), []
    c = s.map(lambda x: x).collect()
    c.sink(lambda t: (time.sleep(0.001), out.extend(t)))
    if compiled:
        s.compile()
    errors = []

    def emitter():
        for i in range(1000):
            try:
                s.emit(i)
            except Exception as e:
                errors.append(e)
            if i % 50 == 0:
                time.sleep(0)

    t = threading.Thread(target=emitter)
    t.start()
    while t.is_alive():
        c.flush()
    t.join()
    c.flush()
    assert errors == []
    assert out == list(range(1000))


def test_a_timer_flush_feeding_back_into_a_busy_source(compiled):
    # The flush's callback emits into the source another thread is emitting
    # into, while that thread's item waits for the flush. Uncompiled, the
    # callback's emit closes the loop and is refused (a ReentrantError the
    # flush raises), while the item goes on: no item is lost. (It used to be
    # the other way round, after the item had waited 50 ms.) Compiled, the
    # graph's one lock lets the two take turns.
    code, last = run_script(
        f"""
        import akayu, threading, time
        s, sizes = akayu.Stream(), []
        c = s.filter(lambda x: x >= 0).collect()
        c.sink(lambda t: (time.sleep(0.002), sizes.append(len(t)), t and s.emit(-1)))
        if {compiled}:
            s.compile()
        errors, flush_errors = [], []
        def emitter():
            for i in range(1000):
                try:
                    s.emit(i)
                except RuntimeError as e:
                    errors.append(e)
                if i % 50 == 0:
                    time.sleep(0)
        t = threading.Thread(target=emitter, daemon=True)
        t.start()
        while t.is_alive():
            try:
                c.flush()
            except RuntimeError as e:
                flush_errors.append(e)
        c.flush()
        def root(e):
            while e.__cause__ is not None:
                e = e.__cause__
            return e
        named = all(isinstance(root(e), akayu.ReentrantError) for e in flush_errors)
        print(sum(sizes) == 1000 and not errors, named, len(flush_errors) if {compiled} else "-")
    """,
        timeout=60,
    )
    assert (code, last) == (0, "True True 0" if compiled else "True True -")


def test_emit_into_a_source_from_its_own_flush(compiled):
    s, got = akayu.Stream(), []
    s.collect().sink(lambda t: (got.append(t), s.emit(0)))
    if compiled:
        s.compile()
    s.emit(1)
    with pytest.raises(RuntimeError) as info:
        s.flush()
    assert "callback of its own flush()" in str(innermost(info.value))
    assert got == [(1,)]


@WHERE
@pytest.mark.parametrize("entry", ["flush", "drain"])
def test_flushing_a_very_deep_graph_raises_recursion_error(entry, where):
    build = f"""
        def run():
            s = akayu.Stream(); node = s
            for _ in range(60000):
                node = node.map(lambda x: x)
            out = []
            node.collect().sink(out.append)
            s.{entry}()
            return out
    """
    run_deep(build, where, "[()]")


# --- batch_map and prefetch ---


@pytest.mark.parametrize("prefetch", [False, True])
def test_batch_map_results_per_item(prefetch, compiled):
    s, out = akayu.Stream(), []
    node = s.prefetch(4) if prefetch else s
    node.batch_map(lambda xs: [y for x in xs if x % 3 for y in (x, -x)]).sink(out.append)
    if compiled:
        s.compile()
    for x in range(1, 7):
        s.emit(x)
    s.flush()
    assert out == [1, -1, 2, -2, 4, -4, 5, -5]  # every result, none for 3 and 6


def test_prefetch_flush_delivers_the_results_before_an_error():
    s, out = akayu.Stream(), []
    s.prefetch(100).map(lambda x: 10 // (x % 5) and x).sink(out.append)
    errors = 0
    for x in range(20):
        s.emit(x)
    for _ in range(10):
        try:
            s.flush()
            break
        except RuntimeError:
            errors += 1
    assert errors == 4
    assert out == [x for x in range(20) if x % 5]


def test_prefetch_size_is_validated():
    for n in (0, -1):
        with pytest.raises(ValueError, match="at least 1"):
            akayu.Stream().prefetch(n)


# --- chunk_by and compile()'s fusion of the collect/flush idiom ---


def test_chunk_by_doesnt_keep_an_item_whose_test_failed():
    s, out = akayu.Stream(), []
    s.chunk_by(lambda x: x == 0).sink(out.append)
    for x in [1, 2]:
        s.emit(x)
    with pytest.raises(RuntimeError):
        s.emit(_Unequal())
    s.emit(3)
    s.emit(0)
    assert out == [(1, 2, 3)]


class _Unequal:
    def __eq__(self, other):
        raise ValueError("no comparing")

    __hash__ = object.__hash__


def fused_idiom(filter_first, compiled, emit_before_compile=()):
    """`collect()` plus `filter(p).sink(lambda _: c.flush())` from one branch
    point, in either order; compile() fuses it into a chunk_by node."""
    s, out = akayu.Stream(), []
    if filter_first:
        f = s.filter(lambda x: x == 0)
        c = s.collect()
    else:
        c = s.collect()
        f = s.filter(lambda x: x == 0)
    f.sink(lambda _: c.flush())
    c.sink(out.append)
    for x in emit_before_compile:
        s.emit(x)
    if compiled:
        s.compile()
    for x in [1, 2, 0, 3, 0, 0, 4]:
        s.emit(x)
    s.flush()
    return out


@pytest.mark.parametrize("filter_first", [False, True])
def test_fused_collect_flush_keeps_branch_order(filter_first):
    assert fused_idiom(filter_first, compiled=True) == fused_idiom(filter_first, compiled=False)


def test_compile_keeps_what_the_collect_holds():
    out = fused_idiom(False, compiled=True, emit_before_compile=[7, 8])
    assert out[0] == (7, 8, 1, 2, 0)


def filter_with_another_branch(compiled):
    s, out, also = akayu.Stream(), [], []
    c = s.collect()
    f = s.filter(lambda x: x == 0)
    f.sink(lambda _: c.flush())
    f.sink(also.append)  # a second branch below the filter
    c.sink(out.append)
    if compiled:
        s.compile()
    for x in [1, 0, 2, 0]:
        s.emit(x)
    return out, also


def test_a_filter_with_other_branches_isnt_fused():
    assert filter_with_another_branch(compiled=True) == filter_with_another_branch(compiled=False)


# --- The lambda analyzer: native forms must compute what Python computes ---

ANALYZED = [
    (lambda x: (x[0] or x[1]) * 10, [3, 2]),
    (lambda x: not (x[0] or x[1]), [1, 0]),
    (lambda x: (x[0] or x[1]) == 0, [5, 0]),
    (lambda x: (x.strip() and x[0]) + "z", "  "),
    (lambda x: not not x["k"], {"k": 5}),
    (lambda x: (x.real.real, x.imag), 3 + 4j),
    (lambda x: (str(x) + "!", x.real), 5),
]


@pytest.mark.parametrize(("func", "item"), ANALYZED)
def test_analyzed_lambdas_match_python(func, item, compiled):
    s, out = akayu.Stream(), []
    s.map(func).sink(out.append)
    if compiled:
        s.compile()
    s.emit(item)
    assert out == [func(item)]
    assert type(out[0]) is type(func(item))


class Expr:
    """A comparison that builds an expression instead of a bool (numpy
    arrays, query builders), and an __eq__ that is never true."""

    def __gt__(self, other):
        return ("gt", other)

    def __eq__(self, other):
        return False

    __hash__ = object.__hash__


def test_comparisons_return_what_the_operator_returns(compiled):
    s, out = akayu.Stream(), []
    s.map(lambda x: x > 3).sink(out.append)
    if compiled:
        s.compile()
    s.emit(Expr())
    assert out == [("gt", 3)]


SENTINEL = Expr()


def test_equality_has_no_identity_shortcut(compiled):
    s, out = akayu.Stream(), []
    s.filter(lambda x: x == SENTINEL).sink(out.append)
    if compiled:
        s.compile()
    s.emit(SENTINEL)
    assert out == []  # SENTINEL == SENTINEL is False


# --- Errors name the node in every mode ---


@pytest.mark.parametrize("batch", [False, True])
def test_flatten_errors_name_the_node(compiled, batch):
    s = akayu.Stream()
    s.flatten().sink(lambda x: None)
    if compiled:
        s.compile()
    with pytest.raises(RuntimeError, match="flatten") as info:
        s.emit_batch([5]) if batch else s.emit(5)
    assert isinstance(info.value.__cause__, TypeError)


class BadBool:
    def __bool__(self):
        raise ValueError("no truth")


def test_a_failing_truth_test_names_the_filter(compiled):
    s = akayu.Stream()
    s.filter(lambda x: x.value).sink(lambda x: None)
    if compiled:
        s.compile()
    item = type("Item", (), {"value": BadBool()})()
    with pytest.raises(RuntimeError, match="filter") as info:
        s.emit(item)
    assert isinstance(info.value.__cause__, ValueError)


# --- Odd callables, API misuse and environments (probing round 3) ---


@pytest.mark.parametrize(
    ("src", "item"),
    [("abs(x) + 1 + 2", -5), ("repr(x)[0][:2]", "ab"), ("sorted(x)[0][1]", [[3, 4], [1, 2]])],
)
def test_a_call_the_optimizer_cant_resolve_still_heads_the_chain(src, item, compiled):
    # abs/repr/sorted aren't builtins compile() knows: the steps after them
    # were run on the item itself.
    f = eval(f"lambda x: {src}")
    s, out = akayu.Stream(), []
    s.map(f).sink(out.append)
    if compiled:
        s.compile()
    s.emit(item)
    assert out == [f(item)]


@pytest.mark.parametrize("form", ["bound", "lambda"])
def test_a_subclass_emit_override_runs_when_compiled(form):
    class Scaled(akayu.Stream):
        def emit(self, x):
            super().emit(x * 10)

    source, target, out = akayu.Stream(), Scaled(), []
    target.sink(out.append)
    source.sink(target.emit if form == "bound" else (lambda x: target.emit(x)))
    source.compile()
    source.emit(2)
    assert out == [20]


def test_a_dotted_attrgetter_compiled():
    s, out = akayu.Stream(), []
    s.map(operator.attrgetter("real.imag")).sink(out.append)
    s.compile()
    s.emit(3 + 4j)
    assert out == [0.0]


@pytest.mark.parametrize("sl", [slice(None, -1), slice(-2, None), slice(None, None, -1), slice(-1, None)])
def test_keyed_multistream_slices_keep_their_keys(sl):
    streams = {"a": akayu.Stream(), "b": akayu.Stream(), "c": akayu.Stream()}
    sub = akayu.MultiStream(streams)[sl]
    assert list(sub.keys()) == list(streams)[sl]
    assert list(sub) == list(streams.values())[sl]


@pytest.mark.parametrize("bad", ["foreign", "empty", "mixed"])
def test_combine_latest_emit_on_must_name_an_input(bad):
    a, b, other = akayu.Stream(), akayu.Stream(), akayu.Stream()
    emit_on = {"foreign": other, "empty": [], "mixed": [a, other]}[bad]
    with pytest.raises(ValueError, match="emit_on"):
        akayu.combine_latest(a, b, emit_on=emit_on)
    with pytest.raises(ValueError, match="emit_on"):
        a.combine_latest(b, emit_on=emit_on)


def test_a_prefetched_batch_map_takes_the_whole_batch(compiled):
    s, out = akayu.Stream(), []
    s.prefetch(2).batch_map(lambda batch: [len(batch)]).sink(out.append)
    if compiled:
        s.compile()
    s.emit_batch([1, 2, 3])
    s.emit(4)
    s.flush()
    assert out == [3, 1]  # as batch_map() without prefetch()


def test_returns_state_errors_name_the_node(compiled):
    s = akayu.Stream()
    s.accumulate(lambda acc, x: acc + x, 0, returns_state=True).sink(lambda x: None)
    if compiled:
        s.compile()
    with pytest.raises(RuntimeError, match="accumulate") as info:
        s.emit(1)
    assert isinstance(info.value.__cause__, TypeError)
    assert "(state, output)" in str(info.value.__cause__)


def test_a_huge_prefetch_size_is_a_value_error():
    with pytest.raises(ValueError, match="at least 1"):
        akayu.Stream().prefetch(2**64)


def test_an_object_with_async_call_is_refused():
    class Fetcher:
        async def __call__(self, x):
            return x

    with pytest.raises(TypeError, match="does not run coroutines"):
        akayu.Stream().map(Fetcher())


def test_a_chunk_by_flushed_from_its_own_sink(compiled):
    # Compiled, the flush ran on the chunk_by while it was mid-update.
    s, out = akayu.Stream(), []
    c = s.map(lambda x: x).chunk_by(lambda x: x % 3 == 0)

    def sink(chunk):
        out.append(chunk)
        try:
            c.flush()
        except RuntimeError as e:
            out.append("refused" if "still processing" in str(innermost(e)) else str(e))

    c.sink(sink)
    if compiled:
        s.compile()
    for i in range(1, 7):
        s.emit(i)
    assert out == [(1, 2), "refused", (4, 5), "refused"]


def test_import_without_package_metadata():
    code, last = run_script("""
        import importlib.metadata as md
        def missing(name):
            raise md.PackageNotFoundError(name)
        md.version = missing
        import akayu
        print(akayu.__version__)
    """)
    assert (code, last) == (0, "unknown")


def test_the_stream_type_names_its_module():
    assert akayu.Stream.__module__ == "akayu"


# --- Many threads at once (probing round 4) ---


def test_a_flush_and_an_emit_from_different_sources_of_a_union(compiled, fast_switching):
    # Below a union, an emit holds the union's lock, not the flushed source's:
    # the flush has to take the union's lock too, or it meets the emit there.
    # Compiled, a union input fused into a chain (here after flatten) lost
    # the lock on its edge, so two sources' items were in the union at once.
    def let_others_in(x):
        time.sleep(0)
        return x

    sources = [akayu.Stream() for _ in range(2)]
    tails = [s.map(lambda x: [x]).flatten().map(let_others_in) for s in sources]
    merged = akayu.union(*tails)
    outs = ([], [])
    branches = merged.par()
    branches.map(let_others_in).sink(outs[0].append)
    branches.map(let_others_in).sink(outs[1].append)
    if compiled:
        sources[0].compile()
    errors, done = [], threading.Event()

    def emitter(t):
        for i in range(300):
            try:
                sources[t].emit((t, i))
            except Exception as e:
                errors.append(e)

    def flusher():
        while not done.is_set():
            for s in sources:
                try:
                    s.flush()
                except Exception as e:
                    errors.append(e)

    threads = [threading.Thread(target=emitter, args=(t,)) for t in range(2)]
    f = threading.Thread(target=flusher)
    for t in [*threads, f]:
        t.start()
    for t in threads:
        t.join()
    done.set()
    f.join()
    assert errors == []
    assert sorted(outs[0]) == sorted(outs[1]) == sorted((t, i) for t in range(2) for i in range(300))


class InterruptingGlobals(dict):
    """Globals whose lookups raise KeyboardInterrupt once armed, as a Ctrl+C
    landing while compile() analyses a function would."""

    armed = False

    def __contains__(self, key):
        if InterruptingGlobals.armed:
            raise KeyboardInterrupt
        return dict.__contains__(self, key)


def test_ctrl_c_during_compile_is_not_swallowed():
    f = eval("lambda x: x + k", InterruptingGlobals(k=1))
    s, out = akayu.Stream(), []
    s.map(f).sink(out.append)
    InterruptingGlobals.armed = True
    try:
        with pytest.raises(KeyboardInterrupt):
            s.compile()
        with pytest.raises(KeyboardInterrupt):
            akayu.Stream().map(f)  # analysed when built, too
    finally:
        InterruptingGlobals.armed = False
    s.emit(1)  # the graph was compiled all the same
    assert out == [2]


def test_the_flush_idiom_below_seq_compiles():
    # compile() bypassed the seq() node but left its children's parent links
    # on it; chunk_by fusion followed them and left a dead node in the graph.
    s, out = akayu.Stream(), []
    q = s.seq()
    c = q.collect()
    q.filter(lambda x: x == 0).sink(lambda _: c.flush())
    c.sink(out.append)
    s.compile()
    s.emit(5)
    s.emit(0)
    assert out == [(5, 0)]


def test_seq_below_par_stays_sequential(compiled):
    s, log = akayu.Stream(), []
    q = s.par().seq()

    def step(name):
        def run(x):
            log.append((f"{name} start", threading.current_thread().name))
            time.sleep(0.02)
            log.append((f"{name} end", threading.current_thread().name))

        return run

    q.sink(step("a"))
    q.sink(step("b"))
    if compiled:
        with pytest.warns(UserWarning, match="par"):
            s.compile()
    s.emit(1)
    assert [event for event, _ in log] == ["a start", "a end", "b start", "b end"]
    assert len({thread for _, thread in log}) == 1


def test_a_failing_branch_below_par_seq_stops_the_next(compiled):
    s, out = akayu.Stream(), []
    q = s.par().seq()
    q.filter(lambda x: x % 2 == 0).sink(lambda x: None)  # raises for a str
    q.map(str).sink(out.append)
    if compiled:
        with pytest.warns(UserWarning, match="par"):
            s.compile()
    with pytest.raises(RuntimeError):
        s.emit("ab")
    assert out == []  # as below a source: the second branch never sees it


@pytest.mark.parametrize("compiled", ["none", "first", "both"])
def test_reentry_is_refused_when_only_one_source_is_compiled(compiled):
    s0, s1, out = akayu.Stream(), akayu.Stream(), []
    merged = akayu.zip(s0, s1)
    merged.sink(out.append)
    merged.sink(lambda t: s0.emit(t[0] + 1000) if t[0] < 1000 else None)
    if compiled in ("first", "both"):
        s0.compile()
    if compiled == "both":
        s1.compile()
    s0.emit(1)
    with pytest.raises(RuntimeError) as info:
        s1.emit("a")
    assert "re-entered" in str(innermost(info.value))


@pytest.mark.parametrize("call", ["emit", "flush"])
def test_a_par_branch_on_a_pool_thread_feeding_back_raises(call, compiled):
    # On the calling thread's branch this raised; on a pool thread it waited
    # for the source's lock, held by the thread waiting for that branch.
    code, last = run_script(
        f"""
        import akayu, time
        s, results = akayu.Stream(), []
        branches = s.par()
        def feed_back(x):
            time.sleep(0.01)
            try:
                s.{call}({"-1" if call == "emit" else ""})
                results.append("ok")
            except RuntimeError:
                results.append("refused")
        for _ in range(3):
            branches.map(lambda x: x).sink(feed_back)
        branches.map(lambda x: x).sink(lambda x: time.sleep(0.03))
        if {compiled}:
            s.compile()
        for i in range(10):
            try:
                s.emit(i)
            except RuntimeError:
                pass
        print(set(results))
    """,
        timeout=60,
    )
    assert (code, last) == (0, "{'refused'}")


def test_a_failed_prefetched_batch_keeps_the_finished_results(compiled):
    def boom3(x):
        if x == 3:
            raise ValueError("boom at 3")
        return x * 10

    s, out = akayu.Stream(), []
    s.prefetch(2).map(boom3).sink(out.append)
    if compiled:
        s.compile()
    s.emit(1)
    with pytest.raises(RuntimeError):
        s.emit_batch([2, 3, 4])
    s.flush()
    assert out == [10, 20, 40]  # as per-item emits deliver


def test_sustained_par_emits_dont_pile_up_in_the_pool():
    # Tasks a submitter ran itself stayed queued until a worker took the GIL
    # to drop each one: memory grew by ~45 B per item while emits continued.
    code, last = run_script("""
        import akayu, psutil
        proc = psutil.Process()
        s = akayu.Stream()
        branches = s.par()
        branches.map(lambda x: x).sink(lambda x: None)
        branches.map(lambda x: x).sink(lambda x: None)
        for i in range(100_000):
            s.emit(i)
        before = proc.memory_info().rss
        for i in range(300_000):
            s.emit(i)
        print((proc.memory_info().rss - before) // 2**20)
    """)
    assert code == 0
    assert int(last) < 6  # MB; it was ~13


def test_the_profile_of_a_very_deep_compiled_graph_prints():
    code, last = run_script("""
        import akayu
        s = akayu.Stream(); node = s
        for i in range(40_000):
            node = node.map(lambda x: x)
        node.sink(lambda x: None)
        s.compile()
        report = s.emit_profiled(1)
        print(len(str(report)) < 50_000_000)
    """)
    assert (code, last) == (0, "True")


def test_a_profile_prints_a_node_below_a_merge_once():
    s = akayu.Stream()
    node = s
    for _ in range(16):  # stacked diamonds: 2**16 paths to the bottom
        node = akayu.union(node.map(lambda x: x), node.map(lambda x: x))
    node.sink(lambda x: None)
    text = str(s.emit_profiled(1))
    assert "(shown above)" in text
    assert len(text.splitlines()) < 400


# --- Signal handlers, finalizers and other threads around par() (probing round 5) ---


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="needs signal.setitimer")
@pytest.mark.parametrize("use_par", [False, True])
def test_a_signal_handler_emitting_into_the_busy_source_is_refused(use_par):
    """The handler runs on the main thread mid-emit (with par(), while it
    waits for the branches): re-entry, not a deadlock on the source's lock."""
    s, out, errors = akayu.Stream(), [], []
    slow = lambda x: (time.sleep(0.3), x)[1]  # noqa: E731
    if use_par:
        p = s.par()
        akayu.union(p.map(slow), p.map(lambda x: x)).seq().sink(out.append)
    else:
        s.map(slow).sink(out.append)
    s.compile()

    def handler(signum, frame):
        try:
            s.emit(100)
        except RuntimeError as e:
            errors.append(str(innermost(e)))

    old = signal.signal(signal.SIGALRM, handler)
    try:
        signal.setitimer(signal.ITIMER_REAL, 0.1)
        s.emit(1)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old)
    assert out == ([1, 1] if use_par else [1])
    assert len(errors) == 1
    assert "Re-entrant emit" in errors[0]


def test_finalizers_emitting_from_par_branches():
    """Garbage collection runs __del__ on whichever thread triggers it, here
    pool threads mid-branch. An emit from it into another graph works; only
    one landing in a source its own thread is emitting into is refused."""
    target, out, lock = akayu.Stream(), [], threading.Lock()

    def keep(x):
        with lock:
            out.append(x)

    tp = target.par()
    akayu.union(tp.map(lambda x: x), tp.map(lambda x: x)).seq().sink(keep)
    target.compile()
    errors = []

    class Cycle:
        def __init__(self):
            self.me = self  # only the cycle collector frees it

        def __del__(self):
            try:
                target.emit(-1)
            except RuntimeError as e:
                errors.append(str(innermost(e)))

    def branch(x):
        for _ in range(5):
            Cycle()
        return x

    s = akayu.Stream()
    p = s.par()
    akayu.union(p.map(branch), p.map(branch)).seq().sink(target.emit)
    s.compile()
    threshold = gc.get_threshold()
    gc.set_threshold(10)
    try:
        for i in range(200):
            s.emit(i)
    finally:
        gc.set_threshold(*threshold)
    gc.collect()
    assert all("Re-entrant emit" in e for e in errors), set(errors)
    finalized = 200 * 2 * 5 - len(errors)
    assert sorted(out) == sorted([-1] * 2 * finalized + [i for i in range(200) for _ in range(4)])


def test_a_par_branch_waiting_on_its_own_executor():
    ex = ThreadPoolExecutor(4)
    s, out = akayu.Stream(), []
    p = s.par()
    akayu.union(
        p.map(lambda x: sum(ex.map(lambda y: y * 2, range(x, x + 8)))),
        p.map(lambda x: ex.submit(lambda: -x).result()),
    ).seq().sink(out.append)
    s.compile()
    try:
        for i in range(200):
            s.emit(i)
    finally:
        ex.shutdown()
    assert sorted(out) == sorted([sum(range(i, i + 8)) * 2 for i in range(200)] + [-i for i in range(200)])


def test_a_par_branch_joining_a_thread_that_emits_into_another_par_graph():
    other, other_out = akayu.Stream(), []
    op = other.par()
    akayu.union(op.map(lambda x: x + 1), op.map(lambda x: x - 1)).seq().sink(other_out.append)
    other.compile()

    def branch(x):
        t = threading.Thread(target=other.emit, args=(x,))
        t.start()
        t.join()
        return x

    s, out = akayu.Stream(), []
    p = s.par()
    akayu.union(p.map(branch), p.map(branch)).seq().sink(out.append)
    s.compile()
    for i in range(100):
        s.emit(i)
    assert sorted(out) == sorted([*range(100), *range(100)])
    assert sorted(other_out) == sorted([i + d for i in range(100) for d in (1, 1, -1, -1)])


def test_many_short_lived_threads_through_par_and_prefetch():
    s, out, lock = akayu.Stream(), [], threading.Lock()

    def keep(x):
        with lock:
            out.append(x)

    p = s.par()
    akayu.union(p.map(lambda x: x * 2), p.prefetch(2).map(lambda x: x * 3)).seq().sink(keep)
    s.compile()
    threads = [threading.Thread(target=s.emit, args=(i,)) for i in range(300)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    s.flush()
    assert sorted(out) == sorted([i * k for i in range(300) for k in (2, 3)])


# --- Waiting for a source's lock (probing round 5) ---
# A thread waits for a source's lock only while its holder isn't waiting for
# it in turn. This used to be judged by who submitted the waiting pool task:
# its submitter holding the source counted as a feedback loop, even when it
# held it only briefly, while running work itself.


def shared_stream():
    """A stream emitted into from several threads, whose pipeline releases
    the GIL, as I/O does."""
    log, logged, lock = akayu.Stream(name="log"), [], threading.Lock()

    def keep(x):
        with lock:
            logged.append(x)

    log.map(lambda x: (time.sleep(0.001), x)[1]).sink(keep)
    return log, logged


def test_par_branches_emitting_into_a_shared_stream(compiled):
    """The calling thread runs one branch itself; its emit into the stream
    made a pool thread's emit fail as a feedback loop: every emit() failed."""
    log, logged = shared_stream()
    s = akayu.Stream()
    p = s.par()
    akayu.union(
        p.map(lambda x: (log.emit(("a", x)), x)[1]),
        p.map(lambda x: (log.emit(("b", x)), x)[1]),
    ).seq().sink(lambda _: None)
    if compiled:
        s.compile()
        log.compile()
    for i in range(30):
        s.emit(i)
    assert sorted(logged) == sorted((k, i) for i in range(30) for k in "ab")


def test_prefetched_calls_emitting_into_a_shared_stream(compiled):
    """A thread waiting for its oldest prefetched call runs it itself if no
    worker has started it, and so held the stream while workers ran later
    calls: about half the items failed."""
    log, logged = shared_stream()
    s = akayu.Stream()
    s.prefetch(4).map(lambda x: (log.emit(("call", x)), x)[1]).sink(log.emit)
    if compiled:
        s.compile()
        log.compile()
    for i in range(30):
        s.emit(i)
    s.flush()
    assert sorted(logged, key=repr) == sorted([*range(30), *(("call", i) for i in range(30))], key=repr)


def test_threads_emitting_into_each_others_sources_raise_instead_of_deadlocking(compiled):
    """Each holds its own source and waits for the other's: a loop through
    the two callbacks, across threads. Exactly one raises, as a re-entrant
    emit; the other finishes. Both used to wait forever, then one (rarely
    both) raised."""
    code, last = run_script(
        f"""
        import threading, time, akayu
        a, b = akayu.Stream(), akayu.Stream()
        got, errors = [], []
        def into(other):
            def callback(x):
                time.sleep(0.05)  # both threads hold their own source by now
                if x == "first":
                    other.emit("crossed")
                got.append(x)
            return callback
        a.sink(into(b))
        b.sink(into(a))
        if {compiled}:
            a.compile()
            b.compile()
        def run(source):
            try:
                source.emit("first")
            except RuntimeError as e:
                errors.append(e)
        threads = [threading.Thread(target=run, args=(s,), daemon=True) for s in (a, b)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(20)
        hung = any(t.is_alive() for t in threads)
        reentrant = all(isinstance(e.__cause__ or e, akayu.ReentrantError) for e in errors)
        # One raises and the other finishes.
        print(hung, len(errors) == 1 and sorted(got) == ["crossed", "first"], reentrant)
    """,
        timeout=60,
    )
    assert (code, last) == (0, "False True True")


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
def test_ctrl_c_reaches_a_thread_waiting_for_a_source(compiled):
    """It used to take effect only once the other thread let go."""
    s = akayu.Stream()
    holding, release = threading.Event(), threading.Event()

    def hold(x):
        if x == "hold":
            holding.set()
            release.wait(10)

    s.sink(hold)
    if compiled:
        s.compile()
    holder = threading.Thread(target=s.emit, args=("hold",))
    holder.start()
    assert holding.wait(10)
    threading.Timer(0.2, signal.raise_signal, (signal.SIGINT,)).start()
    start = time.perf_counter()
    try:
        with pytest.raises(KeyboardInterrupt):
            s.emit("waits")
        assert time.perf_counter() - start < 5
    finally:
        release.set()
        holder.join()
