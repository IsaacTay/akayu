"""Unusual flows people bend akayu into, which used to crash, hang, panic or
silently lose data. Each test pins the behaviour it was fixed to; the ones
that used to crash the interpreter run in a subprocess.
"""

import operator
import subprocess
import sys
import textwrap
import threading
import time

import pytest

import akayu


def run_script(code, timeout=120):
    """Run `code` in a fresh interpreter: (exit code, last line of output)."""
    r = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    lines = (r.stdout + r.stderr).strip().splitlines()
    return r.returncode, lines[-1] if lines else ""


def innermost(e):
    while e.__cause__ is not None:
        e = e.__cause__
    return e


# --- Deep graphs: a clean RecursionError, never a native stack overflow ---


@pytest.mark.parametrize("entry", ["emit", "emit_batch"])
def test_a_very_deep_graph_raises_recursion_error(entry):
    code, last = run_script(f"""
        import akayu
        s = akayu.Stream(); node = s
        def inc(x):
            y = x + 1
            return y
        for _ in range(30000):
            node = node.map(inc)
        node.sink(print)
        try:
            s.{entry}(0) if "{entry}" == "emit" else s.emit_batch([0])
        except BaseException as e:
            while e.__cause__ is not None:
                e = e.__cause__
            print(type(e).__name__)
    """)
    assert (code, last) == (0, "RecursionError")


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


def test_a_long_chain_of_sources():
    code, last = run_script("""
        import akayu
        sources = [akayu.Stream() for _ in range(8000)]
        for a, b in zip(sources, sources[1:]):
            a.sink(b.emit)
        try:
            sources[0].emit(1)
        except BaseException as e:
            while e.__cause__ is not None:
                e = e.__cause__
            print(type(e).__name__)
    """)
    assert (code, last) == (0, "RecursionError")


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


@pytest.mark.parametrize("compiled", [False, True])
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


@pytest.mark.parametrize("compiled", [False, True])
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
@pytest.mark.parametrize("compiled", [False, True])
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


@pytest.mark.parametrize("compiled", [False, True])
def test_one_flush_reaches_a_node_below_a_merge_once(compiled):
    s, out = akayu.Stream(), []
    akayu.union(s.map(lambda x: x), s.map(lambda x: x * 10)).collect().sink(out.append)
    if compiled:
        s.compile()
    s.emit(1)
    s.flush()
    assert out == [(1, 10)]  # used to add an empty () per extra path


@pytest.mark.parametrize("compiled", [False, True])
@pytest.mark.parametrize("kind", ["source", "collect"])
def test_flush_from_another_thread_waits_for_emits(compiled, kind):
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

    old = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        t = threading.Thread(target=flusher)
        t.start()
        emitters = [threading.Thread(target=lambda k=k: [s.emit(k * 300 + i) for i in range(300)]) for k in range(3)]
        for e in emitters:
            e.start()
        for e in emitters:
            e.join()
        stop.set()
        t.join()
    finally:
        sys.setswitchinterval(old)
    s.flush()
    assert errors == []
    assert sorted(out) == list(range(900))


@pytest.mark.parametrize("compiled", [False, True])
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


@pytest.mark.parametrize("compiled", [False, True])
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


@pytest.mark.parametrize("compiled", [False, True])
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


@pytest.mark.parametrize("compiled", [False, True])
def test_a_timer_flush_feeding_back_into_a_busy_source(compiled):
    # The flush's callback emits into the source another thread is emitting
    # into, while that thread waits for the flush: uncompiled, the waiting
    # item fails instead of deadlocking; compiled, the two take turns.
    code, last = run_script(
        f"""
        import akayu, threading, time
        s, sizes = akayu.Stream(), []
        c = s.filter(lambda x: x >= 0).collect()
        c.sink(lambda t: (time.sleep(0.002), sizes.append(len(t)), t and s.emit(-1)))
        if {compiled}:
            s.compile()
        errors = []
        def emitter():
            for i in range(1000):
                try:
                    s.emit(i)
                except RuntimeError as e:
                    while e.__cause__ is not None:
                        e = e.__cause__
                    errors.append(str(e))
                if i % 50 == 0:
                    time.sleep(0)
        t = threading.Thread(target=emitter, daemon=True)
        t.start()
        while t.is_alive():
            c.flush()
        c.flush()
        lost = len(errors)
        named = all("another thread is flushing" in e for e in errors)
        print(sum(sizes) + lost == 1000, named, lost if {compiled} else "-")
    """,
        timeout=60,
    )
    assert (code, last) == (0, "True True 0" if compiled else "True True -")


@pytest.mark.parametrize("compiled", [False, True])
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


@pytest.mark.parametrize("entry", ["flush", "drain"])
def test_flushing_a_very_deep_graph_raises_recursion_error(entry):
    code, last = run_script(f"""
        import akayu
        s = akayu.Stream(); node = s
        for _ in range(60000):
            node = node.map(lambda x: x)
        node.collect().sink(print)
        try:
            s.{entry}()
        except BaseException as e:
            while e.__cause__ is not None:
                e = e.__cause__
            print(type(e).__name__)
    """)
    assert (code, last) == (0, "RecursionError")


# --- batch_map and prefetch ---


@pytest.mark.parametrize("prefetch", [False, True])
@pytest.mark.parametrize("compiled", [False, True])
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
@pytest.mark.parametrize("compiled", [False, True])
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


@pytest.mark.parametrize("compiled", [False, True])
def test_comparisons_return_what_the_operator_returns(compiled):
    s, out = akayu.Stream(), []
    s.map(lambda x: x > 3).sink(out.append)
    if compiled:
        s.compile()
    s.emit(Expr())
    assert out == [("gt", 3)]


SENTINEL = Expr()


@pytest.mark.parametrize("compiled", [False, True])
def test_equality_has_no_identity_shortcut(compiled):
    s, out = akayu.Stream(), []
    s.filter(lambda x: x == SENTINEL).sink(out.append)
    if compiled:
        s.compile()
    s.emit(SENTINEL)
    assert out == []  # SENTINEL == SENTINEL is False


# --- Errors name the node in every mode ---


@pytest.mark.parametrize("compiled", [False, True])
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


@pytest.mark.parametrize("compiled", [False, True])
def test_a_failing_truth_test_names_the_filter(compiled):
    s = akayu.Stream()
    s.filter(lambda x: x.value).sink(lambda x: None)
    if compiled:
        s.compile()
    item = type("Item", (), {"value": BadBool()})()
    with pytest.raises(RuntimeError, match="filter") as info:
        s.emit(item)
    assert isinstance(info.value.__cause__, ValueError)


# --- Round 3: odd callables, API misuse, environments ---


@pytest.mark.parametrize("compiled", [False, True])
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


@pytest.mark.parametrize("compiled", [False, True])
def test_a_prefetched_batch_map_takes_the_whole_batch(compiled):
    s, out = akayu.Stream(), []
    s.prefetch(2).batch_map(lambda batch: [len(batch)]).sink(out.append)
    if compiled:
        s.compile()
    s.emit_batch([1, 2, 3])
    s.emit(4)
    s.flush()
    assert out == [3, 1]  # as batch_map() without prefetch()


@pytest.mark.parametrize("compiled", [False, True])
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


@pytest.mark.parametrize("compiled", [False, True])
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


# --- Round 4: many threads at once ---


@pytest.mark.parametrize("compiled", [False, True])
def test_a_flush_and_an_emit_from_different_sources_of_a_union(compiled):
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

    old = sys.getswitchinterval()
    sys.setswitchinterval(1e-5)
    try:
        threads = [threading.Thread(target=emitter, args=(t,)) for t in range(2)]
        f = threading.Thread(target=flusher)
        for t in [*threads, f]:
            t.start()
        for t in threads:
            t.join()
        done.set()
        f.join()
    finally:
        sys.setswitchinterval(old)
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


@pytest.mark.parametrize("compiled", [False, True])
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


@pytest.mark.parametrize("compiled", [False, True])
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
@pytest.mark.parametrize("compiled", [False, True])
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


@pytest.mark.parametrize("compiled", [False, True])
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
        import akayu
        def rss_kb():
            with open("/proc/self/statm") as f:
                return int(f.read().split()[1]) * 4
        s = akayu.Stream()
        branches = s.par()
        branches.map(lambda x: x).sink(lambda x: None)
        branches.map(lambda x: x).sink(lambda x: None)
        for i in range(100_000):
            s.emit(i)
        before = rss_kb()
        for i in range(300_000):
            s.emit(i)
        print((rss_kb() - before) // 1024)
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
