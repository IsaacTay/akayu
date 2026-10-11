"""Unusual flows people bend akayu into, which used to crash, hang, panic or
silently lose data. Each test pins the behaviour it was fixed to; the ones
that used to crash the interpreter run in a subprocess.
"""

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
        threading.stack_size(64 * 1024)
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
