"""The Rust thread pool behind par() and prefetch().

It replaced a Python ThreadPoolExecutor (a Future, Condition and work item
per task). These pin what it must keep doing: run branches and prefetched
items concurrently, in a copy of the caller's context, report errors after
every branch has finished, survive nesting deeper than the pool, stay
responsive to Ctrl+C, and work in a forked child.
"""

import contextvars
import os
import signal
import subprocess
import sys
import threading
import time

import pytest

import akayu

DELAY = 0.2


def slow(x):
    time.sleep(DELAY)
    return x


# --- par() -------------------------------------------------------------------


def test_par_branches_run_concurrently():
    s, out = akayu.Stream(), []
    p = s.par()
    for k in range(4):
        p.map(lambda x, k=k: slow(x + k)).sink(out.append)
    start = time.perf_counter()
    s.emit(0)
    elapsed = time.perf_counter() - start
    assert sorted(out) == [0, 1, 2, 3]
    assert elapsed < 3 * DELAY, f"branches ran one after another ({elapsed:.2f}s)"


request_id = contextvars.ContextVar("request_id", default=None)


def test_par_branches_see_the_callers_context_but_cannot_change_it():
    s, seen = akayu.Stream(), []
    p = s.par()

    def branch(x):
        seen.append(request_id.get())
        request_id.set("changed in branch")
        return x

    for _ in range(3):
        p.map(branch).sink(lambda _: None)
    token = request_id.set("req-42")
    try:
        s.emit(1)
        assert request_id.get() == "req-42"
    finally:
        request_id.reset(token)
    assert seen == ["req-42"] * 3


def test_par_raises_the_first_branchs_error_after_all_finish():
    s, finished = akayu.Stream(), []
    p = s.par()

    def first(x):
        time.sleep(DELAY)  # fails last in time, but is first in order
        raise ValueError("first branch")

    def last(x):
        finished.append(x)
        raise KeyError("last branch")

    p.map(first).sink(lambda _: None)
    p.map(slow).sink(finished.append)
    p.map(last).sink(lambda _: None)
    with pytest.raises(RuntimeError) as info:
        s.emit(7)
    assert isinstance(info.value.__cause__, ValueError)
    assert sorted(finished) == [7, 7]


def test_nested_par_wider_than_the_pool_does_not_deadlock():
    """With ThreadPoolExecutor, 40 outer branches on 16 workers deadlocked:
    every worker blocked on inner branches queued behind other outer ones."""
    width = 3 * (os.cpu_count() or 4)
    s, out = akayu.Stream(), []
    outer = s.par()
    for i in range(width):
        inner = outer.map(lambda x, i=i: x + i).par()
        for j in range(3):
            inner.map(lambda x, j=j: slow(x * 10 + j)).sink(out.append)
    t = threading.Thread(target=s.emit, args=(0,), daemon=True)
    t.start()
    t.join(30)
    assert not t.is_alive(), "nested par() deadlocked"
    assert len(out) == width * 3


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
def test_waiting_for_par_branches_is_interruptible():
    """Ctrl+C reaches a thread waiting on par() branches promptly."""
    s = akayu.Stream()
    p = s.par()
    started, released = threading.Event(), threading.Event()

    def slow_branch(x):
        started.set()
        released.wait(10)
        return x

    def inline_branch(x):
        # The last branch runs on the calling thread; once the slow one is
        # running on a worker, the caller can only wait for it.
        assert started.wait(10)
        return x

    def interrupt():
        started.wait(10)
        time.sleep(0.2)
        signal.raise_signal(signal.SIGINT)

    p.map(slow_branch).sink(lambda _: None)
    p.map(inline_branch).sink(lambda _: None)
    threading.Thread(target=interrupt, daemon=True).start()
    start = time.perf_counter()
    try:
        with pytest.raises(KeyboardInterrupt):
            s.emit(1)
        assert time.perf_counter() - start < 5
    finally:
        released.set()


# --- prefetch() ----------------------------------------------------------------


@pytest.mark.parametrize("op", ["map", "filter", "batch_map"])
def test_prefetch_overlaps_items_and_keeps_order(op):
    s, out = akayu.Stream(), []
    pre = s.prefetch(4)
    if op == "map":
        pre.map(lambda x: slow(x * 2)).sink(out.append)
        expected = [x * 2 for x in range(8)]
    elif op == "filter":
        pre.filter(lambda x: slow(x) % 2 == 0).sink(out.append)
        expected = [0, 2, 4, 6]
    else:
        pre.batch_map(lambda xs: [slow(x) + 1 for x in xs]).sink(out.append)
        expected = [x + 1 for x in range(8)]
    start = time.perf_counter()
    for x in range(8):
        s.emit(x)
    s.flush()
    elapsed = time.perf_counter() - start
    assert out == expected
    assert elapsed < 6 * DELAY, f"prefetch did not overlap items ({elapsed:.2f}s)"


def test_prefetch_sees_the_callers_context():
    s, seen = akayu.Stream(), []
    s.prefetch(2).map(lambda x: seen.append(request_id.get()) or x).sink(lambda _: None)
    token = request_id.set("req-7")
    try:
        for x in range(3):
            s.emit(x)
        s.flush()
    finally:
        request_id.reset(token)
    assert seen == ["req-7"] * 3


def test_prefetch_error_is_raised_once_then_the_stream_continues():
    s, out = akayu.Stream(), []
    s.prefetch(1).map(lambda x: 1 // x).sink(out.append)
    s.emit(1)
    with pytest.raises(RuntimeError):
        s.emit(0)
    s.emit(2)  # the failed item is gone, not re-raised forever
    s.flush()
    assert out == [1, 0]


def test_prefetch_and_par_combined():
    s, out = akayu.Stream(), []
    p = s.par()
    p.prefetch(3).map(slow).sink(out.append)
    p.prefetch(3).map(lambda x: slow(-x)).sink(out.append)
    for x in range(1, 7):
        s.emit(x)
    s.flush()
    assert sorted(out) == sorted([*range(1, 7), *range(-6, 0)])


# --- fork ----------------------------------------------------------------------


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_par_runs_in_parallel_in_a_forked_child():
    """The parent's worker threads don't exist in a child. Waiters would
    still finish (they run their own unstarted tasks) but one at a time;
    the fork hook gives the child a pool of its own."""
    s, out = akayu.Stream(), []
    p = s.par()
    for k in range(4):
        p.map(lambda x, k=k: slow(x + k)).sink(out.append)
    s.emit(0)  # the parent's pool now has threads

    pid = os.fork()
    if pid == 0:
        code = 1
        try:
            out.clear()
            start = time.perf_counter()
            t = threading.Thread(target=s.emit, args=(10,), daemon=True)
            t.start()
            t.join(10)
            elapsed = time.perf_counter() - start
            ok = not t.is_alive() and sorted(out) == [10, 11, 12, 13]
            code = 0 if ok and elapsed < 3 * DELAY else 1
        finally:
            os._exit(code)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0


# --- interpreter exit ------------------------------------------------------------

EXIT_SCRIPT = """
import sys, threading, time
import akayu
path = sys.argv[1]
def slow(x):
    time.sleep(0.3)
    with open(path, "a") as f:
        f.write("done\\n")
    return x
s = akayu.Stream()
p = s.par()
for _ in range(4):
    p.map(slow).sink(lambda _: None)
threading.Thread(target=s.emit, args=(1,), daemon=True).start()
time.sleep(0.1)  # every branch is now running; the interpreter exits
"""


def test_running_branches_finish_before_the_interpreter_exits(tmp_path):
    """ThreadPoolExecutor let running tasks finish at exit; the Rust pool
    must too, or their side effects are silently lost."""
    out = tmp_path / "out.txt"
    out.touch()
    cmd = [sys.executable, "-c", EXIT_SCRIPT, str(out)]
    r = subprocess.run(cmd, timeout=30, capture_output=True, text=True, check=False)
    assert r.returncode == 0, r.stderr
    assert out.read_text().count("done") == 4
