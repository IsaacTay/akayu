"""Helpers shared by the test files (fixtures are in conftest.py)."""

import math
import os
import subprocess
import sys
import textwrap

# Set by scripts/test-wheel-windows.sh: the Windows wheel's tests run under
# Wine, where a few things fail for reasons that aren't Windows' (see there).
UNDER_WINE = os.environ.get("AKAYU_UNDER_WINE") == "1"


def run_python(code, timeout=120, **env):
    """Run `code` in a fresh interpreter: (exit code, stdout, stderr).

    For what a test can't do in its own process: a deadlock or crash would
    take the suite with it, or the module must be set up differently at
    import. `env` adds environment variables.
    """
    r = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=timeout,
        env={**os.environ, **env},
        check=False,
    )
    return r.returncode, r.stdout, r.stderr


def run_script(code, timeout=120, **env):
    """`run_python`, for scripts that print their verdict last: (exit code,
    last line of the output)."""
    code, out, err = run_python(code, timeout, **env)
    lines = (out + err).strip().splitlines()
    return code, lines[-1] if lines else ""


def innermost(e):
    """The exception at the end of `e`'s `__cause__` chain."""
    while e.__cause__ is not None:
        e = e.__cause__
    return e


def canon(v):
    """A comparable form of a result: its type and repr (nan == nan); an
    exception is ("raise", its type's name)."""
    if isinstance(v, BaseException):
        return ("raise", type(v).__name__)
    if isinstance(v, float) and math.isnan(v):
        return (type(v).__name__, "nan")
    return (type(v).__name__, repr(v))


def seed_chunks(total, chunk):
    """The seeds a fuzzer's tests run, `chunk` per test and `total` in all,
    for `pytest.mark.parametrize`. AKAYU_FUZZ_SEED=n runs only seed n, to
    replay a failure."""
    one = os.environ.get("AKAYU_FUZZ_SEED")
    if one is not None:
        return [range(int(one), int(one) + 1)]
    return [range(start, min(start + chunk, total)) for start in range(0, total, chunk)]


def seed_ids(seeds):
    """A readable test id for a range of seeds."""
    return f"seeds{seeds.start}-{seeds.stop - 1}"


def calls_during(fn, func):
    """How often `func`'s code ran while `fn()` ran (0 for a function akayu
    ran natively)."""
    count = 0

    def profile(frame, event, arg):
        nonlocal count
        if event == "call" and frame.f_code is func.__code__:
            count += 1

    sys.setprofile(profile)
    try:
        fn()
    finally:
        sys.setprofile(None)
    return count


def all_at_once(parties, timeout=10):
    """A function that passes its argument on only once `parties` calls are
    inside it at the same time: proof that they overlap, with no timing
    margin to tune (calls made one after another time out and raise
    `threading.BrokenBarrierError`)."""
    import threading

    barrier = threading.Barrier(parties, timeout=timeout)

    def meet(x):
        barrier.wait()
        return x

    return meet


class Overlap:
    """For calls that may not come in whole groups, such as prefetch()'s,
    where the waiting caller may run an item too: `meet` passes its argument
    on and records in `most` how many calls were inside it at once. Until
    `target` have been, each call waits for that (giving up after `timeout`
    for all), so calls that can overlap do; extra or missing calls can't
    deadlock it, as they can all_at_once."""

    def __init__(self, target, timeout=10):
        import threading

        self.target, self.timeout = target, timeout
        self.most = self._inside = 0
        self._gave_up = False
        self._cond = threading.Condition()

    def _done(self):
        return self.most >= self.target or self._gave_up

    def meet(self, x):
        with self._cond:
            self._inside += 1
            self.most = max(self.most, self._inside)
            if self._done():
                self._cond.notify_all()
            elif not self._cond.wait_for(self._done, self.timeout):
                self._gave_up = True
            self._inside -= 1
        return x
