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
