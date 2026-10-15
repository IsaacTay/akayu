"""The type stub must be valid and match the runtime API exactly.

Before this test the stub had drifted badly: 12 mypy errors kept stubtest
from running at all, and once it ran it found 21 mismatches, including
file I/O functions documented with parameters that do not exist.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STUB = ROOT / "python" / "akayu" / "__init__.pyi"


def _run(*args):
    result = subprocess.run([sys.executable, "-m", *args], cwd=ROOT, capture_output=True, text=True, check=False)
    return result.returncode, result.stdout + result.stderr


def test_stub_passes_mypy():
    code, out = _run("mypy", str(STUB))
    assert code == 0, out


def test_stub_matches_runtime():
    code, out = _run("mypy.stubtest", "akayu", "--allowlist", "tests/stubtest_allowlist.txt")
    assert code == 0, out


def test_stub_docstrings_are_the_runtime_ones():
    """The API is documented once: Rust `///` comments (what `help()`
    shows) and docstrings in `__init__.py`. The stub, which editors and the
    API reference read, carries copies made by `just stub-docs`; the two
    copies used to drift (22 of 31 methods' Raises sections disagreed)."""
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "stub_docs.py"), "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
