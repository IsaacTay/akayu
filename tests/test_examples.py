"""Run every script in examples/ so they can't silently rot."""

import subprocess
import sys
from pathlib import Path

import pytest
from support import UNDER_WINE

EXAMPLES = sorted((Path(__file__).parent.parent / "examples").glob("*.py"))


NUMPY_UNDER_WINE = pytest.mark.skipif(UNDER_WINE, reason="numpy crashes under Wine (ucrtbase crealf)")


@pytest.mark.parametrize(
    "script",
    [pytest.param(p, marks=NUMPY_UNDER_WINE) if "import numpy" in p.read_text() else p for p in EXAMPLES],
    ids=[p.name for p in EXAMPLES],
)
def test_example_runs(script, tmp_path):
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
