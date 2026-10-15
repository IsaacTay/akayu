#!/usr/bin/env bash
# Run the test suite against the wheel in dist/ that fits PYTHON, installed in
# a fresh virtual environment with the test dependencies from uv.lock. The
# Wheels workflow runs it on each platform; here it tests the host's wheel.
#
# usage: scripts/test-wheel.sh PYTHON [pytest args...]
#
# PYTHON is an interpreter's path or version, as `uv venv --python` takes it.
set -euo pipefail

python=$1
shift
venv=target/test-wheel-venv

# The dev shell's settings for its own Python must not reach the new one.
unset PYTHONPATH VIRTUAL_ENV
uv venv -q --clear --python "$python" "$venv"
uv export --frozen --no-hashes --no-emit-project | grep -vE '^(maturin|ruff)==' > "$venv/requirements.txt"
uv pip install -q --python "$venv" -r "$venv/requirements.txt"
# The wheel only: never a build of the sdist that is also in dist/.
uv pip install -q --python "$venv" --no-index --find-links dist --only-binary akayu --no-deps akayu

py=$venv/bin/python
[ -x "$py" ] || py=$venv/Scripts/python.exe
"$py" -c 'import akayu, akayu.akayu as m; print(f"akayu {akayu.__version__} ({m._build}) from {akayu.__file__}")'
"$py" -m pytest -p no:cacheprovider "$@"
