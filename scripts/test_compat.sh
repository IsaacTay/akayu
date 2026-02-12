#!/usr/bin/env bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Running backward compatibility tests for Python 3.8...${NC}"

# Allow unfree packages for steam-run
export NIXPKGS_ALLOW_UNFREE=1

# Use a temporary directory for building
export TMPDIR=/tmp

# Clean up any previous 3.8 wheel artifacts
rm -rf target/wheels/akayu-*-cp38-*.whl

echo "Setting up Python 3.8 environment..."
# Create venv with Python 3.8
# We use steam-run because standard python3.8 binary won't run on NixOS directly
# steam-run provides an FHS environment
steam-run uv venv --python 3.8 .venv-py38

# Activate venv wrapper for commands
run_py38() {
    steam-run bash -c "source .venv-py38/bin/activate && $@"
}

echo "Installing test dependencies..."
run_py38 "uv pip install pytest pytest-asyncio pytest-benchmark pytest-codeblocks streamz psutil toolz tornado exceptiongroup typing-extensions tomli iniconfig pluggy packaging zict"

echo "Building akayu wheel for Python 3.8..."
# Build specifically for the python inside the venv
# Note: we must ensure PYO3_PYTHON points to the correct python binary to avoid maturin picking up system python
# Explicitly tell maturin which python to use via env var and CLI
run_py38 "export PYO3_PYTHON=\$(pwd)/.venv-py38/bin/python3 && uv run maturin build -i \$(pwd)/.venv-py38/bin/python3"

echo "Installing akayu wheel..."
# Find and install the built wheel - look specifically for cp38
WHEEL=$(ls target/wheels/akayu-*-cp38-*.whl | head -n 1)
if [ -z "$WHEEL" ]; then
    echo -e "${RED}Error: Wheel not found! Checking target/wheels content:${NC}"
    ls -la target/wheels/ || true
    exit 1
fi
echo "Found wheel: $WHEEL"
run_py38 "uv pip install $WHEEL"

# Fix potential naming issue in installed package
# Sometimes maturin/wheel/pip interaction inside steam-run environment messes up the SO name
# We force rename it to match the current python version
echo "Checking installed extension name..."
SO_FILE=$(find .venv-py38/lib/python3.8/site-packages/akayu -name "akayu.cpython-*.so")
echo "Found SO file: $SO_FILE"

# Rename if it has 313 in the name (artifact of cross-env confusion)
if [[ "$SO_FILE" == *"cpython-313"* ]]; then
    echo "Renaming incorrect SO file to cpython-38..."
    # Take just the first file if find returned multiple lines (unlikely but safe)
    TARGET=$(echo "$SO_FILE" | head -n 1)
    NEW_NAME="${TARGET/cpython-313/cpython-38}"
    mv "$TARGET" "$NEW_NAME"
fi

# Running all tests might be slow (benchmarks), skip them for compat check
echo -e "${GREEN}Running tests (skipping benchmarks)...${NC}"
run_py38 "python -m pytest tests/ -v --ignore=tests/test_benchmark.py --ignore=tests/test_complex_benchmark.py --ignore=tests/test_compile_benchmark.py --ignore=tests/test_ops_benchmark.py --ignore=tests/test_concurrency_benchmark.py --ignore=tests/test_benchmark_split.py"


echo -e "${GREEN}Success! akayu is compatible with Python 3.8${NC}"
