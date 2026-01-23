# Default recipe - show available commands
default:
    @just --list

# Sync dependencies
sync:
    uv sync --extra dev

# Build the Rust extension in development mode
build: sync
    uv run maturin develop --uv

# Build the Rust extension in release mode
build-release: sync
    uv run maturin develop --release --uv

# Run all tests
test: build
    uv run python -m pytest tests/ -v

# Run tests without benchmarks (faster)
test-quick: build
    uv run python -m pytest tests/ -v \
        --ignore=tests/test_benchmark.py \
        --ignore=tests/test_benchmark_split.py \
        --ignore=tests/test_ops_benchmark.py \
        --ignore=tests/test_concurrency_benchmark.py \
        --ignore=tests/test_memory_comparison.py \
        --ignore=tests/test_memory_leak.py \
        --ignore=tests/test_comparison.py \
        --ignore=tests/test_compile_benchmark.py

# Run benchmarks only
bench: build-release
    uv run python -m pytest tests/test_benchmark.py tests/test_complex_benchmark.py tests/test_compile_benchmark.py -v

# Run clippy lints
clippy:
    cargo clippy

# Format Rust code
fmt:
    cargo fmt

# Run ruff linter on Python tests
lint:
    uv run ruff check tests/

# Clean build artifacts
clean:
    cargo clean
    rm -rf *.so *.pyd

# Clean all caches (use when builds seem stale)
clean-all: clean
    rm -rf .venv/lib/python*/site-packages/akayu/*.so
    uv cache clean akayu 2>/dev/null || true

# Force rebuild (bypasses uv cache)
rebuild: sync
    uv pip install -e . --reinstall-package akayu

# Build wheels for Python 3.12
build-wheels:
    mkdir -p dist
    rm -f dist/*.whl
    maturin build --release --interpreter python3.12 --out dist
    @echo "Wheels built:"
    @ls -la dist/*.whl

# Sync docs dependencies and build module for mkdocstrings
sync-docs:
    uv sync --extra dev --group docs
    uv run maturin develop --uv

# Serve documentation locally with live-reloading
docs: sync-docs
    uv run mkdocs serve -a 0.0.0.0:8002 -w src -w docs --livereload

# Aliases for docs
docs-dev: docs
docs-watch: docs

# Build documentation
docs-build: sync-docs
    uv run mkdocs build

# Format markdown documentation
docs-fmt: sync-docs
    uv run mdformat docs/ README.md

# Check markdown formatting (no changes)
docs-fmt-check: sync-docs
    uv run mdformat --check docs/ README.md
