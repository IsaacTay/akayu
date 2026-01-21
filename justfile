# Default recipe - show available commands
default:
    @just --list

# Build the Rust extension in development mode
build:
    cd rstreamz && maturin develop

# Build the Rust extension in release mode
build-release:
    cd rstreamz && maturin develop --release

# Run all tests
test: build
    cd rstreamz && ../.venv/bin/pytest tests/ -v

# Run tests without benchmarks (faster)
test-quick: build
    cd rstreamz && ../.venv/bin/pytest tests/ -v \
        --ignore=tests/test_benchmark.py \
        --ignore=tests/test_benchmark_split.py \
        --ignore=tests/test_ops_benchmark.py \
        --ignore=tests/test_concurrency_benchmark.py \
        --ignore=tests/test_memory_comparison.py \
        --ignore=tests/test_memory_leak.py \
        --ignore=tests/test_comparison.py

# Run benchmarks only
bench: build-release
    cd rstreamz && ../.venv/bin/pytest tests/test_benchmark.py tests/test_ops_benchmark.py -v

# Run clippy lints
clippy:
    cd rstreamz && cargo clippy

# Format Rust code
fmt:
    cd rstreamz && cargo fmt

# Run ruff linter on Python tests
lint:
    ruff check rstreamz/tests/

# Clean build artifacts
clean:
    cd rstreamz && cargo clean
    rm -rf rstreamz/*.so rstreamz/*.pyd

# Build wheels for Python 3.12 and 3.13
build-wheels:
    mkdir -p rstreamz/dist
    rm -f rstreamz/dist/*.whl
    cd rstreamz && maturin build --release --interpreter python3.12 python3.13 --out dist
    @echo "Wheels built:"
    @ls -la rstreamz/dist/*.whl
