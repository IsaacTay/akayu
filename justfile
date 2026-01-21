# Default recipe - show available commands
default:
    @just --list

# Build the Rust extension in development mode
build:
    maturin develop

# Build the Rust extension in release mode
build-release:
    maturin develop --release

# Run all tests
test: build
    pytest tests/ -v

# Run tests without benchmarks (faster)
test-quick: build
    pytest tests/ -v \
        --ignore=tests/test_benchmark.py \
        --ignore=tests/test_benchmark_split.py \
        --ignore=tests/test_ops_benchmark.py \
        --ignore=tests/test_concurrency_benchmark.py \
        --ignore=tests/test_memory_comparison.py \
        --ignore=tests/test_memory_leak.py \
        --ignore=tests/test_comparison.py

# Run benchmarks only
bench: build-release
    pytest tests/test_benchmark.py tests/test_ops_benchmark.py -v

# Run clippy lints
clippy:
    cargo clippy

# Format Rust code
fmt:
    cargo fmt

# Run ruff linter on Python tests
lint:
    ruff check tests/

# Clean build artifacts
clean:
    cargo clean
    rm -rf *.so *.pyd

# Build wheels for Python 3.12 and 3.13
build-wheels:
    mkdir -p dist
    rm -f dist/*.whl
    maturin build --release --interpreter python3.12 python3.13 --out dist
    @echo "Wheels built:"
    @ls -la dist/*.whl
