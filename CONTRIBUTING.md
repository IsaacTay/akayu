# Contributing to rstreamz

Thank you for your interest in contributing to `rstreamz`! This project aims to provide a high-performance, memory-safe, and async-capable implementation of the `streamz` library using Rust.

## Prerequisites

This project uses **Nix** to manage the development environment and dependencies. This ensures a reproducible environment for all contributors.

- **Install Nix**: [Download Nix](https://nixos.org/download.html)
- **Enable Flakes**: Ensure your Nix configuration has `experimental-features = nix-command flakes`.

## Setting Up the Environment

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/your-username/rstreamz.git
    cd rstreamz
    ```

2.  **Enter the development shell**:
    The `flake.nix` file defines the environment with Rust, Python, `uv`, and `maturin`.
    ```bash
    nix develop
    ```
    This command drops you into a shell with all necessary tools installed. It will also automatically create and activate a Python virtual environment (`.venv`) if one doesn't exist.

## Building the Project

We use `maturin` to build the Rust extension and install it into the virtual environment.

1.  **Standard build**:
    ```bash
    maturin develop
    ```
    This compiles the Rust code and installs the `rstreamz` package in editable mode into the current `.venv`.

2.  **Release build** (optimized):
    ```bash
    maturin develop --release
    ```

3.  **High-performance build** (4-7x faster):
    ```bash
    RUSTFLAGS='--cfg pyo3_disable_reference_pool' maturin develop --release
    ```
    This enables the `pyo3_disable_reference_pool` flag which eliminates synchronization overhead in PyO3's deferred reference counting. See [Performance Optimization](#performance-optimization) for details.

## Running Tests

The project includes a comprehensive test suite using `pytest`.

1.  **Run all tests**:
    ```bash
    python -m pytest tests
    ```

2.  **Run with verbose output**:
    ```bash
    python -m pytest tests -v
    ```

3.  **Run benchmarks**:
    The tests include performance benchmarks comparing `rstreamz` to `streamz`.
    ```bash
    python -m pytest tests/test_benchmark.py -v
    python -m pytest tests/test_comparison.py -v
    ```

## Code Style

We enforce code formatting for both Rust and Python.

1.  **Format Rust code**:
    ```bash
    cargo fmt
    ```

2.  **Lint Rust code**:
    ```bash
    cargo clippy
    ```

3.  **Format Python code**:
    ```bash
    ruff format tests
    ```

## Performance Optimization

The `pyo3_disable_reference_pool` compilation flag provides significant performance improvements (4-7x in benchmarks) by removing PyO3's global reference pool synchronization.

### How it works

By default, when `Py<T>` is dropped without holding the GIL, PyO3 defers the reference count decrement to a global pool. This requires synchronization on every Python-Rust boundary crossing.

With the flag enabled, this overhead is eliminated entirely.

### Requirements

All `Py<T>` objects must be dropped while holding the GIL. This crate is designed to be compatible:
- `from_text_file` wraps its thread body in `Python::attach` with a `move` closure
- All other operations occur within Python callbacks where the GIL is held

### Drawbacks

- **Process abort on violation**: Dropping `Py<T>` outside the GIL aborts the process (no recovery)
- **Compile-time only**: Users must rebuild from source to benefit
- **Ecosystem compatibility**: Third-party PyO3 code may not be compatible
- **Debugging difficulty**: Aborts provide no stack trace or error message

## Project Structure

- `src/lib.rs`: The core Rust implementation using PyO3.
- `tests/`: Python test suite.
    - `test_ops.py`: Unit tests for standard operations (map, filter, union, etc.).
    - `test_async.py`: Tests for async support.
    - `test_comparison.py`: Validation and benchmarking against the original `streamz`.
    - `test_memory_comparison.py`: Memory usage verification.
    - `test_args_kwargs.py`: Tests for args/kwargs binding in operations.
    - `test_errors.py`: Error propagation and handling tests.
    - `test_io.py`: File I/O operations (to_text_file, from_text_file).
    - `test_starmap.py`: Starmap operation tests.
    - `test_ordering.py`: Event ordering in split/union topologies.
    - `test_branch_concurrency.py`: Concurrent branch execution tests.
    - `test_memory_leak.py`: Memory leak detection tests.
    - `test_benchmark.py`: General throughput benchmarking.
    - `test_ops_benchmark.py`: Individual operation benchmarks.
    - `test_benchmark_split.py`: Split expansion efficiency tests.
    - `test_concurrency_benchmark.py`: Async concurrency performance tests.
- `Cargo.toml`: Rust dependencies and configuration.
- `pyproject.toml`: Python package configuration.
- `flake.nix`: Nix environment configuration (in parent directory).

## Development Workflow

1.  Make your changes in `src/lib.rs`.
2.  Run `maturin develop --release` to rebuild and update the Python package.
3.  Add or update tests in `tests/`.
4.  Run `python -m pytest tests` to verify your changes.
5.  Format your code before submitting a PR.

Happy Coding!
