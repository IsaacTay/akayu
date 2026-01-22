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

## Quick Start with Just

The project includes a `justfile` for common development tasks. Run `just` to see all available commands:

```bash
just              # List all commands
just build        # Build dev extension
just test-quick   # Build + run tests (skipping benchmarks)
just test         # Build + run all tests
```

## Building the Project

We use `maturin` to build the Rust extension and install it into the virtual environment.

| Command | Description |
|---------|-------------|
| `just build` | Standard dev build |
| `just build-release` | Optimized release build |
| `just rebuild` | Force rebuild (bypasses cache) |
| `just clean-all` | Clean all caches |

Or manually:

```bash
uv run maturin develop --uv                # Standard build
uv run maturin develop --release --uv      # Release build
```

### Troubleshooting Stale Builds

If you encounter unexpected behavior after changing Rust code, you may have a stale cached build. This can happen because `uv` caches wheel builds aggressively.

**Symptoms of stale builds:**
- Tests fail with errors referencing code you've already changed
- Changes to `src/lib.rs` don't seem to take effect
- Error messages reference functions that no longer exist

**Solutions:**

1. **Force rebuild** (recommended):
   ```bash
   just rebuild
   ```

2. **Clean all caches**:
   ```bash
   just clean-all
   just build
   ```

3. **Manual fix**:
   ```bash
   uv pip install -e . --reinstall-package rstreamz
   ```

The `pyproject.toml` includes `cache-keys` configuration that tells `uv` to track Rust source files, but in some edge cases manual intervention may be needed. See [maturin#2314](https://github.com/PyO3/maturin/issues/2314) for more details.

## Running Tests

The project includes a comprehensive test suite using `pytest`.

| Command | Description |
|---------|-------------|
| `just test-quick` | Build + run tests (skipping benchmarks) |
| `just test` | Build + run all tests |
| `just bench` | Build release + run benchmarks only |

Or manually:

```bash
uv run python -m pytest tests            # Run all tests
uv run python -m pytest tests -v         # Verbose output
uv run python -m pytest tests/test_benchmark.py -v  # Run benchmarks
```

## Code Style

We enforce code formatting for both Rust and Python.

| Command | Description |
|---------|-------------|
| `just fmt` | Format Rust code |
| `just clippy` | Lint Rust code |
| `just lint` | Lint Python tests with ruff |
| `just clean` | Clean build artifacts |

Or manually:

```bash
cargo fmt              # Format Rust
cargo clippy           # Lint Rust
uv run ruff format tests  # Format Python
uv run ruff check tests   # Lint Python
```

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
    - `test_parallel.py`: Parallel branch execution tests (`par()`, `seq()`).
    - `test_compile_safety.py`: Verification that graph modification is blocked after compile.
    - `test_compile_benchmark.py`: Performance comparison of compiled vs uncompiled pipelines.
- `Cargo.toml`: Rust dependencies and configuration.
- `pyproject.toml`: Python package configuration.
- `flake.nix`: Nix environment configuration.

## Development Workflow

1.  Make your changes in `src/lib.rs`.
2.  Run `just build` (or `just build-release` for optimized builds) to rebuild.
3.  Add or update tests in `tests/`.
4.  Use `.compile()` in your benchmarks to test the performance impact of fusion and other optimizations.
5.  Run `just test-quick` to verify your changes.
6.  If tests behave unexpectedly, try `just rebuild` to bypass any cached builds.
7.  Format your code with `just fmt` before submitting a PR.

Happy Coding!
