# Contributing to akayu

Thank you for your interest in contributing to `akayu`! This project aims to provide a high-performance, memory-safe implementation of the `streamz` library using Rust.

## Prerequisites

This project uses **Nix** to manage the development environment and dependencies. This ensures a reproducible environment for all contributors.

- **Install Nix**: [Download Nix](https://nixos.org/download.html)
- **Enable Flakes**: Ensure your Nix configuration has `experimental-features = nix-command flakes`.

## Setting Up the Environment

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/akayu-io/akayu.git
    cd akayu
    ```

2.  **Enter the development shell**:
    The `flake.nix` file defines the environment with Rust, Python, `uv`, and `maturin`.
    ```bash
    direnv allow /mnt/ssd1/vibes/akayu
    ```
    This evaluates the `.envrc` (which runs `use flake`) and activates the development environment with all necessary tools. It will also automatically create and activate a Python virtual environment (`.venv`) if one doesn't exist.

### Without Nix

If you prefer not to use Nix, you can set up the development environment manually:

1.  **Install Rust** via [rustup](https://rustup.rs/):
    ```bash
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    ```

2.  **Install Python 3.13+** (e.g., via [pyenv](https://github.com/pyenv/pyenv) or your system package manager).

3.  **Install uv**:
    ```bash
    pip install uv
    ```

4.  **Install development dependencies**:
    ```bash
    pip install maturin pytest pytest-benchmark
    ```

5.  **Build and install the package**:
    ```bash
    maturin develop
    ```

6.  **Run tests** (excluding benchmarks):
    ```bash
    pytest tests/ -k "not benchmark"
    ```

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
   uv pip install -e . --reinstall-package akayu
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
- `python/akayu/`: The Python package containing the Rust extension.
- `tests/`: Python test suite.
    - `test_ops.py`: Unit tests for standard operations (map, filter, union, etc.).
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
    - `test_parallel.py`: Parallel branch execution tests (`par()`, `seq()`).
    - `test_compile_safety.py`: Verification that graph modification is blocked after compile.
    - `test_compile_benchmark.py`: Performance comparison of compiled vs uncompiled pipelines.
- `Cargo.toml`: Rust dependencies and configuration.
- `pyproject.toml`: Python package configuration.
- `flake.nix`: Nix environment configuration.

### Python Package Structure (maturin convention)

The Python package lives in `python/akayu/` rather than at the project root. In `pyproject.toml`, you will see:

```toml
[tool.maturin]
python-source = "python"
module-name = "akayu.akayu"
```

This means the Rust extension (the `.so` file built by maturin) is placed inside `python/akayu/akayu.so`, and Python imports it as `akayu.akayu`. This follows the maturin convention where `module-name` is `akayu.akayu` because `python-source = "python"` means "the Python package is in the `python/` directory" and the extension module name is `akayu`. If this seems confusing, that's normal—it trips up many contributors. The key takeaway is that `import akayu` Just Works, and the extension binary lives nested inside the Python package directory.

## Testing Your Changes

The easiest way to test stream processing logic is to write small inline scripts:

```python
import akayu

# Test a transform function in isolation
def my_transform(x):
    return x * 2

source = akayu.Stream()
result = source.map(my_transform).to_list()
source.emit_batch([1, 2, 3])
assert result == [2, 4, 6]
```

For more complex testing patterns, see the test suite in `tests/` for examples of filtering, unions, and error handling.

## License

`akayu` is dual-licensed under the MIT License and Apache License 2.0. See [LICENSE](LICENSE) for details. All contributions are accepted under the same terms.

## First Time Contributors

Welcome! Here are some areas where contributions are especially appreciated:

- **Examples**: The `docs/` directory includes documentation with code blocks. Running `just docs-test-examples` will test these automatically. Good examples are always welcome.
- **Documentation**: Improvements to docstrings, README, or this CONTRIBUTING guide.
- **New operations**: If there's a `streamz` operation you'd like to see implemented, open an issue to discuss it first.
- **Bug fixes**: Check the issue tracker for any `help wanted` or `good first issue` tags.

## Development Workflow

1.  Make your changes in `src/lib.rs`.
2.  Run `just build` (or `just build-release` for optimized builds) to rebuild.
3.  Add or update tests in `tests/`.
4.  Use `.compile()` in your benchmarks to test the performance impact of fusion and other optimizations.
5.  Run `just test-quick` to verify your changes.
6.  If tests behave unexpectedly, try `just rebuild` to bypass any cached builds.
7.  Format your code with `just fmt` before submitting a PR.

Happy Coding!
