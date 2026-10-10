# Contributing to akayu

Thank you for your interest in contributing to `akayu`! This project aims to provide a high-performance, memory-safe implementation of the `streamz` library using Rust.

## Prerequisites

This project uses **Nix** to manage the development environment and dependencies. This ensures a reproducible environment for all contributors.

- **Install Nix**: [Download Nix](https://nixos.org/download.html)
- **Enable Flakes**: Ensure your Nix configuration has `experimental-features = nix-command flakes`.

## Setting Up the Environment

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/IsaacTay/akayu.git
    cd akayu
    ```

2.  **Enter the development shell**:
    The `flake.nix` file defines the environment with Rust, Python, `uv`, and `maturin`.
    ```bash
    nix develop     # or, with direnv: echo "use flake" > .envrc && direnv allow
    ```
    The shell provides Rust, Python 3.13, `uv`, `maturin`, `just` and `mermaid-cli`, and runs `uv sync` to create `.venv` with the development dependencies.

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

4.  **Install the development dependencies** (the `dev` dependency group in `pyproject.toml`):
    ```bash
    uv sync
    ```

5.  **Build and install the package**:
    ```bash
    uv run maturin develop --uv
    ```

6.  **Run the quick tests** (skips benchmarks):
    ```bash
    just test-quick
    ```

## Quick Start with Just

The project includes a `justfile` for common development tasks. Run `just` to see all available commands:

```bash
just              # List all commands
just build        # Build dev extension
just test-quick   # Build + run tests (skipping benchmarks)
just check        # Everything: lint, formatting, clippy, quick tests, docs
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
- Changes to the Rust code in `src/` don't seem to take effect
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

The `pyproject.toml` includes `cache-keys` configuration that tells `uv` to rebuild when `pyproject.toml`, `Cargo.toml` or `src/**/*.rs` change, but in some edge cases manual intervention may be needed. See [maturin#2314](https://github.com/PyO3/maturin/issues/2314) for more details.

## Running Tests

The project includes a comprehensive test suite using `pytest`.

| Command | Description |
|---------|-------------|
| `just test-quick` | Build + run tests (skipping benchmarks), about 2 minutes |
| `just test` | Build + run all tests, including benchmarks |
| `just test-py314` | Build a cp314 wheel and run the quick tests on Python 3.14 |
| `just bench` | Build release + run benchmarks only |
| `just docs-check` | Run the documentation examples and validate the diagrams |

Or manually:

```bash
uv run python -m pytest tests            # Run all tests
uv run python -m pytest tests -v         # Verbose output
uv run python -m pytest tests/test_benchmark.py -v  # Run benchmarks
```

## Code Style

We enforce formatting and linting for both Rust and Python. `just check` runs all of it
together with the quick tests and the documentation checks; run it before submitting.

| Command | Description |
|---------|-------------|
| `just fmt` | Format Rust (`cargo fmt`) and Python (`ruff format`) |
| `just fmt-check` | Check formatting without changing files |
| `just clippy` | Lint Rust, treating warnings as errors |
| `just lint` | Lint all Python with ruff |
| `just check` | All of the above, plus `just test-quick` and `just docs-check` |
| `just clean` | Clean build artifacts |

## Project Structure

- `src/`: the Rust extension, built with PyO3.
    - `lib.rs`: the `Stream` type and module setup.
    - `operations.rs`, `combinators.rs`: the Python-facing operations (`map`, `filter`, `union`, ...).
    - `execution.rs`: propagating values through the graph.
    - `optimization.rs`, `builtin.rs`: `compile()`: chain fusion and fast paths for simple lambdas.
    - `node.rs`: the node and builtin-operation types.
    - `gc.rs`: garbage-collection support, so stream graphs are freed.
    - `profiling.rs`, `error.rs`, `helpers.rs`: profiling, errors and shared helpers.
    - `akayu.py`: Python helpers embedded in the extension (thread pool, lambda analysis); importable as `akayu._helpers`.
- `python/akayu/`: the Python package: `__init__.py` (`MultiStream`, `from_text_file`, ...) and the type stub `__init__.pyi`.
- `tests/`: the `pytest` suite. Benchmarks (`test_*benchmark*.py`) and `test_memory_leak.py` are skipped by `just test-quick`.
- `examples/`: runnable examples; `tests/test_examples.py` runs every one of them.
- `docs/`: the documentation site (`mkdocs.yml`); `just docs-check` runs its code blocks.
- `pipeline/`: a copy of application code used only as a test fixture (excluded from linting).
- `scripts/`: development scripts (Mermaid validation, aarch64 cross-build).
- `Cargo.toml`, `pyproject.toml`, `flake.nix`, `justfile`: build, packaging, environment and task configuration.

### Python Package Structure (maturin convention)

The Python package lives in `python/akayu/` rather than at the project root. In `pyproject.toml`, you will see:

```toml
[tool.maturin]
python-source = "python"
module-name = "akayu.akayu"
```

This means the Rust extension (the `.so` file built by maturin) is placed inside `python/akayu/` (as `akayu.cpython-*.so`), and Python imports it as `akayu.akayu`. This follows the maturin convention where `module-name` is `akayu.akayu` because `python-source = "python"` means "the Python package is in the `python/` directory" and the extension module name is `akayu`. If this seems confusing, that's normal—it trips up many contributors. The key takeaway is that `import akayu` Just Works, and the extension binary lives nested inside the Python package directory.

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

1.  Make your changes in `src/` (Rust) or `python/akayu/` (Python).
2.  Run `just build` (or `just build-release` for optimized builds) to rebuild.
3.  Add or update tests in `tests/`.
4.  Use `.compile()` in your benchmarks to test the performance impact of fusion and other optimizations.
5.  Run `just test-quick` to verify your changes.
6.  If tests behave unexpectedly, try `just rebuild` to bypass any cached builds.
7.  Format with `just fmt`, then run `just check` before submitting a PR.

Happy Coding!
