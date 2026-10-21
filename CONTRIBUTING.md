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
    The shell provides Rust, Python 3.13, `uv`, `maturin`, `just` and `mermaid-cli`, and syncs `.venv` with the development dependencies. It doesn't build akayu: run `just build` (debug) or `just build-release`, and the build you chose stays installed until you build again.

3.  **Build the extension**:
    ```bash
    just build
    ```

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

6.  **Install [just](https://github.com/casey/just)** for the task recipes, and
    [mermaid-cli](https://github.com/mermaid-js/mermaid-cli) (`mmdc`) for `just docs-check`
    (part of `just check`).

7.  **Run the quick tests** (skips benchmarks):
    ```bash
    just test-quick
    ```

`just test-py314`, `just wheels`, `just test-wheel-windows` and `just test-wheel-aarch64` fetch their toolchains and
interpreters with Nix, so they need Nix installed even outside the dev shell.

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

## Building Wheels and Releasing

GitHub builds the wheels: `.github/workflows/wheels.yml` builds each platform's wheel for CPython 3.13 and 3.14 (Linux x86_64 and aarch64, macOS arm64 and x86_64, Windows x64) and runs the quick tests against it on that platform, with `scripts/test-wheel.sh`. Each is built on its own platform too, except Linux aarch64: cross-compiled on x86_64 as `just wheels` does, because a native build links its own build scripts with zig as well, which fails. It runs for every push to `main` and every pull request, and the run's page has the wheels for a week.

To release:

1. Set the version in `Cargo.toml` (`pyproject.toml` takes it from there) and give it a `## <version>` section in `CHANGELOG.md`, which becomes the release notes.
1. Push that to `main`.
1. `just release` tags `origin/main` as `v<version>` and pushes the tag. It refuses if `origin/main` has another version, no CHANGELOG section, or the tag exists.

For a version tag the workflow also builds the sdist, attests where every file was built (`gh attestation verify <file> --repo IsaacTay/akayu` checks it), publishes them all as the tag's GitHub release, and then runs the docs workflow. That adds the package index `scripts/wheel_index.py` writes to the docs site, at `https://isaactay.github.io/akayu/simple/`; uv, pip and pixi install from it (the [installation guide](docs/getting-started.md#installation) has the commands). With "Enable release immutability" on in the repository's settings, a published release's files can't be changed or replaced.

`just wheels` builds the same wheels on this machine instead, cross-compiled, plus an sdist, into `dist/`:

| Platform | How |
|----------|-----|
| Linux x86_64, aarch64 (manylinux2014, glibc 2.17+) | linked with zig against old glibc symbols, so no manylinux container is needed |
| macOS arm64, x86_64 | linked with zig; an extension module needs only `libSystem`, so no Apple SDK is needed |
| Windows x64 | `cargo-xwin` (clang-cl + lld-link), with PyO3 generating the `python3x.lib` import libraries |

The tools come from the `wheels` dev shell in `flake.nix` (`nix develop .#wheels`), which `just wheels` enters by itself. On first use `cargo-xwin` downloads the MSVC C runtime and Windows SDK headers to `~/.cache/cargo-xwin`.

There are no free-threaded (3.13t/3.14t), musllinux or Windows arm64 wheels: the extension builds for free-threaded Python, but turns the GIL back on.

The wheels in `dist/` can be tested here too:

| Command | Description |
|---------|-------------|
| `just test-wheel [3.13\|3.14]` | the quick tests on this machine's wheel, installed in a fresh environment, as the workflow tests each platform's |
| `just test-wheel-windows [3.13\|3.14]` | the quick tests on the Windows wheel, under Wine with the embeddable CPython from python.org. Tests that fail only under Wine skip themselves when `AKAYU_UNDER_WINE=1` (`tests/support.py`), among them the modules that import numpy, which calls a C runtime function Wine lacks |
| `just test-wheel-aarch64 [3.13\|3.14]` | the quick tests on the aarch64 wheel, with an aarch64 CPython under qemu user-mode emulation (slow) |

The macOS wheels can't be run here; the workflow tests them on GitHub's Macs.

## Running Tests

The project includes a comprehensive test suite using `pytest`.

| Command | Description |
|---------|-------------|
| `just test-quick` | Build + run tests, skipping benchmarks (`--benchmark-skip`) and tests marked `slow`; about 3 minutes. Tests marked `timing` measure CPU time: on a heavily loaded machine, deselect them with `-m 'not timing'` |
| `just test` | Build + run all tests, including benchmarks |
| `just test-py314` | Build a cp314 wheel and run the quick tests on Python 3.14 |
| `just bench` | Build release + run benchmarks only |
| `just bench-cost` | CPU instructions and memory per item for a set of pipelines (`--save before.json`, then `--compare before.json` after a change) |
| `just docs-check` | Build the docs site strictly (every page in the nav, every link and anchor resolving), run the documentation examples and validate the diagrams |

Or manually:

```bash
uv run python -m pytest tests            # Run all tests
uv run python -m pytest tests -v         # Verbose output
uv run python -m pytest tests/test_benchmark.py -v  # Run benchmarks
```

`just bench` and `just bench-cost` build a release extension, while `just build` and the test
recipes leave a debug build installed: run `just build-release` before timing anything by hand.
`bench-cost` counts instructions rather than time, so its numbers hold up on a busy machine;
`scripts/bench_complex.py` is the wall-clock comparison against streamz behind
`docs/benchmarks.md`.

### Fuzzers

Several tests generate random graphs and inputs and compare akayu with a plain-Python model. Each
runs a modest number of seeds by default; set the variable to run more, for example
`AKAYU_FUZZ_SEEDS=20000 uv run python -m pytest tests -k fuzz`. A failure names its seed:
`AKAYU_FUZZ_SEED=1234` runs only that seed, in every fuzzer, to replay it.

| Test | What it checks | Seeds |
|------|----------------|-------|
| `tests/test_reference_fuzz.py` | random graphs (every operation, feedback sinks, extra sources) and scripts of emit, emit_batch, flush and drain calls with compile() at a random point, some growing the graph between calls, against a pure-Python interpreter; also graphs of 30 to 60 nodes. A public `Stream` method it doesn't call must be listed, with a reason, in `NOT_FUZZED` | `AKAYU_FUZZ_SEEDS` (default 4000), `AKAYU_FUZZ_BIG_SEEDS` (default 300) |
| `tests/test_native_ops_fuzz.py` | random lambdas over awkward values (overflow, NaN, -0.0, subclasses overriding operators) run natively, against plain Python: same results, same exception types. `tests/test_numpy_flows.py` runs it over numpy scalars and arrays too | `AKAYU_OPS_FUZZ_SEEDS` (default 30) |
| `tests/test_concurrency_fuzz.py` | several threads emitting and flushing at once: every sink gets exactly the items its path predicts, with no errors or hangs | `AKAYU_CONCURRENCY_FUZZ_SEEDS` (default 40) |

The property tests (`test_property_based.py` and others) use Hypothesis with the `akayu` profile
from `tests/conftest.py`, which turns off its per-example deadline.

### Where a test goes

A regression test goes in the file for the feature it is about, with a docstring saying what used
to break; `tests/test_wacky_flows.py` holds the probing rounds' findings that fit no feature.

| Concern | File |
|---------|------|
| Errors and their types | `test_errors.py` |
| Locks, waits and deadlocks between threads | `test_node_lock.py` |
| Merges (`union`, `zip`, `combine_latest`) | `test_combinator_parity.py`, `test_merge_concurrency.py` |
| `par()` and the thread pool, `prefetch()` | `test_parallel.py`, `test_pool.py` |
| `flush()` / `drain()` | `test_collect_flush.py`, `test_flush_parity.py` |
| `compile()`: what it may not change, fusions | `test_compile_safety.py`, `test_step_chains.py`, `test_chunk_by.py` |
| The lambda analyzer and native ops | `test_optimizer_recognition.py`, `test_analysis_cache.py`, `test_native_ops_fuzz.py` |
| Batches | `test_batch_fanout.py`, `test_batch_in_place.py` |
| Garbage collection and memory | `test_gc.py`, `test_node_memory.py` |
| Packaging, the package index | `test_packaging.py`, `test_wheel_index.py` |

`tests/support.py` has the shared helpers (`run_script` for a test that must run in a fresh
interpreter, because it could hang or crash, `innermost`, `canon`, the fuzzers' seed handling),
and `tests/conftest.py` the `compiled` fixture (each test runs uncompiled, then compiled) and
`fast_switching` (threads switch every microsecond, so races show up).

## Code Style

We enforce formatting and linting for both Rust and Python. `just check` runs all of it
together with the quick tests and the documentation checks; run it before submitting.

| Command | Description |
|---------|-------------|
| `just fmt` | Format Rust (`cargo fmt`) and Python (`ruff format`) |
| `just fmt-check` | Check formatting without changing files |
| `just clippy` | Lint Rust, treating warnings as errors |
| `just lint` | Lint all Python with ruff |
| `just stub-docs` | Copy the runtime docstrings into the type stub |
| `just check` | All of the above, plus `just test-quick` and `just docs-check` |
| `just clean` | Clean build artifacts |

## Project Structure

- `src/`: the Rust extension, built with PyO3.
    - `lib.rs`: the `Stream` type and module setup.
    - `operations.rs`, `combinators.rs`: the Python-facing operations (`map`, `filter`, `union`, ...).
    - `execution.rs`: propagation: a node's update, and handing results to its children (locked, through compiled pointers, or in parallel).
    - `steps.rs`: what each kind of node does with an item or a batch, fused chains included.
    - `native.rs`: the native operations compile() runs instead of calling recognised functions.
    - `optimization.rs`, `builtin.rs`: `compile()`: chain fusion and fast paths for simple lambdas. `optimize_topology_full()` lists the passes in order; debug builds check the graph's links after them (`assert_graph_consistent`), so a rewrite that forgets one panics in the tests.
    - `numeric.rs`: unboxed int/float arithmetic and comparisons for runs of numeric steps.
    - `call.rs`: calling functions with extra bound arguments (`map(f, *args, **kwargs)`) natively.
    - `node.rs`: the node and builtin-operation types.
    - `gc.rs`: garbage-collection support, so stream graphs are freed.
    - `lock.rs`: the per-node lock behind `emit()`/`flush()` and shared children. It records its holder, and every waiting thread is listed with what it waits for, so a wait that would never end raises instead. Its header has the table of which flag refuses what.
    - `pool.rs`, `prefetch.rs`: the thread pool behind `par()` and `prefetch()`.
    - `stack.rs`: the native stack check that turns very deep graphs into `RecursionError`.
    - `profiling.rs`, `error.rs`, `helpers.rs`: profiling, errors and shared helpers.
    - `akayu.py`: Python helpers embedded in the extension (lambda analysis, wrappers, profiler); importable as `akayu._helpers`.
- `python/akayu/`: the Python package: `__init__.py` (`MultiStream`, `from_text_file`, ...) and the type stub `__init__.pyi`. The API is documented once, where it is implemented: the Rust `///` comments (written as Google-style docstrings, which `help()` shows as they are) and the docstrings in `__init__.py`. `just stub-docs` copies them into the stub, whose docstrings editors and the API reference (`docs/api.md`) read; `tests/test_stubs.py` fails when the copies differ. Docstrings of slot methods (`__repr__`, `Stream.__new__`) live only in the stub: the runtime shows CPython's own. `tests/test_stubs.py` checks the stub against the runtime with mypy's stubtest (`tests/stubtest_allowlist.txt` lists the accepted differences).
- `tests/`: the `pytest` suite. Benchmarks (`test_*benchmark*.py`) and `test_memory_leak.py` are skipped by `just test-quick`.
- `examples/`: runnable examples; `tests/test_examples.py` runs every one of them.
- `docs/`: the documentation site (`mkdocs.yml`); `just docs` serves it locally with live reload, and `just docs-check` builds it strictly and runs its code blocks.
- `scripts/`: development scripts (`bench_cost.py` CPU/memory benchmark, Mermaid validation, `stub_docs.py`, `test-wheel.sh` and testing the Windows and aarch64 wheels under Wine and qemu, `wheel_index.py` for the package index).
- `.github/workflows/`: `wheels.yml` builds, tests and releases the wheels; `docs.yml` publishes the docs site and the package index.
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
4.  For performance changes, measure before and after with `just bench-cost --save before.json` and `--compare before.json`, compiled and uncompiled pipelines alike.
5.  Run `just test-quick` to verify your changes.
6.  If tests behave unexpectedly, try `just rebuild` to bypass any cached builds.
7.  Format with `just fmt`, then run `just check` before submitting a PR.

Happy Coding!
