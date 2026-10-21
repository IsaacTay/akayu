# The quick runs skip benchmarks (tests using pytest-benchmark's fixture) and
# tests marked slow (the memory-leak checks)
quick_select := "--benchmark-skip -m 'not slow'"

# Default recipe - show available commands
default:
    @just --list

# Sync the dependencies (the extension itself is `build`'s, so it stays as built)
sync:
    uv sync --no-install-project --inexact

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
    uv run python -m pytest tests/ -v {{quick_select}}

# Run the quick tests on Python 3.14 (cp314 wheel built outside the 3.13 dev env)
test-py314:
    #!/usr/bin/env bash
    set -euo pipefail
    work=$(mktemp -d -t akayu-py314.XXXXXX)
    trap 'rm -rf "$work"' EXIT
    py=$(nix build --no-link --print-out-paths --inputs-from . nixpkgs#python314)/bin/python3.14
    # PyPI's numpy/scipy wheels need libstdc++, which NixOS keeps off the default path
    libcxx=$(nix build --no-link --print-out-paths --inputs-from . nixpkgs#stdenv.cc.cc.lib)/lib
    env -u PYTHONPATH PYO3_PYTHON="$py" CARGO_TARGET_DIR=target/py314 \
        uv run --no-sync maturin build --release -i "$py" --out "$work"
    uv export --frozen --no-hashes --no-emit-project > "$work/requirements.txt"
    env -u PYTHONPATH uv venv -q -p "$py" "$work/venv"
    env -u PYTHONPATH uv pip install -q --python "$work/venv/bin/python" -r "$work/requirements.txt" "$work"/*.whl
    env -u PYTHONPATH LD_LIBRARY_PATH="$libcxx" "$work/venv/bin/python" -m pytest tests/ -q {{quick_select}}

# Run benchmarks only
bench: build-release
    uv run python -m pytest tests/test_benchmark.py tests/test_benchmark_split.py tests/test_compile_benchmark.py \
        tests/test_lambda_benchmark.py tests/test_vectorcall_benchmark.py -v

# CPU (instructions per item, load-independent) and memory per workload.
# Pass args through, e.g. `just bench-cost --save before.json` / `--compare before.json`
bench-cost *args: build-release
    uv run python scripts/bench_cost.py {{args}}

# Run clippy lints (warnings are errors)
clippy:
    cargo clippy -- -D warnings

# Format Rust and Python code
fmt:
    cargo fmt
    uv run ruff format .

# Check formatting without changing anything
fmt-check:
    cargo fmt --check
    uv run ruff format --check .

# Lint all Python
lint:
    uv run ruff check .

# The gate before committing: lint, formatting, clippy, quick tests and docs checks
check: lint fmt-check clippy test-quick docs-check

# Clean build artifacts
clean:
    cargo clean
    rm -rf *.so *.pyd

# Clean all caches (use when builds seem stale)
clean-all: clean
    rm -f python/akayu/akayu.*.so python/akayu/akayu.*.pyd
    uv cache clean akayu 2>/dev/null || true

# Force rebuild (bypasses uv cache)
rebuild: sync
    uv pip install -e . --reinstall-package akayu

# Build wheels for Python 3.13 (host arch). Used for testing on this machine.
build-wheels:
    mkdir -p dist
    rm -f dist/*.whl
    maturin build --release --interpreter python3.13 --out dist
    @echo "Wheels built:"
    @ls -la dist/*.whl

# The same wheels as the Wheels workflow builds on GitHub, cross-compiled on this
# host: manylinux x86_64 and aarch64 (glibc 2.17+), macOS arm64 and x86_64,
# Windows x64. Enters the `wheels` dev shell (flake.nix) when its tools aren't
# on PATH.
# Build wheels for every platform (CPython 3.13 and 3.14) and an sdist into dist/
wheels:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v cargo-xwin >/dev/null 2>&1 || ! command -v zig >/dev/null 2>&1; then
        exec nix develop .#wheels --command just wheels
    fi
    # The dev shells' Python setup hooks pin the platform tag to the host's.
    unset _PYTHON_HOST_PLATFORM PYTHONPATH PYO3_PYTHON
    export CARGO_TARGET_DIR=target/wheels
    mkdir -p dist
    rm -f dist/*.whl dist/*.tar.gz
    build() { maturin build --release --out dist "$@"; }
    # Linux: zig links against glibc 2.17 symbols, so the wheels are manylinux2014.
    build --zig --target x86_64-unknown-linux-gnu --compatibility manylinux_2_17 -i python3.13 python3.14
    build --zig --target aarch64-unknown-linux-gnu --compatibility manylinux_2_17 -i python3.13 python3.14
    # macOS: zig links without Apple's SDK (an extension module needs only libSystem).
    build --zig --target aarch64-apple-darwin -i python3.13 python3.14
    build --zig --target x86_64-apple-darwin -i python3.13 python3.14
    # Windows: cargo-xwin; PyO3 generates the python3x.lib import libraries.
    build --target x86_64-pc-windows-msvc -i python3.13 python3.14 --features pyo3/generate-import-lib
    maturin sdist --out dist
    echo ""
    ls -la dist

# Test dist/'s wheel for this machine (after `just wheels`) in a fresh environment, as the Wheels workflow does on each platform
test-wheel py="3.13":
    scripts/test-wheel.sh {{py}} {{quick_select}}

# Test dist/'s Windows x64 wheel on this host, under Wine (after `just wheels`)
test-wheel-windows py="3.13":
    scripts/test-wheel-windows.sh {{py}} {{quick_select}}

# Test dist/'s aarch64 Linux wheel on this host, under qemu (after `just wheels`; slow)
test-wheel-aarch64 py="3.13":
    scripts/test-wheel-aarch64.sh {{py}} {{quick_select}}

# GitHub builds and tests the wheels on each platform (.github/workflows/wheels.yml)
# and publishes them for a pushed version tag. This tags origin/main, which must
# already have the version in Cargo.toml and its CHANGELOG.md section.
# Tag the version in Cargo.toml on origin/main and push the tag, which releases it
release:
    #!/usr/bin/env bash
    set -euo pipefail
    # Both read all their input: under pipefail, `git show` killed by an early exit fails the check.
    version() { awk -F '"' '/^version = / && !v {v = $2} END {print v}'; }
    version=$(version < Cargo.toml)
    tag="v$version"
    git fetch -q origin main
    remote=$(git show origin/main:Cargo.toml | version)
    if [ "$remote" != "$version" ]; then
        echo "origin/main is at version $remote, not $version: push the version change first" >&2
        exit 1
    fi
    if ! git show origin/main:CHANGELOG.md | grep -x "## $version" >/dev/null; then
        echo "CHANGELOG.md on origin/main has no '## $version' section (the release notes)" >&2
        exit 1
    fi
    if git rev-parse -q --verify "refs/tags/$tag" >/dev/null || git ls-remote -q --exit-code origin "refs/tags/$tag" >/dev/null; then
        echo "$tag already exists" >&2
        exit 1
    fi
    git tag -a "$tag" -m "akayu $version" origin/main
    git push origin "$tag"
    echo "Pushed $tag; the release appears when the Wheels workflow finishes:"
    echo "  https://github.com/IsaacTay/akayu/actions/workflows/wheels.yml"

# Sync docs dependencies and build module for mkdocstrings
sync-docs:
    uv sync --group docs
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

# Copy the runtime docstrings (Rust `///` comments, `__init__.py`) into the type stub
stub-docs: build
    uv run python scripts/stub_docs.py

# Format markdown documentation
docs-fmt:
    uv run --isolated --only-group docs mdformat docs/ README.md

# Validate Mermaid diagrams in documentation
docs-check-mermaid:
    @echo "Validating Mermaid diagrams..."
    @python3 scripts/validate_mermaid.py

# Test code examples in documentation
docs-test-examples: build
    uv run python -m pytest --codeblocks docs/ README.md --ignore docs/benchmarks.md

# Build the site strictly: every page in the nav, every link and #anchor resolving
docs-check-site:
    uv run --isolated --only-group docs mkdocs build --strict -d target/docs-site

# Run all documentation checks
docs-check: docs-check-mermaid docs-fmt-check docs-check-site docs-test-examples

# Check markdown formatting (no changes), in an isolated env like docs-check-site
docs-fmt-check:
    uv run --isolated --only-group docs mdformat --check docs/ README.md
