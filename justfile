# Benchmarks and slow memory tests, skipped by the quick test runs
quick_ignores := "--ignore=tests/test_benchmark.py --ignore=tests/test_benchmark_split.py --ignore=tests/test_memory_leak.py --ignore=tests/test_compile_benchmark.py --ignore=tests/test_lambda_benchmark.py --ignore=tests/test_vectorcall_benchmark.py"

# Default recipe - show available commands
default:
    @just --list

# Sync dependencies
sync:
    uv sync

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
    uv run python -m pytest tests/ -v {{quick_ignores}}

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
    env -u PYTHONPATH LD_LIBRARY_PATH="$libcxx" "$work/venv/bin/python" -m pytest tests/ -q {{quick_ignores}}

# Run benchmarks only
bench: build-release
    uv run python -m pytest tests/test_benchmark.py tests/test_benchmark_split.py tests/test_compile_benchmark.py \
        tests/test_lambda_benchmark.py tests/test_vectorcall_benchmark.py -v

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

# Build wheels for Python 3.13 (host arch). Used for testing on this machine.
build-wheels:
    mkdir -p dist
    rm -f dist/*.whl
    maturin build --release --interpreter python3.13 --out dist
    @echo "Wheels built:"
    @ls -la dist/*.whl

# Build all target wheels (host + aarch64) and upload each one to the
# configured copyparty share. Each wheel uploads under its own native filename
# so different arches coexist on the server (no clobbering).
#
# Auto-enters the aarch64 nix dev shell (`nix develop .#aarch64-linux`) if
# maturin isn't on PATH already. The cross-build script handles staging the
# cross gcc wrapper to bypass a known /nix/store path-resolution quirk on this
# host.
publish:
    #!/usr/bin/env bash
    set -euo pipefail

    # If maturin isn't on PATH, re-exec into the aarch64 dev shell. `exec`
    # replaces this process so we don't keep going in an under-equipped env.
    if ! command -v maturin >/dev/null 2>&1; then
        echo "maturin not on PATH — entering nix develop .#aarch64-linux ..."
        exec env -u PATH nix develop .#aarch64-linux --command just publish
    fi

    # --- Build host (x86_64) wheel ---------------------------------------
    # The aarch64 dev shell sets PYO3_CROSS_* + _PYTHON_HOST_PLATFORM, which
    # would mislead maturin into tagging the host wheel as aarch64. Strip them
    # for this build only.
    env -u PYO3_CROSS_INCLUDE_DIR \
        -u PYO3_CROSS_LIB_DIR \
        -u PYO3_CROSS_PYTHON_VERSION \
        -u PYO3_CROSS_PYTHON_IMPLEMENTATION \
        -u _PYTHON_HOST_PLATFORM \
        -u AKAYU_CROSS_GCC_WRAPPER \
        -u AKAYU_CROSS_PYTHON \
        -u CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER \
        just build-wheels

    # --- Build aarch64 wheel ---------------------------------------------
    ./scripts/cross-build-aarch64.sh

    echo ""
    echo "All wheels built:"
    ls -la dist/*.whl

    # --- Upload every wheel ----------------------------------------------
    url="https://api-v2.precix.io/share/akayu_wheels/?replace"
    # Enumerate wheels via find — glob expansion in `dist/*.whl` is unreliable
    # under the bash 5.2 sandbox that `just` shells out to on some hosts.
    wheels=()
    while IFS= read -r -d '' w; do
        wheels+=("$w")
    done < <(find dist -maxdepth 1 -name "*.whl" -print0)
    if [ "${#wheels[@]}" -eq 0 ]; then
        echo "No wheels found in dist/. Build steps above failed?" >&2
        exit 1
    fi
    echo "Uploading ${#wheels[@]} wheel(s) to copyparty ..."
    for wheel in "${wheels[@]}"; do
        name=$(basename "$wheel")
        printf "  %-55s " "$name"
        status=$(curl -s -o /dev/null -w '%{http_code}' -F "f=@$wheel" "$url")
        if [ "$status" = "201" ]; then
            echo "OK"
        else
            echo "FAIL (HTTP $status)" >&2
            exit 1
        fi
    done
    echo ""
    echo "Install (pick the wheel for your arch):"
    for wheel in "${wheels[@]}"; do
        echo "  uv add 'akayu @ https://api-v2.precix.io/share/akayu_wheels/$(basename "$wheel")'"
    done

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

# Format markdown documentation
docs-fmt: sync-docs
    uv run mdformat docs/ README.md

# Validate Mermaid diagrams in documentation
docs-check-mermaid:
    @echo "Validating Mermaid diagrams..."
    @python3 scripts/validate_mermaid.py

# Test code examples in documentation
docs-test-examples: build
    uv run python -m pytest --codeblocks docs/ README.md --ignore docs/benchmarks.md

# Run all documentation checks
docs-check: docs-check-mermaid docs-test-examples

# Check markdown formatting (no changes)
docs-fmt-check: sync-docs
    uv run mdformat --check docs/ README.md
