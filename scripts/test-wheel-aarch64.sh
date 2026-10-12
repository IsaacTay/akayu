#!/usr/bin/env bash
# Run the test suite against dist/'s manylinux aarch64 wheel (`just wheels`),
# on this x86_64 host: an aarch64 CPython from nixpkgs under qemu's user-mode
# emulation. Slow (expect an hour or more for the full suite).
#
# usage: scripts/test-wheel-aarch64.sh [3.13|3.14] [pytest args...]
set -euo pipefail

ver=${1:-3.13}
shift || true
tag=cp${ver/./}
wheel=$(find dist -maxdepth 1 -name "akayu-*-$tag-$tag-manylinux*_aarch64*.whl" | head -1)
[ -n "$wheel" ] || { echo "no $tag aarch64 wheel in dist/: run 'just wheels'" >&2; exit 1; }

nixpkgs() { nix build --no-link --print-out-paths --inputs-from . "nixpkgs#$1"; }
qemu=$(nixpkgs qemu-user)/bin/qemu-aarch64
python=$(nixpkgs "legacyPackages.aarch64-linux.python${ver/./}")/bin/python$ver
# PyPI's numpy/scipy wheels need libstdc++, which isn't on any default path here.
libcxx=$(nixpkgs legacyPackages.aarch64-linux.stdenv.cc.cc.lib)/lib

work=$(mktemp -d -t akayu-aarch64.XXXXXX)
trap 'rm -rf "$work"' EXIT
# The wheel and the test dependencies, as aarch64 packages (minus build tools).
uv export --frozen --no-hashes --no-emit-project | grep -vE '^(maturin|ruff)==' > "$work/requirements.txt"
env -u PYTHONPATH uv pip install -q --python-platform aarch64-unknown-linux-gnu --python-version "$ver" \
    --target "$work/site" -r "$work/requirements.txt" "$wheel"

# Python runs through this wrapper, which is also what it reports as
# sys.executable (qemu's -0): tests that start `sys.executable` in a subprocess
# then run it under qemu too.
cat > "$work/python" <<WRAPPER
#!/usr/bin/env bash
exec "$qemu" -0 "$work/python" -E LD_LIBRARY_PATH="$libcxx" "$python" "\$@"
WRAPPER
chmod +x "$work/python"
# The dev shell's settings for its own (x86_64) Python must not reach this one,
# and its bytecode stays out of tests/__pycache__.
env -u _PYTHON_SYSCONFIGDATA_NAME -u _PYTHON_HOST_PLATFORM PYTHONPATH="$work/site" PYTHONDONTWRITEBYTECODE=1 \
    "$work/python" -m pytest tests/ -q -p no:cacheprovider "$@"
