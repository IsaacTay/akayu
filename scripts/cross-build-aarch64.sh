#!/usr/bin/env bash
# Cross-compile a Linux aarch64 wheel from this NixOS host.
#
# Requires `nix develop .#aarch64-linux` to provide the cross Rust + cross
# gcc + cross Python toolchain. The flake's shellHook exports the cross
# toolchain store paths via AKAYU_CROSS_* env vars, so this script doesn't
# need to call `nix eval` / `nix-build` (which behave differently inside
# `nix develop` vs `nix-shell`).
#
# Steps:
#   1. Stage the cross gcc wrapper to a writable tempdir (works around a
#      Nix store path-resolution issue seen in some environments).
#   2. Verify the cross tools are reachable.
#   3. Run maturin with the right cross env vars.
#
# Output: dist/akayu-<ver>-cp313-cp313-linux_aarch64.whl
# Tag:    linux_aarch64 (NOT manylinux). Installable on any glibc arm64 Linux.

set -euo pipefail

# Resolve cross toolchain from the env vars set by the flake's shellHook.
: "${AKAYU_CROSS_GCC_WRAPPER:?ERROR: AKAYU_CROSS_GCC_WRAPPER not set. Run via: nix develop .#aarch64-linux}"
: "${AKAYU_CROSS_PYTHON:?ERROR: AKAYU_CROSS_PYTHON not set. Run via: nix develop .#aarch64-linux}"

WRAPPER="$AKAYU_CROSS_GCC_WRAPPER"
PYTHON="$AKAYU_CROSS_PYTHON"

# Stage the cross gcc + binutils into a writable directory. Some NixOS
# setups have a /nix/store where literal-path access fails even though
# the files are listed by readdir(); staging avoids that.
STAGE=$(mktemp -d -t akayu-cross.XXXXXX)
trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$STAGE/bin"

shopt -s nullglob
files=("$WRAPPER/bin"/*)
ok=0
for f in "${files[@]}"; do
  bn=$(basename "$f")
  if cp "$f" "$STAGE/bin/$bn" 2>/dev/null; then
    ok=$((ok + 1))
  fi
done

# Some files may not survive `cp` in problematic environments. Verify the
# critical ones and re-copy via glob expansion if needed.
needed=(aarch64-unknown-linux-gnu-gcc aarch64-unknown-linux-gnu-ld aarch64-unknown-linux-gnu-ar)
for tool in "${needed[@]}"; do
  if [ ! -x "$STAGE/bin/$tool" ] || ! "$STAGE/bin/$tool" --version >/dev/null 2>&1; then
    echo "Recovering $tool via glob staging..." >&2
    rm -f "$STAGE/bin/$tool"
    # Glob in the wrapper dir for the basename without the aarch64 prefix.
    base=${tool#aarch64-unknown-linux-gnu-}
    for f in "$WRAPPER/bin/"*-"$base"; do
      [ -e "$f" ] && cp "$f" "$STAGE/bin/$tool" && break
    done
  fi
done

# Sanity check.
if ! "$STAGE/bin/aarch64-unknown-linux-gnu-gcc" --version >/dev/null 2>&1; then
  echo "ERROR: Cross gcc is broken on this host even after staging." >&2
  echo "       Try: nix-collect-garbage -d && nix-store --verify --repair" >&2
  exit 1
fi

# Compose the build env.
export PATH="$STAGE/bin:$PATH"
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER="$STAGE/bin/aarch64-unknown-linux-gnu-gcc"
export PYO3_CROSS_INCLUDE_DIR="$PYTHON/include/python3.13"
export PYO3_CROSS_LIB_DIR="$PYTHON/lib"
export PYO3_CROSS_PYTHON_VERSION="3.13"
export PYO3_CROSS_PYTHON_IMPLEMENTATION="CPython"
export MATURIN_AUDITWHEEL=skip

# Build.
mkdir -p dist
rm -f dist/*aarch64*.whl

maturin build --release \
  --target aarch64-unknown-linux-gnu \
  --out dist \
  "$@"

echo
echo "Built wheel:"
ls -la dist/*aarch64*.whl
echo
echo "Install on target:"
echo "  pip install dist/$(ls dist/*aarch64*.whl | head -1 | xargs basename)"