#!/usr/bin/env bash
# Run the test suite against dist/'s Windows x64 wheel (`just wheels`), on this
# Linux host: the embeddable CPython from python.org, under Wine, with a
# virtual X display for Wine's setup.
#
# usage: scripts/test-wheel-windows.sh [3.13|3.14] [pytest args...]
#
# The modules that import numpy skip themselves: numpy's Windows build calls a
# C runtime function (ucrtbase crealf) that Wine doesn't implement, and they
# test nothing Windows-specific. The Wine prefix is kept between runs in
# ${XDG_CACHE_HOME:-~/.cache}/akayu-wine (its first setup takes minutes).
set -euo pipefail

ver=${1:-3.13}
shift || true
case $ver in
    3.13) full=3.13.16 sha256=97dae5274cc54867065e8d5a3226e48c35017ed332a0fdb0e27d5b5821961297 ;;
    3.14) full=3.14.8 sha256=a93abe456ab01bd96d7a085b3cdb6566b3063f4241360d114142fbdb07f0a310 ;;
    *) echo "unsupported Python version: $ver" >&2; exit 2 ;;
esac
tag=cp${ver/./}
wheel=$(find dist -maxdepth 1 -name "akayu-*-$tag-$tag-win_amd64.whl" | head -1)
[ -n "$wheel" ] || { echo "no $tag win_amd64 wheel in dist/: run 'just wheels'" >&2; exit 1; }

nixpkgs() { nix build --no-link --print-out-paths --inputs-from . "nixpkgs#$1"; }
wine_bin=$(nixpkgs wineWow64Packages.stable)/bin
xvfb_run=$(nixpkgs xvfb-run)/bin/xvfb-run

work=$(mktemp -d -t akayu-windows.XXXXXX)
trap 'rm -rf "$work"' EXIT

# Python: the embeddable zip, checked against its pinned hash.
curl -sSfL -o "$work/python.zip" "https://www.python.org/ftp/python/$full/python-$full-embed-amd64.zip"
echo "$sha256  $work/python.zip" | sha256sum -c --quiet -
python3 -I -c 'import sys, zipfile; [zipfile.ZipFile(z).extractall(sys.argv[1]) for z in sys.argv[2:]]' \
    "$work/python" "$work/python.zip" "$wheel"
# The test dependencies, as Windows packages, into site/ (minus build tools).
uv export --frozen --no-hashes --no-emit-project | grep -vE '^(maturin|ruff)==' > "$work/requirements.txt"
env -u PYTHONPATH uv pip install -q --python-platform x86_64-pc-windows-msvc --python-version "$ver" \
    --target "$work/python/site" -r "$work/requirements.txt"
printf 'python%s.zip\n.\nsite\nimport site\n' "${ver/./}" > "$work/python/python${ver/./}._pth"

# The dev shell's settings for its own Python must not reach Windows'. No
# bytecode either: pytest would cache its rewritten test modules in
# tests/__pycache__ with Z:\ paths, which the Linux runs would then reuse.
unset PYTHONPATH _PYTHON_SYSCONFIGDATA_NAME _PYTHON_HOST_PLATFORM
export PYTHONDONTWRITEBYTECODE=1
# No winedbg: a crash then ends the run instead of waiting in the debugger.
export PATH="$wine_bin:$PATH" WINEDEBUG=-all WINEDLLOVERRIDES="mscoree,mshtml=;winemenubuilder.exe,winedbg.exe=d"
export WINEPREFIX="${XDG_CACHE_HOME:-$HOME/.cache}/akayu-wine"
if [ ! -d "$WINEPREFIX/drive_c/windows/system32" ]; then
    echo "Setting up the Wine prefix (first run only) ..."
    "$xvfb_run" -a wineboot -i
fi

# Tests that fail only because of this setup, not on Windows itself, skip
# themselves under AKAYU_UNDER_WINE (tests/support.py has the list): the
# modules that import numpy; the source-location tests, which compare a
# node's recorded file with __file__ (pytest imports the modules by their
# Unix paths, /mnt/..., while __file__ is the Z:\ path); a user module next
# to the script (the embeddable CPython ignores the current directory); and
# concurrent to_text_file() appends, which Wine doesn't make atomic as
# Windows does (worth confirming on a real Windows machine).
export AKAYU_UNDER_WINE=1
# The test files, listed by their Z:\ paths.
files=()
for file in tests/test_*.py; do
    files+=("Z:$PWD/$file")
done
# Output goes to a file, not a pipe: Wine's services outlive python.exe and
# would hold a pipe open.
status=0
(cd "$work/python" && "$xvfb_run" -a wine python.exe -m pytest -q -p no:cacheprovider \
    "${files[@]}" "$@" > "$work/pytest.log" 2>&1) || status=$?
wineserver -k || true
grep -avE '^(Xlib: |X connection)' "$work/pytest.log"
exit "$status"
