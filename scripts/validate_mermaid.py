#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def _browser_env():
    """mmdc's headless Chromium must not pick up host libraries: on NixOS a
    host LD_LIBRARY_PATH makes it load an incompatible libasound and fail."""
    env = dict(os.environ)
    env.pop("LD_LIBRARY_PATH", None)
    return env


def check_mermaid(files):
    """
    Validates Mermaid diagrams in markdown files using mermaid-cli (mmdc).
    """
    failed = False
    with tempfile.TemporaryDirectory() as temp_dir:
        for file_path in files:
            print(f"Checking {file_path}...", end="", flush=True)
            output_file = str(Path(temp_dir) / Path(file_path).name)

            try:
                # Use mmdc to process the file.
                # If it fails to parse/render, it returns non-zero exit code.
                subprocess.check_output(
                    ["mmdc", "-i", file_path, "-o", output_file],
                    stderr=subprocess.STDOUT,
                    env=_browser_env(),
                )
                print(" OK")
            except subprocess.CalledProcessError as e:
                print(" FAILED")
                print(f"\nError in {file_path}:")
                print(e.output.decode())
                failed = True
            except FileNotFoundError:
                print(" ERROR: 'mmdc' not found. Install mermaid-cli, or run inside the dev shell ('nix develop').")
                sys.exit(1)

    if failed:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Validate Mermaid diagrams in markdown files.")
    parser.add_argument(
        "files",
        nargs="*",
        help="Files to check. If empty, checks all .md files in docs/ and README.md",
    )
    args = parser.parse_args()

    files = args.files
    if not files:
        files = [str(p) for p in sorted(Path("docs").rglob("*.md"))] + [str(p) for p in Path().glob("README.md")]

    check_mermaid(files)


if __name__ == "__main__":
    main()
