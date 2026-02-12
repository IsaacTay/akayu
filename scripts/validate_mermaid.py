#!/usr/bin/env python3
import sys
import subprocess
import glob
import os
import tempfile
import argparse


def check_mermaid(files):
    """
    Validates Mermaid diagrams in markdown files using mermaid-cli (mmdc).
    """
    failed = False
    with tempfile.TemporaryDirectory() as temp_dir:
        for file_path in files:
            print(f"Checking {file_path}...", end="", flush=True)
            output_file = os.path.join(temp_dir, os.path.basename(file_path))

            try:
                # Use mmdc to process the file.
                # If it fails to parse/render, it returns non-zero exit code.
                subprocess.check_output(
                    ["mmdc", "-i", file_path, "-o", output_file],
                    stderr=subprocess.STDOUT,
                )
                print(" OK")
            except subprocess.CalledProcessError as e:
                print(" FAILED")
                print(f"\nError in {file_path}:")
                print(e.output.decode())
                failed = True
            except FileNotFoundError:
                print(
                    " ERROR: 'mmdc' command not found. Ensure mermaid-cli is installed (try 'nix develop')."
                )
                sys.exit(1)

    if failed:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Validate Mermaid diagrams in markdown files."
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="Files to check. If empty, checks all .md files in docs/ and README.md",
    )
    args = parser.parse_args()

    files = args.files
    if not files:
        files = glob.glob("docs/**/*.md", recursive=True) + glob.glob("README.md")

    check_mermaid(files)


if __name__ == "__main__":
    main()
