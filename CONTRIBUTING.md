# Contributing to rstreamz

Thank you for your interest in contributing to `rstreamz`! This project aims to provide a high-performance, memory-safe, and async-capable implementation of the `streamz` library using Rust.

## Prerequisites

This project uses **Nix** to manage the development environment and dependencies. This ensures a reproducible environment for all contributors.

- **Install Nix**: [Download Nix](https://nixos.org/download.html)
- **Enable Flakes**: Ensure your Nix configuration has `experimental-features = nix-command flakes`.

## Setting Up the Environment

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/your-username/rstreamz.git
    cd rstreamz
    ```

2.  **Enter the development shell**:
    The `flake.nix` file defines the environment with Rust, Python, `uv`, and `maturin`.
    ```bash
    nix develop
    ```
    This command drops you into a shell with all necessary tools installed. It will also automatically create and activate a Python virtual environment (`.venv`) if one doesn't exist.

## Building the Project

We use `maturin` to build the Rust extension and install it into the virtual environment.

1.  **Build and install**:
    ```bash
    maturin develop
    ```
    This compiles the Rust code and installs the `rstreamz` package in editable mode into the current `.venv`.

## Running Tests

The project includes a comprehensive test suite using `pytest`.

1.  **Run all tests**:
    ```bash
    python -m pytest rstreamz/tests
    ```

2.  **Run benchmarks**:
    The tests include performance benchmarks comparing `rstreamz` to `streamz`.
    ```bash
    python -m pytest rstreamz/tests/test_comparison.py
    ```

## Code Style

We enforce code formatting for both Rust and Python.

1.  **Format Rust code**:
    ```bash
    cargo fmt --manifest-path rstreamz/Cargo.toml
    ```

2.  **Format Python code**:
    ```bash
    ruff format rstreamz
    ```

## Project Structure

- `rstreamz/src/lib.rs`: The core Rust implementation using PyO3.
- `rstreamz/tests/`: Python test suite.
    - `test_ops.py`: Unit tests for standard operations (map, filter, union, etc.).
    - `test_async.py`: Tests for async support.
    - `test_comparison.py`: Validation and benchmarking against the original `streamz`.
    - `test_memory_comparison.py`: Memory usage verification.
- `flake.nix`: Nix environment configuration.

## Development Workflow

1.  Make your changes in `rstreamz/src/lib.rs`.
2.  Run `maturin develop` to rebuild and update the Python package.
3.  Add or update tests in `rstreamz/tests/`.
4.  Run `python -m pytest rstreamz/tests` to verify your changes.
5.  Format your code before submitting a PR.

Happy Coding!
