{
  description = "Rust version of streamz";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # Cross toolchain targeting aarch64-linux-gnu (glibc).
        crossPkgs = pkgs.pkgsCross.aarch64-multiplatform;

        # Rust toolchain with the aarch64-unknown-linux-gnu target preinstalled.
        # We pick a recent stable that's known to work with pyo3 0.27.
        # rust-overlay's mkRustBin takes two arg sets (config + pkgs spec).
        rustBin = rust-overlay.lib.mkRustBin { } pkgs;
        rustWithTarget = rustBin.stable.latest.default.override {
          targets = [ "aarch64-unknown-linux-gnu" ];
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            cargo
            rustc
            rustfmt
            clippy
            python3
            uv
            maturin
            ruff
            just
            mermaid-cli
            pkg-config
            openssl
            zlib
            stdenv.cc.cc.lib
          ] ++ pkgs.lib.optionals pkgs.stdenv.isLinux [
            # Instruction counts for scripts/bench_cost.py
            linuxPackages.perf
          ];

          shellHook = ''
            echo "Environment ready with Cargo, UV, and Python."
            export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath (with pkgs; [ stdenv.cc.cc zlib ])}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
            if [ ! -d ".venv" ]; then
              uv venv
            fi
            uv sync
          '';
        };

        # Cross-compile dev shell: build aarch64-linux wheels from x86_64-linux.
        # Provides:
        #   - rustc/cargo with the aarch64-unknown-linux-gnu stdlib
        #   - aarch64-unknown-linux-gnu-gcc cross toolchain (linker)
        #   - cross Python 3.13 (headers + lib for PyO3 symbol resolution)
        #   - maturin (host) to package the wheel
        #
        # Usage:
        #   nix develop .#aarch64-linux
        #   maturin build --release --target aarch64-unknown-linux-gnu --out dist
        #
        # The resulting wheel is tagged `linux_aarch64` (NOT manylinux-compliant).
        # Install it on any glibc-based arm64 Linux. For manylinux wheels you'd
        # need to run inside the official manylinux_2_28_aarch64 container.
        devShells.aarch64-linux = pkgs.mkShell {
          buildInputs = with pkgs; [
            rustWithTarget
            maturin
            just
            uv

            # HOST Python (so maturin can introspect it normally — we point PyO3 at
            # the cross headers/libs via PYO3_CROSS_* env vars below).
            python3

            # Cross C/C++ toolchain (provides aarch64-unknown-linux-gnu-gcc, ld, ar, ...)
            crossPkgs.stdenv.cc

            # Cross Python (we use it only for headers + libpython; we do NOT
            # add it to PATH because maturin would try to execute it on x86_64).
            crossPkgs.python3

            # Helpers needed by maturin's build script at runtime.
            pkg-config

            # Needed by `just publish` to upload to copyparty.
            curl
          ];

          shellHook = ''
            # Export cross toolchain store paths so cross-build scripts can find
            # them without needing to call `nix-build` / `nix eval` (which don't
            # work inside `nix develop` the same way as `nix-shell`).
            export AKAYU_CROSS_GCC_WRAPPER="${crossPkgs.stdenv.cc}"
            export AKAYU_CROSS_PYTHON="${crossPkgs.python3}"

            # Point cargo at the cross linker for our target.
            export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-unknown-linux-gnu-gcc

            # Tell PyO3's build.rs where to find the cross Python headers/libs.
            # With these set, PyO3 skips its own python introspection entirely.
            export PYO3_CROSS_INCLUDE_DIR="${crossPkgs.python3}/include/python3.13"
            export PYO3_CROSS_LIB_DIR="${crossPkgs.python3}/lib"
            export PYO3_CROSS_PYTHON_VERSION="3.13"
            export PYO3_CROSS_PYTHON_IMPLEMENTATION="CPython"

            # Audit wheel doesn't apply to cross builds (host glibc ≠ target glibc).
            export MATURIN_AUDITWHEEL=skip

            echo "==================================================="
            echo "aarch64-linux cross-compile shell ready"
            echo "==================================================="
            echo "Build: maturin build --release --target aarch64-unknown-linux-gnu --out dist"
            echo "Wheel tag: linux_aarch64 (glibc, not manylinux)"
            echo "==================================================="
          '';
        };
      }
    );
}
