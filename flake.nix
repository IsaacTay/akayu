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

        # Rust toolchain with the standard libraries of every wheel target
        # (`just wheels`). rust-overlay's mkRustBin takes two arg sets
        # (config + pkgs spec).
        rustBin = rust-overlay.lib.mkRustBin { } pkgs;
        rustWithTargets = rustBin.stable.latest.default.override {
          targets = [
            "aarch64-unknown-linux-gnu"
            "aarch64-apple-darwin"
            "x86_64-apple-darwin"
            "x86_64-pc-windows-msvc"
          ];
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

        # Wheels for every platform, built on this host (no CI): `just wheels`.
        #   - Linux x86_64/aarch64: linked with zig against glibc 2.17 symbols,
        #     so the wheels are manylinux2014 (no Docker image needed).
        #   - macOS arm64/x86_64: linked with zig; an extension module needs
        #     only libSystem, so no Apple SDK is needed.
        #   - Windows x64: cargo-xwin (clang-cl + lld-link), which downloads the
        #     MSVC CRT and Windows SDK headers to ~/.cache/cargo-xwin on first
        #     use; PyO3 generates the python3x.lib import libraries
        #     (llvm-dlltool).
        # Host Pythons 3.13 and 3.14 tag the x86_64 Linux wheels; the other
        # targets use maturin's built-in interpreter data.
        devShells.wheels = pkgs.mkShell {
          buildInputs = with pkgs; [
            rustWithTargets
            maturin
            zig
            cargo-xwin
            llvmPackages.clang-unwrapped
            llvmPackages.lld
            llvm
            python313
            python314
            just
            uv
            # Needed by `just publish` to upload to copyparty.
            curl
          ];

          shellHook = ''
            # Python's setup hook pins the wheel platform tag to the host's;
            # maturin must choose it per target.
            unset _PYTHON_HOST_PLATFORM
            # zig's setup hook points its cache into the build sandbox
            # (/build/...), which a dev shell doesn't have.
            export ZIG_GLOBAL_CACHE_DIR="''${XDG_CACHE_HOME:-$HOME/.cache}/zig"
          '';
        };
      }
    );
}
