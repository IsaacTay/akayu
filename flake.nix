{
  description = "Rust version of streamz";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
                      buildInputs = with pkgs; [
                      cargo
                      rustc
                      rustfmt
                      clippy
                      python3
                      python3Packages.pytest
                      python3Packages.pytest-asyncio
                                              python3Packages.pytest-benchmark
                                              python3Packages.psutil
                                              python3Packages.numpy
                                              python3Packages.streamz
                                              python3Packages.setuptools
                                              uv
                                              ruff
                                              maturin
                                              just
                                                        pkg-config
                      openssl
                    ];
          shellHook = ''
            echo "Environment ready with Cargo, UV, and Python."
            # Set up a local virtualenv if it doesn't exist
            if [ ! -d ".venv" ]; then
              uv venv
            fi
            source .venv/bin/activate
            # Install dev dependencies if pytest is missing
            if [ ! -f ".venv/bin/pytest" ]; then
              uv pip install pytest pytest-asyncio pytest-benchmark
            fi
          '';
        };
      }
    );
}
