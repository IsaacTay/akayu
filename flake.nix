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
            # Rust toolchain
            cargo
            rustc
            rustfmt
            clippy

            # Python and package management
            python3
            uv
            maturin

            # Development tools
            ruff
            just

            # Build dependencies
            pkg-config
            openssl
          ];

          shellHook = ''
            echo "Environment ready with Cargo, UV, and Python."
            # Set up venv and sync dependencies
            if [ ! -d ".venv" ]; then
              uv venv
            fi
            uv sync --extra dev
          '';
        };
      }
    );
}
