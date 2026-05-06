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
            uv
            maturin
            ruff
            just
            mermaid-cli
            pkg-config
            openssl
            zlib
            stdenv.cc.cc.lib
          ];

          shellHook = ''
            echo "Environment ready with Cargo, UV, and Python."
            export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath (with pkgs; [ stdenv.cc.cc zlib ])}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
            if [ ! -d ".venv" ]; then
              uv venv
            fi
            uv sync --extra dev
          '';
        };
      }
    );
}
