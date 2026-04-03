{
  description = "Sakura Relational Engine";

  # Flake dependency specification
  #
  # To update all flake inputs:
  #
  #     $ nix flake update --commit-lockfile
  #
  # To update individual flake inputs:
  #
  #     $ nix flake lock --update-input <input> ... --commit-lockfile
  #
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    # Convenience functions for writing flakes
    flake-utils.url = "github:numtide/flake-utils";
    # Precisely filter files copied to the nix store
    nix-filter.url = "github:numtide/nix-filter";
  };

  outputs = { self, nixpkgs, flake-utils, nix-filter }:
    # Construct an output set that supports a number of default systems
    flake-utils.lib.eachDefaultSystem (system:
      let
        # Legacy packages that have not been converted to flakes
        legacyPackages = nixpkgs.legacyPackages.${system};
        # OCaml packages available on nixpkgs
        # Library functions from nixpkgs
        lib = legacyPackages.lib;
        ocamlPackages = legacyPackages.ocamlPackages;
        # merklecpp = legacyPackages.callPackage ./deps/merklecpp.nix { };
        libressl = legacyPackages.callPackage ./deps/libressl.nix { };
        ppx_protocol_conv =
          legacyPackages.callPackage ./deps/ppx_protocol_conv.nix {
            lib = legacyPackages.lib;
            fetchFromGitHub = legacyPackages.fetchFromGitHub;
            ocamlPackages = ocamlPackages; };
        ppx_protocol_conv_xml_light =
          legacyPackages.callPackage ./deps/ppx_protocol_conv_xml_light.nix {
            lib = legacyPackages.lib;
            ppx_protocol_conv = ppx_protocol_conv;
            fetchFromGitHub = legacyPackages.fetchFromGitHub;
            ocamlPackages = ocamlPackages; };
        librelational_engine =
          legacyPackages.callPackage ./deps/relational_engine_lib.nix {
            libressl = libressl;
            # merklecpp = merklecpp;
          };
        # Filtered sources (prevents unecessary rebuilds)
        sources = {
          ocaml = nix-filter.lib {
            root = ./.;
            include = [
              ".ocamlformat"
              "dune-project"
              (nix-filter.lib.inDirectory "bin")
              (nix-filter.lib.inDirectory "lib")
              (nix-filter.lib.inDirectory "test")
            ];
          };

          nix = nix-filter.lib {
            root = ./.;
            include = [ (nix-filter.lib.matchExt "nix") ];
          };
        };
      in {
        # Exposed packages that can be built or run with `nix build` or
        # `nix run` respectively:
        #
        #     $ nix build .#<name>
        #     $ nix run .#<name> -- <args?>
        #
        packages = {
          # The package that will be built or run by default. For example:
          #
          #     $ nix build
          #     $ nix run -- <args?>
          #
          default = self.packages.${system}.relational_engine;

          relational_engine = ocamlPackages.buildDunePackage {
            pname = "sakura";
            version = "0.1.0";
            duneVersion = "3";
            src = sources.ocaml;

            env = {
              # MERKLECPP_INCLUDE_PATH = "${merklecpp}/include";
              LIBRESSL_INCLUDE_PATH = "${libressl}/include";
              LIBRESSL_LIB_PATH = "${libressl}/lib";
              OS_LIB_EXTENSION =
                if legacyPackages.stdenv.isDarwin then "dylib" else "so";
              LIBRELATIONAL_ENGINE_LIB_PATH = "${librelational_engine}/lib";
              LIBRELATIONAL_ENGINE_INCLUDE_PATH = "${librelational_engine}/include";
            };

            nativeInputs = [ ];

            buildInputs = [ ppx_protocol_conv_xml_light
                            ppx_protocol_conv ]
            ++ (with ocamlPackages; [
              ctypes
              ctypes-foreign
              data-encoding
              ppx_inline_test
              ppx_deriving
              ppx_sexp_conv
              lwt
              lwt-exit
              batteries
              num
            ]);

            strictDeps = true;

          };
        };
        # Flake checks
        #
        #     $ nix flake check
        #
        checks = {
          # Run tests for the `relational_engine` package
          relational_engine = let
            # Patches calls to dune commands to produce log-friendly output
            # when using `nix ... --print-build-log`. Ideally there would be
            # support for one or more of the following:
            #
            # In Dune:
            #
            # - have workspace-specific dune configuration files
            #
            # In NixPkgs:
            #
            # - allow dune flags to be set in in `ocamlPackages.buildDunePackage`
            # - alter `ocamlPackages.buildDunePackage` to use `--display=short`
            # - alter `ocamlPackages.buildDunePackage` to allow `--config-file=FILE` to be set
            patchDuneCommand =
              let subcmds = [ "build" "test" "runtest" "install" ];
              in lib.replaceStrings
              (lib.lists.map (subcmd: "dune ${subcmd}") subcmds)
              (lib.lists.map (subcmd: "dune ${subcmd} --display=short")
                subcmds);

          in self.packages.${system}.relational_engine.overrideAttrs
          (oldAttrs: {
            name = "check-${oldAttrs.name}";
            doCheck = true;
            buildPhase = patchDuneCommand oldAttrs.buildPhase;
            checkPhase = patchDuneCommand oldAttrs.checkPhase;
            # skip installation (this will be tested in the `relational_engine-app` check)
            installPhase = "touch $out";
          });

          # Check Dune and OCaml formatting
          dune-fmt = legacyPackages.runCommand "check-dune-fmt" {
            nativeBuildInputs = [
              ocamlPackages.dune_3
              ocamlPackages.ocaml
              legacyPackages.ocamlformat
            ];
          } ''
            echo "checking dune and ocaml formatting"
            dune build \
              --display=short \
              --no-print-directory \
              --root="${sources.ocaml}" \
              --build-dir="$(pwd)/_build" \
              @fmt
            touch $out
          '';

          dune-test = legacyPackages.runCommand "check-dune-test" {
            nativeBuildInputs = [
              ocamlPackages.dune_3
              ocamlPackages.ocaml
              legacyPackages.ocamlformat
              ocamlPackages.ppx_inline_test
              ocamlPackages.ppx_deriving
              ocamlPackages.ppx_sexp_conv
              ocamlPackages.lwt
              ocamlPackages.lwt-exit
              ppx_protocol_conv
              ppx_protocol_conv_xml_light
            ];
          } ''
            echo "checking dune and ocaml formatting"
            dune build \
              --display=short \
              --no-print-directory \
              --root="${sources.ocaml}" \
              --build-dir="$(pwd)/_build" \
              @fmt
            touch $out
          '';

          # Check documentation generation
          dune-doc = legacyPackages.runCommand "check-dune-doc" {
            ODOC_WARN_ERROR = "true";
            nativeBuildInputs =
              [ ocamlPackages.dune_3 ocamlPackages.ocaml ocamlPackages.odoc ];
          } ''
            echo "checking ocaml documentation"
            dune build \
              --display=short \
              --no-print-directory \
              --root="${sources.ocaml}" \
              --build-dir="$(pwd)/_build" \
              @doc
            touch $out
          '';

          # Check Nix formatting
          # nixpkgs-fmt = legacyPackages.runCommand "check-nixpkgs-fmt" {
          #   nativeBuildInputs = [ legacyPackages.nixpkgs-fmt ];
          # } ''
          #   echo "checking nix formatting"
          #   nixpkgs-fmt --check ${sources.nix}
          #   touch $out
          # '';
        };

        # Development shells
        #
        #    $ nix develop .#<name>
        #    $ nix develop .#<name> --command dune build @test
        #
        # [Direnv](https://direnv.net/) is recommended for automatically loading
        # development environments in your shell. For example:
        #
        #    $ echo "use flake" > .envrc && direnv allow
        #    $ dune build @test
        #
        devShells = {
          default = legacyPackages.mkShell {

            env = {
              # MERKLECPP_INCLUDE_PATH = "${merklecpp}/include";
              LIBRESSL_INCLUDE_PATH = "${libressl}/include";
              LIBRESSL_LIB_PATH = "${libressl}/lib";
              OS_LIB_EXTENSION =
                if legacyPackages.stdenv.isDarwin then "dylib" else "so";
              LIBRELATIONAL_ENGINE_LIB_PATH = "${librelational_engine}/lib";
              LIBRELATIONAL_ENGINE_INCLUDE_PATH = "${librelational_engine}/include";
            };

            # Development tools
            packages = [
              # Source file formatting
              legacyPackages.nixpkgs-fmt
              legacyPackages.ocamlformat
              # For `dune build --watch ...`
              # legacyPackages.fswatch
              # For `dune build @doc`
              ocamlPackages.odoc
              # OCaml editor support
              ocamlPackages.ocaml-lsp
              # Nicely formatted types on hover
              ocamlPackages.ocamlformat-rpc-lib
              # Fancy REPL thing
              ocamlPackages.utop
              # Libraries
              # ocamlPackages.menhir
              ocamlPackages.ctypes
              ocamlPackages.ctypes-foreign
              ocamlPackages.data-encoding
              ocamlPackages.ppx_inline_test
              ocamlPackages.ppx_deriving
              ocamlPackages.ppx_sexp_conv
              legacyPackages.cmake
              legacyPackages.gcc
              # legacyPackages.nixfmt-classic
              ocamlPackages.lwt
              ocamlPackages.lwt-exit
              ocamlPackages.batteries
              ocamlPackages.num
              ppx_protocol_conv
              ppx_protocol_conv_xml_light
              ocamlPackages.earlybird
              # Formal verification
              legacyPackages.coq
              legacyPackages.coqPackages.stdlib
              legacyPackages.z3
            ];


            shellHook = ''
              export CAML_LD_LIBRARY_PATH="''${CAML_LD_LIBRARY_PATH:+$CAML_LD_LIBRARY_PATH:}$(ocamlfind query num)"
              # echo MERKLECPP_INCLUDE_PATH=$MERKLECPP_INCLUDE_PATH
              echo LIBRESSL_INCLUDE_PATH=$LIBRESSL_INCLUDE_PATH
              echo LIBRESSL_LIB_PATH=$LIBRESSL_LIB_PATH
              # echo OS_LIB_EXTENSION=$OS_LIB_EXTENSION 
              echo LIBRELATIONAL_ENGINE_LIB_PATH=$LIBRELATIONAL_ENGINE_LIB_PATH
              echo LIBRELATIONAL_ENGINE_INCLUDE_PATH=$LIBRELATIONAL_ENGINE_INCLUDE_PATH
            '';

            # Tools from packages
            inputsFrom = [ self.packages.${system}.relational_engine ];
          };
        };
      });
}
