{ lib, ocamlPackages, fetchFromGitHub, defaultVersion ? "5.2.3" }:
ocamlPackages.buildDunePackage {
  duneVersion = "3";
  
  pname = "ppx_protocol_conv";
  version = defaultVersion;

  minimalOCamlVersion = "4.07";

  src = fetchFromGitHub {
    owner = "andersfugmann";
    repo = "ppx_protocol_conv";
    rev = "425a7a1a26c26fcb740e5574e252043fd03eff46";
    sha256 = "sha256-G6zMgHioPUCh2i50cjQ6talN8CYO6UhEaWgi0xe3CWA=";
  };

  buildInputs = [ocamlPackages.ppxlib ocamlPackages.ppx_sexp_conv ocamlPackages.sexplib ocamlPackages.alcotest ];
  
  propagatedBuildInputs = [ ocamlPackages.ppxlib ocamlPackages.ppx_sexp_conv ocamlPackages.sexplib ocamlPackages.alcotest ];

  doCheck = true;

  meta = {
    license = lib.licenses.bsd3;
    homepage = "https://github.com/andersfugmann/ppx_protocol_conv";
  };
}
