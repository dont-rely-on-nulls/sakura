{ lib, ocamlPackages, fetchFromGitLab }:
ocamlPackages.buildDunePackage {
  duneVersion = "2";
  useDune2 = true;

  pname = "blake2";
  version = "0.3";

  minimalOCamlVersion = "4.06";

  src = fetchFromGitLab {
    owner = "nomadic-labs";
    repo = "ocaml-blake2";
    rev = "v0.3";
    sha256 = "sha256-LueEFWNHuTGp/hZNT9sRx3ZhIbGNlSSnqcO9geMzgag=";
  };

  buildInputs = with ocamlPackages; [ ];

  propagatedBuildInputs = with ocamlPackages; [ ];

  doCheck = false;

  meta = {
    description = "Blake2 cryptographic hash function for OCaml";
    license = lib.licenses.isc;
    homepage = "https://gitlab.com/nomadic-labs/ocaml-blake2";
  };
}
