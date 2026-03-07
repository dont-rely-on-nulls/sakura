{ lib, ocamlPackages, fetchFromGitLab, blake2 }:
ocamlPackages.buildDunePackage {
  duneVersion = "3";

  pname = "plebeia";
  version = "2.2.0";

  minimalOCamlVersion = "4.08";

  src = fetchFromGitLab {
    owner = "dailambda";
    repo = "plebeia";
    rev = "2.2.0";
    sha256 = "sha256-F1/FcCR0gn2kGNps3P4KRrqJlr4skHcuMytIRp4QRac=";
  };

  buildInputs = [ blake2 ] ++ (with ocamlPackages; [
    cstruct
    stdint
    res
    lwt
  ]);

  propagatedBuildInputs = [ blake2 ] ++ (with ocamlPackages; [
    cstruct
    stdint
    res
    lwt
  ]);

  doCheck = false;

  meta = {
    description = "Functional Merkle Patricia tree with disk storage";
    license = lib.licenses.mit;
    homepage = "https://github.com/camlspotter/plebeia";
  };
}
