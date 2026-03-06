{ pkgs, ... }:
pkgs.stdenv.mkDerivation rec {
  name = "merklecpp";

  src = pkgs.fetchFromGitHub {
    owner = "microsoft";
    repo = "merklecpp";
    rev = "master";
    sha256 = "jJJ6FEn+AkMknqun0GkXh5DSXsJnFbdE7U8BQ7Dd/Fw=";
  };

  prePatch = "";
  configurePhase = "";
  buildPhase = "";
  installPhase = ''
    mkdir -p $out/include
    mv merklecpp.h $out/include/merklecpp.h
  '';
}
