{ pkgs, libressl ? pkgs.callPackage ./libressl.nix { }
# , merklecpp ? pkgs.callPackage ./merklecpp.nix { }
, ... }:
let shared_lib_extension = if pkgs.stdenv.isDarwin then "dylib" else "so";
in pkgs.stdenv.mkDerivation {
  name = "librelational_engine";
  src = ../shared/librelational_engine;
  nativeBuildInputs = [ pkgs.cmake pkgs.gcc ];
  buildInputs = [
    libressl
    # merklecpp
  ];
  env = {
    # MERKLECPP_INCLUDE_PATH = "${merklecpp}/include";
    LIBRESSL_INCLUDE_PATH = "${libressl}/include";
    LIBRESSL_LIB_PATH = "${libressl}/lib";
    OS_LIB_EXTENSION = shared_lib_extension;
  };
  buildPhase = ''
    mkdir -p $out/lib
    mkdir -p $out/include
    cmake --build . --target relational_engine
  '';
  installPhase = ''
    mv librelational_engine.${shared_lib_extension} $out/lib/librelational_engine.${shared_lib_extension}
    cp $src/library.h $out/include/library.h
  '';
}
