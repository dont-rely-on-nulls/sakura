{ pkgs, ... }:
pkgs.stdenv.mkDerivation {
  name = "libressl-lib";
  src = ./.;
  buildInputs = with pkgs; [ libressl.dev pkg-config ];
  installPhase = ''
    mkdir -p $out/lib
    mkdir -p $out/include
    libressl_path=$(${pkgs.pkg-config}/bin/pkg-config --libs ${pkgs.libressl.dev}/lib/pkgconfig/libssl.pc | awk '{sub(/^-L/, ""); sub(/ -lssl$/, ""); print}')
    cp -r $libressl_path/* $out/lib
    cp -r ${pkgs.libressl.dev}/include/* $out/include
  '';
}
