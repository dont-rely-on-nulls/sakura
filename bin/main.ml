let main () =
  let digest = Relational_engine.Interop.Sha256.compute_hash (Bytes.of_string "sakura") in
  print_endline digest

let () =
  main ()
