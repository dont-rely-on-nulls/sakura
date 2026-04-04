let main () =
  let digest = Relational_engine.Conventions.Hash.hash_text "sakura" in
  print_endline digest

let () = main ()
