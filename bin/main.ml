let main () =
  let digest = Relational_engine.Interop.Sha256.compute_hash (Bytes.of_string "sakura") in
  print_endline digest

let () =
  (* let response = *)
  (* Relational_engine.Relation.write_and_retrieve() *)
  (* in match response with *)
  (* | Ok response -> print_endline response *)
  (* | Error err -> print_endline err; *)
  main ()
