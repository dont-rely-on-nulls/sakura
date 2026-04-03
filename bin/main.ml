let main () =
  let digest =
    Relational_engine.Interop.Sha256.compute_hash (Bytes.of_string "sakura")
  in
  print_endline digest

let () =
  let tuple =
    Relational_engine.Tuple.make_materialized ~relation:"natural_less_than"
      ~attributes:
        (Relational_engine.Tuple.AttributeMap.of_list
           [ ("left", Obj.magic 0); ("right", Obj.magic 1) ])
  in
  print_endline @@ string_of_bool
  @@ Relational_engine.Prelude.Standard.less_than_natural.membership_criteria
       tuple;
  main ()
