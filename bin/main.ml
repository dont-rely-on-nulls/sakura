let main () =
  let digest = Sha256.to_hex (Sha256.string "sakura") in
  print_endline digest

let () =
  let tuple =
    Relational_engine.Tuple.make_materialized ~relation:"natural_less_than"
      ~attributes:
        (Relational_engine.Tuple.AttributeMap.of_list
           [ ("left", Obj.magic 0); ("right", Obj.magic 1) ])
  in
  let db = Relational_engine.Management.Database.empty ~name:"_" in
  print_endline @@ string_of_bool
  @@ Relational_engine.Prelude.Standard.less_than_natural.membership_criteria
       (fun name ->
         Option.bind
           (Relational_engine.Management.Database.get_relation db name)
           (fun r -> r.Relational_engine.Relation.tree))
       tuple;
  main ()
