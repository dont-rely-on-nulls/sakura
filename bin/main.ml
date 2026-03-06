let main () =
  let digest = Relational_engine.Interop.Sha256.compute_hash (Bytes.of_string "sakura") in
  print_endline digest

let () =
  (* TODO: Since the comparison on the standard function is done atop of the abstract value, a change on the abstract name incurs into an alteration of the order as well *)
  let left_value: Relational_engine.Conventions.AbstractValue.t = {fields = Relational_engine.Conventions.AbstractValue.StringMap.of_list [("value!!!!!", "0")]} in
  let right_value: Relational_engine.Conventions.AbstractValue.t = {fields = Relational_engine.Conventions.AbstractValue.StringMap.of_list [("value", "1")]} in
  let tuple = Relational_engine.Tuple.make_materialized ~relation: "natural_less_than" ~attributes: (Relational_engine.Conventions.AbstractValue.StringMap.of_list [("left", left_value); ("right", right_value)]) in
  print_endline @@ string_of_bool @@ Relational_engine.Prelude.Standard.less_than_natural.membership_criteria tuple;
  main ()
