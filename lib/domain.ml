type t = {
  name : Conventions.Name.t;
  generator : Generator.t;
  membership_criteria : Tuple.t -> bool;
  cardinality : Conventions.Cardinality.t;
  compare : Conventions.AbstractValue.t -> Conventions.AbstractValue.t -> int;
}

let make ~name ~generator ~membership_criteria ~cardinality ~compare =
  { name; generator; membership_criteria; cardinality; compare }
