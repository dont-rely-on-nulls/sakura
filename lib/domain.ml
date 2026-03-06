type t = {
  name : Conventions.Name.t;
  generator : Generator.t;
  membership_criteria : Tuple.t -> bool;
  cardinality : Conventions.Cardinality.t;
}

let make ~name ~generator ~membership_criteria ~cardinality =
  { name; generator; membership_criteria; cardinality }
