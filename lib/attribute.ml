type referenced = { value_hash : Conventions.Hash.t }

type materialized = { value: Conventions.AbstractValue.t }

type t = 
  | Referenced of referenced
  | Materialized of materialized

let make_referenced ~value =
  let value_hash = Conventions.AbstractValue.hash value in
  Referenced { value_hash }

let make_materialized ~value =
  Materialized { value }