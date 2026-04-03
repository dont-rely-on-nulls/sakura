type referenced = { value_hash : Conventions.Hash.t } [@@deriving sexp_of]
type materialized = { value : Conventions.AbstractValue.t } [@@deriving sexp_of]

type t = Referenced of referenced | Materialized of materialized
[@@deriving sexp_of]

let make_referenced ~value =
  let value_hash = Conventions.AbstractValue.hash value in
  Referenced { value_hash }

let make_materialized ~value = Materialized { value }
