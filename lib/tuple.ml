module AttributeMap = Map.Make(String)

type non_materialized = {
  hash : Conventions.Hash.t;
  relation : Conventions.Name.t;
  attributes : Conventions.Hash.t AttributeMap.t;
}

type materialized = {
  relation: Conventions.Name.t;
  attributes: Conventions.AbstractValue.t AttributeMap.t
}

type t = 
  | Materialized of materialized
  | NonMaterialized of non_materialized

let make_materialized ~relation ~attributes = Materialized { relation; attributes }
