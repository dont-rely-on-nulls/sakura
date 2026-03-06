type t = {
  hash : Conventions.Hash.t;
  name : Conventions.Name.t;
  name_hash : Conventions.Hash.t;
  value : Conventions.AbstractValue.t;
  value_hash : Conventions.Hash.t;
}

type materialized = {
  name: Conventions.Name.t;
  value: Conventions.AbstractValue.t
}

let make ~name ~value =
  let name_hash = Conventions.Hash.hash_text name in
  let value_hash = Conventions.AbstractValue.hash_text value in
  let hash = Conventions.Hash.hash_text (name_hash ^ value_hash) in
  { hash; name; name_hash; value; value_hash }

let make_with_hashes ~hash ~name ~name_hash ~value ~value_hash =
  { hash; name; name_hash; value; value_hash }
