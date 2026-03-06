module Hash = struct
  type t = Interop.Sha256.t
  let hash_text text = Interop.Sha256.compute_hash (Bytes.of_string text)
  let compare = String.compare
end

module Name = struct
  type t = string
end

module Cardinality = struct
  type t = Finite of int | AlephZero | Continuum
end

module AbstractValue = struct
  module StringMap = Map.Make(String)
  type t = {
    fields: string StringMap.t;
  }
  let hash_text value = 
    let folder (name: string) (value: string) (acc: Hash.t) = 
      Hash.(hash_text (acc ^ hash_text name ^ hash_text value))
    in StringMap.fold folder value.fields ""
  let compare a b = 
    StringMap.compare String.compare a.fields b.fields
end