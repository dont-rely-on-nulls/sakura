module Hash = struct
  type t = Interop.Sha256.t
  let hash_text text = Interop.Sha256.compute_hash (Bytes.of_string text)
  let compare = String.compare
end

module Name = struct
  type t = string
end

module Cardinality = struct
  type t = Finite of int | ConstrainedFinite | AlephZero | Continuum
end

module AbstractValue = struct
  type t = Obj.t
  let hash (elem: t) = Interop.Sha256.compute_hash @@ Marshal.to_bytes elem []
end