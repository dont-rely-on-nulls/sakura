module type STORAGE = sig
  type t
  val write: t -> Conventions.Hash.t

end

module Storage: STORAGE = struct
  type t
  let write _ =
    failwith "TODO"

end