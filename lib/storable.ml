(** Serialization formats for content-addressed storage.

    These modules define the wire format for persisting relational engine
    objects to storage. Each type has to_bytes/of_bytes for serialization.

    The stored formats capture only the essential data needed for reconstruction:
    - Tuple: relation name and attribute hashes
    - Relation: name, schema, tuple hashes, cardinality
    - Database: name, relation hashes, merkle tree keys, history *)

(** Stored tuple format for serialization *)
module Tuple = struct
  type t = {
    relation : string;
    attributes : (string * Conventions.Hash.t) list;  (* attr_name -> attr_value_hash *)
  }

  let to_bytes (st : t) : bytes =
    Marshal.to_bytes st [Marshal.Closures]

  let of_bytes (b : bytes) : t =
    Marshal.from_bytes b 0
end

(** Stored relation format for serialization *)
module Relation = struct
  type t = {
    name : string;
    schema : Schema.t;
    tree_keys : Conventions.Hash.t list;  (* Merkle tree contents - tuple hashes *)
    cardinality : Conventions.Cardinality.t;
  }

  let to_bytes (sr : t) : bytes =
    Marshal.to_bytes sr [Marshal.Closures]

  let of_bytes (b : bytes) : t =
    Marshal.from_bytes b 0
end

(** Stored database format for serialization *)
module Database = struct
  type t = {
    name : string;
    relations : (string * Conventions.Hash.t) list;  (* name -> relation_hash *)
    tree_keys : Conventions.Hash.t list;  (* Merkle tree contents - relation hashes *)
    history : Conventions.Hash.t list;
    timestamp : float;
  }

  let to_bytes (sd : t) : bytes =
    Marshal.to_bytes sd [Marshal.Closures]

  let of_bytes (b : bytes) : t =
    Marshal.from_bytes b 0
end
