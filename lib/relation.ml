module Hash = struct
  type t = Interop.Sha256.t

  let compare = String.compare
end

module Name = struct
  type t = string

  let compare = String.compare
end

module Tree = struct
  type t = unit
end

module Schema = struct
  type t = (string, string) Hashtbl.t

  let create () = Hashtbl.create 16
end

module RelationConstraints = struct
  type t = (string * string) list
end

module Cardinality = struct
  type t = Finite of int | AlephZero | Continuum
end

module Generator = struct
  type result =
    | Done
    | Value of (string, string) Hashtbl.t * t
    | Error of string

  and t = (string, string) Hashtbl.t -> result
end

module MembershipCriteria = struct
  type t = (string, string) Hashtbl.t

  let create () = Hashtbl.create 16
end

module Provenance = struct
  type t =
    | Undefined
    | Base of Name.t
    | Join of t * t
    | Select of t * MembershipCriteria.t
    | Project of t * string list
    | Take of t * int
end

module Lineage = struct
  type t =
    | Base of Name.t
    | Select of (string -> bool) * t
    | Project of string list * t
    | Join of string * t * t
    | ThetaJoin of (string -> string -> bool) * t * t
    | Sort of (string -> string -> int) * t
    | Take of int * t
    | Aggregate of string * string * t
end

type t = {
  hash : Hash.t;
  name : Name.t;
  tree : Tree.t option;
  schema : Schema.t;
  constraints : RelationConstraints.t;
  cardinality : Cardinality.t;
  generator : Generator.t option;
  membership_criteria : MembershipCriteria.t;
  provenance : Provenance.t;
  lineage : Lineage.t;
}

let make ~hash ~name ~tree ~schema ~constraints ~cardinality ~generator
    ~membership_criteria ~provenance ~lineage =
  {
    hash;
    name;
    tree;
    schema;
    constraints;
    cardinality;
    generator;
    membership_criteria;
    provenance;
    lineage;
  }

let compare left right =
  match Name.compare left.name right.name with
  | 0 -> Hash.compare left.hash right.hash
  | n -> n

module Ord = struct
  type nonrec t = t

  let compare = compare
end

module Set = Set.Make (Ord)
module Map = Map.Make (Ord)

