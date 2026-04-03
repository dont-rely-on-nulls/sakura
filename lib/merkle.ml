(** Merkle tree module signature for content-addressed hash sets.

    This is an opaque interface that can be backed by different implementations:
    - A simple hash set (current placeholder)
    - A proper radix/patricia merkle tree (e.g., plebeia when OCaml 5 support
      arrives)

    The tree stores hashes as keys (like a set) and computes a root hash from
    all contained elements. *)

module type S = sig
  type t

  val empty : t
  (** The empty tree *)

  val is_empty : t -> bool
  (** Check if tree is empty *)

  val insert : Conventions.Hash.t -> t -> t
  (** Insert a hash into the tree. Returns a new tree. *)

  val delete : Conventions.Hash.t -> t -> t
  (** Delete a hash from the tree. Returns a new tree. *)

  val member : Conventions.Hash.t -> t -> bool
  (** Check if a hash is in the tree *)

  val keys : t -> Conventions.Hash.t list
  (** Get all hashes in the tree *)

  val root_hash : t -> Conventions.Hash.t option
  (** Compute the root hash of the tree. None if empty. *)

  val size : t -> int
  (** Number of elements in the tree *)
end

(** Simple hash set implementation. This is a placeholder until a proper
    radix-merkle library with OCaml 5 support becomes available. *)
module HashSet : S = struct
  module StringSet = Set.Make (String)

  type t = StringSet.t

  let empty = StringSet.empty
  let is_empty = StringSet.is_empty
  let insert hash tree = StringSet.add hash tree
  let delete hash tree = StringSet.remove hash tree
  let member hash tree = StringSet.mem hash tree
  let keys tree = StringSet.elements tree
  (* TODO: Streaming/pagination for large tuple sets. Currently materializes
     entire keyset into memory, which fails for relations with billions of tuples.
     Replace with paginated access (keys_paginated offset limit) or lazy generator
     to avoid loading all hashes upfront. Consider replacing HashSet with proper
     radix-merkle tree (plebeia) for native lazy traversal. *)

  let root_hash tree =
    if StringSet.is_empty tree then None
    else
      (* Compute root hash by hashing sorted concatenation of all hashes *)
      let sorted_hashes = StringSet.elements tree in
      let concatenated = String.concat "" sorted_hashes in
      Some (Conventions.Hash.hash_text concatenated)

  let size = StringSet.cardinal
end

include HashSet
(** Default implementation using HashSet *)
