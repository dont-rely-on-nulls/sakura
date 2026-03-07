(** Database state management.

    A database is a collection of relations with:
    - A merkle tree of relation hashes (for integrity verification)
    - A map of relation names to their actual Relation.t objects (integrated cache)
    - A history of previous database hashes (for time-travel queries)

    All mutations produce a new database state with a new hash.
    The history chain allows reconstruction of any past state. *)

module RelationMap = Map.Make(String)

type t = {
  hash : Conventions.Hash.t;
  name : Conventions.Name.t;
  tree : Merkle.t;
  relations : Relation.t RelationMap.t;  (* Stores actual relations, not just hashes *)
  domains : Domain.t RelationMap.t;      (* Domain registry: prelude + user-defined *)
  history : Conventions.Hash.t list;
  timestamp : float;
}

let empty ~name = {
  hash = "";
  name;
  tree = Merkle.empty;
  relations = RelationMap.empty;
  domains = RelationMap.empty;
  history = [];
  timestamp = Unix.gettimeofday ();
}

(** Compute database hash from the merkle tree root *)
let compute_hash db =
  match Merkle.root_hash db.tree with
  | Some root -> root
  | None -> Conventions.Hash.hash_text db.name

(** Update database with new relations map and tree *)
let update_state db ~relations ~tree =
  let new_hash =
    match Merkle.root_hash tree with
    | Some root -> root
    | None -> Conventions.Hash.hash_text db.name
  in
  let history = if db.hash = "" then db.history else db.hash :: db.history in
  {
    hash = new_hash;
    name = db.name;
    tree;
    relations;
    domains = db.domains;
    history;
    timestamp = Unix.gettimeofday ();
  }

(** Get a relation by name *)
let get_relation db name =
  RelationMap.find_opt name db.relations

(** Get a relation's hash by name *)
let get_relation_hash db name =
  match RelationMap.find_opt name db.relations with
  | None -> None
  | Some rel -> rel.Relation.hash

let get_relation_names db =
  RelationMap.fold (fun name _ acc -> name :: acc) db.relations []

let has_relation db name =
  RelationMap.mem name db.relations

(** Add a relation to the database *)
let add_relation db ~(relation : Relation.t) =
  let relation_hash = Option.get relation.hash in
  let tree = Merkle.insert relation_hash db.tree in
  let relations = RelationMap.add relation.name relation db.relations in
  update_state db ~relations ~tree

(** Remove a relation from the database *)
let remove_relation db ~name =
  match RelationMap.find_opt name db.relations with
  | None -> db
  | Some relation ->
    let relation_hash = Option.get relation.hash in
    let tree = Merkle.delete relation_hash db.tree in
    let relations = RelationMap.remove name db.relations in
    update_state db ~relations ~tree

(** Update a relation in the database (after tuple insert/delete) *)
let update_relation db ~(relation : Relation.t) =
  let name = relation.name in
  match RelationMap.find_opt name db.relations with
  | None -> db  (* Relation doesn't exist, no-op *)
  | Some old_relation ->
    let old_hash = Option.get old_relation.hash in
    let new_hash = Option.get relation.hash in
    let tree = db.tree |> Merkle.delete old_hash |> Merkle.insert new_hash in
    let relations = RelationMap.add name relation db.relations in
    update_state db ~relations ~tree

(* ============================================================================
   Domain Registry - prelude and user-defined domains
   ============================================================================ *)

(** Register a domain in the database.  Overwrites any existing domain with
    the same name, allowing user-defined domains to shadow prelude ones. *)
let add_domain db (domain : Domain.t) =
  { db with domains = RelationMap.add domain.name domain db.domains }

(** Look up a domain by name *)
let get_domain db name =
  RelationMap.find_opt name db.domains

(** List all registered domain names *)
let get_domain_names db =
  RelationMap.fold (fun name _ acc -> name :: acc) db.domains []

(** Check whether a domain is registered *)
let has_domain db name =
  RelationMap.mem name db.domains

(** Remove a domain from the registry *)
let remove_domain db name =
  { db with domains = RelationMap.remove name db.domains }
