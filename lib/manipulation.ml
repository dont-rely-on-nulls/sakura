(** Database manipulation operations.

    This module implements the core CRUD operations for the relational engine:
    - Database creation
    - Relation creation and deletion
    - Tuple insertion and deletion
    - Query operations

    All operations are immutable: they return new database/relation states
    rather than modifying existing ones. The storage layer handles persistence.

    This is a functor that takes a storage backend, ensuring all tuple data
    is properly persisted to the content-addressed store. *)

module RelationMap = Management.Database.RelationMap

(** Error types for manipulation operations *)
type error =
  | RelationNotFound of string
  | RelationAlreadyExists of string
  | TupleNotFound of Conventions.Hash.t
  | DuplicateTuple of Conventions.Hash.t
  | ConstraintViolation of string
  | StorageError of string

(** Result type for operations *)
type 'a result = ('a, error) Result.t

(** Re-export Schema for convenience *)
module Schema = Schema

(** Re-export hashing functions for convenience *)
let hash_tuple = Hashing.hash_tuple

(** Build base provenance for a schema *)
let build_base_provenance _schema relation_name =
  Relation.Provenance.Base relation_name

(** Build membership criteria from schema (validates type membership) *)
let build_membership_criteria _schema : (Tuple.t -> bool) =
  (* For now, accept all tuples. Full implementation would check domain membership. *)
  fun _ -> true

(** Functor to create manipulation operations with a storage backend *)
module Make (Storage : Management.Physical.S) = struct
  type storage = Storage.t

  (* ============================================================================
     State Persistence - Store relation and database states
     ============================================================================ *)

  (** Store a relation state to storage *)
  let store_relation (storage : storage) (relation : Relation.t) : (unit, error) Result.t =
    match relation.hash with
    | None -> Error (StorageError "Relation has no hash")
    | Some rel_hash ->
      let tree_keys = match relation.tree with
        | None -> []
        | Some tree -> Merkle.keys tree
      in
      let stored : Storable.Relation.t = {
        name = relation.name;
        schema = relation.schema;
        tree_keys;
        cardinality = relation.cardinality;
      } in
      match Storage.store_raw storage rel_hash (Storable.Relation.to_bytes stored) with
      | Error _ -> Error (StorageError "Failed to store relation")
      | Ok () -> Ok ()

  (** Store a database state to storage *)
  let store_database (storage : storage) (db : Management.Database.t) : (unit, error) Result.t =
    if db.hash = "" then
      Ok ()  (* Don't store empty/initial database *)
    else
      (* Extract relation hashes from the actual relations *)
      let relation_hashes =
        Management.Database.RelationMap.fold
          (fun name rel acc ->
            match rel.Relation.hash with
            | Some h -> (name, h) :: acc
            | None -> acc)
          db.relations
          []
      in
      let stored : Storable.Database.t = {
        name = db.name;
        relations = relation_hashes;
        tree_keys = Merkle.keys db.tree;
        history = db.history;
        timestamp = db.timestamp;
      } in
      match Storage.store_raw storage db.hash (Storable.Database.to_bytes stored) with
      | Error _ -> Error (StorageError "Failed to store database")
      | Ok () -> Ok ()

  (** Load a relation state from storage by hash *)
  let load_relation (storage : storage) (rel_hash : Conventions.Hash.t)
    : (Relation.t option, error) Result.t =
    match Storage.load_raw storage rel_hash with
    | Error _ -> Error (StorageError "Failed to load relation")
    | Ok None -> Ok None
    | Ok (Some bytes) ->
      let stored = Storable.Relation.of_bytes bytes in
      let tree = List.fold_left (fun t h -> Merkle.insert h t) Merkle.empty stored.tree_keys in
      let relation = Relation.make
        ~hash:(Some rel_hash)
        ~name:stored.name
        ~schema:stored.schema
        ~tree:(Some tree)
        ~constraints:None
        ~cardinality:stored.cardinality
        ~generator:None
        ~membership_criteria:(build_membership_criteria stored.schema)
        ~provenance:(build_base_provenance stored.schema stored.name)
        ~lineage:(Relation.Lineage.Base stored.name)
      in
      Ok (Some relation)

  (** Load a database state from storage by hash.
      Note: This loads the database structure with relation hashes,
      but the relations themselves need to be loaded separately. *)
  let load_database (storage : storage) (db_hash : Conventions.Hash.t)
    : (Management.Database.t option, error) Result.t =
    match Storage.load_raw storage db_hash with
    | Error _ -> Error (StorageError "Failed to load database")
    | Ok None -> Ok None
    | Ok (Some bytes) ->
      let stored = Storable.Database.of_bytes bytes in
      let tree = List.fold_left (fun t h -> Merkle.insert h t) Merkle.empty stored.tree_keys in
      (* Load each relation from storage *)
      let rec load_relations acc = function
        | [] -> Ok (List.rev acc)
        | (name, rel_hash) :: rest ->
          match load_relation storage rel_hash with
          | Error e -> Error e
          | Ok None -> Error (StorageError ("Relation not found: " ^ name))
          | Ok (Some rel) -> load_relations ((name, rel) :: acc) rest
      in
      match load_relations [] stored.relations with
      | Error e -> Error e
      | Ok relation_list ->
        let relations = List.fold_left
          (fun m (name, rel) -> Management.Database.RelationMap.add name rel m)
          Management.Database.RelationMap.empty
          relation_list
        in
        Ok (Some {
          Management.Database.hash = db_hash;
          name = stored.name;
          tree;
          relations;
          history = stored.history;
          timestamp = stored.timestamp;
        })

  (* ============================================================================
     Database Operations
     ============================================================================ *)

  (** Create a new database with standard domain relations *)
  let create_database ~name : Management.Database.t =
    let db = Management.Database.empty ~name in
    (* Add standard domain relations: integer, natural, string, etc. *)
    (* For now, just return empty database. Prelude can be added later. *)
    db

  (** Get database history *)
  let database_history (db : Management.Database.t) : Conventions.Hash.t list =
    db.history

  (* ============================================================================
     Relation Operations
     ============================================================================ *)

  (** Create a new empty relation with the given schema *)
  let create_relation
      (storage : storage)
      (db : Management.Database.t)
      ~(name : string)
      ~(schema : Schema.t)
    : (Management.Database.t * Relation.t) result =
    if Management.Database.has_relation db name then
      Error (RelationAlreadyExists name)
    else
      let tree = Merkle.empty in
      let relation_hash = Hashing.hash_relation ~name ~schema ~tree in
      let relation = Relation.make
          ~hash:(Some relation_hash)
          ~name
          ~schema
          ~tree:(Some tree)
          ~constraints:None
          ~cardinality:(Conventions.Cardinality.Finite 0)
          ~generator:None
          ~membership_criteria:(build_membership_criteria schema)
          ~provenance:(build_base_provenance schema name)
          ~lineage:(Relation.Lineage.Base name)
      in
      let new_db = Management.Database.add_relation db ~relation in
      (* Persist relation and database state *)
      match store_relation storage relation with
      | Error e -> Error e
      | Ok () ->
        match store_database storage new_db with
        | Error e -> Error e
        | Ok () -> Ok (new_db, relation)

  (** Create an immutable/generator-based relation *)
  let create_immutable_relation
      (storage : storage)
      (db : Management.Database.t)
      ~(name : string)
      ~(schema : Schema.t)
      ~(generator : Generator.t)
      ~(membership_criteria : Tuple.t -> bool)
      ~(cardinality : Conventions.Cardinality.t)
    : (Management.Database.t * Relation.t) result =
    if Management.Database.has_relation db name then
      Error (RelationAlreadyExists name)
    else
      let relation_hash = Hashing.hash_relation ~name ~schema ~tree:Merkle.empty in
      let relation = Relation.make
          ~hash:(Some relation_hash)
          ~name
          ~schema
          ~tree:None  (* No tree for generator-based relations *)
          ~constraints:None
          ~cardinality
          ~generator:(Some generator)
          ~membership_criteria
          ~provenance:(build_base_provenance schema name)
          ~lineage:(Relation.Lineage.Base name)
      in
      let new_db = Management.Database.add_relation db ~relation in
      (* Persist relation and database state *)
      match store_relation storage relation with
      | Error e -> Error e
      | Ok () ->
        match store_database storage new_db with
        | Error e -> Error e
        | Ok () -> Ok (new_db, relation)

  (** Remove a relation from the database *)
  let retract_relation
      (storage : storage)
      (db : Management.Database.t)
      ~(name : string)
    : Management.Database.t result =
    if not (Management.Database.has_relation db name) then
      Error (RelationNotFound name)
    else
      let new_db = Management.Database.remove_relation db ~name in
      (* Persist database state *)
      match store_database storage new_db with
      | Error e -> Error e
      | Ok () -> Ok new_db

  (** Clear all tuples from a relation (truncate) *)
  let clear_relation
      (storage : storage)
      (db : Management.Database.t)
      (relation : Relation.t)
    : (Management.Database.t * Relation.t) result =
    let name = relation.name in
    match Management.Database.get_relation db name with
    | None -> Error (RelationNotFound name)
    | Some _ ->
      let new_tree = Merkle.empty in
      let new_hash = Hashing.hash_relation ~name ~schema:relation.schema ~tree:new_tree in
      let new_relation = { relation with
        hash = Some new_hash;
        tree = Some new_tree;
        cardinality = Conventions.Cardinality.Finite 0;
      } in
      let new_db = Management.Database.update_relation db ~relation:new_relation in
      (* Persist relation and database state *)
      match store_relation storage new_relation with
      | Error e -> Error e
      | Ok () ->
        match store_database storage new_db with
        | Error e -> Error e
        | Ok () -> Ok (new_db, new_relation)

  (* ============================================================================
     Tuple Operations - Storage Integrated
     ============================================================================ *)

  (** Store each attribute value and return map of attr_name -> attr_hash *)
  let store_attributes
      (storage : storage)
      (tuple : Tuple.materialized)
    : ((string * Conventions.Hash.t) list, error) Result.t =
    let attrs = Tuple.AttributeMap.bindings tuple.attributes in
    let rec store_all acc = function
      | [] -> Ok (List.rev acc)
      | (name, attr) :: rest ->
        let value_bytes = Marshal.to_bytes attr.Attribute.value [Marshal.Closures] in
        let value_hash = Conventions.Hash.hash_text (Hashing.bytes_to_hex value_bytes) in
        match Storage.store_raw storage value_hash value_bytes with
        | Error _ -> Error (StorageError ("Failed to store attribute: " ^ name))
        | Ok () -> store_all ((name, value_hash) :: acc) rest
    in
    store_all [] attrs

  (** Insert a tuple into a relation, storing it in the backend *)
  let create_tuple
      (storage : storage)
      (db : Management.Database.t)
      (relation : Relation.t)
      (tuple : Tuple.materialized)
    : (Management.Database.t * Relation.t * Conventions.Hash.t) result =
    let name = relation.name in
    match Management.Database.get_relation db name with
    | None -> Error (RelationNotFound name)
    | Some _ ->
      (* Check membership criteria *)
      if not (relation.membership_criteria (Tuple.Materialized tuple)) then
        Error (ConstraintViolation "Tuple does not satisfy membership criteria")
      else
        (* Compute tuple hash *)
        let tuple_hash = Hashing.hash_tuple tuple in

        (* Get current tree or create empty one *)
        let current_tree = match relation.tree with
          | Some t -> t
          | None -> Merkle.empty
        in

        (* Check for duplicate *)
        if Merkle.member tuple_hash current_tree then
          Error (DuplicateTuple tuple_hash)
        else
          (* Store attribute values in storage *)
          match store_attributes storage tuple with
          | Error e -> Error e
          | Ok attr_hashes ->
            (* Create stored tuple representation *)
            let stored_tuple : Storable.Tuple.t = {
              relation = tuple.relation;
              attributes = attr_hashes;
            } in

            (* Store the tuple itself *)
            let tuple_bytes = Storable.Tuple.to_bytes stored_tuple in
            (match Storage.store_raw storage tuple_hash tuple_bytes with
             | Error _ -> Error (StorageError "Failed to store tuple")
             | Ok () ->
               (* Insert tuple hash into relation's merkle tree *)
               let new_tree = Merkle.insert tuple_hash current_tree in

               (* Compute new relation hash *)
               let new_relation_hash = Hashing.hash_relation ~name ~schema:relation.schema ~tree:new_tree in

               (* Update cardinality *)
               let new_cardinality = match relation.cardinality with
                 | Conventions.Cardinality.Finite n -> Conventions.Cardinality.Finite (n + 1)
                 | other -> other
               in

               (* Create updated relation *)
               let new_relation = { relation with
                 hash = Some new_relation_hash;
                 tree = Some new_tree;
                 cardinality = new_cardinality;
               } in

               (* Update database with the new relation *)
               let new_db = Management.Database.update_relation db ~relation:new_relation in

               (* Persist relation and database state *)
               match store_relation storage new_relation with
               | Error e -> Error e
               | Ok () ->
                 match store_database storage new_db with
                 | Error e -> Error e
                 | Ok () -> Ok (new_db, new_relation, tuple_hash))

  (** Insert multiple tuples into a relation *)
  let create_tuples
      (storage : storage)
      (db : Management.Database.t)
      (relation : Relation.t)
      (tuples : Tuple.materialized list)
    : (Management.Database.t * Relation.t * Conventions.Hash.t list) result =
    let rec insert_all db rel hashes = function
      | [] -> Ok (db, rel, List.rev hashes)
      | tuple :: rest ->
        match create_tuple storage db rel tuple with
        | Error e -> Error e
        | Ok (new_db, new_rel, hash) ->
          insert_all new_db new_rel (hash :: hashes) rest
    in
    insert_all db relation [] tuples

  (** Load a tuple from storage by its hash *)
  let load_tuple
      (storage : storage)
      (tuple_hash : Conventions.Hash.t)
    : (Tuple.materialized option, error) Result.t =
    match Storage.load_raw storage tuple_hash with
    | Error _ -> Error (StorageError "Failed to load tuple")
    | Ok None -> Ok None
    | Ok (Some tuple_bytes) ->
      let stored = Storable.Tuple.of_bytes tuple_bytes in
      (* Load each attribute value *)
      let rec load_attrs acc = function
        | [] -> Ok (List.rev acc)
        | (name, attr_hash) :: rest ->
          match Storage.load_raw storage attr_hash with
          | Error _ -> Error (StorageError ("Failed to load attribute: " ^ name))
          | Ok None -> Error (StorageError ("Attribute not found: " ^ name))
          | Ok (Some value_bytes) ->
            let value : Conventions.AbstractValue.t = Marshal.from_bytes value_bytes 0 in
            let attr : Attribute.materialized = { value } in
            load_attrs ((name, attr) :: acc) rest
      in
      match load_attrs [] stored.attributes with
      | Error e -> Error e
      | Ok attrs ->
        let attributes = List.fold_left
            (fun m (k, v) -> Tuple.AttributeMap.add k v m)
            Tuple.AttributeMap.empty
            attrs
        in
        Ok (Some { Tuple.relation = stored.relation; attributes })

  (** Load multiple tuples from storage *)
  let load_tuples
      (storage : storage)
      (hashes : Conventions.Hash.t list)
    : (Tuple.materialized list, error) Result.t =
    let rec load_all acc = function
      | [] -> Ok (List.rev acc)
      | hash :: rest ->
        match load_tuple storage hash with
        | Error e -> Error e
        | Ok None -> Error (TupleNotFound hash)
        | Ok (Some tuple) -> load_all (tuple :: acc) rest
    in
    load_all [] hashes

  (** Remove a tuple from a relation by its hash *)
  let retract_tuple
      (storage : storage)
      (db : Management.Database.t)
      (relation : Relation.t)
      ~(tuple_hash : Conventions.Hash.t)
    : (Management.Database.t * Relation.t) result =
    let name = relation.name in
    match Management.Database.get_relation db name with
    | None -> Error (RelationNotFound name)
    | Some _ ->
      match relation.tree with
      | None -> Error (TupleNotFound tuple_hash)
      | Some current_tree ->
        if not (Merkle.member tuple_hash current_tree) then
          Error (TupleNotFound tuple_hash)
        else
          (* Remove tuple hash from tree *)
          (* Note: We don't delete from storage - content-addressed storage is append-only.
             The tuple data remains for historical queries / garbage collection later. *)
          let new_tree = Merkle.delete tuple_hash current_tree in

          (* Compute new relation hash *)
          let new_relation_hash = Hashing.hash_relation ~name ~schema:relation.schema ~tree:new_tree in

          (* Update cardinality *)
          let new_cardinality = match relation.cardinality with
            | Conventions.Cardinality.Finite n when n > 0 ->
              Conventions.Cardinality.Finite (n - 1)
            | other -> other
          in

          (* Create updated relation *)
          let new_relation = { relation with
            hash = Some new_relation_hash;
            tree = Some new_tree;
            cardinality = new_cardinality;
          } in

          (* Update database *)
          let new_db = Management.Database.update_relation db ~relation:new_relation in

          (* Persist relation and database state *)
          match store_relation storage new_relation with
          | Error e -> Error e
          | Ok () ->
            match store_database storage new_db with
            | Error e -> Error e
            | Ok () -> Ok (new_db, new_relation)

  (* ============================================================================
     Query Operations
     ============================================================================ *)

  (** Get all tuple hashes from a relation *)
  let tuple_hashes (relation : Relation.t) : Conventions.Hash.t list =
    match relation.tree with
    | None -> []
    | Some tree -> Merkle.keys tree

  (** Get relation by name from database *)
  let get_relation (db : Management.Database.t) ~(name : string) : Relation.t option =
    Management.Database.get_relation db name

  (** Count tuples in a relation *)
  let tuple_count (relation : Relation.t) : int =
    match relation.tree with
    | None -> 0
    | Some tree -> Merkle.size tree

  (** Check if a tuple exists in a relation *)
  let tuple_exists (relation : Relation.t) (tuple_hash : Conventions.Hash.t) : bool =
    match relation.tree with
    | None -> false
    | Some tree -> Merkle.member tuple_hash tree
end

(** Default instance using in-memory storage for convenience/testing *)
module Memory = Make(Management.Physical.Memory)
