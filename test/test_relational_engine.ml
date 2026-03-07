(** Tests for the relational engine *)

open Relational_engine

(* ============================================================================
   Merkle Tree Tests
   ============================================================================ *)

let%test_unit "merkle: empty tree" =
  let tree = Merkle.empty in
  assert (Merkle.is_empty tree);
  assert (Merkle.size tree = 0);
  assert (Merkle.root_hash tree = None)

let%test_unit "merkle: insert single element" =
  let tree = Merkle.empty in
  let hash = "abc123" in
  let tree = Merkle.insert hash tree in
  assert (not (Merkle.is_empty tree));
  assert (Merkle.size tree = 1);
  assert (Merkle.member hash tree);
  assert (Merkle.root_hash tree <> None)

let%test_unit "merkle: insert multiple elements" =
  let tree = Merkle.empty in
  let tree = Merkle.insert "hash1" tree in
  let tree = Merkle.insert "hash2" tree in
  let tree = Merkle.insert "hash3" tree in
  assert (Merkle.size tree = 3);
  assert (Merkle.member "hash1" tree);
  assert (Merkle.member "hash2" tree);
  assert (Merkle.member "hash3" tree);
  assert (not (Merkle.member "hash4" tree))

let%test_unit "merkle: delete element" =
  let tree = Merkle.empty in
  let tree = Merkle.insert "hash1" tree in
  let tree = Merkle.insert "hash2" tree in
  assert (Merkle.size tree = 2);
  let tree = Merkle.delete "hash1" tree in
  assert (Merkle.size tree = 1);
  assert (not (Merkle.member "hash1" tree));
  assert (Merkle.member "hash2" tree)

let%test_unit "merkle: keys returns all elements" =
  let tree = Merkle.empty in
  let tree = Merkle.insert "c" tree in
  let tree = Merkle.insert "a" tree in
  let tree = Merkle.insert "b" tree in
  let keys = Merkle.keys tree in
  assert (List.length keys = 3);
  assert (List.mem "a" keys);
  assert (List.mem "b" keys);
  assert (List.mem "c" keys)

let%test_unit "merkle: root hash changes on insert" =
  let tree = Merkle.empty in
  let tree1 = Merkle.insert "hash1" tree in
  let root1 = Merkle.root_hash tree1 in
  let tree2 = Merkle.insert "hash2" tree1 in
  let root2 = Merkle.root_hash tree2 in
  assert (root1 <> root2)

let%test_unit "merkle: same elements produce same root hash" =
  let tree1 = Merkle.empty
    |> Merkle.insert "a"
    |> Merkle.insert "b" in
  let tree2 = Merkle.empty
    |> Merkle.insert "b"
    |> Merkle.insert "a" in
  assert (Merkle.root_hash tree1 = Merkle.root_hash tree2)

(* ============================================================================
   Physical Storage Tests
   ============================================================================ *)

let%test_unit "storage: create and close" =
  match Management.Physical.Memory.create () with
  | Error _ -> assert false
  | Ok storage ->
    Management.Physical.Memory.close storage

let%test_unit "storage: store and load attribute" =
  match Management.Physical.Memory.create () with
  | Error _ -> assert false
  | Ok storage ->
    let value = Obj.magic 42 in
    (match Management.Physical.Memory.store_attribute storage value with
     | Error _ -> assert false
     | Ok hash ->
       (match Management.Physical.Memory.load_attribute storage hash with
        | Error _ -> assert false
        | Ok None -> assert false
        | Ok (Some loaded) ->
          assert ((Obj.magic loaded : int) = 42)));
    Management.Physical.Memory.close storage

let%test_unit "storage: store and load raw bytes" =
  match Management.Physical.Memory.create () with
  | Error _ -> assert false
  | Ok storage ->
    let hash = "test_hash" in
    let data = Bytes.of_string "hello world" in
    (match Management.Physical.Memory.store_raw storage hash data with
     | Error _ -> assert false
     | Ok () ->
       (match Management.Physical.Memory.load_raw storage hash with
        | Error _ -> assert false
        | Ok None -> assert false
        | Ok (Some loaded) ->
          assert (Bytes.equal loaded data)));
    Management.Physical.Memory.close storage

let%test_unit "storage: exists check" =
  match Management.Physical.Memory.create () with
  | Error _ -> assert false
  | Ok storage ->
    let hash = "exists_test" in
    let data = Bytes.of_string "data" in
    (match Management.Physical.Memory.exists storage hash with
     | Error _ -> assert false
     | Ok exists -> assert (not exists));
    (match Management.Physical.Memory.store_raw storage hash data with
     | Error _ -> assert false
     | Ok () ->
       (match Management.Physical.Memory.exists storage hash with
        | Error _ -> assert false
        | Ok exists -> assert exists));
    Management.Physical.Memory.close storage

let%test_unit "storage: transaction commit" =
  match Management.Physical.Memory.create () with
  | Error _ -> assert false
  | Ok storage ->
    let result = Management.Physical.Memory.with_transaction storage (fun () ->
      let hash = "tx_test" in
      let data = Bytes.of_string "tx_data" in
      Management.Physical.Memory.store_raw storage hash data
    ) in
    (match result with
     | Error _ -> assert false
     | Ok () ->
       (match Management.Physical.Memory.exists storage "tx_test" with
        | Error _ -> assert false
        | Ok exists -> assert exists));
    Management.Physical.Memory.close storage

(* ============================================================================
   Database Tests
   ============================================================================ *)

let%test_unit "database: create empty" =
  let db = Management.Database.empty ~name:"test_db" in
  assert (db.name = "test_db");
  assert (Merkle.is_empty db.tree);
  assert (Management.Database.RelationMap.is_empty db.relations);
  assert (db.history = [])

let%test_unit "database: add relation" =
  let db = Management.Database.empty ~name:"test_db" in
  (* Create a minimal relation for testing *)
  let relation = Relation.make
    ~hash:(Some "rel_hash_1")
    ~name:"users"
    ~schema:Schema.empty
    ~tree:(Some Merkle.empty)
    ~constraints:None
    ~cardinality:(Conventions.Cardinality.Finite 0)
    ~generator:None
    ~membership_criteria:(fun _ -> true)
    ~provenance:Relation.Provenance.Undefined
    ~lineage:(Relation.Lineage.Base "users")
  in
  let db = Management.Database.add_relation db ~relation in
  assert (Management.Database.has_relation db "users");
  assert (Management.Database.get_relation_hash db "users" = Some "rel_hash_1");
  assert (not (Management.Database.has_relation db "orders"))

let%test_unit "database: remove relation" =
  let db = Management.Database.empty ~name:"test_db" in
  let relation = Relation.make
    ~hash:(Some "hash1")
    ~name:"users"
    ~schema:Schema.empty
    ~tree:(Some Merkle.empty)
    ~constraints:None
    ~cardinality:(Conventions.Cardinality.Finite 0)
    ~generator:None
    ~membership_criteria:(fun _ -> true)
    ~provenance:Relation.Provenance.Undefined
    ~lineage:(Relation.Lineage.Base "users")
  in
  let db = Management.Database.add_relation db ~relation in
  assert (Management.Database.has_relation db "users");
  let db = Management.Database.remove_relation db ~name:"users" in
  assert (not (Management.Database.has_relation db "users"))

let%test_unit "database: update relation" =
  let db = Management.Database.empty ~name:"test_db" in
  let relation = Relation.make
    ~hash:(Some "hash1")
    ~name:"users"
    ~schema:Schema.empty
    ~tree:(Some Merkle.empty)
    ~constraints:None
    ~cardinality:(Conventions.Cardinality.Finite 0)
    ~generator:None
    ~membership_criteria:(fun _ -> true)
    ~provenance:Relation.Provenance.Undefined
    ~lineage:(Relation.Lineage.Base "users")
  in
  let db = Management.Database.add_relation db ~relation in
  let old_db_hash = db.hash in
  let updated_relation = { relation with hash = Some "hash2" } in
  let db = Management.Database.update_relation db ~relation:updated_relation in
  assert (Management.Database.get_relation_hash db "users" = Some "hash2");
  assert (db.hash <> old_db_hash);
  assert (List.mem old_db_hash db.history)

let%test_unit "database: get relation names" =
  let db = Management.Database.empty ~name:"test_db" in
  let rel1 = Relation.make
    ~hash:(Some "h1")
    ~name:"users"
    ~schema:Schema.empty
    ~tree:(Some Merkle.empty)
    ~constraints:None
    ~cardinality:(Conventions.Cardinality.Finite 0)
    ~generator:None
    ~membership_criteria:(fun _ -> true)
    ~provenance:Relation.Provenance.Undefined
    ~lineage:(Relation.Lineage.Base "users")
  in
  let rel2 = Relation.make
    ~hash:(Some "h2")
    ~name:"orders"
    ~schema:Schema.empty
    ~tree:(Some Merkle.empty)
    ~constraints:None
    ~cardinality:(Conventions.Cardinality.Finite 0)
    ~generator:None
    ~membership_criteria:(fun _ -> true)
    ~provenance:Relation.Provenance.Undefined
    ~lineage:(Relation.Lineage.Base "orders")
  in
  let db = Management.Database.add_relation db ~relation:rel1 in
  let db = Management.Database.add_relation db ~relation:rel2 in
  let names = Management.Database.get_relation_names db in
  assert (List.length names = 2);
  assert (List.mem "users" names);
  assert (List.mem "orders" names)

(* ============================================================================
   Manipulation Tests (using Memory storage)
   ============================================================================ *)

(* Helper to create storage for tests *)
let with_storage f =
  match Management.Physical.Memory.create () with
  | Error _ -> assert false
  | Ok storage ->
    let result = f storage in
    Management.Physical.Memory.close storage;
    result

let%test_unit "manipulation: create database" =
  with_storage (fun storage ->
    match Manipulation.Memory.create_database storage ~name:"my_db" with
    | Error _ -> assert false
    | Ok db -> assert (db.name = "my_db"))

let%test_unit "manipulation: create relation" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty
      |> Schema.add "name" "string"
      |> Schema.add "age" "integer" in
    match Manipulation.Memory.create_relation storage db ~name:"users" ~schema with
    | Error _ -> assert false
    | Ok (db, relation) ->
      assert (relation.name = "users");
      assert (relation.schema = schema);
      assert (Management.Database.has_relation db "users"))

let%test_unit "manipulation: create relation already exists" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty in
    match Manipulation.Memory.create_relation storage db ~name:"users" ~schema with
    | Error _ -> assert false
    | Ok (db, _) ->
      match Manipulation.Memory.create_relation storage db ~name:"users" ~schema with
      | Error (Manipulation.RelationAlreadyExists _) -> ()
      | _ -> assert false)

let%test_unit "manipulation: retract relation" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty in
    match Manipulation.Memory.create_relation storage db ~name:"users" ~schema with
    | Error _ -> assert false
    | Ok (db, _) ->
      assert (Management.Database.has_relation db "users");
      match Manipulation.Memory.retract_relation storage db ~name:"users" with
      | Error _ -> assert false
      | Ok db ->
        assert (not (Management.Database.has_relation db "users")))

let%test_unit "manipulation: create tuple with storage" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty
      |> Schema.add "name" "string" in
    match Manipulation.Memory.create_relation storage db ~name:"users" ~schema with
    | Error _ -> assert false
    | Ok (db, relation) ->
      let attrs = Tuple.AttributeMap.empty
        |> Tuple.AttributeMap.add "name" { Attribute.value = Obj.magic "Alice" } in
      let tuple : Tuple.materialized = { relation = "users"; attributes = attrs } in
      match Manipulation.Memory.create_tuple storage db relation tuple with
      | Error _ -> assert false
      | Ok (_db, new_relation, _hash) ->
        assert (Manipulation.Memory.tuple_count new_relation = 1))

let%test_unit "manipulation: create and load tuple" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty
      |> Schema.add "value" "integer" in
    match Manipulation.Memory.create_relation storage db ~name:"numbers" ~schema with
    | Error _ -> assert false
    | Ok (db, relation) ->
      let attrs = Tuple.AttributeMap.empty
        |> Tuple.AttributeMap.add "value" { Attribute.value = Obj.magic 42 } in
      let tuple : Tuple.materialized = { relation = "numbers"; attributes = attrs } in
      match Manipulation.Memory.create_tuple storage db relation tuple with
      | Error _ -> assert false
      | Ok (_db, _relation, tuple_hash) ->
        (* Now load the tuple back from storage *)
        match Manipulation.Memory.load_tuple storage tuple_hash with
        | Error _ -> assert false
        | Ok None -> assert false
        | Ok (Some loaded_tuple) ->
          assert (loaded_tuple.relation = "numbers");
          let loaded_value = Tuple.AttributeMap.find "value" loaded_tuple.attributes in
          assert ((Obj.magic loaded_value.Attribute.value : int) = 42))

let%test_unit "manipulation: create multiple tuples with storage" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty
      |> Schema.add "id" "integer" in
    match Manipulation.Memory.create_relation storage db ~name:"items" ~schema with
    | Error _ -> assert false
    | Ok (db, relation) ->
      let make_tuple id =
        let attrs = Tuple.AttributeMap.empty
          |> Tuple.AttributeMap.add "id" { Attribute.value = Obj.magic id } in
        ({ Tuple.relation = "items"; attributes = attrs } : Tuple.materialized)
      in
      let tuples = [make_tuple 1; make_tuple 2; make_tuple 3] in
      match Manipulation.Memory.create_tuples storage db relation tuples with
      | Error _ -> assert false
      | Ok (_db, new_relation, hashes) ->
        assert (Manipulation.Memory.tuple_count new_relation = 3);
        assert (List.length hashes = 3))

let%test_unit "manipulation: load multiple tuples" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty
      |> Schema.add "n" "integer" in
    match Manipulation.Memory.create_relation storage db ~name:"test" ~schema with
    | Error _ -> assert false
    | Ok (db, relation) ->
      let make_tuple n =
        let attrs = Tuple.AttributeMap.empty
          |> Tuple.AttributeMap.add "n" { Attribute.value = Obj.magic n } in
        ({ Tuple.relation = "test"; attributes = attrs } : Tuple.materialized)
      in
      match Manipulation.Memory.create_tuples storage db relation [make_tuple 10; make_tuple 20] with
      | Error _ -> assert false
      | Ok (_db, _relation, hashes) ->
        match Manipulation.Memory.load_tuples storage hashes with
        | Error _ -> assert false
        | Ok loaded ->
          assert (List.length loaded = 2);
          let values = List.map (fun t ->
            let attr = Tuple.AttributeMap.find "n" t.Tuple.attributes in
            (Obj.magic attr.Attribute.value : int)
          ) loaded in
          assert (List.mem 10 values);
          assert (List.mem 20 values))

let%test_unit "manipulation: retract tuple" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty
      |> Schema.add "value" "integer" in
    match Manipulation.Memory.create_relation storage db ~name:"numbers" ~schema with
    | Error _ -> assert false
    | Ok (db, relation) ->
      let attrs = Tuple.AttributeMap.empty
        |> Tuple.AttributeMap.add "value" { Attribute.value = Obj.magic 42 } in
      let tuple : Tuple.materialized = { relation = "numbers"; attributes = attrs } in
      match Manipulation.Memory.create_tuple storage db relation tuple with
      | Error _ -> assert false
      | Ok (db, relation, tuple_hash) ->
        assert (Manipulation.Memory.tuple_count relation = 1);
        match Manipulation.Memory.retract_tuple storage db relation ~tuple_hash with
        | Error _ -> assert false
        | Ok (_db, relation) ->
          assert (Manipulation.Memory.tuple_count relation = 0);
          (* Tuple data still exists in storage (append-only) *)
          match Manipulation.Memory.load_tuple storage tuple_hash with
          | Error _ -> assert false
          | Ok None -> assert false
          | Ok (Some _) -> () (* Data is still there *))

let%test_unit "manipulation: tuple hashes" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty in
    match Manipulation.Memory.create_relation storage db ~name:"test" ~schema with
    | Error _ -> assert false
    | Ok (db, relation) ->
      let make_tuple n =
        let attrs = Tuple.AttributeMap.empty
          |> Tuple.AttributeMap.add "n" { Attribute.value = Obj.magic n } in
        ({ Tuple.relation = "test"; attributes = attrs } : Tuple.materialized)
      in
      match Manipulation.Memory.create_tuples storage db relation [make_tuple 1; make_tuple 2] with
      | Error _ -> assert false
      | Ok (_db, relation, _) ->
        let hashes = Manipulation.Memory.tuple_hashes relation in
        assert (List.length hashes = 2))

let%test_unit "manipulation: clear relation" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty in
    match Manipulation.Memory.create_relation storage db ~name:"test" ~schema with
    | Error _ -> assert false
    | Ok (db, relation) ->
      let make_tuple n =
        let attrs = Tuple.AttributeMap.empty
          |> Tuple.AttributeMap.add "n" { Attribute.value = Obj.magic n } in
        ({ Tuple.relation = "test"; attributes = attrs } : Tuple.materialized)
      in
      match Manipulation.Memory.create_tuples storage db relation [make_tuple 1; make_tuple 2; make_tuple 3] with
      | Error _ -> assert false
      | Ok (db, relation, _) ->
        assert (Manipulation.Memory.tuple_count relation = 3);
        match Manipulation.Memory.clear_relation storage db relation with
        | Error _ -> assert false
        | Ok (_db, relation) ->
          assert (Manipulation.Memory.tuple_count relation = 0))

let%test_unit "manipulation: duplicate tuple rejected" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty
      |> Schema.add "x" "integer" in
    match Manipulation.Memory.create_relation storage db ~name:"test" ~schema with
    | Error _ -> assert false
    | Ok (db, relation) ->
      let attrs = Tuple.AttributeMap.empty
        |> Tuple.AttributeMap.add "x" { Attribute.value = Obj.magic 1 } in
      let tuple : Tuple.materialized = { relation = "test"; attributes = attrs } in
      match Manipulation.Memory.create_tuple storage db relation tuple with
      | Error _ -> assert false
      | Ok (db, relation, _) ->
        (* Try to insert the same tuple again *)
        match Manipulation.Memory.create_tuple storage db relation tuple with
        | Error (Manipulation.DuplicateTuple _) -> ()
        | _ -> assert false)

let%test_unit "manipulation: tuple_exists check" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty in
    match Manipulation.Memory.create_relation storage db ~name:"test" ~schema with
    | Error _ -> assert false
    | Ok (db, relation) ->
      let attrs = Tuple.AttributeMap.empty
        |> Tuple.AttributeMap.add "v" { Attribute.value = Obj.magic 99 } in
      let tuple : Tuple.materialized = { relation = "test"; attributes = attrs } in
      let tuple_hash = Manipulation.hash_tuple tuple in
      assert (not (Manipulation.Memory.tuple_exists relation tuple_hash));
      match Manipulation.Memory.create_tuple storage db relation tuple with
      | Error _ -> assert false
      | Ok (_db, relation, hash) ->
        assert (hash = tuple_hash);
        assert (Manipulation.Memory.tuple_exists relation tuple_hash))

let%test_unit "manipulation: hash_tuple deterministic" =
  let attrs = Tuple.AttributeMap.empty
    |> Tuple.AttributeMap.add "a" { Attribute.value = Obj.magic 1 }
    |> Tuple.AttributeMap.add "b" { Attribute.value = Obj.magic 2 } in
  let tuple : Tuple.materialized = { relation = "test"; attributes = attrs } in
  let hash1 = Manipulation.hash_tuple tuple in
  let hash2 = Manipulation.hash_tuple tuple in
  assert (hash1 = hash2)

let%test_unit "manipulation: different tuples different hashes" =
  let tuple1 : Tuple.materialized = {
    relation = "test";
    attributes = Tuple.AttributeMap.singleton "x" { Attribute.value = Obj.magic 1 }
  } in
  let tuple2 : Tuple.materialized = {
    relation = "test";
    attributes = Tuple.AttributeMap.singleton "x" { Attribute.value = Obj.magic 2 }
  } in
  let hash1 = Manipulation.hash_tuple tuple1 in
  let hash2 = Manipulation.hash_tuple tuple2 in
  assert (hash1 <> hash2)

let%test_unit "manipulation: get_relation from database" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty
      |> Schema.add "id" "integer" in
    match Manipulation.Memory.create_relation storage db ~name:"items" ~schema with
    | Error _ -> assert false
    | Ok (db, _relation) ->
      (* Now get the relation from the database *)
      match Manipulation.Memory.get_relation db ~name:"items" with
      | None -> assert false
      | Some rel ->
        assert (rel.name = "items");
        assert (rel.schema = schema))

(* ============================================================================
   Schema Persistence Tests
   ============================================================================ *)

let%test_unit "schema: persisted and loaded correctly" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty
      |> Schema.add "id" "integer"
      |> Schema.add "name" "string"
      |> Schema.add "email" "string" in
    match Manipulation.Memory.create_relation storage db ~name:"users" ~schema with
    | Error _ -> assert false
    | Ok (db, _relation) ->
      (* Get the relation hash *)
      let rel_hash = Option.get (Management.Database.get_relation_hash db "users") in
      (* Load the relation from storage *)
      match Manipulation.Memory.load_relation storage rel_hash with
      | Error _ -> assert false
      | Ok None -> assert false
      | Ok (Some loaded_rel) ->
        (* Verify schema was persisted and loaded correctly *)
        assert (loaded_rel.schema = schema);
        assert (List.mem ("id", "integer") loaded_rel.schema);
        assert (List.mem ("name", "string") loaded_rel.schema);
        assert (List.mem ("email", "string") loaded_rel.schema))

(* ============================================================================
   System Catalog Tests
   ============================================================================ *)

let%test_unit "catalog: create_database seeds 6 catalog relations" =
  with_storage (fun storage ->
    match Manipulation.Memory.create_database storage ~name:"test" with
    | Error _ -> assert false
    | Ok db ->
      let catalog_names = Prelude.Catalog.catalog_relation_names in
      List.iter (fun name ->
        assert (Management.Database.has_relation db name)
      ) catalog_names)

let%test_unit "catalog: sakura:relation contains all 6 catalog names" =
  with_storage (fun storage ->
    match Manipulation.Memory.create_database storage ~name:"test" with
    | Error _ -> assert false
    | Ok db ->
      let rel = match Manipulation.Memory.get_relation db ~name:"sakura:relation" with
        | None -> assert false
        | Some r -> r
      in
      assert (Manipulation.Memory.tuple_count rel = 6))

let%test_unit "catalog: sakura:on contains insert, update, delete" =
  with_storage (fun storage ->
    match Manipulation.Memory.create_database storage ~name:"test" with
    | Error _ -> assert false
    | Ok db ->
      let on_rel = match Manipulation.Memory.get_relation db ~name:"sakura:on" with
        | None -> assert false
        | Some r -> r
      in
      assert (Manipulation.Memory.tuple_count on_rel = 3))

let%test_unit "catalog: sakura:timing contains immediate, deferred" =
  with_storage (fun storage ->
    match Manipulation.Memory.create_database storage ~name:"test" with
    | Error _ -> assert false
    | Ok db ->
      let timing_rel = match Manipulation.Memory.get_relation db ~name:"sakura:timing" with
        | None -> assert false
        | Some r -> r
      in
      assert (Manipulation.Memory.tuple_count timing_rel = 2))

let%test_unit "catalog: sakura:domain seeded with 4 prelude domains" =
  with_storage (fun storage ->
    match Manipulation.Memory.create_database storage ~name:"test" with
    | Error _ -> assert false
    | Ok db ->
      let dom_rel = match Manipulation.Memory.get_relation db ~name:"sakura:domain" with
        | None -> assert false
        | Some r -> r
      in
      assert (Manipulation.Memory.tuple_count dom_rel = 4))

let%test_unit "catalog: create_relation updates sakura:relation" =
  with_storage (fun storage ->
    match Manipulation.Memory.create_database storage ~name:"test" with
    | Error _ -> assert false
    | Ok db ->
      let schema = Schema.empty |> Schema.add "id" "natural" in
      let db = match Manipulation.Memory.create_relation storage db ~name:"employees" ~schema with
        | Error _ -> assert false | Ok (db, _) -> db
      in
      let rel = match Manipulation.Memory.get_relation db ~name:"sakura:relation" with
        | None -> assert false | Some r -> r
      in
      (* 6 catalog relations + 1 user relation *)
      assert (Manipulation.Memory.tuple_count rel = 7))

let%test_unit "catalog: create_relation updates sakura:attribute" =
  with_storage (fun storage ->
    match Manipulation.Memory.create_database storage ~name:"test" with
    | Error _ -> assert false
    | Ok db ->
      let schema = Schema.empty |> Schema.add "id" "natural" in
      let db = match Manipulation.Memory.create_relation storage db ~name:"employees" ~schema with
        | Error _ -> assert false | Ok (db, _) -> db
      in
      let attr_rel = match Manipulation.Memory.get_relation db ~name:"sakura:attribute" with
        | None -> assert false | Some r -> r
      in
      (* catalog attribute tuples (sum of schema sizes of all 6 catalog relations) + 1 for employees *)
      (* sakura:relation: 1, sakura:domain: 1, sakura:attribute: 3, sakura:constraint: 2,
         sakura:on: 1, sakura:timing: 1  => 9 catalog attrs + 1 for employees = 10 *)
      let count = Manipulation.Memory.tuple_count attr_rel in
      assert (count = 10))

let%test_unit "catalog: retract_relation removes from sakura:relation" =
  with_storage (fun storage ->
    match Manipulation.Memory.create_database storage ~name:"test" with
    | Error _ -> assert false
    | Ok db ->
      let schema = Schema.empty |> Schema.add "id" "natural" in
      let db = match Manipulation.Memory.create_relation storage db ~name:"employees" ~schema with
        | Error _ -> assert false | Ok (db, _) -> db
      in
      let db = match Manipulation.Memory.retract_relation storage db ~name:"employees" with
        | Error _ -> assert false | Ok db -> db
      in
      let rel = match Manipulation.Memory.get_relation db ~name:"sakura:relation" with
        | None -> assert false | Some r -> r
      in
      (* Back to 6 catalog relations *)
      assert (Manipulation.Memory.tuple_count rel = 6))

let%test_unit "catalog: register_constraint inserts into sakura:constraint" =
  with_storage (fun storage ->
    match Manipulation.Memory.create_database storage ~name:"test" with
    | Error _ -> assert false
    | Ok db ->
      let schema = Schema.empty |> Schema.add "id" "natural" in
      let db = match Manipulation.Memory.create_relation storage db ~name:"orders" ~schema with
        | Error _ -> assert false | Ok (db, _) -> db
      in
      let db = match Manipulation.Memory.register_constraint storage db
          ~constraint_name:"orders_id_positive"
          ~relation_name:"orders" with
        | Error _ -> assert false | Ok db -> db
      in
      let con_rel = match Manipulation.Memory.get_relation db ~name:"sakura:constraint" with
        | None -> assert false | Some r -> r
      in
      assert (Manipulation.Memory.tuple_count con_rel = 1))

(* ============================================================================
   Integration Tests
   ============================================================================ *)

let%test_unit "integration: full workflow with storage" =
  with_storage (fun storage ->
    (* Create database *)
    let db = match Manipulation.Memory.create_database storage ~name:"shop" with
      | Error _ -> assert false | Ok db -> db
    in

    (* Create products relation *)
    let schema = Schema.empty
      |> Schema.add "id" "integer"
      |> Schema.add "name" "string"
      |> Schema.add "price" "integer" in

    let (db, products) = match Manipulation.Memory.create_relation storage db ~name:"products" ~schema with
      | Error _ -> assert false
      | Ok x -> x
    in

    (* Insert some products *)
    let make_product id name price =
      let attrs = Tuple.AttributeMap.empty
        |> Tuple.AttributeMap.add "id" { Attribute.value = Obj.magic id }
        |> Tuple.AttributeMap.add "name" { Attribute.value = Obj.magic name }
        |> Tuple.AttributeMap.add "price" { Attribute.value = Obj.magic price } in
      ({ Tuple.relation = "products"; attributes = attrs } : Tuple.materialized)
    in

    let tuples = [
      make_product 1 "Apple" 100;
      make_product 2 "Banana" 50;
      make_product 3 "Cherry" 200;
    ] in

    let (db, products, hashes) = match Manipulation.Memory.create_tuples storage db products tuples with
      | Error _ -> assert false
      | Ok x -> x
    in

    assert (Manipulation.Memory.tuple_count products = 3);
    assert (List.length hashes = 3);

    (* Verify we can load all tuples back *)
    (match Manipulation.Memory.load_tuples storage hashes with
     | Error _ -> assert false
     | Ok loaded -> assert (List.length loaded = 3));

    (* Delete one product *)
    let banana_hash = Manipulation.hash_tuple (make_product 2 "Banana" 50) in
    let (_db, products) = match Manipulation.Memory.retract_tuple storage db products ~tuple_hash:banana_hash with
      | Error _ -> assert false
      | Ok x -> x
    in

    assert (Manipulation.Memory.tuple_count products = 2);

    (* Verify remaining hashes *)
    let remaining_hashes = Manipulation.Memory.tuple_hashes products in
    assert (List.length remaining_hashes = 2);
    assert (not (List.mem banana_hash remaining_hashes));

    (* But banana data is still in storage (append-only) *)
    match Manipulation.Memory.load_tuple storage banana_hash with
    | Error _ -> assert false
    | Ok None -> assert false
    | Ok (Some loaded) ->
      let name_attr = Tuple.AttributeMap.find "name" loaded.attributes in
      assert ((Obj.magic name_attr.Attribute.value : string) = "Banana"))

let%test_unit "integration: database history tracking" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"versioned" with
      | Error _ -> assert false | Ok db -> db
    in
    let initial_hash = db.hash in

    let schema = Schema.empty in
    let (db, _) = match Manipulation.Memory.create_relation storage db ~name:"rel1" ~schema with
      | Error _ -> assert false
      | Ok x -> x
    in

    (* After first change, initial hash should be in history *)
    assert (db.hash <> initial_hash);

    let hash_after_rel1 = db.hash in

    let (db, _) = match Manipulation.Memory.create_relation storage db ~name:"rel2" ~schema with
      | Error _ -> assert false
      | Ok x -> x
    in

    (* After second change, previous hash should be in history *)
    assert (db.hash <> hash_after_rel1);
    assert (List.mem hash_after_rel1 db.history))

let%test_unit "integration: hash bubbles up correctly" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty in

    let (db, relation) = match Manipulation.Memory.create_relation storage db ~name:"items" ~schema with
      | Error _ -> assert false
      | Ok x -> x
    in

    let db_hash_before = db.hash in
    let rel_hash_before = relation.hash in

    (* Insert a tuple *)
    let attrs = Tuple.AttributeMap.empty
      |> Tuple.AttributeMap.add "x" { Attribute.value = Obj.magic 1 } in
    let tuple : Tuple.materialized = { relation = "items"; attributes = attrs } in

    let (db, relation, _) = match Manipulation.Memory.create_tuple storage db relation tuple with
      | Error _ -> assert false
      | Ok x -> x
    in

    (* Relation hash should change *)
    assert (relation.hash <> rel_hash_before);

    (* Database hash should change *)
    assert (db.hash <> db_hash_before);

    (* Old database hash should be in history *)
    assert (List.mem db_hash_before db.history);

    (* Database's merkle tree should contain the new relation hash *)
    match relation.hash with
    | None -> assert false
    | Some rel_hash ->
      assert (Merkle.member rel_hash db.tree))

(* ============================================================================
   Time-Travel / Branching Tests
   ============================================================================ *)

let%test_unit "branching: load database from historical hash" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty in

    (* Create relation and add a tuple *)
    let (db, relation) = match Manipulation.Memory.create_relation storage db ~name:"items" ~schema with
      | Error _ -> assert false
      | Ok x -> x
    in

    let attrs = Tuple.AttributeMap.empty
      |> Tuple.AttributeMap.add "x" { Attribute.value = Obj.magic 1 } in
    let tuple : Tuple.materialized = { relation = "items"; attributes = attrs } in

    let (db, _relation, _) = match Manipulation.Memory.create_tuple storage db relation tuple with
      | Error _ -> assert false
      | Ok x -> x
    in

    (* Capture hash H1 - database with 1 tuple *)
    let h1 = db.hash in

    (* Add another tuple *)
    let attrs2 = Tuple.AttributeMap.empty
      |> Tuple.AttributeMap.add "x" { Attribute.value = Obj.magic 2 } in
    let tuple2 : Tuple.materialized = { relation = "items"; attributes = attrs2 } in

    (* Get relation from database - it's now integrated! *)
    let relation = match Manipulation.Memory.get_relation db ~name:"items" with
      | None -> assert false
      | Some r -> r
    in

    let (db, _relation, _) = match Manipulation.Memory.create_tuple storage db relation tuple2 with
      | Error _ -> assert false
      | Ok x -> x
    in

    let h2 = db.hash in
    assert (h1 <> h2);

    (* Now load H1 from storage - should get database state with 1 tuple *)
    match Manipulation.Memory.load_database storage h1 with
    | Error _ -> assert false
    | Ok None -> assert false
    | Ok (Some loaded_db) ->
      assert (loaded_db.hash = h1);
      assert (loaded_db.name = "test");
      (* The relation is now loaded directly in the database *)
      match Manipulation.Memory.get_relation loaded_db ~name:"items" with
      | None -> assert false
      | Some loaded_rel ->
        (* Should have 1 tuple *)
        assert (Manipulation.Memory.tuple_count loaded_rel = 1))

let%test_unit "branching: branch from historical state" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty in

    (* Create relation *)
    let (db, relation) = match Manipulation.Memory.create_relation storage db ~name:"data" ~schema with
      | Error _ -> assert false
      | Ok x -> x
    in

    (* Add tuple A *)
    let tuple_a : Tuple.materialized = {
      relation = "data";
      attributes = Tuple.AttributeMap.singleton "v" { Attribute.value = Obj.magic "A" }
    } in
    let (db, relation, _) = match Manipulation.Memory.create_tuple storage db relation tuple_a with
      | Error _ -> assert false
      | Ok x -> x
    in

    (* Capture H1 - state with tuple A *)
    let h1 = db.hash in

    (* Continue on main branch: add tuple B *)
    let tuple_b : Tuple.materialized = {
      relation = "data";
      attributes = Tuple.AttributeMap.singleton "v" { Attribute.value = Obj.magic "B" }
    } in
    let (db_main, relation_main, _) = match Manipulation.Memory.create_tuple storage db relation tuple_b with
      | Error _ -> assert false
      | Ok x -> x
    in
    let h2 = db_main.hash in

    (* Now branch from H1 and add tuple C instead *)
    let db_branch = match Manipulation.Memory.load_database storage h1 with
      | Error _ -> assert false
      | Ok None -> assert false
      | Ok (Some d) -> d
    in
    (* Relation is now loaded directly in the database *)
    let relation_branch = match Manipulation.Memory.get_relation db_branch ~name:"data" with
      | None -> assert false
      | Some r -> r
    in

    let tuple_c : Tuple.materialized = {
      relation = "data";
      attributes = Tuple.AttributeMap.singleton "v" { Attribute.value = Obj.magic "C" }
    } in
    let (db_branch, _relation_branch, _) = match Manipulation.Memory.create_tuple storage db_branch relation_branch tuple_c with
      | Error _ -> assert false
      | Ok x -> x
    in
    let h3 = db_branch.hash in

    (* Verify: H1, H2, H3 are all different *)
    assert (h1 <> h2);
    assert (h1 <> h3);
    assert (h2 <> h3);

    (* H2 (main branch) has tuples A and B *)
    assert (Manipulation.Memory.tuple_count relation_main = 2);

    (* H1 is still loadable and unchanged *)
    match Manipulation.Memory.load_database storage h1 with
    | Error _ -> assert false
    | Ok None -> assert false
    | Ok (Some loaded_h1) ->
      match Manipulation.Memory.get_relation loaded_h1 ~name:"data" with
      | None -> assert false
      | Some r ->
        assert (Manipulation.Memory.tuple_count r = 1))

let%test_unit "branching: full reconstruction from hash" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"shop" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty in

    (* Create relation *)
    let (db, relation) = match Manipulation.Memory.create_relation storage db ~name:"products" ~schema with
      | Error _ -> assert false
      | Ok x -> x
    in

    (* Add products *)
    let make_product name =
      { Tuple.relation = "products";
        attributes = Tuple.AttributeMap.singleton "name" { Attribute.value = Obj.magic name } }
    in
    let (db, _relation, _) = match Manipulation.Memory.create_tuples storage db relation
        [make_product "Apple"; make_product "Banana"; make_product "Cherry"] with
      | Error _ -> assert false
      | Ok x -> x
    in

    let saved_hash = db.hash in

    (* Now reconstruct everything from just the hash *)
    match Manipulation.Memory.load_database storage saved_hash with
    | Error _ -> assert false
    | Ok None -> assert false
    | Ok (Some loaded_db) ->
      (* Relation is now loaded directly in the database *)
      match Manipulation.Memory.get_relation loaded_db ~name:"products" with
      | None -> assert false
      | Some loaded_rel ->
        (* Get tuple hashes from relation *)
        let tuple_hashes = Manipulation.Memory.tuple_hashes loaded_rel in
        assert (List.length tuple_hashes = 3);

        (* Load all tuples *)
        match Manipulation.Memory.load_tuples storage tuple_hashes with
        | Error _ -> assert false
        | Ok tuples ->
          assert (List.length tuples = 3);
          let names = List.map (fun t ->
            let attr = Tuple.AttributeMap.find "name" t.Tuple.attributes in
            (Obj.magic attr.Attribute.value : string)
          ) tuples in
          assert (List.mem "Apple" names);
          assert (List.mem "Banana" names);
          assert (List.mem "Cherry" names))

(* ============================================================================
   Algebra Tests
   ============================================================================ *)

(* Helper: build a materialized tuple with int attributes *)
let make_int_tuple relation pairs =
  let attributes =
    List.fold_left (fun acc (k, v) ->
      Tuple.AttributeMap.add k { Attribute.value = Obj.repr (v : int) } acc)
      Tuple.AttributeMap.empty pairs
  in
  (Tuple.Materialized { Tuple.relation; attributes } : Tuple.t)

(* Helper: build a stored relation with one integer column and given values *)
let stored_relation_with_ints storage db ~name ~attr values =
  let schema = Schema.empty |> Schema.add attr "integer" in
  let (db, rel) = match Manipulation.Memory.create_relation storage db ~name ~schema with
    | Error _ -> assert false | Ok x -> x
  in
  let tuples = List.map (fun v ->
    let attributes =
      Tuple.AttributeMap.singleton attr { Attribute.value = Obj.repr (v : int) }
    in
    ({ Tuple.relation = name; attributes } : Tuple.materialized)
  ) values in
  let (db, rel, _) = match Manipulation.Memory.create_tuples storage db rel tuples with
    | Error _ -> assert false | Ok x -> x
  in
  (db, rel)

let%test_unit "algebra: const_relation single tuple" =
  with_storage (fun storage ->
    let rel = Algebra.Memory.const_relation
        [ "x", Obj.repr (42 : int);
          "y", Obj.repr (99 : int) ] in
    match Algebra.Memory.materialize storage rel with
    | Error _    -> assert false
    | Ok [tup]   ->
      let x: int = Obj.obj (Tuple.AttributeMap.find "x" tup.Tuple.attributes).Attribute.value in
      assert (x = 42)
    | Ok _       -> assert false)

let%test_unit "algebra: select_fn with predicate" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let (_, rel) = stored_relation_with_ints storage db ~name:"nums" ~attr:"n" [1;2;3;4;5] in
    let predicate = function
      | Tuple.Materialized t ->
        let v: int = Obj.obj (Tuple.AttributeMap.find "n" t.Tuple.attributes).Attribute.value in
        v > 3
      | _ -> false
    in
    match Algebra.Memory.select_fn storage predicate rel with
    | Error _   -> assert false
    | Ok result ->
      match Algebra.Memory.materialize storage result with
      | Error _ -> assert false
      | Ok rows ->
        assert (List.length rows = 2);
        List.iter (fun t ->
          let v: int = Obj.obj (Tuple.AttributeMap.find "n" t.Tuple.attributes).Attribute.value in
          assert (v > 3)) rows)

let%test_unit "algebra: project restricts schema" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let schema = Schema.empty |> Schema.add "a" "integer" |> Schema.add "b" "integer" in
    let (db, rel) = match Manipulation.Memory.create_relation storage db ~name:"ab" ~schema with
      | Error _ -> assert false | Ok x -> x
    in
    let attrs = Tuple.AttributeMap.empty
      |> Tuple.AttributeMap.add "a" { Attribute.value = Obj.repr (1 : int) }
      |> Tuple.AttributeMap.add "b" { Attribute.value = Obj.repr (2 : int) } in
    let (_, rel, _) = match Manipulation.Memory.create_tuple storage db rel
        { Tuple.relation = "ab"; attributes = attrs } with
      | Error _ -> assert false | Ok x -> x
    in
    match Algebra.Memory.project storage ["a"] rel with
    | Error _ -> assert false
    | Ok projected ->
      match Algebra.Memory.materialize storage projected with
      | Error _ -> assert false
      | Ok [t] ->
        assert (Tuple.AttributeMap.mem "a" t.Tuple.attributes);
        assert (not (Tuple.AttributeMap.mem "b" t.Tuple.attributes))
      | Ok _ -> assert false)

let%test_unit "algebra: rename changes attr names" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let (_, rel) = stored_relation_with_ints storage db ~name:"r" ~attr:"x" [7] in
    match Algebra.Memory.rename storage [("x", "y")] rel with
    | Error _ -> assert false
    | Ok renamed ->
      match Algebra.Memory.materialize storage renamed with
      | Error _ -> assert false
      | Ok [t] ->
        assert (not (Tuple.AttributeMap.mem "x" t.Tuple.attributes));
        assert (Tuple.AttributeMap.mem "y" t.Tuple.attributes);
        let v: int = Obj.obj (Tuple.AttributeMap.find "y" t.Tuple.attributes).Attribute.value in
        assert (v = 7)
      | Ok _ -> assert false)

let%test_unit "algebra: equijoin merges matching tuples" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    (* left: {id=1, name="Alice"}, {id=2, name="Bob"} *)
    let schema_l = Schema.empty |> Schema.add "id" "integer" |> Schema.add "name" "string" in
    let (db, left) = match Manipulation.Memory.create_relation storage db ~name:"L" ~schema:schema_l with
      | Error _ -> assert false | Ok x -> x
    in
    let make_left id name =
      let attrs = Tuple.AttributeMap.empty
        |> Tuple.AttributeMap.add "id"   { Attribute.value = Obj.repr (id : int) }
        |> Tuple.AttributeMap.add "name" { Attribute.value = Obj.repr (name : string) } in
      ({ Tuple.relation = "L"; attributes = attrs } : Tuple.materialized)
    in
    let (db, left, _) = match Manipulation.Memory.create_tuples storage db left
        [make_left 1 "Alice"; make_left 2 "Bob"] with
      | Error _ -> assert false | Ok x -> x
    in
    (* right: {id=1, score=100}, {id=3, score=999} *)
    let schema_r = Schema.empty |> Schema.add "id" "integer" |> Schema.add "score" "integer" in
    let (db, right) = match Manipulation.Memory.create_relation storage db ~name:"R" ~schema:schema_r with
      | Error _ -> assert false | Ok x -> x
    in
    let make_right id score =
      let attrs = Tuple.AttributeMap.empty
        |> Tuple.AttributeMap.add "id"    { Attribute.value = Obj.repr (id : int) }
        |> Tuple.AttributeMap.add "score" { Attribute.value = Obj.repr (score : int) } in
      ({ Tuple.relation = "R"; attributes = attrs } : Tuple.materialized)
    in
    let (_, right, _) = match Manipulation.Memory.create_tuples storage db right
        [make_right 1 100; make_right 3 999] with
      | Error _ -> assert false | Ok x -> x
    in
    match Algebra.Memory.equijoin storage ["id"] left right with
    | Error _  -> assert false
    | Ok joined ->
      match Algebra.Memory.materialize storage joined with
      | Error _ -> assert false
      | Ok rows ->
        (* Only id=1 matches *)
        assert (List.length rows = 1);
        let t = List.hd rows in
        let score: int = Obj.obj (Tuple.AttributeMap.find "score" t.Tuple.attributes).Attribute.value in
        assert (score = 100))

let%test_unit "algebra: equijoin empty match" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let (db, left) = stored_relation_with_ints storage db ~name:"L" ~attr:"id" [1;2] in
    let (_, right) = stored_relation_with_ints storage db ~name:"R" ~attr:"id" [9;8] in
    match Algebra.Memory.equijoin storage ["id"] left right with
    | Error _  -> assert false
    | Ok joined ->
      match Algebra.Memory.materialize storage joined with
      | Error _ -> assert false
      | Ok rows -> assert (List.length rows = 0))

let%test_unit "algebra: union concatenates streams" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let (db, r1) = stored_relation_with_ints storage db ~name:"A" ~attr:"n" [1;2] in
    let (_, r2) = stored_relation_with_ints storage db ~name:"B" ~attr:"n" [3;4] in
    match Algebra.Memory.union storage r1 r2 with
    | Error _    -> assert false
    | Ok unioned ->
      match Algebra.Memory.materialize storage unioned with
      | Error _ -> assert false
      | Ok rows ->
        assert (List.length rows = 4);
        let vals = List.map (fun t ->
          Obj.obj (Tuple.AttributeMap.find "n" t.Tuple.attributes).Attribute.value
        ) rows in
        assert (List.mem 1 vals);
        assert (List.mem 2 vals);
        assert (List.mem 3 vals);
        assert (List.mem 4 vals))

let%test_unit "algebra: diff removes right from left" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let (db, r1) = stored_relation_with_ints storage db ~name:"A" ~attr:"n" [1;2;3] in
    let (_, r2) = stored_relation_with_ints storage db ~name:"B" ~attr:"n" [2;3] in
    match Algebra.Memory.diff storage r1 r2 with
    | Error _   -> assert false
    | Ok result ->
      match Algebra.Memory.materialize storage result with
      | Error _ -> assert false
      | Ok rows ->
        assert (List.length rows = 1);
        let v: int = Obj.obj (Tuple.AttributeMap.find "n" (List.hd rows).Tuple.attributes).Attribute.value in
        assert (v = 1))

let%test_unit "algebra: take limits output" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let (_, rel) = stored_relation_with_ints storage db ~name:"big" ~attr:"n" [10;20;30;40;50] in
    match Algebra.Memory.take storage 3 rel with
    | Error _   -> assert false
    | Ok result ->
      match Algebra.Memory.materialize storage result with
      | Error _ -> assert false
      | Ok rows -> assert (List.length rows = 3))

(* ============================================================================
   DRL Parser Tests
   ============================================================================ *)

let%test_unit "drl: parse Base" =
  match Drl.Parser.of_string {|(Base "users")|} with
  | Error _              -> assert false
  | Ok (Drl.Ast.Base s)  -> assert (s = "users")
  | Ok _                 -> assert false

let%test_unit "drl: parse Const" =
  match Drl.Parser.of_string {|(Const (("age" (Int 18))))|} with
  | Error _                    -> assert false
  | Ok (Drl.Ast.Const [("age", Drl.Ast.Int 18)]) -> ()
  | Ok _                       -> assert false

let%test_unit "drl: parse Join" =
  match Drl.Parser.of_string {|(Join (id) (Base "L") (Base "R"))|} with
  | Error _                             -> assert false
  | Ok (Drl.Ast.Join (["id"], Drl.Ast.Base "L", Drl.Ast.Base "R")) -> ()
  | Ok _                                -> assert false

let%test_unit "drl: parse Select" =
  let s = {|(Select (Const (("age" (Int 18)))) (Base "users"))|} in
  match Drl.Parser.of_string s with
  | Error _ -> assert false
  | Ok (Drl.Ast.Select (Drl.Ast.Const _, Drl.Ast.Base "users")) -> ()
  | Ok _    -> assert false

(* ============================================================================
   DRL Executor end-to-end Tests
   ============================================================================ *)

let%test_unit "drl: execute Base" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    let (db, _) = stored_relation_with_ints storage db ~name:"items" ~attr:"v" [10; 20] in
    match Drl.Parser.of_string {|(Base "items")|} with
    | Error _   -> assert false
    | Ok query  ->
      match Drl.Executor.Memory.execute storage db query with
      | Error _   -> assert false
      | Ok rel    ->
        match Algebra.Memory.materialize storage rel with
        | Error _ -> assert false
        | Ok rows -> assert (List.length rows = 2))

let%test_unit "drl: execute Select+Const" =
  with_storage (fun storage ->
    let db = match Manipulation.Memory.create_database storage ~name:"test_db" with
      | Error _ -> assert false | Ok db -> db
    in
    (* users: {age=18, name="Alice"}, {age=25, name="Bob"} *)
    let schema = Schema.empty |> Schema.add "age" "integer" |> Schema.add "name" "string" in
    let (db, users) = match Manipulation.Memory.create_relation storage db ~name:"users" ~schema with
      | Error _ -> assert false | Ok x -> x
    in
    let make_user age name =
      let attrs = Tuple.AttributeMap.empty
        |> Tuple.AttributeMap.add "age"  { Attribute.value = Obj.repr (age : int) }
        |> Tuple.AttributeMap.add "name" { Attribute.value = Obj.repr (name : string) } in
      ({ Tuple.relation = "users"; attributes = attrs } : Tuple.materialized)
    in
    let (db, _, _) = match Manipulation.Memory.create_tuples storage db users
        [make_user 18 "Alice"; make_user 25 "Bob"] with
      | Error _ -> assert false | Ok x -> x
    in
    (* Query: select users where age = 18 *)
    let s = {|(Select (Const (("age" (Int 18)))) (Base "users"))|} in
    match Drl.Parser.of_string s with
    | Error _  -> assert false
    | Ok query ->
      match Drl.Executor.Memory.execute storage db query with
      | Error _  -> assert false
      | Ok rel   ->
        match Algebra.Memory.materialize storage rel with
        | Error _ -> assert false
        | Ok rows ->
          assert (List.length rows = 1);
          let name_v: string =
            Obj.obj (Tuple.AttributeMap.find "name" (List.hd rows).Tuple.attributes).Attribute.value
          in
          assert (name_v = "Alice"))
