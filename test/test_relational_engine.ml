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
  let db = Manipulation.Memory.create_database ~name:"my_db" in
  assert (db.name = "my_db")

let%test_unit "manipulation: create relation" =
  with_storage (fun storage ->
    let db = Manipulation.Memory.create_database ~name:"test_db" in
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
    let db = Manipulation.Memory.create_database ~name:"test_db" in
    let schema = Schema.empty in
    match Manipulation.Memory.create_relation storage db ~name:"users" ~schema with
    | Error _ -> assert false
    | Ok (db, _) ->
      match Manipulation.Memory.create_relation storage db ~name:"users" ~schema with
      | Error (Manipulation.RelationAlreadyExists _) -> ()
      | _ -> assert false)

let%test_unit "manipulation: retract relation" =
  with_storage (fun storage ->
    let db = Manipulation.Memory.create_database ~name:"test_db" in
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
    let db = Manipulation.Memory.create_database ~name:"test_db" in
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
    let db = Manipulation.Memory.create_database ~name:"test_db" in
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
    let db = Manipulation.Memory.create_database ~name:"test_db" in
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
    let db = Manipulation.Memory.create_database ~name:"test_db" in
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
    let db = Manipulation.Memory.create_database ~name:"test_db" in
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
    let db = Manipulation.Memory.create_database ~name:"test_db" in
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
    let db = Manipulation.Memory.create_database ~name:"test_db" in
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
    let db = Manipulation.Memory.create_database ~name:"test_db" in
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
    let db = Manipulation.Memory.create_database ~name:"test_db" in
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
    let db = Manipulation.Memory.create_database ~name:"test_db" in
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
    let db = Manipulation.Memory.create_database ~name:"test_db" in
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
   Integration Tests
   ============================================================================ *)

let%test_unit "integration: full workflow with storage" =
  with_storage (fun storage ->
    (* Create database *)
    let db = Manipulation.Memory.create_database ~name:"shop" in

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
    let db = Manipulation.Memory.create_database ~name:"versioned" in
    let initial_hash = db.hash in

    let schema = Schema.empty in
    let (db, _) = match Manipulation.Memory.create_relation storage db ~name:"rel1" ~schema with
      | Error _ -> assert false
      | Ok x -> x
    in

    (* After first change, initial hash should be in history *)
    assert (db.hash <> initial_hash);
    (* Note: empty initial hash ("") is not added to history *)

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
    let db = Manipulation.Memory.create_database ~name:"test" in
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
    let db = Manipulation.Memory.create_database ~name:"test" in
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
    let db = Manipulation.Memory.create_database ~name:"test" in
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
    let db = Manipulation.Memory.create_database ~name:"shop" in
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
