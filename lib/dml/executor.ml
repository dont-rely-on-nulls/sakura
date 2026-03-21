module Make (Storage : Management.Physical.S) = struct
  module Ops = Manipulation.Make(Storage)
  module Alg = Algebra.Make(Storage)
  module DrlExec = Drl.Executor.Make(Storage)

  type error =
    | ManipulationError of Manipulation.error
    | RelationNotFound of string
    | ParseError of string

  let wrap_manip r = Result.map_error (fun e -> ManipulationError e) r

  (** Build a materialized tuple from a list of (attr_name, ast_value) pairs *)
  let build_tuple ~relation (attributes : Ast.attr_value list) : Tuple.materialized =
    let attr_map =
      List.fold_left
        (fun acc (name, value) ->
          Tuple.AttributeMap.add name
            { Attribute.value = Drl.Ast.value_to_abstract value }
            acc)
        Tuple.AttributeMap.empty
        attributes
    in
    { Tuple.relation; attributes = attr_map }

  let eval_query storage db query =
    match DrlExec.execute storage db query with
    | Ok rel -> Ok rel
    | Error (DrlExec.RelationNotFound s) -> Error (RelationNotFound s)
    | Error (DrlExec.AlgebraError (Algebra.StorageError s)) -> Error (ParseError ("StorageError: " ^ s))
    | Error (DrlExec.AlgebraError (Algebra.GeneratorError s)) -> Error (ParseError ("GeneratorError: " ^ s))

  let materialize_tuples storage rel =
    match Alg.materialize storage rel with
    | Ok tuples -> Ok tuples
    | Error (Algebra.StorageError s) -> Error (ParseError ("StorageError: " ^ s))
    | Error (Algebra.GeneratorError s) -> Error (ParseError ("GeneratorError: " ^ s))

  let execute
      (storage : Storage.t)
      (db : Management.Database.t)
      (stmt : Ast.statement)
    : (Management.Database.t, error) result =
    match stmt with
    | Ast.InsertTuple { relation; attributes } ->
      (match Ops.get_relation db ~name:relation with
       | None -> Error (RelationNotFound relation)
       | Some rel ->
         let tuple = build_tuple ~relation attributes in
         match Ops.create_tuple storage db rel tuple |> wrap_manip with
         | Ok (db, _rel, _hash) -> Ok db
         | Error e -> Error e)

    | Ast.InsertTuples { relation; tuples } ->
      (match Ops.get_relation db ~name:relation with
       | None -> Error (RelationNotFound relation)
       | Some rel ->
         let tuple_list = List.map (build_tuple ~relation) tuples in
         match Ops.create_tuples storage db rel tuple_list |> wrap_manip with
         | Ok (db, _rel, _hashes) -> Ok db
         | Error e -> Error e)

    | Ast.DeleteTuple { relation; attributes } ->
      (match Ops.get_relation db ~name:relation with
       | None -> Error (RelationNotFound relation)
       | Some rel ->
         let tuple = build_tuple ~relation attributes in
         let tuple_hash = Hashing.hash_tuple tuple in
         match Ops.retract_tuple storage db rel ~tuple_hash |> wrap_manip with
         | Ok (db, _rel) -> Ok db
         | Error e -> Error e)

    | Ast.Assign { target; body } ->
      (match Ops.get_relation db ~name:target with
       | None -> Error (RelationNotFound target)
       | Some _rel ->
         match eval_query storage db body with
         | Error e -> Error e
         | Ok result_rel ->
           match materialize_tuples storage result_rel with
           | Error e -> Error e
           | Ok tuples ->
             (* Clear target, then insert all result tuples *)
             let rel = match Ops.get_relation db ~name:target with
               | None -> assert false | Some r -> r in
             match Ops.clear_relation storage db rel |> wrap_manip with
             | Error e -> Error e
             | Ok (db, _rel) ->
               let rel = match Ops.get_relation db ~name:target with
                 | None -> assert false | Some r -> r in
               let mat_tuples = List.map (fun (t : Tuple.materialized) ->
                 { t with Tuple.relation = target }) tuples in
               match Ops.create_tuples storage db rel mat_tuples |> wrap_manip with
               | Ok (db, _rel, _hashes) -> Ok db
               | Error e -> Error e)

    | Ast.InsertFrom { target; source } ->
      (match Ops.get_relation db ~name:target with
       | None -> Error (RelationNotFound target)
       | Some rel ->
         match eval_query storage db source with
         | Error e -> Error e
         | Ok result_rel ->
           match materialize_tuples storage result_rel with
           | Error e -> Error e
           | Ok tuples ->
             let mat_tuples = List.map (fun (t : Tuple.materialized) ->
               { t with Tuple.relation = target }) tuples in
             match Ops.create_tuples storage db rel mat_tuples |> wrap_manip with
             | Ok (db, _rel, _hashes) -> Ok db
             | Error e -> Error e)

    | Ast.DeleteWhere { target; predicate } ->
      (match Ops.get_relation db ~name:target with
       | None -> Error (RelationNotFound target)
       | Some rel ->
         match eval_query storage db predicate with
         | Error e -> Error e
         | Ok pred_rel ->
           (* Compute semijoin: tuples in target matching predicate *)
           let common = List.filter_map (fun (n, _) ->
             if List.exists (fun (m, _) -> m = n) pred_rel.Relation.schema
             then Some n else None) rel.Relation.schema in
           let target_attrs = List.map fst rel.Relation.schema in
           (match Alg.equijoin storage common rel pred_rel with
            | Error (Algebra.StorageError s) -> Error (ParseError ("StorageError: " ^ s))
            | Error (Algebra.GeneratorError s) -> Error (ParseError ("GeneratorError: " ^ s))
            | Ok joined ->
              match Alg.project storage target_attrs joined with
              | Error (Algebra.StorageError s) -> Error (ParseError ("StorageError: " ^ s))
              | Error (Algebra.GeneratorError s) -> Error (ParseError ("GeneratorError: " ^ s))
              | Ok to_delete ->
                match materialize_tuples storage to_delete with
                | Error e -> Error e
                | Ok tuples ->
                  (* Retract each matching tuple *)
                  List.fold_left (fun acc t ->
                    match acc with
                    | Error e -> Error e
                    | Ok db ->
                      let tuple = { t with Tuple.relation = target } in
                      let tuple_hash = Hashing.hash_tuple tuple in
                      let rel = match Ops.get_relation db ~name:target with
                        | None -> assert false | Some r -> r in
                      match Ops.retract_tuple storage db rel ~tuple_hash |> wrap_manip with
                      | Ok (db, _rel) -> Ok db
                      | Error e -> Error e)
                    (Ok db) tuples))
end

module Memory = Make(Management.Physical.Memory)
