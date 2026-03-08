module Make (Storage : Management.Physical.S) = struct
  module Ops = Manipulation.Make(Storage)

  type error =
    | ManipulationError of Manipulation.error
    | RelationNotFound of string
    | ParseError of string

  let wrap_manip r = Result.map_error (fun e -> ManipulationError e) r

  (** Convert a cardinality_spec AST node to a Conventions.Cardinality.t *)
  let convert_cardinality : Ast.cardinality_spec -> Conventions.Cardinality.t = function
    | Ast.Finite n -> Conventions.Cardinality.Finite n
    | Ast.AlephZero -> Conventions.Cardinality.AlephZero
    | Ast.Continuum -> Conventions.Cardinality.Continuum
    | Ast.ConstrainedFinite -> Conventions.Cardinality.ConstrainedFinite

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

  let execute
      (storage : Storage.t)
      (db : Management.Database.t)
      (stmt : Ast.statement)
    : (Management.Database.t, error) result =
    match stmt with
    | Ast.CreateDatabase name ->
      Ops.create_database storage ~name |> wrap_manip

    | Ast.CreateRelation { name; schema = schema_pairs } ->
      let schema =
        List.fold_left
          (fun s (attr, dom) -> Schema.add attr dom s)
          Schema.empty
          schema_pairs
      in
      (match Ops.create_relation storage db ~name ~schema |> wrap_manip with
       | Ok (db, _rel) -> Ok db
       | Error e -> Error e)

    | Ast.RetractRelation name ->
      Ops.retract_relation storage db ~name |> wrap_manip

    | Ast.ClearRelation name ->
      (match Ops.get_relation db ~name with
       | None -> Error (RelationNotFound name)
       | Some rel ->
         match Ops.clear_relation storage db rel |> wrap_manip with
         | Ok (db, _rel) -> Ok db
         | Error e -> Error e)

    | Ast.RegisterDomain { name; cardinality } ->
      (* TODO: Custom generators, membership_criteria, and comparators
         are not supported via DML. Use the OCaml API for full-featured
         domain definitions (e.g., custom enumeration, type-specific
         comparison, or domain-specific validation). *)
      let domain = Domain.make
        ~name
        ~generator:(fun _ -> Generator.Error "not enumerable via DML")
        ~membership_criteria:(fun _ -> true)
        ~cardinality:(convert_cardinality cardinality)
        ~compare:Stdlib.compare
      in
      Ops.register_domain storage db domain |> wrap_manip

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
end

module Memory = Make(Management.Physical.Memory)
