module Make (Storage : Management.Physical.S) = struct
  module Ops = Manipulation.Make(Storage)

  type error =
    | ParseError        of string
    | ManipulationError of Manipulation.error
    | RelationNotFound  of string

  let sexp_of_error = function
    | ParseError s        -> Sexplib.Sexp.(List [Atom "parse-error";        Atom s])
    | ManipulationError e -> Manipulation.sexp_of_error e
    | RelationNotFound s  -> Sexplib.Sexp.(List [Atom "relation-not-found"; Atom s])

  let wrap_manip r = Result.map_error (fun e -> ManipulationError e) r

  let convert_cardinality : Ast.cardinality_spec -> Conventions.Cardinality.t = function
    | Ast.Finite n -> Conventions.Cardinality.Finite n
    | Ast.AlephZero -> Conventions.Cardinality.AlephZero
    | Ast.Continuum -> Conventions.Cardinality.Continuum
    | Ast.ConstrainedFinite -> Conventions.Cardinality.ConstrainedFinite

  let execute
      (storage : Storage.t)
      (db : Management.Database.t)
      (stmt : Ast.statement)
    : (Management.Database.t * string, error) result =
    match stmt with
    | Ast.CreateDatabase name ->
      (match Ops.create_database storage ~name |> wrap_manip with
       | Ok new_db -> Ok (new_db, "Database created: " ^ name)
       | Error e -> Error e)

    | Ast.CreateRelation { name; schema = schema_pairs } ->
      let schema =
        List.fold_left
          (fun s (attr, dom) -> Schema.add attr dom s)
          Schema.empty
          schema_pairs
      in
      (match Ops.create_relation storage db ~name ~schema |> wrap_manip with
       | Ok (db, _rel) -> Ok (db, "Relation created: " ^ name)
       | Error e -> Error e)

    | Ast.RetractRelation name ->
      (match Ops.retract_relation storage db ~name |> wrap_manip with
       | Ok new_db -> Ok (new_db, "Relation retracted: " ^ name)
       | Error e -> Error e)

    | Ast.ClearRelation name ->
      (match Ops.get_relation db ~name with
       | None -> Error (RelationNotFound name)
       | Some rel ->
         match Ops.clear_relation storage db rel |> wrap_manip with
         | Ok (db, _rel) -> Ok (db, "Relation cleared: " ^ name)
         | Error e -> Error e)

    | Ast.RegisterDomain { name; cardinality } ->
      let domain = Domain.make
        ~name
        ~generator:(fun _ -> Generator.Error "not enumerable via DDL")
        ~membership_criteria:(fun _ -> true)
        ~cardinality:(convert_cardinality cardinality)
        ~compare:Stdlib.compare
      in
      (match Ops.register_domain storage db domain |> wrap_manip with
       | Ok new_db -> Ok (new_db, "Domain registered: " ^ name)
       | Error e -> Error e)
end

module Memory = Make(Management.Physical.Memory)
