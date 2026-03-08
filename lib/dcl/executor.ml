module Make (Storage : Management.Physical.S) = struct
  module Ops = Manipulation.Make(Storage)

  type error =
    | ManipulationError of Manipulation.error
    | ConversionError of string

  let wrap_manip = Result.map_error (fun e -> ManipulationError e)

  (** Convert a DCL binding_expr to a runtime Constraint.binding_expr *)
  let convert_binding_expr : Ast.binding_expr -> Constraint.binding_expr = function
    | Ast.Var name -> Constraint.Var name
    | Ast.Const value -> Constraint.Const (Drl.Ast.value_to_abstract value)

  (** Convert a list of (target_attr, binding_expr) pairs to a Constraint.binding (BindingMap) *)
  let convert_binding (pairs : (string * Ast.binding_expr) list) : Constraint.binding =
    List.fold_left
      (fun acc (key, expr) ->
        Constraint.BindingMap.add key (convert_binding_expr expr) acc)
      Constraint.BindingMap.empty
      pairs

  (** Convert a DCL constraint_body AST to a runtime Constraint.t *)
  let rec convert_body : Ast.constraint_body -> Constraint.t = function
    | Ast.MemberOf { target; binding } ->
      Constraint.MemberOf { target; binding = convert_binding binding }
    | Ast.Not { body; universe } ->
      Constraint.Not { body = convert_body body; universe }
    | Ast.And bodies ->
      Constraint.And (List.map convert_body bodies)
    | Ast.Or bodies ->
      Constraint.Or (List.map convert_body bodies)
    | Ast.Exists { variable; quantifier; body } ->
      Constraint.Exists { variable; quantifier; body = convert_body body }
    | Ast.Forall { variable; quantifier; body } ->
      Constraint.Forall { variable; quantifier; body = convert_body body }

  let execute
      (storage : Storage.t)
      (db : Management.Database.t)
      (stmt : Ast.statement)
    : (Management.Database.t, error) result =
    match stmt with
    | Ast.RegisterConstraint { constraint_name; relation_name; body } ->
      (* TODO: Additional constraint operations (drop constraint, list constraints,
         validate existing tuples against new constraint) are not supported via DCL.
         Use the OCaml API for these advanced operations. *)
      let runtime_body = convert_body body in
      Ops.register_constraint storage db
        ~constraint_name ~relation_name ~body:runtime_body
      |> wrap_manip
end

module Memory = Make(Management.Physical.Memory)
