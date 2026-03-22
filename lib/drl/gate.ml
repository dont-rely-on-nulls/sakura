(** Finiteness gate for DRL queries.

    Converts Sakura types to Coq-extracted types and calls the
    verified [predicted_finite] function. Rejects queries whose
    result cannot be statically proven finite. *)

(* -- string -> char list impedance --------------------------------- *)

let string_to_chars (s : string) : char list =
  List.init (String.length s) (String.get s)

(* -- Sakura AST -> extracted AST ----------------------------------- *)

let rec query_of_ast : Ast.query -> Drl_verified.query = function
  | Ast.Base name ->
    Drl_verified.Base (string_to_chars name)
  | Ast.Const pairs ->
    Drl_verified.Const
      (List.map
         (fun (k, v) ->
           (string_to_chars k, value_of_ast_value v))
         pairs)
  | Ast.Select (filter, source) ->
    Drl_verified.Select (query_of_ast filter, query_of_ast source)
  | Ast.Join (attrs, q1, q2) ->
    Drl_verified.Join
      (List.map string_to_chars attrs, query_of_ast q1, query_of_ast q2)
  | Ast.Cartesian (q1, q2) ->
    Drl_verified.Cartesian (query_of_ast q1, query_of_ast q2)
  | Ast.Project (attrs, q) ->
    Drl_verified.Project (List.map string_to_chars attrs, query_of_ast q)
  | Ast.Rename (renames, q) ->
    Drl_verified.Rename
      (List.map
         (fun (o, n) -> (string_to_chars o, string_to_chars n))
         renames,
       query_of_ast q)
  | Ast.Union (q1, q2) ->
    Drl_verified.Union (query_of_ast q1, query_of_ast q2)
  | Ast.Diff (q1, q2) ->
    Drl_verified.Diff (query_of_ast q1, query_of_ast q2)
  | Ast.Take (n, q) ->
    Drl_verified.Take (n, query_of_ast q)

and value_of_ast_value : Ast.value -> Drl_verified.literal = function
  | Ast.Int n   -> Drl_verified.LitInt n
  (* The Coq model has no float literal. This truncation is harmless:
     the finiteness gate inspects only relation names and query structure,
     never literal values. *)
  | Ast.Float f -> Drl_verified.LitInt (int_of_float f)
  | Ast.Str s   -> Drl_verified.LitStr (string_to_chars s)
  | Ast.Bool b  -> Drl_verified.LitBool b

(* -- database snapshot --------------------------------------------- *)

let is_finite (c : Conventions.Cardinality.t) : bool =
  match c with
  | Conventions.Cardinality.Finite _ | Conventions.Cardinality.ConstrainedFinite -> true
  | Conventions.Cardinality.AlephZero | Conventions.Cardinality.Continuum -> false

let database_snapshot (db : Management.Database.t) : Drl_verified.database =
  Management.Database.RelationMap.fold
    (fun name (rel : Relation.t) acc ->
       let entry : Drl_verified.relation =
         { rel_name   = string_to_chars name;
           rel_finite = is_finite rel.cardinality }
       in
       (string_to_chars name, entry) :: acc)
    db.relations
    []

(* -- public interface ---------------------------------------------- *)

let admit (db : Management.Database.t) (ast : Ast.query) : (unit, string) result =
  let vdb = database_snapshot db in
  let vq = query_of_ast ast in
  match Drl_verified.predicted_finite vdb vq with
  | Some true -> Ok ()
  | Some false ->
    Error "query produces potentially infinite result; use Take to bound it"
  | None ->
    Error "referenced relation not found in finiteness check"
