open Sexplib.Std

type attr_value = string * Drl.Ast.value
[@@deriving sexp]

type statement =
  | InsertTuple of { relation: string; attributes: attr_value list }
  | InsertTuples of { relation: string; tuples: attr_value list list }
  | DeleteTuple of { relation: string; attributes: attr_value list }
  | Assign of { target: string; body: Drl.Ast.query }
  | InsertFrom of { target: string; source: Drl.Ast.query }
  | DeleteWhere of { target: string; predicate: Drl.Ast.query }
[@@deriving sexp]
