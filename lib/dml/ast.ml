open Sexplib.Std

type attr_value = string * Drl.Ast.value
[@@deriving sexp]

type cardinality_spec =
  | Finite of int
  | AlephZero
  | Continuum
  | ConstrainedFinite
[@@deriving sexp]

type statement =
  | CreateDatabase of string
  | CreateRelation of { name: string; schema: (string * string) list }
  | RetractRelation of string
  | ClearRelation of string
  | RegisterDomain of { name: string; cardinality: cardinality_spec }
  | InsertTuple of { relation: string; attributes: attr_value list }
  | InsertTuples of { relation: string; tuples: attr_value list list }
  | DeleteTuple of { relation: string; attributes: attr_value list }
[@@deriving sexp]
