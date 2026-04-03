val eqb : char list -> char list -> bool

type attr_name = char list
type relation = { rel_name : char list; rel_finite : bool }
type database = (char list * relation) list

val db_lookup : char list -> database -> relation option

type literal = LitInt of int | LitStr of char list | LitBool of bool

type query =
  | Base of char list
  | Const of (attr_name * literal) list
  | Select of query * query
  | Join of attr_name list * query * query
  | Cartesian of query * query
  | Project of attr_name list * query
  | Rename of (attr_name * attr_name) list * query
  | Union of query * query
  | Diff of query * query
  | Take of int * query

val predicted_finite : database -> query -> bool option
