
val negb : bool -> bool

val fst : ('a1 * 'a2) -> 'a1

val snd : ('a1 * 'a2) -> 'a2

val app : 'a1 list -> 'a1 list -> 'a1 list

val map : ('a1 -> 'a2) -> 'a1 list -> 'a2 list

val firstn : int -> 'a1 list -> 'a1 list

val flat_map : ('a1 -> 'a2 list) -> 'a1 list -> 'a2 list

val existsb : ('a1 -> bool) -> 'a1 list -> bool

val forallb : ('a1 -> bool) -> 'a1 list -> bool

val filter : ('a1 -> bool) -> 'a1 list -> 'a1 list

val find : ('a1 -> bool) -> 'a1 list -> 'a1 option

val eqb : char list -> char list -> bool

val append : char list -> char list -> char list

type attr_name = char list

type domain_name = char list

type schema = (attr_name * domain_name) list

val has_attr : attr_name -> schema -> bool

val attr_names : schema -> attr_name list

val filter_schema : attr_name list -> schema -> schema

val merge_schema : schema -> schema -> schema

val remove_attrs : attr_name list -> schema -> schema

val rename_in_schema : (attr_name * attr_name) list -> schema -> schema

type value = Obj.t

val value_eqb : value -> value -> bool

type tuple = (attr_name * value) list

val tuple_lookup : attr_name -> tuple -> value option

val project_tuple : attr_name list -> tuple -> tuple

val rename_tuple : (attr_name * attr_name) list -> tuple -> tuple

val merge_tuples : tuple -> tuple -> tuple

val tuples_agree_on : attr_name list -> tuple -> tuple -> bool

type relation = { rel_name : char list; rel_schema : schema;
                  rel_tuples : tuple list; rel_finite : bool }

type database = (char list * relation) list

val db_lookup : char list -> database -> relation option

type literal =
| LitInt of int
| LitStr of char list
| LitBool of bool

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

val predicted_schema : database -> query -> schema option

val predicted_finite : database -> query -> bool option

val literal_to_value : literal -> value

val eval_const : (attr_name * literal) list -> relation

val semijoin : relation -> relation -> relation

val equijoin : attr_name list -> relation -> relation -> relation

val cartesian : relation -> relation -> relation

val project : attr_name list -> relation -> relation

val rename : (attr_name * attr_name) list -> relation -> relation

val rel_union : relation -> relation -> relation

val rel_diff : relation -> relation -> relation

val take : int -> relation -> relation

val eval : database -> query -> relation option
