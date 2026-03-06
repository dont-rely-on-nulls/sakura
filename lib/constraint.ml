type relation_name = string
type t = 
  | MemberOf of relation_name
  | Not of t * relation_name option
  | And of t list
  | Or of t list
  | Exists of {variable: string; quantifier: relation_name; constraint': t}
  | Forall of {variable: string; quantifier: relation_name; constraint': t}
