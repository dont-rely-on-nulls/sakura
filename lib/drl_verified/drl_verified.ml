(** val eqb : char list -> char list -> bool **)

let rec eqb s1 s2 =
  match s1 with
  | [] -> ( match s2 with [] -> true | _ :: _ -> false)
  | c1 :: s1' -> (
      match s2 with
      | [] -> false
      | c2 :: s2' -> if c1 = c2 then eqb s1' s2' else false)

type attr_name = char list
type relation = { rel_name : char list; rel_finite : bool }
type database = (char list * relation) list

(** val db_lookup : char list -> database -> relation option **)

let rec db_lookup name = function
  | [] -> None
  | p :: rest ->
      let n, r = p in
      if eqb name n then Some r else db_lookup name rest

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

(** val predicted_finite : database -> query -> bool option **)

let rec predicted_finite db = function
  | Base name -> (
      match db_lookup name db with Some r -> Some r.rel_finite | None -> None)
  | Select (_, source) -> predicted_finite db source
  | Join (_, q1, q2) -> (
      match predicted_finite db q1 with
      | Some f1 -> (
          match predicted_finite db q2 with
          | Some f2 -> Some (f1 && f2)
          | None -> None)
      | None -> None)
  | Cartesian (q1, q2) -> (
      match predicted_finite db q1 with
      | Some f1 -> (
          match predicted_finite db q2 with
          | Some f2 -> Some (f1 && f2)
          | None -> None)
      | None -> None)
  | Project (_, q0) -> predicted_finite db q0
  | Rename (_, q0) -> predicted_finite db q0
  | Union (q1, q2) -> (
      match predicted_finite db q1 with
      | Some f1 -> (
          match predicted_finite db q2 with
          | Some f2 -> Some (f1 && f2)
          | None -> None)
      | None -> None)
  | Diff (q1, _) -> predicted_finite db q1
  | _ -> Some true
