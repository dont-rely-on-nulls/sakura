(** val negb : bool -> bool **)

let negb = function true -> false | false -> true

(** val fst : ('a1 * 'a2) -> 'a1 **)

let fst = function x, _ -> x

(** val snd : ('a1 * 'a2) -> 'a2 **)

let snd = function _, y -> y

(** val app : 'a1 list -> 'a1 list -> 'a1 list **)

let rec app l m = match l with [] -> m | a :: l1 -> a :: app l1 m

(** val map : ('a1 -> 'a2) -> 'a1 list -> 'a2 list **)

let rec map f = function [] -> [] | a :: l0 -> f a :: map f l0

(** val firstn : int -> 'a1 list -> 'a1 list **)

let rec firstn n l =
  (fun fO fS n -> if n = 0 then fO () else fS (n - 1))
    (fun _ -> [])
    (fun n0 -> match l with [] -> [] | a :: l0 -> a :: firstn n0 l0)
    n

(** val flat_map : ('a1 -> 'a2 list) -> 'a1 list -> 'a2 list **)

let rec flat_map f = function [] -> [] | x :: l0 -> app (f x) (flat_map f l0)

(** val existsb : ('a1 -> bool) -> 'a1 list -> bool **)

let rec existsb f = function [] -> false | a :: l0 -> f a || existsb f l0

(** val forallb : ('a1 -> bool) -> 'a1 list -> bool **)

let rec forallb f = function [] -> true | a :: l0 -> f a && forallb f l0

(** val filter : ('a1 -> bool) -> 'a1 list -> 'a1 list **)

let rec filter f = function
  | [] -> []
  | x :: l0 -> if f x then x :: filter f l0 else filter f l0

(** val find : ('a1 -> bool) -> 'a1 list -> 'a1 option **)

let rec find f = function
  | [] -> None
  | x :: tl -> if f x then Some x else find f tl

(** val eqb : char list -> char list -> bool **)

let rec eqb s1 s2 =
  match s1 with
  | [] -> ( match s2 with [] -> true | _ :: _ -> false)
  | c1 :: s1' -> (
      match s2 with
      | [] -> false
      | c2 :: s2' -> if c1 = c2 then eqb s1' s2' else false)

(** val append : char list -> char list -> char list **)

let rec append s1 s2 = match s1 with [] -> s2 | c :: s1' -> c :: append s1' s2

type attr_name = char list
type domain_name = char list
type schema = (attr_name * domain_name) list

(** val has_attr : attr_name -> schema -> bool **)

let has_attr a s = existsb (fun p -> eqb a (fst p)) s

(** val attr_names : schema -> attr_name list **)

let attr_names s = map fst s

(** val filter_schema : attr_name list -> schema -> schema **)

let filter_schema attrs s = filter (fun p -> existsb (eqb (fst p)) attrs) s

(** val merge_schema : schema -> schema -> schema **)

let merge_schema s1 s2 =
  app s1 (filter (fun p -> negb (has_attr (fst p) s1)) s2)

(** val remove_attrs : attr_name list -> schema -> schema **)

let remove_attrs attrs s =
  filter (fun p -> negb (existsb (eqb (fst p)) attrs)) s

(** val rename_in_schema : (attr_name * attr_name) list -> schema -> schema **)

let rename_in_schema renames s =
  map
    (fun p ->
      let a = fst p in
      let d = snd p in
      match find (fun r -> eqb a (fst r)) renames with
      | Some p0 ->
          let _, a' = p0 in
          (a', d)
      | None -> (a, d))
    s

type value (* AXIOM TO BE REALIZED *)

(** val value_eqb : value -> value -> bool **)

let value_eqb = failwith "AXIOM TO BE REALIZED (Sakura.Relation.value_eqb)"

type tuple = (attr_name * value) list

(** val tuple_lookup : attr_name -> tuple -> value option **)

let rec tuple_lookup a = function
  | [] -> None
  | p :: rest ->
      let a', v = p in
      if eqb a a' then Some v else tuple_lookup a rest

(** val project_tuple : attr_name list -> tuple -> tuple **)

let project_tuple attrs t = filter (fun p -> existsb (eqb (fst p)) attrs) t

(** val rename_tuple : (attr_name * attr_name) list -> tuple -> tuple **)

let rename_tuple renames t =
  map
    (fun p ->
      let a = fst p in
      let v = snd p in
      match find (fun r -> eqb a (fst r)) renames with
      | Some p0 ->
          let _, a' = p0 in
          (a', v)
      | None -> (a, v))
    t

(** val merge_tuples : tuple -> tuple -> tuple **)

let merge_tuples t1 t2 =
  app t1 (filter (fun p -> negb (existsb (eqb (fst p)) (map fst t1))) t2)

(** val tuples_agree_on : attr_name list -> tuple -> tuple -> bool **)

let tuples_agree_on attrs t1 t2 =
  forallb
    (fun a ->
      match tuple_lookup a t1 with
      | Some v1 -> (
          match tuple_lookup a t2 with
          | Some v2 -> value_eqb v1 v2
          | None -> false)
      | None -> ( match tuple_lookup a t2 with Some _ -> false | None -> true))
    attrs

type relation = {
  rel_name : char list;
  rel_schema : schema;
  rel_tuples : tuple list;
}

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

(** val predicted_schema : database -> query -> schema option **)

let rec predicted_schema db = function
  | Base name -> (
      match db_lookup name db with Some r -> Some r.rel_schema | None -> None)
  | Const pairs ->
      Some
        (map
           (fun p -> (fst p, [ 'a'; 'b'; 's'; 't'; 'r'; 'a'; 'c'; 't' ]))
           pairs)
  | Select (_, source) -> predicted_schema db source
  | Join (attrs, q1, q2) -> (
      match predicted_schema db q1 with
      | Some s1 -> (
          match predicted_schema db q2 with
          | Some s2 ->
              Some
                (merge_schema s1
                   (remove_attrs (attr_names s1) (remove_attrs attrs s2)))
          | None -> None)
      | None -> None)
  | Cartesian (q1, q2) -> (
      match predicted_schema db q1 with
      | Some s1 -> (
          match predicted_schema db q2 with
          | Some s2 -> Some (merge_schema s1 s2)
          | None -> None)
      | None -> None)
  | Project (attrs, q') -> (
      match predicted_schema db q' with
      | Some s -> Some (filter_schema attrs s)
      | None -> None)
  | Rename (renames, q') -> (
      match predicted_schema db q' with
      | Some s -> Some (rename_in_schema renames s)
      | None -> None)
  | Union (q1, _) -> predicted_schema db q1
  | Diff (q1, _) -> predicted_schema db q1
  | Take (_, q') -> predicted_schema db q'

(** val literal_to_value : literal -> value **)

let literal_to_value =
  failwith "AXIOM TO BE REALIZED (Sakura.Drl.literal_to_value)"

(** val eval_const : (attr_name * literal) list -> relation **)

let eval_const pairs =
  let tuple0 = map (fun p -> (fst p, literal_to_value (snd p))) pairs in
  let sch =
    map (fun p -> (fst p, [ 'a'; 'b'; 's'; 't'; 'r'; 'a'; 'c'; 't' ])) pairs
  in
  {
    rel_name = [ 'c'; 'o'; 'n'; 's'; 't' ];
    rel_schema = sch;
    rel_tuples = tuple0 :: [];
  }

(** val semijoin : relation -> relation -> relation **)

let semijoin filter_rel source =
  let common =
    filter
      (fun a -> has_attr a filter_rel.rel_schema)
      (attr_names source.rel_schema)
  in
  let matches =
   fun t ->
    existsb (fun ft -> tuples_agree_on common t ft) filter_rel.rel_tuples
  in
  {
    rel_name = source.rel_name;
    rel_schema = source.rel_schema;
    rel_tuples = filter matches source.rel_tuples;
  }

(** val equijoin : attr_name list -> relation -> relation -> relation **)

let equijoin attrs left right =
  let joined =
    flat_map
      (fun lt ->
        flat_map
          (fun rt ->
            if tuples_agree_on attrs lt rt then merge_tuples lt rt :: [] else [])
          right.rel_tuples)
      left.rel_tuples
  in
  let new_schema =
    merge_schema left.rel_schema
      (remove_attrs
         (attr_names left.rel_schema)
         (remove_attrs attrs right.rel_schema))
  in
  {
    rel_name =
      append
        [ 'j'; 'o'; 'i'; 'n'; '_' ]
        (append left.rel_name (append ('_' :: []) right.rel_name));
    rel_schema = new_schema;
    rel_tuples = joined;
  }

(** val cartesian : relation -> relation -> relation **)

let cartesian left right =
  let joined =
    flat_map
      (fun lt -> map (fun rt -> merge_tuples lt rt) right.rel_tuples)
      left.rel_tuples
  in
  {
    rel_name =
      append
        [ 'c'; 'a'; 'r'; 't'; '_' ]
        (append left.rel_name (append ('_' :: []) right.rel_name));
    rel_schema = merge_schema left.rel_schema right.rel_schema;
    rel_tuples = joined;
  }

(** val project : attr_name list -> relation -> relation **)

let project attrs r =
  {
    rel_name = append [ 'p'; 'r'; 'o'; 'j'; '_' ] r.rel_name;
    rel_schema = filter_schema attrs r.rel_schema;
    rel_tuples = map (project_tuple attrs) r.rel_tuples;
  }

(** val rename : (attr_name * attr_name) list -> relation -> relation **)

let rename renames r =
  {
    rel_name = append [ 'r'; 'e'; 'n'; 'a'; 'm'; 'e'; '_' ] r.rel_name;
    rel_schema = rename_in_schema renames r.rel_schema;
    rel_tuples = map (rename_tuple renames) r.rel_tuples;
  }

(** val rel_union : relation -> relation -> relation **)

let rel_union r1 r2 =
  {
    rel_name =
      append
        [ 'u'; 'n'; 'i'; 'o'; 'n'; '_' ]
        (append r1.rel_name (append ('_' :: []) r2.rel_name));
    rel_schema = r1.rel_schema;
    rel_tuples = app r1.rel_tuples r2.rel_tuples;
  }

(** val rel_diff : relation -> relation -> relation **)

let rel_diff r1 r2 =
  let in_r2 =
   fun t ->
    existsb
      (fun t2 -> tuples_agree_on (attr_names r1.rel_schema) t t2)
      r2.rel_tuples
  in
  {
    rel_name =
      append
        [ 'd'; 'i'; 'f'; 'f'; '_' ]
        (append r1.rel_name (append ('_' :: []) r2.rel_name));
    rel_schema = r1.rel_schema;
    rel_tuples = filter (fun t -> negb (in_r2 t)) r1.rel_tuples;
  }

(** val take : int -> relation -> relation **)

let take n r =
  {
    rel_name = append [ 't'; 'a'; 'k'; 'e'; '_' ] r.rel_name;
    rel_schema = r.rel_schema;
    rel_tuples = firstn n r.rel_tuples;
  }

(** val eval : database -> query -> relation option **)

let rec eval db = function
  | Base name -> db_lookup name db
  | Const pairs -> Some (eval_const pairs)
  | Select (filter_q, source_q) -> (
      match eval db filter_q with
      | Some filter_rel -> (
          match eval db source_q with
          | Some source_rel -> Some (semijoin filter_rel source_rel)
          | None -> None)
      | None -> None)
  | Join (attrs, q1, q2) -> (
      match eval db q1 with
      | Some r1 -> (
          match eval db q2 with
          | Some r2 -> Some (equijoin attrs r1 r2)
          | None -> None)
      | None -> None)
  | Cartesian (q1, q2) -> (
      match eval db q1 with
      | Some r1 -> (
          match eval db q2 with
          | Some r2 -> Some (cartesian r1 r2)
          | None -> None)
      | None -> None)
  | Project (attrs, q') -> (
      match eval db q' with Some r -> Some (project attrs r) | None -> None)
  | Rename (renames, q') -> (
      match eval db q' with Some r -> Some (rename renames r) | None -> None)
  | Union (q1, q2) -> (
      match eval db q1 with
      | Some r1 -> (
          match eval db q2 with
          | Some r2 -> Some (rel_union r1 r2)
          | None -> None)
      | None -> None)
  | Diff (q1, q2) -> (
      match eval db q1 with
      | Some r1 -> (
          match eval db q2 with
          | Some r2 -> Some (rel_diff r1 r2)
          | None -> None)
      | None -> None)
  | Take (n, q') -> (
      match eval db q' with Some r -> Some (take n r) | None -> None)
