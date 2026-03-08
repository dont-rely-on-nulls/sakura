(** Constraint system for relational membership criteria.

    A relation's intension includes constraints that refine the space of
    admissible tuples. Since constraints are themselves relations, checking
    a constraint = checking membership in that constraint-relation.

    Negation requires an explicit universe relation name, forcing
    closed-world reasoning in first-order logic. *)

type relation_name = string
type attr_name = string

(** Binding expressions resolve against a tuple's attributes *)
type binding_expr =
  | Var of attr_name
  | Const of Conventions.AbstractValue.t

module BindingMap = Map.Make (String)

type binding = binding_expr BindingMap.t

type t =
  | MemberOf of { target : relation_name; binding : binding }
  | Not of { body : t; universe : relation_name }
  | And of t list
  | Or of t list
  | Exists of { variable : attr_name; quantifier : relation_name; body : t }
  | Forall of { variable : attr_name; quantifier : relation_name; body : t }

(* ============================================================================
   Diagnostics
   ============================================================================ *)

type diagnostic =
  | MembershipFailed of {
      target : string;
      bound_values : (attr_name * Conventions.AbstractValue.t) list;
    }
  | RelationNotFound of string
  | UnboundedQuantifier of {
      variable : attr_name;
      quantifier : relation_name;
    }
  | ConstraintFailures of (string * diagnostic) list

(* ============================================================================
   Utility functions
   ============================================================================ *)

(** Collect all [Var] names referenced in a constraint *)
let rec vars_in : t -> attr_name list = function
  | MemberOf { binding; _ } ->
    BindingMap.fold
      (fun _ expr acc ->
        match expr with
        | Var v -> if List.mem v acc then acc else v :: acc
        | Const _ -> acc)
      binding []
  | Not { body; _ } -> vars_in body
  | And cs | Or cs -> List.concat_map vars_in cs |> List.sort_uniq String.compare
  | Exists { variable; body; _ } | Forall { variable; body; _ } ->
    let inner = vars_in body in
    if List.mem variable inner then inner
    else variable :: inner

(** Rename [Var] references according to a rename list *)
let rec rename_vars (renames : (attr_name * attr_name) list) : t -> t = function
  | MemberOf { target; binding } ->
    let binding' =
      BindingMap.fold
        (fun k expr acc ->
          let k' =
            match List.assoc_opt k renames with Some n -> n | None -> k
          in
          let expr' =
            match expr with
            | Var v -> (
              match List.assoc_opt v renames with
              | Some n -> Var n
              | None -> expr)
            | Const _ -> expr
          in
          BindingMap.add k' expr' acc)
        binding BindingMap.empty
    in
    MemberOf { target; binding = binding' }
  | Not { body; universe } ->
    Not { body = rename_vars renames body; universe }
  | And cs -> And (List.map (rename_vars renames) cs)
  | Or cs -> Or (List.map (rename_vars renames) cs)
  | Exists { variable; quantifier; body } ->
    let variable' =
      match List.assoc_opt variable renames with
      | Some n -> n
      | None -> variable
    in
    Exists
      { variable = variable'; quantifier; body = rename_vars renames body }
  | Forall { variable; quantifier; body } ->
    let variable' =
      match List.assoc_opt variable renames with
      | Some n -> n
      | None -> variable
    in
    Forall
      { variable = variable'; quantifier; body = rename_vars renames body }

(** Drop constraints referencing attributes not in the given set.
    Returns [None] if the constraint cannot be kept. *)
let rec filter_by_attrs (attrs : attr_name list) (c : t) : t option =
  let all_present c =
    List.for_all (fun v -> List.mem v attrs) (vars_in c)
  in
  match c with
  | MemberOf _ -> if all_present c then Some c else None
  | Not { body; universe } -> (
    match filter_by_attrs attrs body with
    | Some body' -> Some (Not { body = body'; universe })
    | None -> None)
  | And cs ->
    let kept = List.filter_map (filter_by_attrs attrs) cs in
    if kept = [] then None else Some (And kept)
  | Or cs ->
    (* For Or, all branches must survive *)
    let kept = List.filter_map (filter_by_attrs attrs) cs in
    if List.length kept = List.length cs then Some (Or kept) else None
  | Exists { variable; quantifier; body } -> (
    match filter_by_attrs (variable :: attrs) body with
    | Some body' -> Some (Exists { variable; quantifier; body = body' })
    | None -> None)
  | Forall { variable; quantifier; body } -> (
    match filter_by_attrs (variable :: attrs) body with
    | Some body' -> Some (Forall { variable; quantifier; body = body' })
    | None -> None)

(** AND-merge two named constraint lists *)
let merge (cs1 : (string * t) list) (cs2 : (string * t) list)
    : (string * t) list =
  let combined = cs1 @ cs2 in
  (* Group by name and AND-merge duplicates *)
  let tbl = Hashtbl.create 16 in
  List.iter
    (fun (name, c) ->
      match Hashtbl.find_opt tbl name with
      | None -> Hashtbl.replace tbl name [ c ]
      | Some cs -> Hashtbl.replace tbl name (c :: cs))
    combined;
  Hashtbl.fold
    (fun name cs acc ->
      let merged = match cs with [ c ] -> c | _ -> And cs in
      (name, merged) :: acc)
    tbl []

(* ============================================================================
   Smart constructors
   ============================================================================ *)

let member_of ~target ~binding = MemberOf { target; binding }

let not_ ~universe body = Not { body; universe }

let and_ cs =
  match cs with
  | [ c ] -> c
  | _ -> And cs

let or_ cs =
  match cs with
  | [ c ] -> c
  | _ -> Or cs

let exists ~variable ~quantifier body =
  Exists { variable; quantifier; body }

let forall ~variable ~quantifier body =
  Forall { variable; quantifier; body }

(* ============================================================================
   Evaluation context (no dependency on Relation/Database/Manipulation)
   ============================================================================ *)

type eval_context = {
  check_membership :
    relation_name ->
    (attr_name * Conventions.AbstractValue.t) list ->
    bool;
  iterate_finite :
    relation_name ->
    (attr_name * Conventions.AbstractValue.t) list list option;
}

(* ============================================================================
   Binding resolution
   ============================================================================ *)

(** Resolve binding expressions against a materialized tuple *)
let bind (b : binding) (tuple : Tuple.materialized)
    : (attr_name * Conventions.AbstractValue.t) list =
  BindingMap.fold
    (fun target_attr expr acc ->
      let value =
        match expr with
        | Const v -> Some v
        | Var src_attr -> (
          match Tuple.AttributeMap.find_opt src_attr tuple.attributes with
          | Some attr -> Some attr.Attribute.value
          | None -> None)
      in
      match value with
      | Some v -> (target_attr, v) :: acc
      | None -> acc)
    b []

(* ============================================================================
   Constraint evaluation
   ============================================================================ *)

(** Evaluate a constraint against a tuple in the given context.
    Returns [Ok true] if satisfied, [Ok false] should not occur —
    failures return [Error diagnostic]. *)
let rec evaluate (ctx : eval_context) (tuple : Tuple.materialized) (c : t)
    : (bool, diagnostic) result =
  match c with
  | MemberOf { target; binding } ->
    let bound_values = bind binding tuple in
    if ctx.check_membership target bound_values then Ok true
    else Error (MembershipFailed { target; bound_values })
  | Not { body; universe = _ } ->
    (* The universe is a declarative annotation for closed-world reasoning.
       At runtime, we simply negate the body. The universe ensures logical
       soundness at the schema level, not as a runtime tuple-existence check
       (the tuple being validated hasn't been stored yet). *)
    (match evaluate ctx tuple body with
     | Ok true -> Ok false
     | Ok false -> Ok true
     | Error (MembershipFailed _) -> Ok true
     | Error d -> Error d)
  | And cs -> evaluate_and ctx tuple cs
  | Or cs -> evaluate_or ctx tuple cs
  | Exists { variable; quantifier; body } -> (
    match ctx.iterate_finite quantifier with
    | None ->
      Error (UnboundedQuantifier { variable; quantifier })
    | Some rows ->
      let rec check = function
        | [] -> Ok false
        | row :: rest ->
          (* Bind the quantifier variable into scope *)
          let extended_tuple = extend_tuple tuple variable row in
          (match evaluate ctx extended_tuple body with
           | Ok true -> Ok true
           | Ok false -> check rest
           | Error _ -> check rest)
      in
      check rows)
  | Forall { variable; quantifier; body } -> (
    match ctx.iterate_finite quantifier with
    | None ->
      Error (UnboundedQuantifier { variable; quantifier })
    | Some rows ->
      let rec check = function
        | [] -> Ok true
        | row :: rest ->
          let extended_tuple = extend_tuple tuple variable row in
          (match evaluate ctx extended_tuple body with
           | Ok true -> check rest
           | Ok false -> Ok false
           | Error (MembershipFailed _) -> Ok false
           | Error d -> Error d)
      in
      check rows)

and evaluate_and ctx tuple = function
  | [] -> Ok true
  | c :: rest -> (
    match evaluate ctx tuple c with
    | Ok true -> evaluate_and ctx tuple rest
    | Ok false -> Ok false
    | Error d -> Error d)

and evaluate_or ctx tuple = function
  | [] -> Ok false
  | c :: rest -> (
    match evaluate ctx tuple c with
    | Ok true -> Ok true
    | Ok false -> evaluate_or ctx tuple rest
    | Error _ -> evaluate_or ctx tuple rest)

(** Extend a tuple with bindings from a quantifier row.
    Each row entry [(attr, value)] gets added with [variable] as prefix
    if the attr name doesn't match an existing attribute. *)
and extend_tuple (tuple : Tuple.materialized)
    (_variable : attr_name)
    (row : (attr_name * Conventions.AbstractValue.t) list)
    : Tuple.materialized =
  let extra_attrs =
    List.fold_left
      (fun acc (k, v) ->
        Tuple.AttributeMap.add k { Attribute.value = v } acc)
      tuple.attributes row
  in
  { tuple with attributes = extra_attrs }

(** Evaluate a list of named constraints, collecting all failures *)
let evaluate_named (ctx : eval_context) (tuple : Tuple.materialized)
    (named : (string * t) list) : (bool, diagnostic) result =
  let failures =
    List.filter_map
      (fun (name, c) ->
        match evaluate ctx tuple c with
        | Ok true -> None
        | Ok false ->
          Some (name, MembershipFailed { target = name; bound_values = [] })
        | Error d -> Some (name, d))
      named
  in
  match failures with
  | [] -> Ok true
  | fs -> Error (ConstraintFailures fs)

(* ============================================================================
   Comparison shorthand constructors
   ============================================================================ *)

let make_comparison_binding ~left ~right =
  BindingMap.empty
  |> BindingMap.add "left" left
  |> BindingMap.add "right" right

let lt ~left ~right =
  MemberOf
    { target = "less_than"; binding = make_comparison_binding ~left ~right }

let lte ~left ~right =
  MemberOf
    {
      target = "less_than_or_equal";
      binding = make_comparison_binding ~left ~right;
    }

let gt ~left ~right =
  MemberOf
    {
      target = "greater_than";
      binding = make_comparison_binding ~left ~right;
    }

let gte ~left ~right =
  MemberOf
    {
      target = "greater_than_or_equal";
      binding = make_comparison_binding ~left ~right;
    }

let eq ~left ~right =
  MemberOf
    { target = "equal"; binding = make_comparison_binding ~left ~right }

let neq ~left ~right =
  MemberOf
    { target = "not_equal"; binding = make_comparison_binding ~left ~right }

let between ~value ~low ~high =
  And
    [
      MemberOf
        {
          target = "greater_than_or_equal";
          binding = make_comparison_binding ~left:value ~right:low;
        };
      MemberOf
        {
          target = "less_than_or_equal";
          binding = make_comparison_binding ~left:value ~right:high;
        };
    ]
