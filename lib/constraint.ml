(** Constraint system for relational membership criteria.

    A relation's intension includes constraints that refine the space of
    admissible tuples. Since constraints are themselves relations, checking
    a constraint = checking membership in that constraint-relation.

    Negation requires an explicit universe relation name, forcing
    closed-world reasoning in first-order logic. *)

(* TODO: lt/lte/gt/gte/eq/neq/between at the bottom reference target names
   like "less_than" that don't match the prelude relation names
   (e.g. "natural_natural_less_than"). These constructors are currently dead
   code and will fail at runtime if used. *)

type relation_name = string
type attr_name = string

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
    Exists { variable = variable'; quantifier; body = rename_vars renames body }
  | Forall { variable; quantifier; body } ->
    let variable' =
      match List.assoc_opt variable renames with
      | Some n -> n
      | None -> variable
    in
    Forall { variable = variable'; quantifier; body = rename_vars renames body }

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
    (* All branches must survive for Or to remain sound *)
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

let merge (cs1 : (string * t) list) (cs2 : (string * t) list)
    : (string * t) list =
  let combined = cs1 @ cs2 in
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

let member_of ~target ~binding = MemberOf { target; binding }
let not_ ~universe body = Not { body; universe }
let and_ cs = match cs with [ c ] -> c | _ -> And cs
let or_  cs = match cs with [ c ] -> c | _ -> Or cs
let exists ~variable ~quantifier body = Exists { variable; quantifier; body }
let forall ~variable ~quantifier body = Forall { variable; quantifier; body }

type eval_context = {
  check_membership :
    relation_name ->
    (attr_name * Conventions.AbstractValue.t) list ->
    bool;
  iterate_finite :
    relation_name ->
    (attr_name * Conventions.AbstractValue.t) list list option;
}

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

let rec evaluate (ctx : eval_context) (tuple : Tuple.materialized) (c : t)
    : (bool, diagnostic) result =
  match c with
  | MemberOf { target; binding } ->
    let bound_values = bind binding tuple in
    if ctx.check_membership target bound_values then Ok true
    else Error (MembershipFailed { target; bound_values })
  | Not { body; universe = _ } ->
    (* The universe is a declarative annotation for closed-world reasoning.
       At runtime we negate the body; the universe enforces logical soundness
       at the schema level, not as a runtime tuple-existence check. *)
    (match evaluate ctx tuple body with
     | Ok true -> Ok false
     | Ok false -> Ok true
     | Error (MembershipFailed _) -> Ok true
     | Error d -> Error d)
  | And cs -> evaluate_and ctx tuple cs
  | Or cs -> evaluate_or ctx tuple cs
  | Exists { variable; quantifier; body } -> (
    match ctx.iterate_finite quantifier with
    | None -> Error (UnboundedQuantifier { variable; quantifier })
    | Some rows ->
      let rec check = function
        | [] -> Ok false
        | row :: rest ->
          let extended_tuple = extend_tuple tuple variable row in
          (match evaluate ctx extended_tuple body with
           | Ok true -> Ok true
           | Ok false -> check rest
           | Error _ -> check rest)
      in
      check rows)
  | Forall { variable; quantifier; body } -> (
    match ctx.iterate_finite quantifier with
    | None -> Error (UnboundedQuantifier { variable; quantifier })
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

and extend_tuple (tuple : Tuple.materialized)
    (variable : attr_name)
    (row : (attr_name * Conventions.AbstractValue.t) list)
    : Tuple.materialized =
  (* Quantifier attributes are namespaced by the variable name (for example "d.dept_id")
     so they cannot silently overwrite same-named attributes from the base tuple
     or from other quantifiers. Without such namespacing, a constraint like
     Exists d in Department, MemberOf Department (dept_id = Var "dept_id")
     would potentially overwrite the base tuple's dept_id with the Department row's dept_id,
     causing Var "dept_id" to resolve to the wrong value and accept invalid inserts.
     Constraints reference quantifier attributes as Var "variable.attr"
     (for example Var "d.dept_id") and base tuple attributes as Var "attr" (like Var "dept_id").
     This convention is the prerequisite for universal variable substitution
     (see docs/incremental_constraint_checking.org, specifically Technique 2).
     A better long-term solution would attach provenance metadata directly
     to each attribute rather than relying on name conventions, but we don't have that yet. *)
  let extra_attrs =
    List.fold_left
      (fun acc (k, v) ->
        Tuple.AttributeMap.add (variable ^ "." ^ k) { Attribute.value = v } acc)
      tuple.attributes row
  in
  { tuple with attributes = extra_attrs }

(* TODO: Sometimes it's more efficient and sufficient to simply halt at the first constraint violated. However, it's also important to be able to adjudicate which constraints failed on the tuple materialization. For that reason, we should likely return a lazy stream that computes the constraints on demand. *)
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

(** Polarity analysis: determines whether each relation name appears in
    a positive (must-exist) or negative (must-not-exist) position within
    a constraint tree. Used for cascade checking: when a tuple is deleted,
    only relations with Positive or Both polarity need re-validation. *)

type polarity = Positive | Negative | Both

let merge_polarity a b =
  match a, b with
  | Positive, Positive -> Positive
  | Negative, Negative -> Negative
  | _                  -> Both

let flip_polarity = function
  | Positive -> Negative
  | Negative -> Positive
  | Both     -> Both

let merge_polarity_maps m1 m2 =
  List.fold_left (fun acc (name, pol) ->
    match List.assoc_opt name acc with
    | None      -> (name, pol) :: acc
    | Some prev ->
      (name, merge_polarity prev pol)
      :: List.filter (fun (n, _) -> n <> name) acc)
    m1 m2

let rec polarity_of ?(neg = false) (c : t) : (relation_name * polarity) list =
  let pos p = if neg then flip_polarity p else p in
  match c with
  | MemberOf { target; _ } ->
    [(target, pos Positive)]
  | Not { body; _ } ->
    polarity_of ~neg:(not neg) body
  | And cs | Or cs ->
    List.fold_left (fun acc c ->
      merge_polarity_maps acc (polarity_of ~neg c)) [] cs
  | Exists { quantifier; body; _ } ->
    merge_polarity_maps
      [(quantifier, pos Positive)]
      (polarity_of ~neg body)
  | Forall { quantifier; body; _ } ->
    merge_polarity_maps
      [(quantifier, pos Negative)]
      (polarity_of ~neg body)

(** Constraint timing: immediate (checked on every mutation) or deferred
    (checked at transaction commit). *)
type timing = Immediate | Deferred

(** Given a constraint, a deleted relation name [dep_rel], and the deleted
    tuple's attributes, return a filter: [(attr_name, value)] pairs that
    narrow which constrained-relation tuples could be affected.

    For a Var binding like [dept_id = Var "dept_id"] targeting [dep_rel],
    the deleted tuple's [dept_id] value becomes the filter: only constrained
    tuples with that exact [dept_id] need re-checking.

    Returns [] if no narrowing is possible (Const bindings, unrelated rel). *)
let rec focused_filter (c : t) (dep_rel : relation_name)
    (deleted : (attr_name * Conventions.AbstractValue.t) list)
  : (attr_name * Conventions.AbstractValue.t) list =
  match c with
  | MemberOf { target; binding } when target = dep_rel ->
    BindingMap.fold (fun _target_attr expr acc ->
      match expr with
      | Var src_attr ->
        (match List.assoc_opt src_attr deleted with
         | Some v -> (src_attr, v) :: acc
         | None   -> acc)
      | Const _ -> acc)
      binding []
  | MemberOf _ -> []
  | Not { body; _ } -> focused_filter body dep_rel deleted
  | And cs | Or cs ->
    List.concat_map (fun c -> focused_filter c dep_rel deleted) cs
  | Exists { body; _ } | Forall { body; _ } ->
    focused_filter body dep_rel deleted

(** Extract Const binding values from a constraint for a given [dep_rel].

    These are fixed-value preconditions: if the deleted tuple doesn't have
    these exact values, the constraint cannot be violated by the deletion.
    The cascade checker uses this to bail out early. *)
let rec trigger_constants (c : t) (dep_rel : relation_name)
  : (attr_name * Conventions.AbstractValue.t) list =
  match c with
  | MemberOf { target; binding } when target = dep_rel ->
    BindingMap.fold (fun target_attr expr acc ->
      match expr with
      | Const v -> (target_attr, v) :: acc
      | Var _   -> acc)
      binding []
  | MemberOf _ -> []
  | Not { body; _ } -> trigger_constants body dep_rel
  | And cs | Or cs ->
    List.concat_map (fun c -> trigger_constants c dep_rel) cs
  | Exists { body; _ } | Forall { body; _ } ->
    trigger_constants body dep_rel

(** Substitute transition tuple values into a constraint for universal variable
    substitution. When [dep_rel] is being mutated (deleted from or inserted
    into), any [Exists] or [Forall] that quantifies over [dep_rel] has its body
    rewritten: occurrences of [Var "variable.attr"] are replaced by
    [Const value] using the transition tuple's attribute values.

    The convention that quantifier attributes are namespaced as [variable.attr]
    (established by [extend_tuple]) means base tuple [Var] references like
    [Var "dept_id"] are never substituted, only [Var "d.dept_id"] would match
    a quantifier variable [d] over a deleted [Department] row.

    This transforms the recheck from a full relation evaluation into a targeted
    expression that tests only the rows affected by the transition tuple.
    See Technique 2 in docs/incremental_constraint_checking.org. *)
let substitute_transition
    (c : t)
    (dep_rel : relation_name)
    (transition : (attr_name * Conventions.AbstractValue.t) list)
  : t =
  let apply_subs subs binding =
    BindingMap.map (function
      | Var v -> (match List.assoc_opt v subs with
                  | Some value -> Const value
                  | None -> Var v)
      | Const _ as expr -> expr)
      binding
  in
  let rec sub_body subs = function
    | MemberOf { target; binding } ->
      MemberOf { target; binding = apply_subs subs binding }
    | Not { body; universe } -> Not { body = sub_body subs body; universe }
    | And cs -> And (List.map (sub_body subs) cs)
    | Or cs  -> Or  (List.map (sub_body subs) cs)
    | Exists { variable; quantifier; body } ->
      Exists { variable; quantifier; body = sub_body subs body }
    | Forall { variable; quantifier; body } ->
      Forall { variable; quantifier; body = sub_body subs body }
  in
  let rec go = function
    | MemberOf _ as leaf -> leaf
    | Not { body; universe } -> Not { body = go body; universe }
    | And cs -> And (List.map go cs)
    | Or cs  -> Or  (List.map go cs)
    | Exists { variable; quantifier; body } ->
      let body' =
        if quantifier = dep_rel then
          sub_body (List.map (fun (a, v) -> (variable ^ "." ^ a, v)) transition) body
        else go body
      in
      Exists { variable; quantifier; body = body' }
    | Forall { variable; quantifier; body } ->
      let body' =
        if quantifier = dep_rel then
          sub_body (List.map (fun (a, v) -> (variable ^ "." ^ a, v)) transition) body
        else go body
      in
      Forall { variable; quantifier; body = body' }
  in
  go c

let make_comparison_binding ~left ~right =
  BindingMap.empty
  |> BindingMap.add "left" left
  |> BindingMap.add "right" right

let lt ~left ~right =
  MemberOf { target = "less_than"; binding = make_comparison_binding ~left ~right }

let lte ~left ~right =
  MemberOf { target = "less_than_or_equal"; binding = make_comparison_binding ~left ~right }

let gt ~left ~right =
  MemberOf { target = "greater_than"; binding = make_comparison_binding ~left ~right }

let gte ~left ~right =
  MemberOf { target = "greater_than_or_equal"; binding = make_comparison_binding ~left ~right }

let eq ~left ~right =
  MemberOf { target = "equal"; binding = make_comparison_binding ~left ~right }

let neq ~left ~right =
  MemberOf { target = "not_equal"; binding = make_comparison_binding ~left ~right }

let between ~value ~low ~high =
  And
    [ MemberOf { target = "greater_than_or_equal";
                 binding = make_comparison_binding ~left:value ~right:low }
    ; MemberOf { target = "less_than_or_equal";
                 binding = make_comparison_binding ~left:value ~right:high }
    ]
