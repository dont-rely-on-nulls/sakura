(** * Relation: abstract model of tuples and relations

    A relation is an instance functor I : S -> Set, mapping each
    schema attribute to a set of values. We model tuples as total
    functions from attribute names to an abstract value type, and
    relations as sets (represented as lists) of tuples with a
    fixed schema.

    This module is intentionally abstract — it models the semantics
    that Sakura's [Relation.t] implements, not the implementation
    itself. The extraction module will bridge the gap. *)

From Stdlib Require Import List.
From Stdlib Require Import String.
From Stdlib Require Import Bool.
Require Import Schema.
Import ListNotations.

(** Abstract value type. In Sakura this is [Conventions.AbstractValue.t]
    (an [Obj.t]). Here we keep it opaque — proofs about DRL do not
    depend on the representation of values, only on their identity. *)
Parameter value : Type.
Parameter value_eqb : value -> value -> bool.
Axiom value_eqb_refl : forall v, value_eqb v v = true.
Axiom value_eqb_eq : forall v1 v2, value_eqb v1 v2 = true <-> v1 = v2.

(** A tuple is a partial function from attribute names to values,
    represented as an association list. This mirrors Sakura's
    [Tuple.AttributeMap]. *)
Definition tuple := list (attr_name * value).

(** Look up a value in a tuple. *)
Fixpoint tuple_lookup (a : attr_name) (t : tuple) : option value :=
  match t with
  | [] => None
  | (a', v) :: rest =>
    if String.eqb a a' then Some v else tuple_lookup a rest
  end.

(** Project a tuple to a subset of attributes. *)
Definition project_tuple (attrs : list attr_name) (t : tuple) : tuple :=
  filter (fun p => existsb (String.eqb (fst p)) attrs) t.

(** Rename attributes in a tuple. *)
Definition rename_tuple (renames : list (attr_name * attr_name)) (t : tuple) : tuple :=
  map (fun p =>
    let a := fst p in
    let v := snd p in
    match find (fun r => String.eqb a (fst r)) renames with
    | Some (_, a') => (a', v)
    | None => (a, v)
    end) t.

(** Merge two tuples, preferring the left on conflict. *)
Definition merge_tuples (t1 t2 : tuple) : tuple :=
  t1 ++ filter (fun p => negb (existsb (String.eqb (fst p)) (map fst t1))) t2.

(** Tuple equality on a given set of attributes. *)
Definition tuples_agree_on (attrs : list attr_name) (t1 t2 : tuple) : bool :=
  forallb (fun a =>
    match tuple_lookup a t1, tuple_lookup a t2 with
    | Some v1, Some v2 => value_eqb v1 v2
    | None, None => true
    | _, _ => false
    end) attrs.

(** A relation is a schema together with a set of tuples (extension).
    We represent the extension as a list. This is the semantic model;
    Sakura's [Relation.t] additionally carries a Merkle tree, generator,
    constraints, provenance, and lineage.

    [rel_finite] models whether the relation has finite cardinality.
    In Sakura, this corresponds to
    [cardinality <> AlephZero && cardinality <> Continuum]. *)
Record relation := mkRelation {
  rel_name : string;
  rel_schema : schema;
  rel_tuples : list tuple;
  rel_finite : bool;
}.

(** A database is a named collection of relations. *)
Definition database := list (string * relation).

(** Look up a relation by name. *)
Fixpoint db_lookup (name : string) (db : database) : option relation :=
  match db with
  | [] => None
  | (n, r) :: rest =>
    if String.eqb name n then Some r else db_lookup name rest
  end.

(** The schema of a tuple: the attribute names it carries. *)
Definition tuple_attrs (t : tuple) : list attr_name :=
  map fst t.
