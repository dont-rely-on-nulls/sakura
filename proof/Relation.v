(** * Relation: minimal model for finiteness checking

    A relation is reduced to the two fields that [predicted_finite]
    actually inspects: its name (for database lookup) and its
    finiteness flag (for the gate decision). *)

From Stdlib Require Import List.
From Stdlib Require Import String.
Require Import Schema.
Import ListNotations.

Record relation := mkRelation {
  rel_name   : string;
  rel_finite : bool;
}.

(** A database is a named collection of relations. *)
Definition database := list (string * relation).

(** Look up a relation by name. *)
Fixpoint db_lookup (name : string) (db : database) : option relation :=
  match db with
  | []            => None
  | (n, r) :: rest =>
    if String.eqb name n then Some r else db_lookup name rest
  end.
