(** * DRL: Data Retrieval Language — Finiteness Gate

    This file defines:
    - The DRL query AST (mirroring [lib/drl/ast.ml])
    - [predicted_finite]: statically decides whether a query can
      produce an infinite result based on the cardinality flags of
      the relations it references.

    [predicted_finite] is extracted to OCaml and called by
    [lib/drl/gate.ml] before every query execution.  Queries whose
    result cannot be statically proven finite are rejected unless
    wrapped in a [Take]. *)

From Stdlib Require Import List.
From Stdlib Require Import String.
From Stdlib Require Import Bool.
Require Import Schema.
Require Import Relation.
Import ListNotations.

(* ================================================================== *)
(** ** AST                                                             *)
(* ================================================================== *)

Inductive literal :=
  | LitInt  : nat    -> literal
  | LitStr  : string -> literal
  | LitBool : bool   -> literal.

Inductive query : Type :=
  | Base      : string                           -> query
  | Const     : list (attr_name * literal)       -> query
  | Select    : query -> query                   -> query
  | Join      : list attr_name -> query -> query -> query
  | Cartesian : query -> query                   -> query
  | Project   : list attr_name -> query          -> query
  | Rename    : list (attr_name * attr_name) -> query -> query
  | Union     : query -> query                   -> query
  | Diff      : query -> query                   -> query
  | Take      : nat -> query                     -> query.

(* ================================================================== *)
(** ** Finiteness Prediction                                           *)
(* ================================================================== *)

(** Conservatively predict whether a query produces a finite result.
    Returns [Some true] if guaranteed finite, [Some false] if not,
    [None] if a referenced relation is not found in the database. *)
Fixpoint predicted_finite (db : database) (q : query) : option bool :=
  match q with
  | Base name =>
    match db_lookup name db with
    | Some r => Some (rel_finite r)
    | None   => None
    end
  | Const _  => Some true
  | Take _ _ => Some true
  | Select _ source =>
    predicted_finite db source
  | Join _ q1 q2 | Cartesian q1 q2 | Union q1 q2 =>
    match predicted_finite db q1, predicted_finite db q2 with
    | Some f1, Some f2 => Some (f1 && f2)
    | _,       _       => None
    end
  | Diff q1 _  => predicted_finite db q1
  | Project _ q' | Rename _ q' => predicted_finite db q'
  end.
