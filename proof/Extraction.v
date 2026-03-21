(** * Extraction to OCaml

    This module extracts the verified DRL semantics to OCaml code.
    The extracted code can be linked into Sakura's build, providing
    a verified reference implementation alongside the hand-written
    one.

    The extraction targets are:
    - [predicted_schema]: schema prediction (can replace ad-hoc
      schema computation in the executor)
    - [predicted_finite]: finiteness gate — rejects queries that
      may produce infinite results
    - [eval]: a verified reference evaluator

    To extract:
      cd proof && make

    This produces [drl_verified.ml] and [drl_verified.mli] in
    [lib/drl_verified/]. *)

From Stdlib Require Import Extraction.
From Stdlib Require Import ExtrOcamlBasic.
From Stdlib Require Import ExtrOcamlString.
From Stdlib Require Import ExtrOcamlNatInt.

Require Import Schema.
Require Import Relation.
Require Import Drl.

(** Realize axioms so the extracted module can be loaded.
    [value] is Sakura's [Conventions.AbstractValue.t] = [Obj.t].
    [value_eqb] uses structural equality.
    [literal_to_value] maps each literal constructor via [Obj.repr]. *)
Extract Constant value => "Obj.t".
Extract Constant value_eqb => "(fun a b -> a = b)".
Extract Constant literal_to_value =>
  "(function LitInt n -> Obj.repr n | LitStr s -> Obj.repr s | LitBool b -> Obj.repr b)".

(** Output extracted code into the OCaml library tree. *)
Set Extraction Output Directory "../lib/drl_verified".

(** Extract the core types and functions.
    ExtrOcamlBasic, ExtrOcamlString, and ExtrOcamlNatInt handle
    the mapping of Coq base types (bool, string, nat) to efficient
    OCaml representations. *)
Extraction "drl_verified" predicted_schema predicted_finite eval.
