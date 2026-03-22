(** * Extraction to OCaml — finiteness gate only

    Extracts [predicted_finite] to [lib/drl_verified/drl_verified.ml].
    To regenerate: cd proof && make *)

From Stdlib Require Import Extraction.
From Stdlib Require Import ExtrOcamlBasic.
From Stdlib Require Import ExtrOcamlString.
From Stdlib Require Import ExtrOcamlNatInt.

Require Import Schema.
Require Import Relation.
Require Import Drl.

Set Extraction Output Directory "../lib/drl_verified".

Extraction "drl_verified" predicted_finite.
