(** * Schema: attribute names and their domain assignments

    A schema is a finite map from attribute names to domain names.
    Categorically, it is the presentation of a finitely-presented
    category whose objects are attribute names and whose morphisms
    encode domain assignments. *)

From Stdlib Require Import List.
From Stdlib Require Import String.
From Stdlib Require Import Bool.
Import ListNotations.

(** Attribute name *)
Definition attr_name := string.

(** Domain name *)
Definition domain_name := string.

(** A schema is an association list from attribute names to domain names.
    This mirrors Sakura's [Schema.t = (string * string) list]. *)
Definition schema := list (attr_name * domain_name).

(** Schema membership: an attribute name is in a schema if it appears
    as the first component of some pair. *)
Definition has_attr (a : attr_name) (s : schema) : bool :=
  existsb (fun p => String.eqb a (fst p)) s.

(** Look up the domain of an attribute in a schema. *)
Fixpoint lookup_attr (a : attr_name) (s : schema) : option domain_name :=
  match s with
  | [] => None
  | (a', d) :: rest =>
    if String.eqb a a' then Some d else lookup_attr a rest
  end.

(** The attribute names of a schema. *)
Definition attr_names (s : schema) : list attr_name :=
  map fst s.

(** Schema subset: every attribute in [s1] also appears in [s2]. *)
Definition schema_subset (s1 s2 : schema) : bool :=
  forallb (fun a => has_attr a s2) (attr_names s1).

(** Schema equality: same attribute-domain pairs (as sets, ignoring order).
    We use a simple bidirectional subset check. *)
Definition schema_equiv (s1 s2 : schema) : bool :=
  schema_subset s1 s2 && schema_subset s2 s1.

(** Filter a schema to keep only named attributes. *)
Definition filter_schema (attrs : list attr_name) (s : schema) : schema :=
  filter (fun p => existsb (String.eqb (fst p)) attrs) s.

(** Merge two schemas, preferring the left on conflict. *)
Definition merge_schema (s1 s2 : schema) : schema :=
  s1 ++ filter (fun p => negb (has_attr (fst p) s1)) s2.

(** Remove attributes present in a given list from a schema. *)
Definition remove_attrs (attrs : list attr_name) (s : schema) : schema :=
  filter (fun p => negb (existsb (String.eqb (fst p)) attrs)) s.

(** Apply a rename mapping to a schema. *)
Definition rename_in_schema (renames : list (attr_name * attr_name)) (s : schema) : schema :=
  map (fun p =>
    let a := fst p in
    let d := snd p in
    match find (fun r => String.eqb a (fst r)) renames with
    | Some (_, a') => (a', d)
    | None => (a, d)
    end) s.

(** Helper: [forallb] is monotone in the predicate. *)
Lemma forallb_impl : forall {A : Type} (f g : A -> bool) l,
  (forall x, f x = true -> g x = true) ->
  forallb f l = true -> forallb g l = true.
Proof.
  intros A f g l Himpl.
  induction l as [| x xs IHl]; simpl; auto.
  intros H. apply andb_true_iff in H. destruct H as [Hx Hxs].
  apply andb_true_iff. split.
  - apply Himpl. exact Hx.
  - apply IHl. exact Hxs.
Qed.

(** Key lemma: filtering preserves the subset relationship. *)
Lemma filter_schema_subset : forall attrs s,
  schema_subset (filter_schema attrs s) s = true.
Proof.
  intros attrs s.
  unfold schema_subset, filter_schema, attr_names.
  induction s as [| [a d] rest IH]; simpl.
  - reflexivity.
  - destruct (existsb (String.eqb a) attrs) eqn:E; simpl.
    + rewrite String.eqb_refl. simpl.
      apply (forallb_impl
               (fun a0 => has_attr a0 rest)
               (fun a0 => (a0 =? a)%string || has_attr a0 rest)).
      * intros x Hx. rewrite Hx. apply orb_true_r.
      * exact IH.
    + apply (forallb_impl
               (fun a0 => has_attr a0 rest)
               (fun a0 => (a0 =? a)%string || has_attr a0 rest)).
      * intros x Hx. rewrite Hx. apply orb_true_r.
      * exact IH.
Qed.

(** Merging with an empty schema is identity. *)
Lemma merge_schema_nil_r : forall s, merge_schema s [] = s.
Proof.
  intros s. unfold merge_schema.
  rewrite app_nil_r. reflexivity.
Qed.
