(** * DRL: Data Retrieval Language — Semantics and Metatheory

    DRL is the query sublanguage of Sakura. It is a first-order
    relational algebra over named relations, with the following
    key properties:

    1. **Totality**: Every well-formed DRL query terminates.
       (No recursion, no self-reference.)

    2. **Schema preservation**: The output schema of a query is
       determined by the query structure and the database schemas,
       independent of the tuple data.

    3. **Query = natural transformation**: A DRL query maps one
       arrangement of sets (input relations) to another (output
       relation) while respecting the schema structure. The database
       state is not modified.

    This file defines:
    - The DRL AST (mirroring [lib/drl/ast.ml])
    - A denotational semantics ([eval])
    - A schema prediction function ([predicted_schema])
    - A proof that [eval] preserves the predicted schema
    - Totality is witnessed by the [eval] function itself being
      structurally recursive on the AST (Coq rejects non-terminating
      definitions) *)

From Stdlib Require Import List.
From Stdlib Require Import String.
From Stdlib Require Import Bool.
From Stdlib Require Import PeanoNat.
Require Import Schema.
Require Import Relation.
Import ListNotations.

(* ================================================================== *)
(** ** AST                                                             *)
(* ================================================================== *)

(** Literal values for [Const] nodes. *)
Inductive literal :=
  | LitInt : nat -> literal
  | LitStr : string -> literal
  | LitBool : bool -> literal.

(** The DRL query AST, mirroring [Drl.Ast.query]. *)
Inductive query : Type :=
  | Base : string -> query
      (** Base relation by name *)
  | Const : list (attr_name * literal) -> query
      (** Constant single-tuple relation *)
  | Select : query -> query -> query
      (** σ semijoin: Select(filter, source) *)
  | Join : list attr_name -> query -> query -> query
      (** ⋈ natural equijoin on named attrs *)
  | Cartesian : query -> query -> query
      (** × Cartesian product *)
  | Project : list attr_name -> query -> query
      (** π restrict columns *)
  | Rename : list (attr_name * attr_name) -> query -> query
      (** ρ rename (old, new) pairs *)
  | Union : query -> query -> query
      (** ∪ *)
  | Diff : query -> query -> query
      (** − *)
  | Take : nat -> query -> query
      (** τ limit *).

(** Totality witness: [query] is an inductive type. Any function
    defined by structural recursion on [query] is guaranteed to
    terminate by Coq's termination checker. This is the formal
    content of "DRL has no recursion." *)

(* ================================================================== *)
(** ** Schema Prediction                                               *)
(* ================================================================== *)

(** Predict the output schema of a query given a database.
    This function mirrors what Sakura's type system would enforce
    if it had dependent types. Returns [None] if the query
    references a nonexistent relation. *)
Fixpoint predicted_schema (db : database) (q : query) : option schema :=
  match q with
  | Base name =>
    match db_lookup name db with
    | Some r => Some (rel_schema r)
    | None => None
    end

  | Const pairs =>
    Some (map (fun p => (fst p, "abstract"%string)) pairs)

  | Select _filter source =>
    (* Semijoin preserves the source schema *)
    predicted_schema db source

  | Join attrs q1 q2 =>
    match predicted_schema db q1, predicted_schema db q2 with
    | Some s1, Some s2 =>
      (* Left schema + non-join, non-duplicate attrs from right *)
      Some (merge_schema s1 (remove_attrs (attr_names s1) (remove_attrs attrs s2)))
    | _, _ => None
    end

  | Cartesian q1 q2 =>
    match predicted_schema db q1, predicted_schema db q2 with
    | Some s1, Some s2 => Some (merge_schema s1 s2)
    | _, _ => None
    end

  | Project attrs q' =>
    match predicted_schema db q' with
    | Some s => Some (filter_schema attrs s)
    | None => None
    end

  | Rename renames q' =>
    match predicted_schema db q' with
    | Some s => Some (rename_in_schema renames s)
    | None => None
    end

  | Union q1 _q2 =>
    (* Schema comes from left; right must be compatible *)
    predicted_schema db q1

  | Diff q1 _q2 =>
    predicted_schema db q1

  | Take _n q' =>
    predicted_schema db q'
  end.

(* ================================================================== *)
(** ** Finiteness Prediction                                           *)
(* ================================================================== *)

(** Conservatively predict whether a query produces a finite result,
    based on the query structure and each base relation's [rel_finite]
    flag. Returns [Some true] if guaranteed finite, [Some false] if
    not, [None] if a referenced relation doesn't exist. *)
Fixpoint predicted_finite (db : database) (q : query) : option bool :=
  match q with
  | Base name =>
    match db_lookup name db with
    | Some r => Some (rel_finite r)
    | None => None
    end
  | Const _ => Some true
  | Take _ _ => Some true
  | Select _ source => predicted_finite db source
  | Join _ q1 q2 | Cartesian q1 q2 | Union q1 q2 =>
    match predicted_finite db q1, predicted_finite db q2 with
    | Some f1, Some f2 => Some (f1 && f2)
    | _, _ => None
    end
  | Diff q1 _ => predicted_finite db q1
  | Project _ q | Rename _ q => predicted_finite db q
  end.

(* ================================================================== *)
(** ** Denotational Semantics                                          *)
(* ================================================================== *)

(** Convert a literal to a value. Axiomatised: the proof does not
    depend on the representation. *)
Parameter literal_to_value : literal -> value.

(** Evaluate a [Const] node: build a single-tuple relation. *)
Definition eval_const (pairs : list (attr_name * literal)) : relation :=
  let tuple := map (fun p => (fst p, literal_to_value (snd p))) pairs in
  let sch := map (fun p => (fst p, "abstract"%string)) pairs in
  mkRelation "const" sch [tuple] true.

(** Semijoin: tuples from [source] that have a matching tuple in
    [filter_rel] on common attributes. *)
Definition semijoin (filter_rel source : relation) : relation :=
  let common := filter (fun a => has_attr a (rel_schema filter_rel))
                       (attr_names (rel_schema source)) in
  let matches t :=
    existsb (fun ft => tuples_agree_on common t ft)
            (rel_tuples filter_rel) in
  mkRelation (rel_name source)
             (rel_schema source)
             (filter (fun t => matches t) (rel_tuples source))
             (rel_finite source).

(** Equijoin on named attributes. *)
Definition equijoin (attrs : list attr_name) (left right : relation) : relation :=
  let joined :=
    flat_map (fun lt =>
      flat_map (fun rt =>
        if tuples_agree_on attrs lt rt
        then [merge_tuples lt rt]
        else [])
      (rel_tuples right))
    (rel_tuples left) in
  let new_schema := merge_schema (rel_schema left)
    (remove_attrs (attr_names (rel_schema left))
       (remove_attrs attrs (rel_schema right))) in
  mkRelation ("join_" ++ rel_name left ++ "_" ++ rel_name right)
             new_schema
             joined
             (rel_finite left && rel_finite right).

(** Cartesian product. *)
Definition cartesian (left right : relation) : relation :=
  let joined :=
    flat_map (fun lt =>
      map (fun rt => merge_tuples lt rt)
      (rel_tuples right))
    (rel_tuples left) in
  mkRelation ("cart_" ++ rel_name left ++ "_" ++ rel_name right)
             (merge_schema (rel_schema left) (rel_schema right))
             joined
             (rel_finite left && rel_finite right).

(** Project: keep only named attributes. *)
Definition project (attrs : list attr_name) (r : relation) : relation :=
  mkRelation ("proj_" ++ rel_name r)
             (filter_schema attrs (rel_schema r))
             (map (project_tuple attrs) (rel_tuples r))
             (rel_finite r).

(** Rename attributes. *)
Definition rename (renames : list (attr_name * attr_name)) (r : relation) : relation :=
  mkRelation ("rename_" ++ rel_name r)
             (rename_in_schema renames (rel_schema r))
             (map (rename_tuple renames) (rel_tuples r))
             (rel_finite r).

(** Union: concatenate tuple lists. *)
Definition rel_union (r1 r2 : relation) : relation :=
  mkRelation ("union_" ++ rel_name r1 ++ "_" ++ rel_name r2)
             (rel_schema r1)
             (rel_tuples r1 ++ rel_tuples r2)
             (rel_finite r1 && rel_finite r2).

(** Difference: tuples in r1 not appearing in r2. *)
Definition rel_diff (r1 r2 : relation) : relation :=
  let in_r2 t := existsb (fun t2 =>
    tuples_agree_on (attr_names (rel_schema r1)) t t2)
    (rel_tuples r2) in
  mkRelation ("diff_" ++ rel_name r1 ++ "_" ++ rel_name r2)
             (rel_schema r1)
             (filter (fun t => negb (in_r2 t)) (rel_tuples r1))
             (rel_finite r1).

(** Take: first n tuples. *)
Definition take (n : nat) (r : relation) : relation :=
  mkRelation ("take_" ++ rel_name r)
             (rel_schema r)
             (firstn n (rel_tuples r))
             true.

(** The denotational semantics of DRL.

    [eval db q] returns [Some r] if the query is well-formed
    (all referenced relations exist), or [None] on error.

    **Totality**: This function is structurally recursive on [q].
    Coq's termination checker accepts it, which constitutes a
    machine-checked proof that DRL evaluation always terminates.
    This is the formal content of the design decision that DRL
    has no recursion and no self-reference. *)
Fixpoint eval (db : database) (q : query) : option relation :=
  match q with
  | Base name =>
    db_lookup name db

  | Const pairs =>
    Some (eval_const pairs)

  | Select filter_q source_q =>
    match eval db filter_q, eval db source_q with
    | Some filter_rel, Some source_rel =>
      Some (semijoin filter_rel source_rel)
    | _, _ => None
    end

  | Join attrs q1 q2 =>
    match eval db q1, eval db q2 with
    | Some r1, Some r2 => Some (equijoin attrs r1 r2)
    | _, _ => None
    end

  | Cartesian q1 q2 =>
    match eval db q1, eval db q2 with
    | Some r1, Some r2 => Some (cartesian r1 r2)
    | _, _ => None
    end

  | Project attrs q' =>
    match eval db q' with
    | Some r => Some (project attrs r)
    | None => None
    end

  | Rename renames q' =>
    match eval db q' with
    | Some r => Some (rename renames r)
    | None => None
    end

  | Union q1 q2 =>
    match eval db q1, eval db q2 with
    | Some r1, Some r2 => Some (rel_union r1 r2)
    | _, _ => None
    end

  | Diff q1 q2 =>
    match eval db q1, eval db q2 with
    | Some r1, Some r2 => Some (rel_diff r1 r2)
    | _, _ => None
    end

  | Take n q' =>
    match eval db q' with
    | Some r => Some (take n r)
    | None => None
    end
  end.

(* ================================================================== *)
(** ** Schema Preservation Theorem                                     *)
(* ================================================================== *)

(** The central metatheorem: if evaluation succeeds, the output
    relation's schema matches the predicted schema.

    This says that DRL queries are natural in the schema — the
    schema structure of the output is determined entirely by the
    query's syntactic structure and the database's schema assignments,
    not by the tuple data. *)
Theorem schema_preservation : forall db q r sch,
  eval db q = Some r ->
  predicted_schema db q = Some sch ->
  rel_schema r = sch.
Proof.
  intros db q.
  revert db.
  induction q; intros db r sch Heval Hpred; simpl in *.

  - (* Base *)
    destruct (db_lookup s db) eqn:E; try discriminate.
    inversion Heval. inversion Hpred. subst. reflexivity.

  - (* Const *)
    inversion Heval. inversion Hpred. subst. simpl. reflexivity.

  - (* Select *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    apply (IHq2 db r1 sch E2 Hpred).

  - (* Join *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    destruct (predicted_schema db q1) eqn:P1; try discriminate.
    destruct (predicted_schema db q2) eqn:P2; try discriminate.
    inversion Heval. inversion Hpred. subst. simpl.
    specialize (IHq1 db r0 s E1 P1).
    specialize (IHq2 db r1 s0 E2 P2).
    subst. reflexivity.

  - (* Cartesian *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    destruct (predicted_schema db q1) eqn:P1; try discriminate.
    destruct (predicted_schema db q2) eqn:P2; try discriminate.
    inversion Heval. inversion Hpred. subst. simpl.
    specialize (IHq1 db r0 s E1 P1).
    specialize (IHq2 db r1 s0 E2 P2).
    subst. reflexivity.

  - (* Project *)
    destruct (eval db q) eqn:E; try discriminate.
    destruct (predicted_schema db q) eqn:P; try discriminate.
    inversion Heval. inversion Hpred. subst. simpl.
    specialize (IHq db r0 s E P).
    subst. reflexivity.

  - (* Rename *)
    destruct (eval db q) eqn:E; try discriminate.
    destruct (predicted_schema db q) eqn:P; try discriminate.
    inversion Heval. inversion Hpred. subst. simpl.
    specialize (IHq db r0 s E P).
    subst. reflexivity.

  - (* Union *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    apply (IHq1 db r0 sch E1 Hpred).

  - (* Diff *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    apply (IHq1 db r0 sch E1 Hpred).

  - (* Take *)
    destruct (eval db q) eqn:E; try discriminate.
    inversion Heval. subst. simpl.
    apply (IHq db r0 sch E Hpred).
Qed.

(** Corollary: evaluation either fails or produces a relation
    with the predicted schema. There is no third case. *)
Corollary eval_schema_determined : forall db q r,
  eval db q = Some r ->
  predicted_schema db q = Some (rel_schema r).
Proof.
  intros db q.
  revert db.
  induction q; intros db r Heval; simpl in *.

  - (* Base *)
    destruct (db_lookup s db) eqn:E; try discriminate.
    inversion Heval. subst. reflexivity.

  - (* Const *)
    inversion Heval. subst. simpl. reflexivity.

  - (* Select *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    apply (IHq2 db r1 E2).

  - (* Join *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    rewrite (IHq1 db r0 E1). rewrite (IHq2 db r1 E2).
    reflexivity.

  - (* Cartesian *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    rewrite (IHq1 db r0 E1). rewrite (IHq2 db r1 E2).
    reflexivity.

  - (* Project *)
    destruct (eval db q) eqn:E; try discriminate.
    inversion Heval. subst. simpl.
    rewrite (IHq db r0 E). reflexivity.

  - (* Rename *)
    destruct (eval db q) eqn:E; try discriminate.
    inversion Heval. subst. simpl.
    rewrite (IHq db r0 E). reflexivity.

  - (* Union *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    apply (IHq1 db r0 E1).

  - (* Diff *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    apply (IHq1 db r0 E1).

  - (* Take *)
    destruct (eval db q) eqn:E; try discriminate.
    inversion Heval. subst. simpl.
    apply (IHq db r0 E).
Qed.

(* ================================================================== *)
(** ** Finiteness Preservation Theorem                                *)
(* ================================================================== *)

(** The finiteness metatheorem: if evaluation succeeds, the output
    relation's [rel_finite] field matches the value that
    [predicted_finite] computes.

    This justifies the finiteness gate: if [predicted_finite]
    returns [Some false], we know the evaluator would produce a
    relation with [rel_finite = false], so rejecting the query
    is sound. *)
Theorem finiteness_preservation : forall db q r b,
  eval db q = Some r ->
  predicted_finite db q = Some b ->
  rel_finite r = b.
Proof.
  intros db q.
  revert db.
  induction q; intros db r b Heval Hpred; simpl in *.

  - (* Base *)
    destruct (db_lookup s db) eqn:E; try discriminate.
    inversion Heval. inversion Hpred. subst. reflexivity.

  - (* Const *)
    inversion Heval. inversion Hpred. subst. simpl. reflexivity.

  - (* Select *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    apply (IHq2 db r1 b E2 Hpred).

  - (* Join *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    destruct (predicted_finite db q1) eqn:P1; try discriminate.
    destruct (predicted_finite db q2) eqn:P2; try discriminate.
    inversion Heval. inversion Hpred. subst. simpl.
    specialize (IHq1 db r0 b0 E1 P1).
    specialize (IHq2 db r1 b1 E2 P2).
    subst. reflexivity.

  - (* Cartesian *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    destruct (predicted_finite db q1) eqn:P1; try discriminate.
    destruct (predicted_finite db q2) eqn:P2; try discriminate.
    inversion Heval. inversion Hpred. subst. simpl.
    specialize (IHq1 db r0 b0 E1 P1).
    specialize (IHq2 db r1 b1 E2 P2).
    subst. reflexivity.

  - (* Project *)
    destruct (eval db q) eqn:E; try discriminate.
    inversion Heval. subst. simpl.
    apply (IHq db r0 b E Hpred).

  - (* Rename *)
    destruct (eval db q) eqn:E; try discriminate.
    inversion Heval. subst. simpl.
    apply (IHq db r0 b E Hpred).

  - (* Union *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    destruct (predicted_finite db q1) eqn:P1; try discriminate.
    destruct (predicted_finite db q2) eqn:P2; try discriminate.
    inversion Heval. inversion Hpred. subst. simpl.
    specialize (IHq1 db r0 b0 E1 P1).
    specialize (IHq2 db r1 b1 E2 P2).
    subst. reflexivity.

  - (* Diff *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    apply (IHq1 db r0 b E1 Hpred).

  - (* Take *)
    destruct (eval db q) eqn:E; try discriminate.
    inversion Heval. inversion Hpred. subst. simpl. reflexivity.
Qed.

(** Corollary: evaluation either fails or produces a relation
    whose [rel_finite] matches [predicted_finite]. *)
Corollary eval_finite_determined : forall db q r,
  eval db q = Some r ->
  predicted_finite db q = Some (rel_finite r).
Proof.
  intros db q.
  revert db.
  induction q; intros db r Heval; simpl in *.

  - (* Base *)
    destruct (db_lookup s db) eqn:E; try discriminate.
    inversion Heval. subst. reflexivity.

  - (* Const *)
    inversion Heval. subst. simpl. reflexivity.

  - (* Select *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    apply (IHq2 db r1 E2).

  - (* Join *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    rewrite (IHq1 db r0 E1). rewrite (IHq2 db r1 E2).
    reflexivity.

  - (* Cartesian *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    rewrite (IHq1 db r0 E1). rewrite (IHq2 db r1 E2).
    reflexivity.

  - (* Project *)
    destruct (eval db q) eqn:E; try discriminate.
    inversion Heval. subst. simpl.
    rewrite (IHq db r0 E). reflexivity.

  - (* Rename *)
    destruct (eval db q) eqn:E; try discriminate.
    inversion Heval. subst. simpl.
    rewrite (IHq db r0 E). reflexivity.

  - (* Union *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    rewrite (IHq1 db r0 E1). rewrite (IHq2 db r1 E2).
    reflexivity.

  - (* Diff *)
    destruct (eval db q1) eqn:E1; try discriminate.
    destruct (eval db q2) eqn:E2; try discriminate.
    inversion Heval. subst. simpl.
    apply (IHq1 db r0 E1).

  - (* Take *)
    destruct (eval db q) eqn:E; try discriminate.
    inversion Heval. subst. simpl. reflexivity.
Qed.

(** The query does not modify the database.
    This is trivially true since [eval] takes [db] as a pure argument
    and returns only a relation. The type signature itself is the proof:

      [eval : database -> query -> option relation]

    There is no [database] in the output type. In the categorical
    reading, DRL operations are natural transformations (Query effect),
    not state morphisms (Transition effect). *)

(** Totality is also witnessed by the definition itself: Coq accepted
    [eval] as structurally recursive on [query]. If DRL allowed
    recursion or self-reference, this definition would be rejected
    by the termination checker. *)

(* ================================================================== *)
(** ** Future Proof Work                                               *)
(* ================================================================== *)

(** The following theorems are not yet proved but are natural next
    steps for strengthening the DRL metatheory:

    - [eval_deterministic]: [eval] is a function — the same database
      and query always produce the same result.

      [forall db q r1 r2,
        eval db q = Some r1 ->
        eval db q = Some r2 ->
        r1 = r2.]

      Trivially true since [eval] is a pure Coq function, but an
      explicit proof would clarify what "deterministic" means in the
      presence of opaque [value_eqb].

    - [finiteness_soundness]: if [predicted_finite] returns [Some true]
      and evaluation succeeds, the output tuple list is finite. This
      is trivially true in the Coq model (all Coq lists are finite),
      but becomes non-trivial when relating to Sakura's generators,
      which can be unbounded iterators.

      [forall db q r,
        eval db q = Some r ->
        predicted_finite db q = Some true ->
        rel_finite r = true.]

      (This is an immediate corollary of [finiteness_preservation].)

    - [predicted_finite_total]: if [eval db q] succeeds, then
      [predicted_finite db q] is not [None]. In other words, the
      finiteness gate never rejects a well-formed query with
      "relation not found."

      [forall db q r,
        eval db q = Some r ->
        exists b, predicted_finite db q = Some b.]

      This follows from the same structural argument as
      [eval_finite_determined]. *)
