(** Structural delta between two Database.t states. Computes what changed from
    ancestor -> target at the relation level. *)

type relation_diff =
  | RelationAdded of Relation.t
  | RelationRemoved of string
  | RelationModified of {
      name : string;
      added_tuples : Conventions.Hash.t list;
      removed_tuples : Conventions.Hash.t list;
      schema_changed : bool;
    }

type t = {
  ancestor : Conventions.Hash.t;
  left : Conventions.Hash.t;
  right : Conventions.Hash.t;
  relation_diffs : relation_diff list;
}

module StringSet = BatSet.String

let relation_keys rel =
  match rel.Relation.tree with
  | None -> StringSet.empty
  | Some tree ->
      BatEnum.fold
        (fun acc h -> StringSet.add h acc)
        StringSet.empty (Merkle.to_enum tree)

(** Compute relation-level diffs from ancestor → target. *)
let diff ~ancestor ~(target : Database.t) =
  let ancestor_names =
    Database.get_relation_names ancestor |> StringSet.of_list
  in
  let target_names = Database.get_relation_names target |> StringSet.of_list in
  let all_names = StringSet.union ancestor_names target_names in
  StringSet.fold
    (fun name acc ->
      let in_ancestor = StringSet.mem name ancestor_names in
      let in_target = StringSet.mem name target_names in
      match (in_ancestor, in_target) with
      | false, true -> (
          match Database.get_relation target name with
          | Some rel -> RelationAdded rel :: acc
          | None -> acc)
      | true, false -> RelationRemoved name :: acc
      | true, true -> (
          match
            ( Database.get_relation ancestor name,
              Database.get_relation target name )
          with
          | Some anc_rel, Some tgt_rel ->
              (* Compare hashes — if identical, no diff *)
              if anc_rel.Relation.hash = tgt_rel.Relation.hash then acc
              else
                let anc_keys = relation_keys anc_rel in
                let tgt_keys = relation_keys tgt_rel in
                let added_tuples =
                  StringSet.diff tgt_keys anc_keys |> StringSet.elements
                in
                let removed_tuples =
                  StringSet.diff anc_keys tgt_keys |> StringSet.elements
                in
                let schema_changed =
                  anc_rel.Relation.schema <> tgt_rel.Relation.schema
                in
                RelationModified
                  { name; added_tuples; removed_tuples; schema_changed }
                :: acc
          | _ -> acc)
      | false, false -> acc (* impossible *))
    all_names []
