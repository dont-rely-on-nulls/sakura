module Make (Storage : Management.Physical.S with type error = string) = struct
  module Ops      = Manipulation.Make(Storage)
  module Branch   = Management.Branch.Make(Storage)
  module MergeOps = Management.Merge.Make(Storage)(Ops)

  type error =
    | BranchError of string
    | MergeError  of string

  let convert_strategy : Ast.merge_strategy -> Management.Merge.strategy = function
    | Ast.PreferLeft       -> Management.Merge.PreferLeft
    | Ast.PreferRight      -> Management.Merge.PreferRight
    | Ast.RevertToAncestor -> Management.Merge.RevertToAncestor

  let manip_err = function
    | Manipulation.RelationNotFound s      -> "RelationNotFound: " ^ s
    | Manipulation.RelationAlreadyExists s -> "RelationAlreadyExists: " ^ s
    | Manipulation.TupleNotFound h         -> "TupleNotFound: " ^ h
    | Manipulation.DuplicateTuple h        -> "DuplicateTuple: " ^ h
    | Manipulation.ConstraintViolation s   -> "ConstraintViolation: " ^ s
    | Manipulation.StorageError s          -> "StorageError: " ^ s

  let execute
      (storage : Storage.t)
      (db : Management.Database.t)
      (stmt : Ast.statement)
    : (Management.Database.t * string, error) result =
    match stmt with

    | Ast.CreateBranch { name; hash } ->
      let tip = match hash with
        | Some h -> h
        | None   -> db.Management.Database.hash
      in
      (match Branch.create storage ~name ~tip with
       | Error e -> Error (BranchError e)
       | Ok ()   -> Ok (db, "Branch " ^ name ^ " created"))

    | Ast.Checkout branch_name ->
      (match Branch.get_tip storage branch_name with
       | Error e       -> Error (BranchError e)
       | Ok None       -> Error (BranchError ("Branch not found: " ^ branch_name))
       | Ok (Some tip) ->
         (match Branch.checkout storage branch_name with
          | Error e -> Error (BranchError e)
          | Ok ()   ->
            (match Ops.load_database storage tip with
             | Error e -> Error (BranchError (manip_err e))
             | Ok None -> Error (BranchError ("No database at hash: " ^ tip))
             | Ok (Some loaded_db) ->
               Ok (loaded_db, "HEAD:" ^ branch_name))))

    | Ast.GetHead ->
      (match Branch.get_head storage with
       | Error e        -> Error (BranchError e)
       | Ok None        -> Ok (db, "HEAD is unset")
       | Ok (Some name) -> Ok (db, "HEAD:" ^ name))

    | Ast.GetBranchTip name ->
      (match Branch.get_tip storage name with
       | Error e        -> Error (BranchError e)
       | Ok None        -> Error (BranchError ("Branch not found: " ^ name))
       | Ok (Some hash) -> Ok (db, "branch:" ^ name ^ "=" ^ hash))

    | Ast.UpdateBranchTip { name; hash } ->
      (match Branch.update_tip storage ~name ~tip:hash with
       | Error e -> Error (BranchError e)
       | Ok ()   -> Ok (db, "Branch " ^ name ^ " updated"))

    | Ast.Merge { left; right; strategy } ->
      (match Branch.get_tip storage left, Branch.get_tip storage right with
       | Error e, _ | _, Error e -> Error (BranchError e)
       | Ok None, _  -> Error (BranchError ("Branch not found: " ^ left))
       | _, Ok None  -> Error (BranchError ("Branch not found: " ^ right))
       | Ok (Some left_tip), Ok (Some right_tip) ->
         let strat = convert_strategy strategy in
         (match MergeOps.merge ~storage ~strategy:strat ~left_tip ~right_tip with
          | Error e -> Error (MergeError (manip_err e))
          | Ok (Management.Merge.Failed conflicts) ->
            let msgs = List.map (function
              | Management.Merge.TupleConflict    { relation; hash } ->
                "tuple conflict in " ^ relation ^ " (" ^ hash ^ ")"
              | Management.Merge.SchemaConflict   r -> "schema conflict in " ^ r
              | Management.Merge.ConstraintConflict r -> "constraint conflict in " ^ r
            ) conflicts in
            Error (MergeError ("Merge failed: " ^ String.concat "; " msgs))
          | Ok (Management.Merge.Clean merged_db) ->
            let _ = Branch.update_tip storage ~name:left
                      ~tip:merged_db.Management.Database.hash in
            Ok (merged_db,
                "Merged:" ^ right ^ "→" ^ left)))
end

module Memory = Make(Management.Physical.Memory)
