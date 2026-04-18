open Relational_engine

module B = Management.Physical.Memory
module T = Transport.TCP
module Listener = Listener.Make(T)(B)

module Memory = Manipulation.Make (B)

(* TODO: registration failures are silently ignored; a broken prelude
   relation leaves the catalog in a partially-seeded state with no
   indication to the user. *)
(* TODO: think of a better way to initialize new databases *)
let register_prelude_relations storage db =
  let open Prelude.Standard in
  List.fold_left
    (fun db (rel : Relation.t) ->
      match
        Memory.create_immutable_relation storage db ~name:rel.name
          ~schema:rel.schema ~generator:(Option.get rel.generator)
          ~membership_criteria:rel.membership_criteria
          ~cardinality:rel.cardinality
      with
      | Ok (new_db, _) -> new_db
      | Error e ->
          Printf.eprintf
            "Warning: failed to register prelude relation %s: %s\n%!" rel.name
            (Sexplib.Sexp.to_string (Error.sexp_of_error e));
          db)
    db
    [
      less_than_natural;
      less_than_or_equal_natural;
      greater_than_natural;
      greater_than_or_equal_natural;
      equal_natural;
      not_equal_natural;
      plus_natural;
      times_natural;
      minus_natural;
      divide_natural;
    ]

(* let register_branch_relations storage db = *)
(*   (\* Register ephemeral sakura:branch and sakura:head relations backed by *)
(*      the raw branch registry — no stored tuples, no circularity. *\) *)
(*   let register db rel = *)
(*     match *)
(*       Memory.create_immutable_relation storage db *)
(*         ~name:rel.Relation.name ~schema:rel.Relation.schema *)
(*         ~generator:(Option.get rel.Relation.generator) *)
(*         ~membership_criteria:rel.Relation.membership_criteria *)
(*         ~cardinality:rel.Relation.cardinality *)
(*     with *)
(*     | Ok (db, _) -> db *)
(*     | Error e -> *)
(*         Printf.eprintf "Warning: failed to register %s: %s\n%!" *)
(*           rel.Relation.name *)
(*           (Manipulation.Error.string_of_error e); *)
(*         db *)
(*   in *)
(*   db *)
(*   |> Fun.flip register (BranchOps.branch_relation storage) *)
(*   |> Fun.flip register (BranchOps.head_relation storage) *)

let setup () =
  let ( let* ) = Result.bind in
  let* storage =
    Management.Physical.Memory.create ()
    |> Result.map_error (Fun.const "Failed to create storage")
  in
  let* db =
    Memory.create_database storage ~name:"sakura"
    |> Result.map_error (Fun.const "Failed to create initial database")
  in
  Ok (storage, db)

let address = "127.0.0.1"
let port = 7777

let () =
  match setup () with
  | Error msg -> Printf.eprintf "Couldn't initialize: %s" msg
  | Ok (storage, db) ->
     let db =
       db
       |> register_prelude_relations storage
       (* |> register_branch_relations storage *)
     in
     let transport =
       T.create
         { address_family = Unix.PF_INET;
           address = Unix.ADDR_INET (Unix.inet_addr_of_string address, port) }
     in
     Printf.printf "Listening on %s:%d" address port; print_newline ();
     Listener.run transport db storage
