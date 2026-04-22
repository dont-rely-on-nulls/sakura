open Relational_engine
module Memory = Manipulation.Make (Management.Physical.Memory)

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

let () =
  let ( let* ) = Result.bind in
  if Array.length Sys.argv < 2 then (
    Printf.eprintf "Usage: %s <config-file>\n%!" Sys.argv.(0);
    exit 1);
  let config_path = Sys.argv.(1) in
  match
    let* config =
      Configuration.load ~expected_keys:[ "storage"; "transport" ] config_path
    in
    (* -- Storage --------------------------------------------------------- *)
    let* _storage_tag, storage_body =
      Configuration.require_section ~name:"storage" ~valid_tags:[ "memory" ]
        config
    in
    let* storage_config =
      Management.Physical.MemoryBackend.parse storage_body
    in
    let* storage =
      Management.Physical.Memory.create storage_config
      |> Result.map_error (fun e ->
          Printf.sprintf "Failed to create storage: %s" e)
    in
    (* -- Database -------------------------------------------------------- *)
    let* db =
      Memory.create_database storage ~name:"sakura"
      |> Result.map_error (fun _ -> "Failed to create initial database")
    in
    let db = register_prelude_relations storage db in
    (* -- Transport ------------------------------------------------------- *)
    let* _transport_tag, transport_body =
      Configuration.require_section ~name:"transport" ~valid_tags:[ "tcp" ]
        config
    in
    let* transport_config = Transport.TCP.parse transport_body in
    let transport = Transport.TCP.create transport_config in
    Ok (transport, db, storage)
  with
  | Error msg ->
      Printf.eprintf "Couldn't initialize: %s\n%!" msg;
      exit 1
  | Ok (transport, db, storage) ->
      let module Listener =
        Listener.Make (Transport.TCP) (Management.Physical.Memory)
      in
      Listener.run transport db storage
