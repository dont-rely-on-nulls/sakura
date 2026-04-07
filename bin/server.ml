open Relational_engine

let default_port = 7777
let default_limit = 50

(* TODO: AbstractValue carries no type tag of its own; we recover the OCaml
   runtime tag here via Obj introspection. This is fragile — it breaks the
   moment a domain stores a boxed integer or a custom block. The right fix is
   a typed value representation in Conventions. *)

(* TODO: Generator.Error is silently treated as end-of-stream here, hiding
   real failures from the client. *)
let materialize_generator gen limit =
  let rec go gen pos acc count =
    if count >= limit then (List.rev acc, true)
    else
      match gen (Some pos) with
      | Generator.Done -> (List.rev acc, false)
      | Generator.Error _ -> (List.rev acc, false)
      | Generator.Value (t, next) -> (
          let mat =
            match t with
            | Tuple.Materialized m -> Some m
            | Tuple.NonMaterialized _ -> None
          in
          match mat with
          | None -> go next (pos + 1) acc count
          | Some m -> go next (pos + 1) (m :: acc) (count + 1))
  in
  go gen 0 [] 0

let materialize_relation storage (rel : Relation.t) limit =
  let gen = Algebra.Memory.to_generator storage rel in
  materialize_generator gen limit

let short_hash (h : string) = String.sub h 0 (min 8 (String.length h))

module BranchOps = Management.Branch.Make (Management.Physical.Memory)
module Dispatch = Dbms_language.Memory

let get_branch storage =
  match BranchOps.get_head storage with Ok (Some name) -> name | _ -> "--"

let tuple_to_sexp (t : Tuple.materialized) =
  let open Sexplib.Sexp in
  List
    (Tuple.AttributeMap.bindings t.Tuple.attributes
    |> List.map (fun (k, attr) ->
        List
          [ Atom k; Conventions.AbstractValue.sexp_of_t attr.Attribute.value ])
    )

let relation_to_sexp storage db_name db_hash (rel : Relation.t) limit =
  let open Sexplib.Sexp in
  let schema_sexp =
    List (List.map (fun (a, d) -> List [ Atom a; Atom d ]) rel.Relation.schema)
  in
  let tuples, truncated = materialize_relation storage rel limit in
  let rows_sexp = List (List.map tuple_to_sexp tuples) in
  to_string
  @@ List
       [
         Atom "relation";
         List [ Atom "name"; Atom rel.Relation.name ];
         List [ Atom "schema"; schema_sexp ];
         List [ Atom "rows"; rows_sexp ];
         List [ Atom "row_count"; Atom (string_of_int (List.length tuples)) ];
         List [ Atom "truncated"; Atom (string_of_bool truncated) ];
         List [ Atom "db_hash"; Atom (short_hash db_hash) ];
         List [ Atom "db_name"; Atom db_name ];
         List [ Atom "branch"; Atom (get_branch storage) ];
       ]

let ok_response storage db_name db_hash msg =
  let open Sexplib.Sexp in
  to_string
  @@ List
       [
         Atom "ok";
         List [ Atom "message"; Atom msg ];
         List [ Atom "db_hash"; Atom (short_hash db_hash) ];
         List [ Atom "db_name"; Atom db_name ];
         List [ Atom "branch"; Atom (get_branch storage) ];
       ]

let error_response storage db_name db_hash err_sexp =
  let open Sexplib.Sexp in
  to_string
  @@ List
       [
         Atom "error";
         err_sexp;
         List [ Atom "db_hash"; Atom (short_hash db_hash) ];
         List [ Atom "db_name"; Atom db_name ];
         List [ Atom "branch"; Atom (get_branch storage) ];
       ]

let advance_head_branch storage new_hash =
  match BranchOps.get_head storage with
  | Ok (Some branch_name) ->
      ignore (BranchOps.update_tip storage ~name:branch_name ~tip:new_hash)
  | _ -> ()

let dbms_dispatch =
  Dispatch.create
    [
      (module Drl.Sublanguage.Memory : Dispatch.SubS);
      (module Ddl.Sublanguage.Memory : Dispatch.SubS);
      (module Dml.Sublanguage.Memory : Dispatch.SubS);
      (module Icl.Sublanguage.Memory : Dispatch.SubS);
      (module Dcl.Sublanguage.Memory : Dispatch.SubS);
      (module Scl.Sublanguage.Memory : Dispatch.SubS);
    ]

let cursor_to_sexp storage db_name db_hash cursor_id rows has_more =
  let open Sexplib.Sexp in
  let row_sexps = List.map tuple_to_sexp rows in
  let rows_sexp = List row_sexps in
  to_string
  @@ List
       [
         Atom "cursor";
         List [ Atom "id"; Atom cursor_id ];
         List [ Atom "rows"; rows_sexp ];
         List [ Atom "row_count"; Atom (string_of_int (List.length row_sexps)) ];
         List [ Atom "has_more"; Atom (string_of_bool has_more) ];
         List [ Atom "db_hash"; Atom (short_hash db_hash) ];
         List [ Atom "db_name"; Atom db_name ];
         List [ Atom "branch"; Atom (get_branch storage) ];
       ]

let execute_command storage db_ref cmd =
  let db = !db_ref in
  let h = db.Management.Database.hash in
  let name = db.Management.Database.name in
  let ok = ok_response storage name in
  let err = error_response storage name in
  match Dispatch.execute dbms_dispatch storage db cmd with
  | Ok (Sublanguage.Query rel) ->
      relation_to_sexp storage name h rel default_limit
  | Ok (Sublanguage.Transition (new_db, msg)) ->
      db_ref := new_db;
      advance_head_branch storage new_db.Management.Database.hash;
      ok new_db.Management.Database.hash msg
  | Ok (Sublanguage.Cursor { cursor_id; rows; has_more }) ->
      cursor_to_sexp storage name h cursor_id rows has_more
  | Error e -> err h (Dispatch.sexp_of_dispatch_error e)

(* TODO: registration failures are silently ignored; a broken prelude
   relation leaves the catalog in a partially-seeded state with no
   indication to the user. *)
let register_prelude_relations storage db =
  let open Prelude.Standard in
  List.fold_left
    (fun db (rel : Relation.t) ->
      match
        Manipulation.Memory.create_immutable_relation storage db ~name:rel.name
          ~schema:rel.schema ~generator:(Option.get rel.generator)
          ~membership_criteria:rel.membership_criteria
          ~cardinality:rel.cardinality
      with
      | Ok (new_db, _) -> new_db
      | Error e ->
          Printf.eprintf
            "Warning: failed to register prelude relation %s: %s\n%!" rel.name
            (Manipulation.Error.string_of_error e);
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

let print_with_time str =
  let now = Unix.gettimeofday () in
  let tm = Unix.localtime now in
  let orange = "\027[38;5;208m" in
  let reset = "\027[0m" in
  let formatted_time =
    Printf.sprintf "%04d-%02d-%02d %02d:%02d:%02d" (tm.Unix.tm_year + 1900)
      (tm.Unix.tm_mon + 1) tm.Unix.tm_mday tm.Unix.tm_hour tm.Unix.tm_min
      tm.Unix.tm_sec
  in
  print_endline @@ Printf.sprintf "%s[%s]%s %s" orange formatted_time reset str

(* TODO: single-threaded accept loop — one slow or hung client blocks all
   others. Should use threads or non-blocking I/O. *)
let handle_client storage db_ref fd =
  let ic = Unix.in_channel_of_descr fd in
  let oc = Unix.out_channel_of_descr fd in
  try
    while true do
      let line = input_line ic in
      let line = String.trim line in
      if line <> "" then begin
        let response =
          try execute_command storage db_ref line
          with e ->
            let db = !db_ref in
            error_response storage db.Management.Database.name
              db.Management.Database.hash
              Sexplib.Sexp.(
                List [ Atom "uncaught-error"; Atom (Printexc.to_string e) ])
        in
        print_with_time response;
        output_string oc (response ^ "\n");
        flush oc
      end
    done
  with End_of_file | Unix.Unix_error _ -> ()

let setup () =
  let ( let* ) = Result.bind in
  let* storage =
    Management.Physical.Memory.create ()
    |> Result.map_error (Fun.const "Failed to create storage")
  in
  let* db =
    Manipulation.Memory.create_database storage ~name:"sakura"
    |> Result.map_error (Fun.const "Failed to create initial database")
  in
  Ok (storage, db)

let register_branch_relations storage db =
  (* Register ephemeral sakura:branch and sakura:head relations backed by
     the raw branch registry — no stored tuples, no circularity. *)
  let register db rel =
    match
      Manipulation.Memory.create_immutable_relation storage db
        ~name:rel.Relation.name ~schema:rel.Relation.schema
        ~generator:(Option.get rel.Relation.generator)
        ~membership_criteria:rel.Relation.membership_criteria
        ~cardinality:rel.Relation.cardinality
    with
    | Ok (db, _) -> db
    | Error e ->
        Printf.eprintf "Warning: failed to register %s: %s\n%!"
          rel.Relation.name
          (Manipulation.Error.string_of_error e);
        db
  in
  db
  |> Fun.flip register (BranchOps.branch_relation storage)
  |> Fun.flip register (BranchOps.head_relation storage)

let warn_on_error fmt = function Ok () -> () | Error e -> Printf.eprintf fmt e
let cursor_gc_max_age = 300.0 (* 5 minutes *)
let cursor_gc_interval = 60.0 (* run GC at most once per minute *)

let () =
  match setup () with
  | Error msg -> failwith msg
  | Ok (storage, db) ->
      let db =
        db
        |> register_prelude_relations storage
        |> register_branch_relations storage
      in
      (* Initialise cursor session registry for SCL *)
      let session_reg = Session.create () in
      Scl.Executor.set_sessions session_reg;
      (* Create default master branch in raw registry and set HEAD *)
      BranchOps.create storage ~name:"master" ~tip:db.Management.Database.hash
      |> warn_on_error "Warning: failed to create master branch: %s\n%!";
      BranchOps.checkout storage "master"
      |> warn_on_error "Warning: failed to checkout master: %s\n%!";
      let db_ref = ref db in
      let last_gc = ref 0.0 in
      let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
      Unix.setsockopt sock Unix.SO_REUSEADDR true;
      Unix.setsockopt sock Unix.SO_REUSEPORT true;
      Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_loopback, default_port));
      Unix.listen sock 5;
      Printf.printf "Sakura server listening on 127.0.0.1:%d\n" default_port;
      Printf.printf "Database hash: %s\n%!"
        (short_hash db.Management.Database.hash);
      while true do
        let client_fd, _addr = Unix.accept sock in
        handle_client storage db_ref client_fd;
        (try Unix.close client_fd with _ -> ());
        let now = Unix.gettimeofday () in
        if now -. !last_gc >= cursor_gc_interval then begin
          Session.gc session_reg ~max_age:cursor_gc_max_age;
          last_gc := now
        end
      done
