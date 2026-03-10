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
      | Generator.Done       -> (List.rev acc, false)
      | Generator.Error _    -> (List.rev acc, false)
      | Generator.Value (t, next) ->
        let mat = match t with
          | Tuple.Materialized m -> Some m
          | Tuple.NonMaterialized _ -> None
        in
        (match mat with
         | None   -> go next (pos + 1) acc count
         | Some m -> go next (pos + 1) (m :: acc) (count + 1))
  in
  go gen 0 [] 0

let materialize_relation storage (rel : Relation.t) limit =
  let gen = Algebra.Memory.to_generator storage rel in
  materialize_generator gen limit

let short_hash (h : string) = String.sub h 0 (min 8 (String.length h))

let tuple_to_sexp (t : Tuple.materialized) =
  let open Sexplib.Sexp in
  List (Tuple.AttributeMap.bindings t.Tuple.attributes
        |> List.map (fun (k, attr) ->
               List [Atom k; Conventions.AbstractValue.sexp_of_t attr.Attribute.value]))

let relation_to_sexp storage (db_hash : string) (rel : Relation.t) limit =
  let open Sexplib.Sexp in
  let schema_sexp =
    List (List.map (fun (a, d) -> List [Atom a; Atom d]) rel.Relation.schema)
  in
  let (tuples, truncated) = materialize_relation storage rel limit in
  let rows_sexp = List (List.map tuple_to_sexp tuples) in
  to_string @@ List [
    Atom "relation";
    List [Atom "name";      Atom rel.Relation.name];
    List [Atom "schema";    schema_sexp];
    List [Atom "rows";      rows_sexp];
    List [Atom "row_count"; Atom (string_of_int (List.length tuples))];
    List [Atom "truncated"; Atom (string_of_bool truncated)];
    List [Atom "db_hash";   Atom (short_hash db_hash)];
  ]

let ok_response db_hash msg =
  let open Sexplib.Sexp in
  to_string @@ List [
    Atom "ok";
    List [Atom "message"; Atom msg];
    List [Atom "db_hash"; Atom (short_hash db_hash)];
  ]

let error_response db_hash msg =
  let open Sexplib.Sexp in
  to_string @@ List [
    Atom "error";
    List [Atom "message"; Atom msg];
    List [Atom "db_hash"; Atom (short_hash db_hash)];
  ]

let manip_err = function
  | Manipulation.RelationNotFound s      -> "RelationNotFound: " ^ s
  | Manipulation.RelationAlreadyExists s -> "RelationAlreadyExists: " ^ s
  | Manipulation.TupleNotFound h         -> "TupleNotFound: " ^ h
  | Manipulation.DuplicateTuple h        -> "DuplicateTuple: " ^ h
  | Manipulation.ConstraintViolation s   -> "ConstraintViolation: " ^ s
  | Manipulation.StorageError s          -> "StorageError: " ^ s

(* TODO: language dispatch by trying parsers in sequence is a code smell —
   a valid DRL expression that happens to also parse as DML would silently
   be treated as DRL. The three sublanguages should share a common envelope,
   e.g. a top-level tag distinguishing (Query ...) from (Exec ...). *)
let execute_command storage db_ref cmd =
  let db = !db_ref in
  let h  = db.Management.Database.hash in
  (* TODO: (schema) is a stopgap. Introspection should be expressible in
     DRL itself via the catalog relations, not special-cased here. *)
  let cmd = match String.trim cmd with
    | "(schema)" -> {|(Base sakura:attribute)|}
    | s -> s
  in
  match Drl.Parser.of_string cmd with
  | Ok query ->
    (match Drl.Executor.Memory.execute storage db query with
     | Ok rel -> relation_to_sexp storage h rel default_limit
     | Error (Drl.Executor.Memory.RelationNotFound s) ->
       error_response h ("RelationNotFound: " ^ s)
     | Error (Drl.Executor.Memory.AlgebraError (Algebra.StorageError s)) ->
       error_response h ("StorageError: " ^ s)
     | Error (Drl.Executor.Memory.AlgebraError (Algebra.GeneratorError s)) ->
       error_response h ("GeneratorError: " ^ s))
  | Error _ ->
    match Dml.Parser.of_string cmd with
    | Ok stmt ->
      (match Dml.Executor.Memory.execute storage db stmt with
       | Ok new_db ->
         db_ref := new_db;
         ok_response new_db.Management.Database.hash "Database updated"
       | Error (Dml.Executor.Memory.ManipulationError me) ->
         error_response h (manip_err me)
       | Error (Dml.Executor.Memory.RelationNotFound s) ->
         error_response h ("RelationNotFound: " ^ s)
       | Error (Dml.Executor.Memory.ParseError s) ->
         error_response h ("ParseError: " ^ s))
    | Error _ ->
      match Dcl.Parser.of_string cmd with
      | Ok stmt ->
        (match Dcl.Executor.Memory.execute storage db stmt with
         | Ok new_db ->
           db_ref := new_db;
           ok_response new_db.Management.Database.hash "Constraint registered"
         | Error (Dcl.Executor.Memory.ManipulationError me) ->
           error_response h (manip_err me)
         | Error (Dcl.Executor.Memory.ConversionError s) ->
           error_response h ("ConversionError: " ^ s))
      | Error _ ->
        error_response h "Parse error: not valid DRL, DML, or DCL"

(* TODO: registration failures are silently ignored; a broken prelude
   relation leaves the catalog in a partially-seeded state with no
   indication to the user. *)
let register_prelude_relations storage db =
  let open Prelude.Standard in
  List.fold_left
    (fun db (rel : Relation.t) ->
      match Manipulation.Memory.create_immutable_relation storage db
              ~name:rel.name
              ~schema:rel.schema
              ~generator:(Option.get rel.generator)
              ~membership_criteria:rel.membership_criteria
              ~cardinality:rel.cardinality
      with
      | Ok (new_db, _) -> new_db
      | Error _        -> db)
    db
    [ less_than_natural
    ; less_than_or_equal_natural
    ; greater_than_natural
    ; greater_than_or_equal_natural
    ; equal_natural
    ; not_equal_natural
    ; plus_natural
    ; times_natural
    ; minus_natural
    ; divide_natural
    ]

(* TODO: single-threaded accept loop — one slow or hung client blocks all
   others. Should use threads or non-blocking I/O. *)
let handle_client storage db_ref fd =
  let ic = Unix.in_channel_of_descr fd in
  let oc = Unix.out_channel_of_descr fd in
  (try
     while true do
       let line = input_line ic in
       let line = String.trim line in
       if line <> "" then begin
         let response = execute_command storage db_ref line in
         output_string oc (response ^ "\n");
         flush oc
       end
     done
   with End_of_file | Unix.Unix_error _ -> ())

let () =
  let port = default_port in
  match Management.Physical.Memory.create () with
  | Error _ -> failwith "Failed to create storage"
  | Ok storage ->
    match Manipulation.Memory.create_database storage ~name:"sakura" with
    | Error _ -> failwith "Failed to create initial database"
    | Ok db ->
      let db = register_prelude_relations storage db in
      let db_ref = ref db in
      let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
      Unix.setsockopt sock Unix.SO_REUSEADDR true;
      Unix.setsockopt sock Unix.SO_REUSEPORT true;
      Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_loopback, port));
      Unix.listen sock 5;
      Printf.printf "Sakura server listening on 127.0.0.1:%d\n" port;
      Printf.printf "Database hash: %s\n%!" (short_hash db.Management.Database.hash);
      while true do
        let (client_fd, _addr) = Unix.accept sock in
        handle_client storage db_ref client_fd;
        (try Unix.close client_fd with _ -> ())
      done
