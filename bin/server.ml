open Relational_engine

let default_port = 7777
let default_limit = 50

(* ============================================================================
   Minimal JSON serialisation — no external deps
   ============================================================================ *)

let js_str s =
  let buf = Buffer.create (String.length s + 2) in
  Buffer.add_char buf '"';
  String.iter (function
    | '"'  -> Buffer.add_string buf {|\"|}
    | '\\' -> Buffer.add_string buf {|\\|}
    | '\n' -> Buffer.add_string buf {|\n|}
    | '\r' -> Buffer.add_string buf {|\r|}
    | '\t' -> Buffer.add_string buf {|\t|}
    | c    -> Buffer.add_char buf c) s;
  Buffer.add_char buf '"';
  Buffer.contents buf

let js_obj pairs =
  "{" ^
  String.concat ","
    (List.map (fun (k, v) -> js_str k ^ ":" ^ v) pairs) ^
  "}"

let js_arr elems = "[" ^ String.concat "," elems ^ "]"
let js_bool b    = if b then "true" else "false"
let js_int  n    = string_of_int n

(* ============================================================================
   Value serialisation
   ============================================================================ *)

(** Convert an AbstractValue (Obj.t) to a JSON fragment.
    Uses Obj tag inspection to recover Int / String / Float. *)
let abstract_to_json (v : Conventions.AbstractValue.t) =
  if Obj.is_int v then js_int (Obj.obj v : int)
  else
    let tag = Obj.tag v in
    if tag = Obj.string_tag then js_str (Obj.obj v : string)
    else if tag = Obj.double_tag then
      let f = (Obj.obj v : float) in
      if Float.is_nan f || Float.is_infinite f then "null"
      else string_of_float f
    else js_str "<opaque>"

let tuple_to_json (t : Tuple.materialized) =
  let pairs = Tuple.AttributeMap.bindings t.Tuple.attributes in
  js_obj (List.map (fun (k, attr) -> (k, abstract_to_json attr.Attribute.value)) pairs)

(* ============================================================================
   Relation materialisation
   ============================================================================ *)

(** Iterate a generator up to [limit] tuples; returns (rows, truncated). *)
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

(** Materialise either a stored or generator-backed relation. *)
let materialize_relation storage (rel : Relation.t) limit =
  let gen = Algebra.Memory.to_generator storage rel in
  materialize_generator gen limit

(* ============================================================================
   JSON response builders
   ============================================================================ *)

let short_hash (h : string) = String.sub h 0 (min 8 (String.length h))

let relation_to_json storage (db_hash : string) (rel : Relation.t) limit =
  let schema_json =
    js_arr (List.map (fun (a, d) ->
      js_obj [("attr", js_str a); ("domain", js_str d)]
    ) rel.Relation.schema)
  in
  let (tuples, truncated) = materialize_relation storage rel limit in
  let rows_json = js_arr (List.map tuple_to_json tuples) in
  js_obj [
    ("type",      js_str "relation");
    ("name",      js_str rel.Relation.name);
    ("schema",    schema_json);
    ("rows",      rows_json);
    ("row_count", js_int (List.length tuples));
    ("truncated", js_bool truncated);
    ("db_hash",   js_str (short_hash db_hash));
  ]

let ok_response db_hash msg =
  js_obj [
    ("type",    js_str "ok");
    ("message", js_str msg);
    ("db_hash", js_str (short_hash db_hash));
  ]

let error_response db_hash msg =
  js_obj [
    ("type",    js_str "error");
    ("message", js_str msg);
    ("db_hash", js_str (short_hash db_hash));
  ]

(* ============================================================================
   Error formatting
   ============================================================================ *)

let manip_err = function
  | Manipulation.RelationNotFound s    -> "RelationNotFound: " ^ s
  | Manipulation.RelationAlreadyExists s -> "RelationAlreadyExists: " ^ s
  | Manipulation.TupleNotFound h       -> "TupleNotFound: " ^ h
  | Manipulation.DuplicateTuple h      -> "DuplicateTuple: " ^ h
  | Manipulation.ConstraintViolation s -> "ConstraintViolation: " ^ s
  | Manipulation.StorageError s        -> "StorageError: " ^ s

(* ============================================================================
   Command execution
   ============================================================================ *)

let execute_command storage db_ref cmd =
  let db = !db_ref in
  let h  = db.Management.Database.hash in
  (* 0. Special commands *)
  let cmd = match String.trim cmd with
    | "(schema)" -> {|(Base sakura:attribute)|}
    | s -> s
  in
  (* 1. Try DRL *)
  match Drl.Parser.of_string cmd with
  | Ok query ->
    (match Drl.Executor.Memory.execute storage db query with
     | Ok rel -> relation_to_json storage h rel default_limit
     | Error (Drl.Executor.Memory.RelationNotFound s) ->
       error_response h ("RelationNotFound: " ^ s)
     | Error (Drl.Executor.Memory.AlgebraError (Algebra.StorageError s)) ->
       error_response h ("StorageError: " ^ s)
     | Error (Drl.Executor.Memory.AlgebraError (Algebra.GeneratorError s)) ->
       error_response h ("GeneratorError: " ^ s))
  | Error _ ->
    (* 2. Try DML *)
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
      (* 3. Try DCL *)
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

(* ============================================================================
   Prelude registration
   ============================================================================ *)

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

(* ============================================================================
   TCP server
   ============================================================================ *)

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
