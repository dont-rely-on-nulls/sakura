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

module BranchOps = Management.Branch.Make(Management.Physical.Memory)

let get_branch storage =
  match BranchOps.get_head storage with
  | Ok (Some name) -> name
  | _ -> "--"

let tuple_to_sexp (t : Tuple.materialized) =
  let open Sexplib.Sexp in
  List (Tuple.AttributeMap.bindings t.Tuple.attributes
        |> List.map (fun (k, attr) ->
               List [Atom k; Conventions.AbstractValue.sexp_of_t attr.Attribute.value]))

let relation_to_sexp storage db_name db_hash (rel : Relation.t) limit =
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
    List [Atom "db_name";   Atom db_name];
    List [Atom "branch";    Atom (get_branch storage)];
  ]

let ok_response storage db_name db_hash msg =
  let open Sexplib.Sexp in
  to_string @@ List [
    Atom "ok";
    List [Atom "message"; Atom msg];
    List [Atom "db_hash"; Atom (short_hash db_hash)];
    List [Atom "db_name"; Atom db_name];
    List [Atom "branch";  Atom (get_branch storage)];
  ]

let error_response storage db_name db_hash msg =
  let open Sexplib.Sexp in
  to_string @@ List [
    Atom "error";
    List [Atom "message"; Atom msg];
    List [Atom "db_hash"; Atom (short_hash db_hash)];
    List [Atom "db_name"; Atom db_name];
    List [Atom "branch";  Atom (get_branch storage)];
  ]

let manip_err = function
  | Manipulation.RelationNotFound s      -> "RelationNotFound: " ^ s
  | Manipulation.RelationAlreadyExists s -> "RelationAlreadyExists: " ^ s
  | Manipulation.TupleNotFound h         -> "TupleNotFound: " ^ h
  | Manipulation.DuplicateTuple h        -> "DuplicateTuple: " ^ h
  | Manipulation.ConstraintViolation s   -> "ConstraintViolation: " ^ s
  | Manipulation.StorageError s          -> "StorageError: " ^ s

let advance_head_branch storage new_hash =
  match BranchOps.get_head storage with
  | Ok (Some branch_name) ->
    ignore (BranchOps.update_tip storage ~name:branch_name ~tip:new_hash)
  | _ -> ()

let dbms_dispatch = Dbms_language.create [
  (module Drl.Sublanguage : Sublanguage.S);
  (module Ddl.Sublanguage : Sublanguage.S);
  (module Dml.Sublanguage : Sublanguage.S);
  (module Icl.Sublanguage : Sublanguage.S);
  (module Dcl.Sublanguage : Sublanguage.S);
]

let execute_command storage db_ref cmd =
  let db   = !db_ref in
  let h    = db.Management.Database.hash in
  let name = db.Management.Database.name in
  let ok   = ok_response    storage name in
  let err  = error_response storage name in
  (* (schema) is a stopgap — introspection should be expressible in
     DRL via catalog relations, not special-cased here. *)
  let cmd = match String.trim cmd with
    | "(schema)" -> {|(Base sakura:attribute)|}
    | s -> s
  in
  match Dbms_language.execute dbms_dispatch storage db cmd with
  | Ok (Sublanguage.Query rel) ->
    relation_to_sexp storage name h rel default_limit
  | Ok (Sublanguage.Transition (new_db, msg)) ->
    db_ref := new_db;
    advance_head_branch storage new_db.Management.Database.hash;
    ok new_db.Management.Database.hash msg
  | Error e ->
    err h (Dbms_language.string_of_dispatch_error e)

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
      | Error e        ->
        Printf.eprintf "Warning: failed to register prelude relation %s: %s\n%!"
          rel.name (manip_err e);
        db)
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
         let response =
           try execute_command storage db_ref line
           with e ->
             let db = !db_ref in
             error_response storage db.Management.Database.name
               db.Management.Database.hash
               ("Uncaught error: " ^ Printexc.to_string e)
         in
         output_string oc (response ^ "\n");
         flush oc
       end
     done
   with End_of_file | Unix.Unix_error _ -> ())

let depart_emp = [
    "(ddl (CreateRelation (name \"Department\") (schema ((dept_id \"integer\") (dept_name \"string\") (location \"string\")))))";
    "(ddl (CreateRelation (name \"Employee\") (schema ((emp_id \"integer\") (emp_name \"string\") (salary \"float\") (dept_id \"integer\") (hire_date \"string\")))))";
    "(icl (RegisterConstraint(constraint_name \"salary_positive\")(relation_name \"Employee\")(body (MemberOf(target \"positive_reals\")(binding ((salary (Var \"salary\"))))))))";
    "(icl (RegisterConstraint
   (constraint_name \"fk_employee_dept\")
   (relation_name \"Employee\")
   (body (MemberOf
     (target \"Department\")
     (binding ((dept_id (Var \"dept_id\"))))))))";
    "(dml (InsertTuple (relation \"Department\") (attributes ((dept_id (Int 1)) (location (Str \"Gifu\")) (dept_name (Str \"Nippon Ichi\"))))))";
    "(dml (InsertTuples (relation \"Employee\") (tuples (((emp_id (Int 101)) (emp_name (Str \"Alice Johnson\")) (salary (Float 95000.0)) (dept_id (Int 1)) (hire_date (Str \"2020-01-15\")))))))"
  ]

let _depart_emp () =
  match Management.Physical.Memory.create () with
  | Error _ -> failwith "Failed to create storage"
  | Ok storage ->
    match Manipulation.Memory.create_database storage ~name:"sakura" with
    | Error _ -> failwith "Failed to create initial database"
    | Ok db ->
      let db = register_prelude_relations storage db in
      let db_ref = ref db in
      Printf.printf "Database hash: %s\n%!" (short_hash db.Management.Database.hash);
      List.iter (fun cmd -> print_endline @@ execute_command storage db_ref cmd)
        depart_emp;
      ()

let n_way = [
    "(ddl (CreateRelation
  (name \"Building\")
  (schema
    ((building_id \"integer\")
     (building_name \"string\")
     (floors \"integer\")))))";
    "(ddl (CreateRelation
     (name \"Room\")
  (schema
    ((room_id \"integer\")
     (building_id \"integer\")
     (floor \"integer\")
     (room_number \"string\")))))";
"(ddl (CreateRelation
  (name \"Suite\")
  (schema
    ((suite_id \"integer\")
     (room_id \"integer\")
     (suite_name \"string\")
 (capacity \"integer\")))))";
(* FK: every Room must belong to an existing Building *)
"(icl (RegisterConstraint
  (constraint_name \"fk_room_building\")
  (relation_name \"Room\")
  (body (MemberOf
    (target \"Building\")
    (binding ((building_id (Var \"building_id\"))))))))";
(* FK: every Suite must belong to an existing Room *)
"(icl (RegisterConstraint
  (constraint_name \"fk_suite_room\")
  (relation_name \"Suite\")
  (body (MemberOf
    (target \"Room\")
 (binding ((room_id (Var \"room_id\"))))))))";
(* A suite is only valid if its room belongs to a building with more than 3 floors *)
"(icl (RegisterConstraint
  (constraint_name \"suite_in_tall_building\")
  (relation_name \"Suite\")
  (body
    (Exists (variable \"r\") (quantifier \"Room\")
      (body
        (Exists (variable \"b\") (quantifier \"Building\")
          (body
            (And
              ((MemberOf (target \"Room\")
                 (binding ((room_id (Var \"room_id\")))))
               (MemberOf (target \"Building\")
                 (binding ((building_id (Var \"r.building_id\")))))
               (MemberOf (target \"greater_than_natural\")
                 (binding ((left (Var \"b.floors\")) (right (Const (Int 3))))))))))))))))";
"(dml (InsertTuple
  (relation \"Building\")
  (attributes
    ((building_id (Int 2))
     (building_name (Str \"Tower B\"))
 (floors (Int 2))))))";
    "(dml (InsertTuple
  (relation \"Building\")
  (attributes
    ((building_id (Int 1))
     (building_name (Str \"Tower A\"))
     (floors (Int 10))))))";
"(dml (InsertTuples
  (relation \"Room\")
  (tuples
    (((room_id (Int 101)) (building_id (Int 1)) (floor (Int 1)) (room_number (Str \"1A\")))
     ((room_id (Int 102)) (building_id (Int 1)) (floor (Int 2)) (room_number (Str \"2A\")))
 ((room_id (Int 201)) (building_id (Int 2)) (floor (Int 1)) (room_number (Str \"1B\")))))))";
"(dml (InsertTuples
  (relation \"Suite\")
  (tuples
    (((suite_id (Int 1001)) (room_id (Int 101)) (suite_name (Str \"Presidential\")) (capacity (Int 4)))
     ((suite_id (Int 1002)) (room_id (Int 101)) (suite_name (Str \"Standard\"))     (capacity (Int 2)))
     ((suite_id (Int 1003)) (room_id (Int 102)) (suite_name (Str \"Deluxe\"))       (capacity (Int 3)))))))";
  ]

let _n_way () =
  match Management.Physical.Memory.create () with
  | Error _ -> failwith "Failed to create storage"
  | Ok storage ->
    match Manipulation.Memory.create_database storage ~name:"sakura" with
    | Error _ -> failwith "Failed to create initial database"
    | Ok db ->
      let db = register_prelude_relations storage db in
      let db_ref = ref db in
      Printf.printf "Database hash: %s\n%!" (short_hash db.Management.Database.hash);
      List.iter (fun cmd -> print_endline @@ execute_command storage db_ref cmd)
        n_way;
      ()

let () =
  let port = default_port in
  match Management.Physical.Memory.create () with
  | Error _ -> failwith "Failed to create storage"
  | Ok storage ->
    match Manipulation.Memory.create_database storage ~name:"sakura" with
    | Error _ -> failwith "Failed to create initial database"
    | Ok db ->
      let db = register_prelude_relations storage db in
      (* Register ephemeral sakura:branch and sakura:head relations backed by
         the raw branch registry — no stored tuples, no circularity. *)
      let db =
        let register db rel =
          match Manipulation.Memory.create_immutable_relation storage db
                  ~name:rel.Relation.name
                  ~schema:rel.Relation.schema
                  ~generator:(Option.get rel.Relation.generator)
                  ~membership_criteria:rel.Relation.membership_criteria
                  ~cardinality:rel.Relation.cardinality
          with
          | Ok (db, _) -> db
          | Error e ->
            Printf.eprintf "Warning: failed to register %s: %s\n%!"
              rel.Relation.name (match e with
                | Manipulation.RelationAlreadyExists s -> "already exists: " ^ s
                | Manipulation.StorageError s -> s | _ -> "error");
            db
        in
        let db = register db (BranchOps.branch_relation storage) in
        let db = register db (BranchOps.head_relation storage) in
        db
      in
      (* Create default master branch in raw registry and set HEAD *)
      (match BranchOps.create storage ~name:"master"
               ~tip:db.Management.Database.hash with
       | Error e -> Printf.eprintf "Warning: failed to create master branch: %s\n%!" e
       | Ok () -> ());
      (match BranchOps.checkout storage "master" with
       | Error e -> Printf.eprintf "Warning: failed to checkout master: %s\n%!" e
       | Ok () -> ());
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
