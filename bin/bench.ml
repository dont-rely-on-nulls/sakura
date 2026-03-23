open Relational_engine

(*  helpers  *)

let time label f =
  let t0 = Unix.gettimeofday () in
  f ();
  let elapsed = Unix.gettimeofday () -. t0 in
  Printf.printf "  %-60s %.4f s\n%!" label elapsed

let mk_int (n : int) : Conventions.AbstractValue.t = Obj.magic n

let make_tuple relation pairs =
  let attributes =
    List.fold_left
      (fun acc (k, v) ->
        Tuple.AttributeMap.add k { Attribute.value = mk_int v } acc)
      Tuple.AttributeMap.empty
      pairs
  in
  { Tuple.relation; attributes }

let unwrap label = function
  | Ok v    -> v
  | Error _ -> failwith label

let get_rel db name =
  match Manipulation.Memory.get_relation db ~name with
  | Some r -> r
  | None   -> failwith ("relation not found: " ^ name)

(*  database setup  *)

(*
  Schema:
    Department { dept_id }
    Employee   { emp_id, dept_id }

  Constraint on Employee:
    Exists d in Department, MemberOf Department (dept_id = Var "dept_id")

  This is a classic FK: every employee must reference an existing department.
  On the sublanguages branch, deleting a referenced Department triggers a
  cascade re-check of affected Employee tuples and rejects the delete.
  On master (no cascade), the delete succeeds unconditionally.
*)
let setup storage ~n_depts ~employees_per_dept =
  let db =
    unwrap "create_database"
      (Manipulation.Memory.create_database storage ~name:"bench")
  in
  let db =
    fst (unwrap "create Department"
      (Manipulation.Memory.create_relation storage db ~name:"Department"
         ~schema:(Schema.empty |> Schema.add "dept_id" "natural")))
  in
  let db =
    fst (unwrap "create Employee"
      (Manipulation.Memory.create_relation storage db ~name:"Employee"
         ~schema:(Schema.empty
                  |> Schema.add "emp_id"  "natural"
                  |> Schema.add "dept_id" "natural")))
  in
  let fk_body =
    Constraint.Exists {
      variable   = "d";
      quantifier = "Department";
      body = Constraint.MemberOf {
        target  = "Department";
        binding = Constraint.BindingMap.singleton "dept_id" (Constraint.Var "dept_id");
      };
    }
  in
  let db =
    unwrap "register_constraint"
      (Manipulation.Memory.register_constraint storage db
         ~constraint_name:"fk_dept"
         ~relation_name:"Employee"
         ~body:fk_body)
  in
  let dept_tuples =
    List.init n_depts (fun i ->
      make_tuple "Department" [("dept_id", i + 1)])
  in
  let db =
    let (db, _, _) = unwrap "insert departments"
      (Manipulation.Memory.create_tuples storage db
         (get_rel db "Department") dept_tuples)
    in db
  in
  let emp_tuples =
    List.init (n_depts * employees_per_dept) (fun i ->
      make_tuple "Employee" [
        ("emp_id",  i + 1);
        ("dept_id", (i mod n_depts) + 1);
      ])
  in
  let db =
    let (db, _, _) = unwrap "insert employees"
      (Manipulation.Memory.create_tuples storage db
         (get_rel db "Employee") emp_tuples)
    in db
  in
  db

(*  benchmark cases  *)

(*
  Attempt to delete each department that has employees referencing it.

  sublanguages: retract_tuple runs the cascade check — uses polarity analysis
  to identify Employee as the constrained relation, then focused_filter to
  narrow the scan to only employees with dept_id = deleted dept_id. Finds a
  match → returns ConstraintViolation. Delete is rejected.

  master: no cascade logic; retract_tuple succeeds unconditionally and db
  becomes inconsistent.

  db is not threaded through since on sublanguages every delete is rejected
  (db state is unchanged after each Error), and on master we deliberately
  want to measure per-delete cost against a consistent employee set.
*)
let bench_retract_referenced storage db ~n_depts =
  let rel = get_rel db "Department" in
  for i = 1 to n_depts do
    let hash = Hashing.hash_tuple (make_tuple "Department" [("dept_id", i)]) in
    ignore (Manipulation.Memory.retract_tuple storage db rel ~tuple_hash:hash)
  done

(*
  Delete departments that have NO referencing employees. Neither branch
  needs to scan Employee tuples (focused_filter finds no matches), so this
  measures raw retract_tuple cost: Merkle tree update + storage write.

  We thread db through the loop so each delete builds on the previous state
  and only one live tree version accumulates in memory at a time.

  Orphan dept ids start at offset to avoid colliding with the employee-owned
  depts (1..n_depts).
*)
let bench_retract_unreferenced storage db ~n_depts ~offset =
  let orphan_tuples =
    List.init n_depts (fun i ->
      make_tuple "Department" [("dept_id", offset + i + 1)])
  in
  let db =
    let (db, _, _) = unwrap "insert orphan depts"
      (Manipulation.Memory.create_tuples storage db
         (get_rel db "Department") orphan_tuples)
    in db
  in
  let _db =
    List.fold_left (fun db i ->
      let rel  = get_rel db "Department" in
      let hash = Hashing.hash_tuple (make_tuple "Department" [("dept_id", offset + i)]) in
      match Manipulation.Memory.retract_tuple storage db rel ~tuple_hash:hash with
      | Ok (db, _) -> db
      | Error _    -> db)
      db
      (List.init n_depts (fun i -> i + 1))
  in
  ()

(*  entry point  *)

let () =
  let n_depts            = 100 in
  let employees_per_dept = 200 in
  let total_employees    = n_depts * employees_per_dept in

  Printf.printf "\nSakura constraint benchmark\n";
  Printf.printf "  Departments    : %d\n"   n_depts;
  Printf.printf "  Employees/dept : %d  (%d total)\n\n"
    employees_per_dept total_employees;

  let make_storage () =
    match Management.Physical.Memory.create () with
    | Ok s    -> s
    | Error _ -> failwith "create storage"
  in

  (* Warmup build — not measured *)
  let storage = make_storage () in
  ignore (setup storage ~n_depts ~employees_per_dept);

  (* Fresh storage for measured runs *)
  let storage = make_storage () in
  let db = setup storage ~n_depts ~employees_per_dept in

  Printf.printf "Scenarios:\n";

  time (Printf.sprintf
          "retract %d referenced depts (cascade check, violations expected)"
          n_depts)
    (fun () -> bench_retract_referenced storage db ~n_depts);

  time (Printf.sprintf
          "retract %d unreferenced depts (no matching employees)"
          n_depts)
    (fun () -> bench_retract_unreferenced storage db ~n_depts ~offset:1_000_000);

  Printf.printf "\n"
