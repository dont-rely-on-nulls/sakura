open Js_of_ocaml
open Relational_engine

let default_limit = 50

let materialize_generator gen limit =
  let rec go gen pos acc count =
    if count >= limit then (List.rev acc, true)
    else
      match gen (Some pos) with
      | Generator.Done -> (List.rev acc, false)
      | Generator.Error _ -> (List.rev acc, false)
      | Generator.Value (t, next) -> (
          let materialized =
            match t with
            | Tuple.Materialized m -> Some m
            | Tuple.NonMaterialized _ -> None
          in
          match materialized with
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
  let hash = db.Management.Database.hash in
  let name = db.Management.Database.name in
  let ok = ok_response storage name in
  let err = error_response storage name in
  match Dispatch.execute dbms_dispatch storage db cmd with
  | Ok (Sublanguage.Query rel) ->
      relation_to_sexp storage name hash rel default_limit
  | Ok (Sublanguage.Transition (new_db, msg)) ->
      db_ref := new_db;
      advance_head_branch storage new_db.Management.Database.hash;
      ok new_db.Management.Database.hash msg
  | Ok (Sublanguage.Cursor { cursor_id; rows; has_more }) ->
      cursor_to_sexp storage name hash cursor_id rows has_more
  | Error e -> err hash (Dispatch.sexp_of_dispatch_error e)

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
      | Error _ -> db)
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

let register_branch_relations storage db =
  let register db rel =
    match
      Manipulation.Memory.create_immutable_relation storage db
        ~name:rel.Relation.name ~schema:rel.Relation.schema
        ~generator:(Option.get rel.Relation.generator)
        ~membership_criteria:rel.Relation.membership_criteria
        ~cardinality:rel.Relation.cardinality
    with
    | Ok (new_db, _) -> new_db
    | Error _ -> db
  in
  db
  |> Fun.flip register (BranchOps.branch_relation storage)
  |> Fun.flip register (BranchOps.head_relation storage)

type app_state = {
  storage : Management.Physical.Memory.t;
  db_ref : Management.Database.t ref;
}

let setup_state () =
  let ( let* ) = Result.bind in
  let* storage =
    Management.Physical.Memory.create ()
    |> Result.map_error (fun _ -> "Failed to create storage")
  in
  let* db =
    Manipulation.Memory.create_database storage ~name:"sakura"
    |> Result.map_error (fun _ -> "Failed to create initial database")
  in
  let seeded =
    db
    |> register_prelude_relations storage
    |> register_branch_relations storage
  in
  ignore
    (BranchOps.create storage ~name:"master"
       ~tip:seeded.Management.Database.hash);
  ignore (BranchOps.checkout storage "master");
  Ok { storage; db_ref = ref seeded }

let get_element id coerce =
  match Dom_html.getElementById_coerce id coerce with
  | Some element -> element
  | None -> failwith ("Missing element with id: " ^ id)

let html_escape s =
  let b = Buffer.create (String.length s) in
  String.iter
    (function
      | '&' -> Buffer.add_string b "&amp;"
      | '<' -> Buffer.add_string b "&lt;"
      | '>' -> Buffer.add_string b "&gt;"
      | '"' -> Buffer.add_string b "&quot;"
      | '\'' -> Buffer.add_string b "&#39;"
      | c -> Buffer.add_char b c)
    s;
  Buffer.contents b

let sexp_atom = function Sexplib.Sexp.Atom s -> Some s | _ -> None

let sexp_assoc key =
  let rec go = function
    | [] -> None
    | Sexplib.Sexp.List [ Sexplib.Sexp.Atom k; v ] :: _ when k = key -> Some v
    | _ :: rest -> go rest
  in
  go

let render_table table_root ~title ~headers ~rows =
  let header =
    List.map (fun attr -> "<th>" ^ html_escape attr ^ "</th>") headers
    |> String.concat ""
  in
  let row_html row =
    let cells =
      List.map (fun v -> "<td>" ^ html_escape v ^ "</td>") row
      |> String.concat ""
    in
    "<tr>" ^ cells ^ "</tr>"
  in
  let body = List.map row_html rows |> String.concat "" in
  let title_html =
    "<div class=\"result-title\">" ^ html_escape title ^ "</div>"
  in
  let table_html =
    "<div class=\"result-table-wrap\"><table><thead><tr>" ^ header
    ^ "</tr></thead><tbody>" ^ body ^ "</tbody></table></div>"
  in
  table_root##.innerHTML := Js.string (title_html ^ table_html)

let clear_table table_root = table_root##.innerHTML := Js.string ""

let push_debug message =
  try
    ignore
      (Js.Unsafe.fun_call
         (Js.Unsafe.get Dom_html.window "sakuraDebug")
         [| Js.Unsafe.inject (Js.string message) |])
  with _ -> ()

let set_status status text =
  status##.textContent := Js.some (Js.string text);
  push_debug ("status: " ^ text)

(* Do not trust this trash, just a hack *)
let show_error_popup message =
  try
    ignore
      (Js.Unsafe.fun_call
         (Js.Unsafe.get Dom_html.window "sakuraShowPopup")
         [|
           Js.Unsafe.inject (Js.string "Sakura error");
           Js.Unsafe.inject (Js.string message);
         |])
  with _ -> Dom_html.window##alert (Js.string ("Sakura error:\n\n" ^ message))

let render_response_table table_root response =
  let open Sexplib.Sexp in
  try
    match Sexplib.Sexp.of_string response with
    | List (Atom "relation" :: fields) ->
        let name =
          match sexp_assoc "name" fields with
          | Some (Atom v) -> v
          | _ -> "relation"
        in
        let schema_cols =
          match sexp_assoc "schema" fields with
          | Some (List items) ->
              List.filter_map
                (function
                  | List [ Atom attr; _domain ] -> Some attr | _ -> None)
                items
          | _ -> []
        in
        let rows =
          match sexp_assoc "rows" fields with
          | Some (List row_items) -> row_items
          | _ -> []
        in
        let row_values row =
          let attrs =
            match row with
            | List pairs ->
                List.filter_map
                  (function List [ Atom k; v ] -> Some (k, v) | _ -> None)
                  pairs
            | _ -> []
          in
          let lookup attr =
            match List.assoc_opt attr attrs with
            | Some v -> (
                match sexp_atom v with
                | Some raw -> html_escape raw
                | None -> html_escape (Sexplib.Sexp.to_string v))
            | None -> ""
          in
          List.map lookup schema_cols
        in
        let rendered_rows = List.map row_values rows in
        if rendered_rows = [] then clear_table table_root
        else
          render_table table_root ~title:("Relation: " ^ name)
            ~headers:schema_cols ~rows:rendered_rows
    | List (Atom "ok" :: _fields) -> clear_table table_root
    | List (Atom "error" :: fields) ->
        let detail =
          String.concat " " (List.map Sexplib.Sexp.to_string fields)
        in
        show_error_popup detail;
        clear_table table_root
    | List (Atom "cursor" :: fields) ->
        let has_more =
          match sexp_assoc "has_more" fields with
          | Some (Atom v) -> v
          | _ -> "false"
        in
        let rows =
          match sexp_assoc "rows" fields with
          | Some (List row_items) -> row_items
          | _ -> []
        in
        let cols =
          match rows with
          | List pairs :: _ ->
              List.filter_map
                (function List [ Atom k; _ ] -> Some k | _ -> None)
                pairs
          | _ -> [ "rows" ]
        in
        let row_values =
          List.map
            (function
              | List pairs ->
                  List.map
                    (fun col ->
                      match
                        List.find_opt
                          (function
                            | List [ Atom k; _ ] -> k = col | _ -> false)
                          pairs
                      with
                      | Some (List [ _; v ]) -> Sexplib.Sexp.to_string v
                      | _ -> "")
                    cols
              | other -> [ Sexplib.Sexp.to_string other ])
            rows
        in
        if row_values = [] then clear_table table_root
        else
          let headers = cols @ [ "has_more" ] in
          let rows = List.map (fun r -> r @ [ has_more ]) row_values in
          render_table table_root ~title:"Cursor" ~headers ~rows
    | other ->
        push_debug ("non-tabular response: " ^ Sexplib.Sexp.to_string other);
        clear_table table_root
  with exn ->
    show_error_popup (Printexc.to_string exn);
    clear_table table_root

let initialize_ui state =
  push_debug "initialize_ui start";
  let input = get_element "command-input" Dom_html.CoerceTo.textarea in
  let run_button = get_element "run-command" Dom_html.CoerceTo.button in
  let table_root = get_element "result-table" Dom_html.CoerceTo.div in
  let status = get_element "status" Dom_html.CoerceTo.div in
  push_debug "elements bound";
  let run_command () =
    push_debug "run_command invoked";
    let cmd = String.trim (Js.to_string input##.value) in
    if cmd = "" then set_status status "Type a command first."
    else (
      set_status status "Running command...";
      push_debug ("command: " ^ cmd);
      let response =
        try execute_command state.storage state.db_ref cmd
        with exn ->
          push_debug ("execute exception: " ^ Printexc.to_string exn);
          let db = !(state.db_ref) in
          error_response state.storage db.Management.Database.name
            db.Management.Database.hash
            Sexplib.Sexp.(
              List [ Atom "uncaught-error"; Atom (Printexc.to_string exn) ])
      in
      let rendered =
        if response = "" then "(error (message empty-response))" else response
      in
      push_debug ("response size: " ^ string_of_int (String.length rendered));
      render_response_table table_root rendered;
      if String.starts_with ~prefix:"(error" rendered then
        set_status status "Error."
      else set_status status "Done.";
      input##.value := Js.string "")
  in
  Dom_html.addEventListener run_button Dom_html.Event.click
    (Dom_html.handler (fun _event ->
         run_command ();
         Js._false))
    Js._false
  |> ignore;
  Dom_html.addEventListener input Dom_html.Event.keydown
    (Dom_html.handler (fun event ->
         if Js.to_bool event##.ctrlKey && event##.keyCode = 13 then (
           run_command ();
           Js._false)
         else Js._true))
    Js._false
  |> ignore;
  set_status status "Runtime ready.";
  clear_table table_root;
  push_debug "initialize_ui end"

let () =
  push_debug "runtime boot";
  match setup_state () with
  | Error msg ->
      push_debug ("setup_state error: " ^ msg);
      Dom_html.window##alert (Js.string msg)
  | Ok state ->
      push_debug "setup_state ok";
      initialize_ui state
