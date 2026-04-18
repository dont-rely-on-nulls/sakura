module type LISTENER = functor (T : Transport.TRANSPORT)(S : Management.Physical.S with type error = string) -> sig
  val run : T.t -> Management.Database.t -> S.t -> unit
end

module Make : LISTENER =
  functor (T : Transport.TRANSPORT)(S : Management.Physical.S with type error = string) -> struct
    module type SubS = Sublanguage.S with type storage = S.t
    module BranchOps = Management.Branch.Make (S)
    module AlgebraOps = Algebra.Make (S)

    let read_command input =
      try
        Ok (Sexplib.Sexp.input_sexp input)
      with Sexplib.Sexp.Parse_error { err_msg ; _} ->
        Error (Error.SyntaxError err_msg)

    let sublanguages =          (* should this go here? *)
      List.fold_right
        (fun (module Language : SubS) -> Utilities.StringMap.add Language.name (module Language : SubS))
        [
          (module Drl.Sublanguage.Make(S) : SubS);
          (module Ddl.Sublanguage.Make(S) : SubS);
          (module Dml.Sublanguage.Make(S) : SubS);
          (module Icl.Sublanguage.Make(S) : SubS);
          (module Dcl.Sublanguage.Make(S) : SubS);
          (module Scl.Sublanguage.Make(S) : SubS);
        ]
        Utilities.StringMap.empty

    let fmap f m = Result.bind m f

    let find_language tag =
      Utilities.StringMap.find_opt tag sublanguages
      |> Option.to_result ~none:(Error.UnrecognizedSublanguage tag)

    let execute storage db expr (module Language : SubS) =
      Language.parse_sexp expr
      |> fmap (Language.execute storage db)
      |> Result.map_error (fun e -> Error.SublanguageError (Language.sexp_of_error e))

    let execute_command storage db = function
      | Sexplib.Sexp.(List [Atom tag; expr]) ->
         find_language tag
         |> fmap (execute storage db expr)
      | s -> Error (Error.MalformedExpression s)

    let advance_head_branch storage new_hash =
      match BranchOps.get_head storage with
      | Ok (Some branch_name) ->
         ignore (BranchOps.update_tip storage ~name:branch_name ~tip:new_hash)
      | _ -> ()

    let perform storage db_head old_db result =
      match result with
      | Sublanguage.Transition (new_db, _) ->
         if Atomic.compare_and_set db_head old_db new_db
         then (advance_head_branch storage new_db.hash; Ok result)
         else Error (Error.Conflict { old_db; new_db })
      | _ -> Ok result

    let get_branch storage =
      match BranchOps.get_head storage with Ok (Some name) -> name | _ -> "--"

    let current_limit = 16      (* TODO: make the limit per-connection? *)

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
      let gen = AlgebraOps.to_generator storage rel in
      materialize_generator gen limit

    let tuple_to_sexp (t : Tuple.materialized) =
      let open Sexplib.Sexp in
      List (Tuple.AttributeMap.bindings t.Tuple.attributes
            |> List.map (fun (k, attr) -> List [ Atom k; Conventions.AbstractValue.sexp_of_t attr.Attribute.value ]))

    (* TODO: does our network protocol make sense? *)
    let serialize storage (db : Management.Database.t) =
      let open Sexplib.Sexp in
      function
      | Error e -> List [ Atom "error"; (Error.sexp_of_error e) ]
      (* TODO: forsake the global cursor register in favour of per-connection bookkeeping *)
      | Ok (Sublanguage.Cursor { cursor_id; rows; has_more }) ->
         let row_sexps = List.map tuple_to_sexp rows in
         let rows_sexp = List row_sexps in
         List [ Atom "cursor";
                List [ Atom "id"; Atom cursor_id ];
                List [ Atom "rows"; rows_sexp ];
                List [ Atom "row_count"; Atom (string_of_int (List.length row_sexps)) ];
                List [ Atom "has_more"; Atom (string_of_bool has_more) ];
                List [ Atom "db_hash"; Atom db.hash ];
                List [ Atom "db_name"; Atom db.name ];
                List [ Atom "branch"; Atom (get_branch storage) ] ]
      | Ok (Sublanguage.Query rel) ->
         let tuples, truncated = materialize_relation storage rel current_limit in
         let schema_sexp = List (List.map (fun (a, d) -> List [ Atom a; Atom d ]) rel.Relation.schema) in
         let rows_sexp = List (List.map tuple_to_sexp tuples) in
         List [ Atom "relation";
                List [ Atom "name"; Atom rel.Relation.name ];
                List [ Atom "schema"; schema_sexp ];
                List [ Atom "rows"; rows_sexp ];
                List [ Atom "row_count"; Atom (string_of_int (List.length tuples)) ];
                List [ Atom "truncated"; Atom (string_of_bool truncated) ];
                List [ Atom "db_hash"; Atom db.hash ];
                List [ Atom "db_name"; Atom db.name ];
                List [ Atom "branch"; Atom (get_branch storage) ] ]
      | Ok (Sublanguage.Transition (new_db, message)) ->
         List [ Atom "ok";
                List [ Atom "message"; Atom message ];
                List [ Atom "db_hash"; Atom new_db.hash ];
                List [ Atom "db_name"; Atom new_db.name ];
                (* TODO: `serialize' shouldn't need access to the storage layer *)
                List [ Atom "branch"; Atom (get_branch storage) ] ]

    let print_with_time str =   (* TODO: a proper logger *)
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

    let output_response output sexp =
      let response = Sexplib.Sexp.to_string sexp in
      output_string output response;
      (* Some programs, like our current iteration of
       * relational-explorer, expect server responses to always end
       * with a newline. Although that is technically incorrect,
       * acommodating for it doesn't bring us much harm; thus:
       *)
      output_string output "\n";
      flush output;
      print_with_time response

    let handle_client connection db_head (storage : S.t) =
      let input = T.input connection in
      let output = T.output connection in
      try
        while true do
          let db = Atomic.get db_head in
          read_command input
          |> fmap (execute_command storage db)
          |> fmap (perform storage db_head db)
          |> serialize storage db
          |> output_response output
          |> ignore
        done
      with
        End_of_file -> ()
      (* TODO: use the logger instead *)
      | e -> Printf.eprintf "Error handling connection: %s" (Printexc.to_string e)

    let spawn_handler db_head storage connection =
      Stdlib.Domain.spawn (fun () -> handle_client connection db_head storage)

    let run transport db storage =
      (* TODO: consider how to handle multiple databases within a
       * single server (a catalog perhaps?)
       *)
      let db_head = Atomic.make db in
      T.listen transport;
      while true do
        T.accept transport
        |> spawn_handler db_head storage
        |> ignore
      done
  end
