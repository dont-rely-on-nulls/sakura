module type LISTENER = functor (T : Transport.TRANSPORT)(S : Management.Physical.S with type error = string) -> sig
  val run : T.t -> Management.Database.t -> S.t -> unit
end

module StringKey = struct
  type t = string
  let compare = String.compare
end

module StringMap = BatMap.Make(StringKey)

module Make : LISTENER =
  functor (T : Transport.TRANSPORT)(S : Management.Physical.S with type error = string) -> struct
    module type SubS = Sublanguage.S with type storage = S.t

    (* let dispatch = *)
    (*   Dispatch.create [ *)
    (*       (module Drl.Make(S) : Dispatch.SubS); *)
    (*       (module Ddl.Make(S) : Dispatch.SubS); *)
    (*       (module Dml.Make(S) : Dispatch.SubS); *)
    (*       (module Icl.Make(S) : Dispatch.SubS); *)
    (*       (module Dcl.Make(S) : Dispatch.SubS); *)
    (*       (module Scl.Make(S) : Dispatch.SubS); *)
    (*     ] *)

    let read_command input =
      try
        Ok (Sexplib.Sexp.input_sexp input)
      with e -> Error (Error.SyntaxError (Printexc.to_string e))

    (* let execute_command storage db_ref cmd = *)
    (*   let db = !db_ref in *)
    (*   let h = db.Management.Database.hash in *)
    (*   let name = db.Management.Database.name in *)
    (*   let ok = ok_response storage name in *)
    (*   let err = error_response storage name in *)
    (*   match Dispatch.execute dbms_dispatch storage db cmd with *)
    (*   | Ok (Sublanguage.Query rel) -> *)
    (*       relation_to_sexp storage name h rel default_limit *)
    (*   | Ok (Sublanguage.Transition (new_db, msg)) -> *)
    (*       db_ref := new_db; *)
    (*       advance_head_branch storage new_db.Management.Database.hash; *)
    (*       ok new_db.Management.Database.hash msg *)
    (*   | Ok (Sublanguage.Cursor { cursor_id; rows; has_more }) -> *)
    (*       cursor_to_sexp storage name h cursor_id rows has_more *)
    (*   | Error e -> err h (Dispatch.sexp_of_dispatch_error e) *)

    let sublanguages =          (* should this go here? *)
      List.fold_right
        (fun (module Language : SubS) -> StringMap.add Language.name (module Language : SubS))
        [
          (module Drl.Sublanguage.Make(S) : SubS);
          (module Ddl.Sublanguage.Make(S) : SubS);
          (module Dml.Sublanguage.Make(S) : SubS);
          (module Icl.Sublanguage.Make(S) : SubS);
          (module Dcl.Sublanguage.Make(S) : SubS);
          (module Scl.Sublanguage.Make(S) : SubS);
        ]
        StringMap.empty

    let fmap f m = Result.bind m f

    let find_language tag =
      StringMap.find_opt tag sublanguages
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

    let perform db_head old_db = function
      | Sublanguage.Transition (new_db, msg) ->
         if Atomic.compare_and_set db_head old_db new_db
         then Ok ()
         else Error (Error.Conflict { old_db; new_db })
      | _ -> Ok ()

    let handle_client connection db_head storage =
      let input = T.input connection in
      let output = T.output connection in
      try
        while true do
          let db = Atomic.get db_head in
          read_command input
          |> fmap (execute_command storage db)
          |> fmap (perform db_head db)
          (* |> serialize_result *)
          (* |> output_response output *)
          |> ignore
        done
      with End_of_file -> ()
      (* TODO: log other exceptions *)

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

(* module Make : LISTENER = *)
(*   functor (S : Management.Physical.S with type error = string) -> struct *)
(*     type options = { *)
(*       address : string; *)
(*       port : string; *)
(*     } *)

(*     type server = { *)
(*       socket : Unix.file_descr *)
(*     } *)

(*     let create { address; port } = *)
(*       (\* What about IPv6? *\) *)
(*       let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in *)
(*       Unix.setsockopt socket Unix.SO_REUSEADDR true; *)
(*       Unix.setsockopt socket Unix.SO_REUSEPORT true; *)
(*       { socket } *)

(*     let read_command input = () *)

(*     let execute_command dispatch storage db_ref = () *)

(*     let handle_client dispatch storage db_ref fd = *)
(*       let input = Unix.in_channel_of_descr fd in *)
(*       let output = Unix.out_channel_of_descr fd in *)
(*       try *)
(*         while true do *)
(*           Result.bind *)
(*             (read_command input) *)
(*             (execute_command dispatch storage db_ref) *)
(*           |> output_response output *)
(*         done *)
(*       with End_of_file -> ()    (\* TODO, also handle  *\) *)

(*     let spawn_handler = handle_client (\* TODO *\) *)

(*     let run { socket } db storage = *)
(*       let db_ref = ref db in *)
(*       while true do *)
(*         let (fd, _) = Unix.accept socket in *)
(*         spawn_handler dispatch storage db_ref db *)
(*       done *)
(*   end *)
