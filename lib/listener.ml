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

    (* let execute_command command = *)
    (*   match  *)

    let handle_client connection db storage =
      let input = T.input connection in
      let output = T.output connection in
      try
        while true do
          ()
          (* Result.bind *)
          (*   (read_command input) *)
          (*   (execute_command dispatch) *)
          (* |> serialize_result *)
          (* |> output_response output *)
        done
      with End_of_file -> ()
      (* TODO: log other exceptions *)

    let spawn_handler db storage connection =
      Stdlib.Domain.spawn (fun () -> handle_client connection db storage)

    let run transport db storage =
      (* TODO: consider how to handle multiple databases within a
       * single server (a catalog perhaps?)
       *)
      let atomic_db = Atomic.make db in
      T.listen transport;
      while true do
        let _ =
          T.accept transport
          |> spawn_handler atomic_db storage
        in
        ()
      done
  end
  [@@warning "-26-27-32"]

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
