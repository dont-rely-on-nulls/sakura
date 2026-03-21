(** Composable dispatch over sublanguages.

    Replaces the hand-written try-parse cascade in the server with a
    data-driven composition of sublanguage modules. Adding a new
    sublanguage (TCL, PPL, ACL) is just extending the list. *)

type dispatch_error = NoMatch of string list

type dispatch = (module Sublanguage.S) list

let create (langs : (module Sublanguage.S) list) : dispatch = langs

(** Try envelope-based dispatch: parse (tag payload), look up tag in
    the sublanguage registry, then parse_sexp the payload directly. *)
let execute_envelope
    (langs : dispatch)
    (storage : Management.Physical.Memory.t)
    (db : Management.Database.t)
    (input : string)
  : (Sublanguage.result, dispatch_error) Result.t option =
  match Envelope.parse input with
  | Error _ -> None
  | Ok { tag; payload } ->
    let lang = List.find_opt
      (fun (module L : Sublanguage.S) -> L.name = tag) langs
    in
    match lang with
    | None -> None
    | Some (module L) ->
      match L.parse_sexp payload with
      | Error e -> Some (Error (NoMatch [L.name ^ ": " ^ L.string_of_error e]))
      | Ok ast ->
        match L.execute storage db ast with
        | Ok result -> Some (Ok result)
        | Error e -> Some (Error (NoMatch [L.name ^ ": " ^ L.string_of_error e]))

(** Legacy fallback: try each sublanguage's string parser in order. *)
let execute_cascade
    (langs : dispatch)
    (storage : Management.Physical.Memory.t)
    (db : Management.Database.t)
    (input : string)
  : (Sublanguage.result, dispatch_error) Result.t =
  let rec try_langs remaining errors =
    match remaining with
    | [] -> Error (NoMatch (List.rev errors))
    | (module L : Sublanguage.S) :: rest ->
      match L.parse input with
      | Error e -> try_langs rest (L.string_of_error e :: errors)
      | Ok ast ->
        match L.execute storage db ast with
        | Ok result -> Ok result
        | Error e -> Error (NoMatch [L.name ^ ": " ^ L.string_of_error e])
  in
  try_langs langs []

(** Unified dispatch: try envelope first, then fall back to legacy cascade. *)
let execute
    (langs : dispatch)
    (storage : Management.Physical.Memory.t)
    (db : Management.Database.t)
    (input : string)
  : (Sublanguage.result, dispatch_error) Result.t =
  match execute_envelope langs storage db input with
  | Some result -> result
  | None -> execute_cascade langs storage db input

let string_of_dispatch_error = function
  | NoMatch msgs -> "ParseError: " ^ String.concat "; " msgs
