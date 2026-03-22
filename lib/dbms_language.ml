(** Composable dispatch over sublanguages.

    All commands must use the tagged envelope format: (tag payload)
    where [tag] is the sublanguage name (drl, dml, ddl, icl, dcl, ...).
    Adding a new sublanguage is just extending the list passed to [create]. *)

type dispatch_error = NoMatch of Sexplib.Sexp.t

type dispatch = (module Sublanguage.S) list

let create (langs : (module Sublanguage.S) list) : dispatch = langs

let execute
    (langs : dispatch)
    (storage : Management.Physical.Memory.t)
    (db : Management.Database.t)
    (input : string)
  : (Sublanguage.result, dispatch_error) Result.t =
  match Envelope.parse input with
  | Error msg ->
    Error (NoMatch Sexplib.Sexp.(List [Atom "parse-error"; Atom msg]))
  | Ok { tag; payload } ->
    match List.find_opt (fun (module L : Sublanguage.S) -> L.name = tag) langs with
    | None ->
      Error (NoMatch Sexplib.Sexp.(List [Atom "parse-error"; Atom ("unknown sublanguage: " ^ tag)]))
    | Some (module L) ->
      match L.parse_sexp payload with
      | Error e -> Error (NoMatch (L.sexp_of_error e))
      | Ok ast ->
        match L.execute storage db ast with
        | Ok result -> Ok result
        | Error e -> Error (NoMatch (L.sexp_of_error e))

let sexp_of_dispatch_error = function
  | NoMatch sexp -> sexp
