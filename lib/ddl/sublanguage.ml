module Make (Storage : Management.Physical.S) = struct
  module Exec = Executor.Make(Storage)

  type storage = Storage.t
  type ast = Ast.statement
  type error = Exec.error

  let name = "ddl"

  let parse s =
    match Parser.of_string s with
    | Ok ast -> Ok ast
    | Error (Parser.ParseError s) -> Error (Exec.ParseError s)

  let parse_sexp sexp =
    match Parser.of_sexp sexp with
    | Ok ast -> Ok ast
    | Error (Parser.ParseError s) -> Error (Exec.ParseError s)

  let execute storage db ast =
    match Exec.execute storage db ast with
    | Ok (db, msg) -> Ok (Sublanguage.Transition (db, msg))
    | Error e -> Error e

  let sexp_of_error = Exec.sexp_of_error
end

module Memory = Make(Management.Physical.Memory)
