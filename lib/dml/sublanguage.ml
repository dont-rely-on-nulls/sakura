module Make (Storage : Management.Physical.S) = struct
  module Exec = Executor.Make (Storage)

  type storage = Storage.t
  type ast = Ast.statement
  type error = Exec.error

  let name = "dml"

  let parse_sexp sexp =
    match Parser.of_sexp sexp with
    | Ok r -> Ok r
    | Error (Parser.ParseError s) -> Error (Exec.ParseError s)

  let execute storage db ast =
    match Exec.execute storage db ast with
    | Ok db -> Ok (Sublanguage.Transition (db, "updated"))
    | Error e -> Error e

  let sexp_of_error = Exec.sexp_of_error
end

module Memory = Make (Management.Physical.Memory)
