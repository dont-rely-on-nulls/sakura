include (struct
  type ast = Ast.statement
  type error = Executor.Memory.error

  let name = "dcl"

  let parse s =
    match Parser.of_string s with
    | Ok ast -> Ok ast
    | Error (Parser.ParseError s) -> Error (Executor.Memory.ParseError s)

  let parse_sexp sexp =
    match Parser.of_sexp sexp with
    | Ok ast -> Ok ast
    | Error (Parser.ParseError s) -> Error (Executor.Memory.ParseError s)

  let execute storage db ast =
    match Executor.Memory.execute storage db ast with
    | Ok (db, msg) -> Ok (Sublanguage.Transition (db, msg))
    | Error e -> Error e

  let sexp_of_error = Executor.Memory.sexp_of_error
end : Sublanguage.S)
