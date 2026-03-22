include (struct
  type ast = Ast.query
  type error = Executor.Memory.error

  let name = "drl"

  let parse s =
    match Parser.of_string s with
    | Ok ast -> Ok ast
    | Error (Parser.ParseError s) -> Error (Executor.Memory.ParseError s)

  let parse_sexp sexp =
    match Parser.of_sexp sexp with
    | Ok ast -> Ok ast
    | Error (Parser.ParseError s) -> Error (Executor.Memory.ParseError s)

  let execute storage db ast =
    match Gate.admit db ast with
    | Error msg -> Error (Executor.Memory.ParseError msg)
    | Ok () ->
      match Executor.Memory.execute storage db ast with
      | Ok rel -> Ok (Sublanguage.Query rel)
      | Error e -> Error e

  let sexp_of_error = Executor.Memory.sexp_of_error
end : Sublanguage.S)
