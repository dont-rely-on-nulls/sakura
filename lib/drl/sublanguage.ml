include (struct
  type ast = Ast.query
  type error = Parser.error

  let name = "drl"

  let parse = Parser.of_string

  let parse_sexp = Parser.of_sexp

  let execute storage db ast =
    match Gate.admit db ast with
    | Error msg -> Error (Parser.ParseError msg)
    | Ok () ->
      match Executor.Memory.execute storage db ast with
      | Ok rel -> Ok (Sublanguage.Query rel)
      | Error (Executor.Memory.RelationNotFound s) ->
        Error (Parser.ParseError ("RelationNotFound: " ^ s))
      | Error (Executor.Memory.AlgebraError (Algebra.StorageError s)) ->
        Error (Parser.ParseError ("StorageError: " ^ s))
      | Error (Executor.Memory.AlgebraError (Algebra.GeneratorError s)) ->
        Error (Parser.ParseError ("GeneratorError: " ^ s))

  let string_of_error = function
    | Parser.ParseError s -> s
end : Sublanguage.S)
