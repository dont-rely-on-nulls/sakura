include (struct
  type ast = Ast.statement
  type error = Parser.error

  let name = "dcl"

  let parse = Parser.of_string

  let parse_sexp = Parser.of_sexp

  let execute storage db ast =
    match Executor.Memory.execute storage db ast with
    | Ok (db, msg) -> Ok (Sublanguage.Transition (db, msg))
    | Error (Executor.Memory.BranchError s) ->
      Error (Parser.ParseError ("BranchError: " ^ s))
    | Error (Executor.Memory.MergeError s) ->
      Error (Parser.ParseError ("MergeError: " ^ s))

  let string_of_error = function
    | Parser.ParseError s -> s
end : Sublanguage.S)
