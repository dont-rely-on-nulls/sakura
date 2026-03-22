let manip_err = function
  | Manipulation.RelationNotFound s      -> "RelationNotFound: " ^ s
  | Manipulation.RelationAlreadyExists s -> "RelationAlreadyExists: " ^ s
  | Manipulation.TupleNotFound h         -> "TupleNotFound: " ^ h
  | Manipulation.DuplicateTuple h        -> "DuplicateTuple: " ^ h
  | Manipulation.ConstraintViolation s   -> "ConstraintViolation: " ^ s
  | Manipulation.StorageError s          -> "StorageError: " ^ s

include (struct
  type ast = Ast.statement
  type error = Parser.error

  let name = "dml"

  let parse = Parser.of_string

  let parse_sexp = Parser.of_sexp

  let execute storage db ast =
    match Executor.Memory.execute storage db ast with
    | Ok db -> Ok (Sublanguage.Transition (db, "updated"))
    | Error (Executor.Memory.ManipulationError e) ->
      Error (Parser.ParseError (manip_err e))
    | Error (Executor.Memory.RelationNotFound s) ->
      Error (Parser.ParseError ("RelationNotFound: " ^ s))
    | Error (Executor.Memory.ParseError s) ->
      Error (Parser.ParseError ("ParseError: " ^ s))

  let string_of_error = function
    | Parser.ParseError s -> s
end : Sublanguage.S)
