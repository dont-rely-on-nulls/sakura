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

  let name = "icl"

  let parse = Parser.of_string

  let parse_sexp = Parser.of_sexp

  let execute storage db ast =
    match Executor.Memory.execute storage db ast with
    | Ok (db, msg) -> Ok (Sublanguage.Transition (db, msg))
    | Error (Executor.Memory.ManipulationError e) ->
      Error (Parser.ParseError (manip_err e))
    | Error (Executor.Memory.ConversionError s) ->
      Error (Parser.ParseError ("ConversionError: " ^ s))

  let string_of_error = function
    | Parser.ParseError s -> s
end : Sublanguage.S)
