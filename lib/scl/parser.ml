type error = ParseError of string

let of_sexp sexp =
  match Ast.statement_of_sexp sexp with
  | (Begin { limit = Some limit; _ } | Fetch { limit = Some limit; _ })
    when limit <= 0 ->
      Error (ParseError "The provision of `limit` is expected to be positive.")
  | stmt -> Ok stmt
  | exception exn -> Error (ParseError (Printexc.to_string exn))

let of_string s =
  match Sexplib.Sexp.of_string s |> Ast.statement_of_sexp with
  | (Begin { limit = Some limit; _ } | Fetch { limit = Some limit; _ })
    when limit <= 0 ->
      Error (ParseError "The provision of `limit` is expected to be positive.")
  | stmt -> Ok stmt
  | exception exn -> Error (ParseError (Printexc.to_string exn))

let to_string stmt = Ast.sexp_of_statement stmt |> Sexplib.Sexp.to_string_hum
