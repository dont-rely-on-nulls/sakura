type error = ParseError of string

let of_string s =
  match Sexplib.Sexp.of_string s |> Ast.query_of_sexp with
  | q             -> Ok q
  | exception exn -> Error (ParseError (Printexc.to_string exn))

let to_string q =
  Ast.sexp_of_query q |> Sexplib.Sexp.to_string_hum
