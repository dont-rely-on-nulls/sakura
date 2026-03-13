type error = ParseError of string

let of_string s =
  (* ppx_sexp_conv encodes no-argument variants as bare atoms, so
     unwrap a single-element list (e.g. (GetHead)) → atom (GetHead). *)
  let sexp = match Sexplib.Sexp.of_string s with
    | Sexplib.Sexp.List [atom] -> atom
    | other -> other
  in
  match Ast.statement_of_sexp sexp with
  | stmt          -> Ok stmt
  | exception exn -> Error (ParseError (Printexc.to_string exn))

let to_string stmt =
  Ast.sexp_of_statement stmt |> Sexplib.Sexp.to_string_hum
