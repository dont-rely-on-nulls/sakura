(** Envelope parser for tagged S-expressions.

    Parses commands of the form (tag payload...) where the tag selects which
    sublanguage handles the payload. The tag set is open — future sublanguages
    (tcl, ppl, acl) just add new tags. *)

type tagged = { tag : string; payload : Sexplib.Sexp.t }

let parse (input : string) : (tagged, string) Result.t =
  match Sexplib.Sexp.of_string input with
  | Sexplib.Sexp.List (Sexplib.Sexp.Atom tag :: rest) ->
      let payload =
        match rest with [ single ] -> single | many -> Sexplib.Sexp.List many
      in
      Ok { tag; payload }
  | _ -> Error "expected (tag payload...)"
  | exception exn -> Error (Printexc.to_string exn)
