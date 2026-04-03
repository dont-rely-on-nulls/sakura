type scope = { id : int; mutable active : bool }

type 'a cursor = {
  scope_id : int;
  mutable closed : bool;
  step : unit -> ('a option, string) Result.t;
}

type error = ScopeViolation | ScopeClosed | CursorClosed

let next_scope_id =
  let counter = ref 0 in
  fun () ->
    let id = !counter in
    incr counter;
    id

let create_scope () = { id = next_scope_id (); active = true }
let close_scope scope = scope.active <- false
let of_step (scope : scope) step = { scope_id = scope.id; closed = false; step }

let of_seq scope seq =
  let state = ref seq in
  of_step scope (fun () ->
      match !state () with
      | Seq.Nil -> Ok None
      | Seq.Cons (x, tl) ->
          state := tl;
          Ok (Some x))

let next (scope : scope) (cursor : 'a cursor) =
  if not scope.active then Ok (Error ScopeClosed)
  else if cursor.closed then Ok (Error CursorClosed)
  else if cursor.scope_id <> scope.id then Ok (Error ScopeViolation)
  else
    match cursor.step () with
    | Error e -> Error e
    | Ok None ->
        cursor.closed <- true;
        Ok (Ok None)
    | Ok (Some x) -> Ok (Ok (Some x))

let drain (scope : scope) (cursor : 'a cursor) =
  let rec go acc =
    match next scope cursor with
    | Error e -> Error e
    | Ok (Error err) -> Ok (Error err)
    | Ok (Ok None) -> Ok (Ok (List.rev acc))
    | Ok (Ok (Some x)) -> go (x :: acc)
  in
  go []
