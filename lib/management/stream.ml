type scope = { id : int; mutable active : bool }
type 'a continuation = Continue of 'a | Done

type 'a cursor = {
  scope_id : int;
  mutable closed : bool;
  step : unit -> ('a continuation, string) Result.t;
}

type error =
  | ScopeViolation
  | ScopeClosed
  | CursorClosed
  | SourceError of string

module FingerTree = BatFingerTree

let next_scope_id =
  let counter = ref 0 in
  fun () ->
    let id = !counter in
    incr counter;
    id

let create_scope () = { id = next_scope_id (); active = true }
let close_scope scope = scope.active <- false
let of_step (scope : scope) step = { scope_id = scope.id; closed = false; step }

let of_enum scope enum =
  of_step scope (fun () ->
      match BatEnum.get enum with None -> Ok Done | Some x -> Ok (Continue x))

let next (scope : scope) (cursor : 'a cursor) =
  if not scope.active then Error ScopeClosed
  else if cursor.closed then Error CursorClosed
  else if cursor.scope_id <> scope.id then Error ScopeViolation
  else
    match cursor.step () with
    | Error e -> Error (SourceError e)
    | Ok Done ->
        cursor.closed <- true;
        Ok Done
    | Ok (Continue x) -> Ok (Continue x)

let drain (scope : scope) (cursor : 'a cursor) =
  let rec go acc =
    match next scope cursor with
    | Error err -> Error err
    | Ok Done -> Ok (BatList.of_enum (FingerTree.enum acc))
    | Ok (Continue x) -> go (FingerTree.snoc acc x)
  in
  go FingerTree.empty
