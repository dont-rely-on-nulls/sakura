type scope
type 'a cursor

type error =
  | ScopeViolation
  | ScopeClosed
  | CursorClosed
  | SourceError of string

type 'a continuation = Continue of 'a | Done

val create_scope : unit -> scope
val close_scope : scope -> unit
val of_step : scope -> (unit -> ('a continuation, string) Result.t) -> 'a cursor
val of_enum : scope -> 'a BatEnum.t -> 'a cursor
val next : scope -> 'a cursor -> ('a continuation, error) Result.t
val drain : scope -> 'a cursor -> ('a list, error) Result.t
