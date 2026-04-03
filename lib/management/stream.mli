type scope
type 'a cursor
type error = ScopeViolation | ScopeClosed | CursorClosed

val create_scope : unit -> scope
val close_scope : scope -> unit
val of_step : scope -> (unit -> ('a option, string) Result.t) -> 'a cursor
val of_enum : scope -> 'a BatEnum.t -> 'a cursor
val next : scope -> 'a cursor -> (('a option, error) Result.t, string) Result.t
val drain : scope -> 'a cursor -> (('a list, error) Result.t, string) Result.t
