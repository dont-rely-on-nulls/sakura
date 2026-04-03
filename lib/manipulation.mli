type error =
  | RelationNotFound of string
  | RelationAlreadyExists of string
  | TupleNotFound of Conventions.Hash.t
  | DuplicateTuple of Conventions.Hash.t
  | ConstraintViolation of string
  | StorageError of string

val string_of_error : error -> string
val sexp_of_error : error -> Sexplib.Sexp.t

type 'a result = ('a, error) Result.t

module Schema = Schema

val hash_tuple : Tuple.materialized -> Conventions.Hash.t

module Make (Storage : Management.Physical.S) : sig
  type storage = Storage.t
  type nonrec error = error

  val of_string_error : string -> error

  val store_database :
    storage -> Management.Database.t -> (unit, error) Result.t

  val load_relation :
    storage -> Conventions.Hash.t -> (Relation.t option, error) Result.t

  val load_database :
    storage ->
    Conventions.Hash.t ->
    (Management.Database.t option, error) Result.t

  val load_tuple :
    storage -> Conventions.Hash.t -> (Tuple.materialized option, error) Result.t

  val load_tuples :
    storage ->
    Conventions.Hash.t list ->
    (Tuple.materialized list, error) Result.t

  val create_tuple :
    storage ->
    Management.Database.t ->
    Relation.t ->
    Tuple.materialized ->
    (Management.Database.t * Relation.t * Conventions.Hash.t, error) Result.t

  val create_tuples :
    storage ->
    Management.Database.t ->
    Relation.t ->
    Tuple.materialized list ->
    ( Management.Database.t * Relation.t * Conventions.Hash.t list,
      error )
    Result.t

  val retract_tuple :
    storage ->
    Management.Database.t ->
    Relation.t ->
    tuple_hash:Conventions.Hash.t ->
    (Management.Database.t * Relation.t, error) Result.t

  val register_domain :
    storage ->
    Management.Database.t ->
    Domain.t ->
    (Management.Database.t, error) Result.t

  val create_database :
    storage -> name:string -> (Management.Database.t, error) Result.t

  val database_history : Management.Database.t -> Conventions.Hash.t list

  val create_relation :
    storage ->
    Management.Database.t ->
    name:string ->
    schema:Schema.t ->
    (Management.Database.t * Relation.t, error) Result.t

  val create_immutable_relation :
    storage ->
    Management.Database.t ->
    name:string ->
    schema:Schema.t ->
    generator:Generator.t ->
    membership_criteria:(Tuple.t -> bool) ->
    cardinality:Conventions.Cardinality.t ->
    (Management.Database.t * Relation.t, error) Result.t

  val retract_relation :
    storage ->
    Management.Database.t ->
    name:string ->
    Management.Database.t result

  val clear_relation :
    storage ->
    Management.Database.t ->
    Relation.t ->
    (Management.Database.t * Relation.t, error) Result.t

  val register_constraint :
    storage ->
    Management.Database.t ->
    constraint_name:string ->
    relation_name:string ->
    body:Constraint.t ->
    (Management.Database.t, error) Result.t

  val tuple_hashes : Relation.t -> Conventions.Hash.t list
  val tuple_hash_seq : Relation.t -> Conventions.Hash.t Seq.t
  val get_relation : Management.Database.t -> name:string -> Relation.t option
  val tuple_count : Relation.t -> int
  val tuple_exists : Relation.t -> Conventions.Hash.t -> bool

  val attach_constraint :
    storage ->
    Management.Database.t ->
    constraint_name:string ->
    relation_name:string ->
    body:Constraint.t ->
    timing:Constraint.timing ->
    (Management.Database.t, error) Result.t

  val check_deferred_constraints :
    storage -> Management.Database.t -> (unit, error) Result.t

  val commit :
    storage -> Management.Database.t -> (Management.Database.t, error) Result.t
end

module Memory : sig
  include module type of Make (Management.Physical.Memory)
end
