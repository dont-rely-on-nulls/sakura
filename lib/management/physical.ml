(** Physical storage abstraction.

    This module defines a functor-based storage interface that can be backed
    by any key-value store (etcd, Redis, DynamoDB, PostgreSQL, filesystem, etc.).

    All data is content-addressed: keys are hashes, values are serialized objects.
    The storage is append-only - objects are never modified, only new versions
    are written with new hashes. *)

(** Backend module signature that any storage driver must implement *)
module type BACKEND = sig
  type connection
  type error

  (** Connect to the storage backend *)
  val connect : unit -> (connection, error) result

  (** Disconnect from the backend *)
  val disconnect : connection -> unit

  (** Get a value by its hash. Returns None if not found. *)
  val get : connection -> Conventions.Hash.t -> (bytes option, error) result

  (** Store a value at its hash *)
  val put : connection -> Conventions.Hash.t -> bytes -> (unit, error) result

  (** Check if a hash exists *)
  val exists : connection -> Conventions.Hash.t -> (bool, error) result

  (** Begin a transaction *)
  val begin_transaction : connection -> (unit, error) result

  (** Commit a transaction *)
  val commit : connection -> (unit, error) result

  (** Rollback a transaction *)
  val rollback : connection -> (unit, error) result

  (** Batch get multiple values *)
  val get_many : connection -> Conventions.Hash.t list -> (bytes option list, error) result

  (** Batch put multiple values *)
  val put_many : connection -> (Conventions.Hash.t * bytes) list -> (unit, error) result
end

(** Storage operations built on top of a backend *)
module type S = sig
  type t
  type error

  (** Create a storage instance from a backend connection *)
  val create : unit -> (t, error) result

  (** Close the storage *)
  val close : t -> unit

  (** Store an attribute value, returns its hash *)
  val store_attribute : t -> Conventions.AbstractValue.t -> (Conventions.Hash.t, error) result

  (** Load an attribute value by hash *)
  val load_attribute : t -> Conventions.Hash.t -> (Conventions.AbstractValue.t option, error) result

  (** Store raw bytes at a hash *)
  val store_raw : t -> Conventions.Hash.t -> bytes -> (unit, error) result

  (** Load raw bytes by hash *)
  val load_raw : t -> Conventions.Hash.t -> (bytes option, error) result

  (** Check if a hash exists *)
  val exists : t -> Conventions.Hash.t -> (bool, error) result

  (** Execute a function within a transaction *)
  val with_transaction : t -> (unit -> ('a, error) result) -> ('a, error) result
end

(** Functor to create storage operations from a backend *)
module Make (B : BACKEND) : S with type error = B.error = struct
  type t = B.connection
  type error = B.error

  let create () = B.connect ()

  let close = B.disconnect

  let store_attribute conn value =
    let bytes = Marshal.to_bytes value [] in
    let hash = Conventions.AbstractValue.hash value in
    match B.put conn hash bytes with
    | Ok () -> Ok hash
    | Error e -> Error e

  let load_attribute conn hash =
    match B.get conn hash with
    | Ok (Some bytes) -> Ok (Some (Marshal.from_bytes bytes 0))
    | Ok None -> Ok None
    | Error e -> Error e

  let store_raw conn hash bytes =
    B.put conn hash bytes

  let load_raw conn hash =
    B.get conn hash

  let exists conn hash =
    B.exists conn hash

  let with_transaction conn f =
    match B.begin_transaction conn with
    | Error e -> Error e
    | Ok () ->
      match f () with
      | Ok result ->
        (match B.commit conn with
         | Ok () -> Ok result
         | Error e -> Error e)
      | Error e ->
        let _ = B.rollback conn in
        Error e
end

(** In-memory backend for testing and development *)
module MemoryBackend : BACKEND with type error = string = struct
  type connection = {
    data : (string, bytes) Hashtbl.t;
    mutable in_transaction : bool;
    mutable transaction_buffer : (string * bytes) list;
  }
  type error = string

  let connect () =
    Ok {
      data = Hashtbl.create 1024;
      in_transaction = false;
      transaction_buffer = [];
    }

  let disconnect _ = ()

  let get conn hash =
    Ok (Hashtbl.find_opt conn.data hash)

  let put conn hash value =
    if conn.in_transaction then begin
      conn.transaction_buffer <- (hash, value) :: conn.transaction_buffer;
      Ok ()
    end else begin
      Hashtbl.replace conn.data hash value;
      Ok ()
    end

  let exists conn hash =
    Ok (Hashtbl.mem conn.data hash)

  let begin_transaction conn =
    if conn.in_transaction then
      Error "Already in transaction"
    else begin
      conn.in_transaction <- true;
      conn.transaction_buffer <- [];
      Ok ()
    end

  let commit conn =
    if not conn.in_transaction then
      Error "Not in transaction"
    else begin
      List.iter (fun (hash, value) ->
        Hashtbl.replace conn.data hash value
      ) conn.transaction_buffer;
      conn.in_transaction <- false;
      conn.transaction_buffer <- [];
      Ok ()
    end

  let rollback conn =
    if not conn.in_transaction then
      Error "Not in transaction"
    else begin
      conn.in_transaction <- false;
      conn.transaction_buffer <- [];
      Ok ()
    end

  let get_many conn hashes =
    Ok (List.map (fun h -> Hashtbl.find_opt conn.data h) hashes)

  let put_many conn pairs =
    List.iter (fun (hash, value) ->
      if conn.in_transaction then
        conn.transaction_buffer <- (hash, value) :: conn.transaction_buffer
      else
        Hashtbl.replace conn.data hash value
    ) pairs;
    Ok ()
end

(** Default in-memory storage for development *)
module Memory = Make(MemoryBackend)
