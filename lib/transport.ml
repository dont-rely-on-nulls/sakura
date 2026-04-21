module type TRANSPORT = sig
  include Configuration.CONFIGURABLE

  type t
  type connection

  val create : configuration -> t
  val listen : t -> unit
  val accept : t -> connection
  val input : connection -> in_channel
  val output : connection -> out_channel
end

module TCP : TRANSPORT = struct
  type configuration = {
    address_family : Unix.socket_domain;
    address : string;
    port : int;
  }

  type t = Unix.file_descr
  type connection = Unix.file_descr

  let parse sexp =
    let open Sexplib.Sexp in
    match sexp with
    | List children ->
        let rec go addr port = function
          | [] -> (
              match (addr, port) with
              | Some a, Some p ->
                  Ok { address_family = Unix.PF_INET; address = a; port = p }
              | None, _ -> Error "transport/tcp: missing (address ...) field"
              | _, None -> Error "transport/tcp: missing (port ...) field")
          | List [ Atom "address"; Atom a ] :: rest -> go (Some a) port rest
          | List [ Atom "port"; Atom p ] :: rest -> (
              match int_of_string_opt p with
              | Some n -> go addr (Some n) rest
              | None ->
                  Error (Printf.sprintf "transport/tcp: invalid port: %s" p))
          | bad :: _ ->
              Error
                (Printf.sprintf "transport/tcp: unexpected: %s" (to_string bad))
        in
        go None None children
    | Atom _ -> Error "transport/tcp: expected a list of fields"

  let create { address_family; address; port } =
    let socket = Unix.socket address_family Unix.SOCK_STREAM 0 in
    Unix.setsockopt socket Unix.SO_REUSEADDR true;
    Unix.setsockopt socket Unix.SO_REUSEPORT true;
    Unix.bind socket (Unix.ADDR_INET (Unix.inet_addr_of_string address, port));
    Printf.printf "Listening on %s:%d\n%!" address port;
    socket

  let listen fd = Unix.listen fd 5

  let accept fd =
    let conn, _ = Unix.accept fd in
    conn

  let input = Unix.in_channel_of_descr
  let output = Unix.out_channel_of_descr
end
