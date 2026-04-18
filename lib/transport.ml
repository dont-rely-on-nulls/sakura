module type TRANSPORT = sig
  type t
  type connection
  type options

  val create : options -> t

  val listen : t -> unit
  val accept : t -> connection

  val input : connection -> in_channel
  val output : connection -> out_channel
end

(* TODO: move this into TCP once we have a proper configuration system *)
type tcp_options = {
  address_family : Unix.socket_domain;
  address : Unix.sockaddr
}

module TCP : (TRANSPORT with type options = tcp_options) = struct
  type t = Unix.file_descr
  type connection = Unix.file_descr
  type options = tcp_options

  let create { address_family; address } =
    let socket = Unix.socket address_family Unix.SOCK_STREAM 0 in
    Unix.setsockopt socket Unix.SO_REUSEADDR true;
    Unix.setsockopt socket Unix.SO_REUSEPORT true;
    Unix.bind socket address;
    socket

  let listen fd = Unix.listen fd 5

  let accept fd =
    let (conn, _) = Unix.accept fd in
    conn

  let input = Unix.in_channel_of_descr
  let output = Unix.out_channel_of_descr
end
