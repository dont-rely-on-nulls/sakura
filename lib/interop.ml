open Ctypes

module Sha256 = struct
  type t = string
  let compute_hash content: t =
    let size_with_null_terminator = C.Types.hash_size + 1 in
    let buf = Ctypes.allocate_n char ~count:size_with_null_terminator in
    let () = C.Functions.compute_hash buf (Bytes.to_string content) in
    Ctypes.coerce (ptr char) Ctypes.string buf
end
