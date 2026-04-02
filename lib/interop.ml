module Sha256 = struct
  type t = string

  let compute_hash content : t = Digest.to_hex (Digest.bytes content)
end
