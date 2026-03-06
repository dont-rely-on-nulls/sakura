module Types (F : Ctypes.TYPE) = struct
  let hash_size = F.constant "HASH_SIZE" F.int
end
