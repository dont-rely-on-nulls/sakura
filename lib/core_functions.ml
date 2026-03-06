module Types = Types_generated

module Functions (F : Ctypes.FOREIGN) = struct
  open F

  let compute_hash =
    foreign "compute_hash"
      (F.(@->) (Ctypes.ptr Ctypes.char)
         (F.(@->) Ctypes.string (F.returning Ctypes.void)))

end
