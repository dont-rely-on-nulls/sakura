(* open Ctypes *)

module Types = Types_generated

module Functions (F : Ctypes.FOREIGN) = struct
  (* open F *)
  open Ctypes_static

  let os_extension =
    if Sys.os_type = "Unix" then
      let ic = Unix.open_process_in "uname" in
      let uname = input_line ic in
      let () = close_in ic in
      if uname = "Darwin" then "dylib" else "so"
    else "dll"

  let libmerklecpp =
    Dl.dlopen ~flags:[ RTLD_GLOBAL; RTLD_NOW ]
      ~filename:
        (Sys.getenv "LIBRELATIONAL_ENGINE_LIB_PATH"
        ^ "/librelational_engine." ^ os_extension)

  let compute_hash =
    Foreign.foreign ~from:libmerklecpp "compute_hash"
      (ptr char @-> Ctypes.string @-> returning void)

end
