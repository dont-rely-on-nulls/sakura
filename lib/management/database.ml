module RelationMap = Map.Make (String)

type t = {
  hash : Conventions.Hash.t;
  name : Conventions.Name.t;
  tree : unit;
  relations : Relation.t RelationMap.t;
  timestamp : float;
}

let create_database (storage: Physical.Storage.t) ~name =
  let relations =
    RelationMap.of_list [("less_than", Prelude.Standard.less_than_natural)]
  in
  let state =
    {
      hash = "";
      name;
      tree = ();
      relations = relations;
      timestamp = Unix.time ();
    }
  in Physical.Storage.write storage
