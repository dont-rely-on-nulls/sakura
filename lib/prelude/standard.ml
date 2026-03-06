let less_than_natural: Relation.t =
  let rec generator (position: int option): Generator.result =
    let pair_of_nat n =
      let nf = float_of_int n in
      let r =
        int_of_float (floor ((1. +. sqrt (1. +. 8. *. nf)) /. 2.))
      in
      let t_prev = (r * (r - 1)) / 2 in
      let left = n - t_prev in
      (left, r) in
    match position with
    | Some position -> 
      let left, right = pair_of_nat position in
      (* let left: Attribute.t = Attribute.make_materialized ~value: (Obj.magic left) in *)
      (* let right: Attribute.t = Attribute.make_materialized ~value: (Obj.magic right) in *)
      let left = Obj.magic left in
      let right = Obj.magic right in
      let attributes: Attribute.materialized Tuple.AttributeMap.t =
        Tuple.AttributeMap.of_list [("left", left); ("right", right)]
      in Generator.Value (Tuple.make_materialized ~relation: "less_than" ~attributes: attributes, generator)
    | None -> Error "Cannot produce a randomly enumerated value for less_than over naturals."
  in let membership_criteria: Tuple.t -> bool = function 
        | Tuple.Materialized materialized -> 
            let left = Tuple.AttributeMap.find "left" materialized.attributes
            in let right = Tuple.AttributeMap.find "right" materialized.attributes
            in left < right
        | Tuple.NonMaterialized _non_materialized -> false (* TODO: Handle references later *)
  in Relation.make 
    ~hash: None
    ~name: "natural_natural_less_than"
    ~tree: None
    ~constraints: None
    ~cardinality: Conventions.Cardinality.AlephZero
    ~generator: (Some generator)
    ~membership_criteria: membership_criteria
    ~provenance: (Relation.Provenance.Base "less_than")
    ~lineage: (Relation.Lineage.Base "less_than")