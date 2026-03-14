(** System catalog helpers.
    Pure definitions — no storage access, no manipulation dependency.
    Consumed by {!Manipulation.Make} to bootstrap and maintain the six
    catalog relations that make the database self-describing. *)

(* TODO: on_rel_name and timing_rel_name are defined and seeded but never
   written to by any executor. They appear to be stubs for trigger/timing
   metadata that was never implemented. *)

let catalog_prefix = "sakura:"

let relation_rel_name   = catalog_prefix ^ "relation"
let domain_rel_name     = catalog_prefix ^ "domain"
let attribute_rel_name  = catalog_prefix ^ "attribute"
let constraint_rel_name = catalog_prefix ^ "constraint"
let on_rel_name         = catalog_prefix ^ "on"
let timing_rel_name     = catalog_prefix ^ "timing"
let branch_rel_name     = catalog_prefix ^ "branch"
let head_rel_name       = catalog_prefix ^ "head"

let catalog_relation_names =
  [ relation_rel_name
  ; domain_rel_name
  ; attribute_rel_name
  ; constraint_rel_name
  ; on_rel_name
  ; timing_rel_name
  ]

let is_catalog_relation name = List.mem name catalog_relation_names

let relation_schema : Schema.t =
  Schema.empty |> Schema.add "name" "string"

let domain_schema : Schema.t =
  Schema.empty |> Schema.add "name" "string"

let attribute_schema : Schema.t =
  Schema.empty
  |> Schema.add "relation_name" "string"
  |> Schema.add "attr_name" "string"
  |> Schema.add "domain_name" "string"

let constraint_schema : Schema.t =
  Schema.empty
  |> Schema.add "name" "string"
  |> Schema.add "relation_name" "string"

let on_schema : Schema.t =
  Schema.empty |> Schema.add "event" "string"

let timing_schema : Schema.t =
  Schema.empty |> Schema.add "timing" "string"

let catalog_definitions =
  [ (relation_rel_name,   relation_schema)
  ; (domain_rel_name,     domain_schema)
  ; (attribute_rel_name,  attribute_schema)
  ; (constraint_rel_name, constraint_schema)
  ; (on_rel_name,         on_schema)
  ; (timing_rel_name,     timing_schema)
  ]

(* TODO: all tuple builders use Obj.magic to coerce string values into
   Attribute.value (Obj.t). This is the same UB pattern fixed in standard.ml;
   should use Obj.repr instead. *)

let build_relation_tuple rel_name : Tuple.materialized =
  { Tuple.relation = relation_rel_name
  ; attributes =
      Tuple.AttributeMap.singleton "name"
        { Attribute.value = Obj.magic rel_name }
  }

let build_domain_tuple dom_name : Tuple.materialized =
  { Tuple.relation = domain_rel_name
  ; attributes =
      Tuple.AttributeMap.singleton "name"
        { Attribute.value = Obj.magic dom_name }
  }

let build_attribute_tuples ~relation_name (schema : Schema.t) : Tuple.materialized list =
  List.map (fun (attr_n, dom_n) ->
    { Tuple.relation = attribute_rel_name
    ; attributes =
        Tuple.AttributeMap.of_list
          [ ("relation_name", { Attribute.value = Obj.magic relation_name })
          ; ("attr_name",     { Attribute.value = Obj.magic attr_n })
          ; ("domain_name",   { Attribute.value = Obj.magic dom_n })
          ]
    }
  ) schema

let build_constraint_tuple cname rel_name : Tuple.materialized =
  { Tuple.relation = constraint_rel_name
  ; attributes =
      Tuple.AttributeMap.of_list
        [ ("name",          { Attribute.value = Obj.magic cname })
        ; ("relation_name", { Attribute.value = Obj.magic rel_name })
        ]
  }

let build_on_tuple event : Tuple.materialized =
  { Tuple.relation = on_rel_name
  ; attributes =
      Tuple.AttributeMap.singleton "event"
        { Attribute.value = Obj.magic event }
  }

let build_timing_tuple timing : Tuple.materialized =
  { Tuple.relation = timing_rel_name
  ; attributes =
      Tuple.AttributeMap.singleton "timing"
        { Attribute.value = Obj.magic timing }
  }

