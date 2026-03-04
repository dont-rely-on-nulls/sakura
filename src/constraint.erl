-module(constraint).

-include("../include/operations.hrl").

-export([
    less_than/2,
    less_than_or_equal/2,
    greater_than/2,
    greater_than_or_equal/2,
    equal/2,
    not_equal/2,
    plus/2,
    times/2,
    minus/2,
    divide/2,
    member/2,
    member/3,
    'not'/2,
    'and'/2,
    'or'/2,
    resolve/4,
    validate_tuple/3,
    validate_tuple_against_schema/3,
    build_membership_criteria/2,
    lt/2,
    lte/2,
    gt/2,
    gte/2,
    eq/2,
    neq/2,
    between/2,
    example_supertype_exclusive_constraints/0,
    example_order_item_references_constraints/0,
    example_manager_constraints/0,
    example_ticket_status_exclusive_constraints/0,
    example_weak_entity_dependency_constraints/0,
    example_department_has_employee_constraints/0,
    example_symmetry_antisymmetry_constraints/0,
    run_example_supertype_exclusive/0,
    run_example_order_item_references/0,
    run_example_manager_constraints/0,
    run_example_ticket_status_exclusive/0,
    run_example_weak_entity_dependency/0,
    run_example_department_has_employee/0,
    run_example_symmetry_antisymmetry/0,
    run_all_examples/0,
    empty_constraints/0,
    merge_constraints/2,
    filter_constraints/2,
    rename_constraint_attrs/2,
    infer_constraints_from_pred/2,
    get_domain_from_db/2
]).

-type relational_constraint() ::
    {member_of, relation_name() | #relation{} | #domain{}, binding()}
  | {'not', relational_constraint()}
  | {'not', relational_constraint(), relation_name()}
  | {'and', [relational_constraint()]}
  | {'or', [relational_constraint()]}
  | {exists, atom(), relation_name(), relational_constraint()}
  | {forall, atom(), relation_name(), relational_constraint()}.

-type relation_name() :: atom().
-type binding() :: #{atom() => term() | {var, atom()} | {const, term()}}.

-export_type([relational_constraint/0, binding/0]).

-spec less_than(atom(), cardinality()) -> #relation{}.
less_than(DomainName, Cardinality) ->
    #relation{
        hash = undefined,
        name = less_than,
        tree = undefined,
        schema = #{left => DomainName, right => DomainName},
        constraints = empty_constraints(),
        cardinality = Cardinality,
        generator = {comparison, '<', DomainName},
        membership_criteria = #{
            intension => fun(#{left := L, right := R}) -> L < R end
        },
        provenance = undefined,
        lineage = {base, less_than}
    }.

-spec less_than_or_equal(atom(), cardinality()) -> #relation{}.
less_than_or_equal(DomainName, Cardinality) ->
    #relation{
        hash = undefined,
        name = less_than_or_equal,
        tree = undefined,
        schema = #{left => DomainName, right => DomainName},
        constraints = empty_constraints(),
        cardinality = Cardinality,
        generator = {comparison, '=<', DomainName},
        membership_criteria = #{
            intension => fun(#{left := L, right := R}) -> L =< R end
        },
        provenance = undefined,
        lineage = {base, less_than_or_equal}
    }.

-spec greater_than(atom(), cardinality()) -> #relation{}.
greater_than(DomainName, Cardinality) ->
    #relation{
        hash = undefined,
        name = greater_than,
        tree = undefined,
        schema = #{left => DomainName, right => DomainName},
        constraints = empty_constraints(),
        cardinality = Cardinality,
        generator = {comparison, '>', DomainName},
        membership_criteria = #{
            intension => fun(#{left := L, right := R}) -> L > R end
        },
        provenance = undefined,
        lineage = {base, greater_than}
    }.

-spec greater_than_or_equal(atom(), cardinality()) -> #relation{}.
greater_than_or_equal(DomainName, Cardinality) ->
    #relation{
        hash = undefined,
        name = greater_than_or_equal,
        tree = undefined,
        schema = #{left => DomainName, right => DomainName},
        constraints = empty_constraints(),
        cardinality = Cardinality,
        generator = {comparison, '>=', DomainName},
        membership_criteria = #{
            intension => fun(#{left := L, right := R}) -> L >= R end
        },
        provenance = undefined,
        lineage = {base, greater_than_or_equal}
    }.

-spec equal(atom(), cardinality()) -> #relation{}.
equal(DomainName, Cardinality) ->
    #relation{
        hash = undefined,
        name = equal,
        tree = undefined,
        schema = #{left => DomainName, right => DomainName},
        constraints = empty_constraints(),
        cardinality = Cardinality,
        generator = {comparison, '=:=', DomainName},
        membership_criteria = #{
            intension => fun(#{left := L, right := R}) -> L =:= R end
        },
        provenance = undefined,
        lineage = {base, equal}
    }.

-spec not_equal(atom(), cardinality()) -> #relation{}.
not_equal(DomainName, Cardinality) ->
    #relation{
        hash = undefined,
        name = not_equal,
        tree = undefined,
        schema = #{left => DomainName, right => DomainName},
        constraints = empty_constraints(),
        cardinality = Cardinality,
        generator = {comparison, '/=', DomainName},
        membership_criteria = #{
            intension => fun(#{left := L, right := R}) -> L /= R end
        },
        provenance = undefined,
        lineage = {base, not_equal}
    }.

-spec plus(atom(), cardinality()) -> #relation{}.
plus(DomainName, Cardinality) ->
    #relation{
        hash = undefined,
        name = plus,
        tree = undefined,
        schema = #{a => DomainName, b => DomainName, sum => DomainName},
        constraints = empty_constraints(),
        cardinality = Cardinality,
        generator = {arithmetic, plus, DomainName},
        membership_criteria = #{
            intension => fun(#{a := A, b := B, sum := S}) ->
                is_number(A) andalso is_number(B) andalso is_number(S) andalso (A + B =:= S)
            end
        },
        provenance = undefined,
        lineage = {base, plus}
    }.

-spec times(atom(), cardinality()) -> #relation{}.
times(DomainName, Cardinality) ->
    #relation{
        hash = undefined,
        name = times,
        tree = undefined,
        schema = #{a => DomainName, b => DomainName, product => DomainName},
        constraints = empty_constraints(),
        cardinality = Cardinality,
        generator = {arithmetic, times, DomainName},
        membership_criteria = #{
            intension => fun(#{a := A, b := B, product := P}) ->
                is_number(A) andalso is_number(B) andalso is_number(P) andalso (A * B =:= P)
            end
        },
        provenance = undefined,
        lineage = {base, times}
    }.

-spec minus(atom(), cardinality()) -> #relation{}.
minus(DomainName, Cardinality) ->
    #relation{
        hash = undefined,
        name = minus,
        tree = undefined,
        schema = #{a => DomainName, b => DomainName, difference => DomainName},
        constraints = empty_constraints(),
        cardinality = Cardinality,
        generator = {arithmetic, minus, DomainName},
        membership_criteria = #{
            intension => fun(#{a := A, b := B, difference := D}) ->
                is_number(A) andalso is_number(B) andalso is_number(D) andalso (A - B =:= D)
            end
        },
        provenance = undefined,
        lineage = {base, minus}
    }.

-spec divide(atom(), cardinality()) -> #relation{}.
divide(DomainName, Cardinality) ->
    #relation{
        hash = undefined,
        name = divide,
        tree = undefined,
        schema = #{a => DomainName, b => DomainName, quotient => DomainName, remainder => DomainName},
        constraints = empty_constraints(),
        cardinality = Cardinality,
        generator = {arithmetic, divide, DomainName},
        membership_criteria = #{
            intension => fun(#{a := A, b := B, quotient := Q, remainder := R}) ->
                is_integer(A) andalso is_integer(B) andalso is_integer(Q) andalso is_integer(R)
                andalso B =/= 0
                andalso (A div B =:= Q)
                andalso (A rem B =:= R)
            end
        },
        provenance = undefined,
        lineage = {base, divide}
    }.

-spec member(map(), #domain{} | #relation{}) -> boolean().
member(Tuple, #relation{membership_criteria = #{intension := Intension}}) ->
    Intension(Tuple);
member(Tuple, #relation{membership_criteria = #{test := TestFun}}) ->
    TestFun(Tuple);
member(Tuple, #relation{hash = RelationHash, tree = Tree}) when Tree =/= undefined ->
    BindingHashes = binding_to_hashes(Tuple),
    TupleHashes = operations:hashes_from_tuple(RelationHash),
    lists:any(
      fun(TupleHash) ->
          tuple_hash_matches_binding(TupleHash, BindingHashes)
      end,
      TupleHashes);
member(_Tuple, #relation{tree = undefined, generator = Generator}) when Generator =/= undefined ->
    false;
member(_Tuple, #relation{}) ->
    false;
member(Tuple, #domain{membership_criteria = #{test := TestFun}}) ->
    TestFun(Tuple);
member(Tuple, #domain{membership_criteria = Criteria}) when is_map(Criteria) ->
    validate_tuple_against_criteria(Tuple, Criteria).

binding_to_hashes(Binding) when is_map(Binding) ->
    maps:map(
      fun(_Attr, Value) ->
          operations:value_hash(Value)
      end,
      Binding).

tuple_hash_matches_binding(TupleHash, BindingHashes) ->
    [#tuple{attribute_map = AttributeMap}] = mnesia:dirty_read(tuple, TupleHash),
    tuple_matches_binding_hashes(BindingHashes, AttributeMap).

tuple_matches_binding_hashes(BindingHashes, TupleAttributeMap)
  when is_map(BindingHashes), is_map(TupleAttributeMap) ->
    maps:fold(
      fun(Key, ValueHash, Acc) ->
          Acc andalso maps:get(Key, TupleAttributeMap, undefined) =:= ValueHash
      end,
      true,
      BindingHashes).

-spec member(binding(), #domain{} | #relation{}, map()) -> {true, map()} | false.
member(Binding, Relation, Substitution) ->
    BoundTuple = apply_substitution(Binding, Substitution),
    case find_variables(BoundTuple) of
        [] ->
            case member(BoundTuple, Relation) of
                true -> {true, Substitution};
                false -> false
            end;
        _ ->
            false
    end.

-spec 'not'(#relation{}, #relation{}) -> #relation{}.
'not'(R, Universe) ->
    RName = get_name(R),
    UniverseName = get_name(Universe),
    ResultName = list_to_atom("not_" ++ atom_to_list(RName) ++ "_in_" ++ atom_to_list(UniverseName)),
    #relation{
        hash = undefined,
        name = ResultName,
        tree = undefined,
        schema = get_schema(R),
        constraints = empty_constraints(),
        cardinality = complement_cardinality(R, Universe),
        generator = {complement, R, Universe},
        membership_criteria = #{
            intension => fun(Tuple) ->
                member(Tuple, Universe) andalso not member(Tuple, R)
            end
        },
        provenance = undefined,
        lineage = {complement, get_lineage(R), get_lineage(Universe)}
    }.

-spec 'and'(#relation{}, #relation{}) -> #relation{}.
'and'(R1, R2) ->
    Schema1 = get_schema(R1),
    Schema2 = get_schema(R2),
    case schemas_compatible(Schema1, Schema2) of
        false ->
            error({incompatible_schemas, Schema1, Schema2});
        true ->
            R1Name = get_name(R1),
            R2Name = get_name(R2),
            ResultName = list_to_atom(atom_to_list(R1Name) ++ "_and_" ++ atom_to_list(R2Name)),
            #relation{
                hash = undefined,
                name = ResultName,
                tree = undefined,
                schema = merge_schemas(Schema1, Schema2),
                constraints = empty_constraints(),
                cardinality = intersection_cardinality(R1, R2),
                generator = {intersection, R1, R2},
                membership_criteria = #{
                    intension => fun(Tuple) ->
                        member(Tuple, R1) andalso member(Tuple, R2)
                    end
                },
                provenance = undefined,
                lineage = {intersection, get_lineage(R1), get_lineage(R2)}
            }
    end.

-spec 'or'(#relation{}, #relation{}) -> #relation{}.
'or'(R1, R2) ->
    Schema1 = get_schema(R1),
    Schema2 = get_schema(R2),
    case schemas_compatible(Schema1, Schema2) of
        false ->
            error({incompatible_schemas, Schema1, Schema2});
        true ->
            R1Name = get_name(R1),
            R2Name = get_name(R2),
            ResultName = list_to_atom(atom_to_list(R1Name) ++ "_or_" ++ atom_to_list(R2Name)),
            #relation{
                hash = undefined,
                name = ResultName,
                tree = undefined,
                schema = merge_schemas(Schema1, Schema2),
                constraints = empty_constraints(),
                cardinality = union_cardinality(R1, R2),
                generator = {union, R1, R2},
                membership_criteria = #{
                    intension => fun(Tuple) ->
                        member(Tuple, R1) orelse member(Tuple, R2)
                    end
                },
                provenance = undefined,
                lineage = {union, get_lineage(R1), get_lineage(R2)}
            }
    end.

-spec resolve(#database_state{}, atom(), #domain{} | #relation{} | relational_constraint(), map()) ->
    {ok, boolean()} | {error, term()}.
resolve(_Database, domain, #domain{} = Domain, Values) ->
    {ok, member(Values, Domain)};
resolve(_Database, relation, #relation{} = Relation, Values) ->
    {ok, member(Values, Relation)};
resolve(Database, constraint, Constraint, Binding) ->
    evaluate_constraint(Database, Constraint, Binding).

-spec validate_tuple(#database_state{}, map(), #relation{}) -> ok | {error, term()}.
validate_tuple(Database, Tuple, #relation{schema = Schema, constraints = Constraints}) ->
    case validate_tuple_against_schema(Database, Tuple, Schema) of
        ok ->
            validate_tuple_against_constraints(Database, Tuple, Constraints);
        Error ->
            Error
    end.

-spec validate_tuple_against_schema(#database_state{}, map(), map()) -> ok | {error, term()}.
validate_tuple_against_schema(Database, Tuple, Schema) ->
    SchemaAttrs = maps:keys(Schema),
    TupleAttrs = maps:keys(Tuple),
    Missing = SchemaAttrs -- TupleAttrs,
    case Missing of
        [] ->
            Extra = TupleAttrs -- SchemaAttrs,
            case Extra of
                [] ->
                    validate_all_attributes(Database, Tuple, Schema);
                _ ->
                    {error, {extra_attributes, Extra}}
            end;
        _ ->
            {error, {missing_attributes, Missing}}
    end.

validate_all_attributes(Database, Tuple, Schema) ->
    maps:fold(
        fun(AttrName, DomainName, Acc) ->
            case Acc of
                {error, _} = Err ->
                    Err;
                ok ->
                    Value = maps:get(AttrName, Tuple),
                    case get_domain_from_db(Database, DomainName) of
                        {ok, Domain} ->
                            case member(#{value => Value}, Domain) of
                                true -> ok;
                                false -> {error, {not_in_domain, AttrName, Value, DomainName}}
                            end;
                        {error, not_found} ->
                            {error, {unknown_domain, AttrName, DomainName}}
                    end
            end
        end,
        ok,
        Schema
    ).

validate_tuple_against_constraints(_Database, _Tuple, undefined) ->
    ok;
validate_tuple_against_constraints(Database, Tuple, Constraints) ->
    Normalized = normalize_constraints(Constraints),
    NamedConstraints = Normalized#relation_constraints.constraints,
    case evaluate_named_constraints(Database, NamedConstraints, Tuple) of
        {true, _Violations} ->
            ok;
        {false, Violations} ->
            {error, {constraint_violations, Violations}}
    end.

evaluate_named_constraints(_Database, [], _Context) ->
    {true, []};
evaluate_named_constraints(Database, NamedConstraints, Context) ->
    ViolationsRev = lists:foldl(
      fun({Name, Formula}, Acc) ->
          {Result, Diagnostics} = evaluate_constraint_with_diagnostics(Database, Formula, Context),
          case Result of
              true ->
                  Acc;
              false ->
                  [#{constraint_name => Name,
                     formula => Formula,
                     diagnostics => Diagnostics} | Acc]
          end
      end,
      [],
      NamedConstraints),
    case ViolationsRev of
        [] -> {true, []};
        _ -> {false, lists:reverse(ViolationsRev)}
    end.

-spec evaluate_constraint_list(#database_state{}, [term()], map()) -> {boolean(), [map()]}.
evaluate_constraint_list(_Database, [], _Context) ->
    {true, []};
evaluate_constraint_list(Database, Constraints, Context) ->
    {AllTrue, DiagnosticsRev} = lists:foldl(
        fun(ConstraintTerm, {AccTrue, AccDiags}) ->
            {Result, Diags} = evaluate_constraint_with_diagnostics(Database, ConstraintTerm, Context),
            {AccTrue andalso Result, lists:reverse(Diags) ++ AccDiags}
        end,
        {true, []},
        Constraints
    ),
    {AllTrue, lists:reverse(DiagnosticsRev)}.

-spec evaluate_constraint_with_diagnostics(#database_state{}, term(), map()) -> {boolean(), [map()]}.
evaluate_constraint_with_diagnostics(Database, {member_of, RelName, Binding}, Context)
  when is_atom(RelName), is_map(Binding) ->
    BoundBinding = bind_variables(Binding, Context),
    case get_domain_from_db(Database, RelName) of
        {ok, Relation} ->
            case member(BoundBinding, Relation) of
                true -> {true, []};
                false ->
                    {false, [#{kind => member_of_false, relation => RelName, binding => BoundBinding}]}
            end;
        {error, not_found} ->
            {false, [#{kind => relation_not_found, relation => RelName, binding => BoundBinding}]}
    end;
evaluate_constraint_with_diagnostics(_Database, {member_of, #relation{} = Rel, Binding}, Context) ->
    BoundBinding = bind_variables(Binding, Context),
    case member(BoundBinding, Rel) of
        true -> {true, []};
        false -> {false, [#{kind => member_of_false, relation => get_name(Rel), binding => BoundBinding}]}
    end;
evaluate_constraint_with_diagnostics(_Database, {member_of, #domain{} = Rel, Binding}, Context) ->
    BoundBinding = bind_variables(Binding, Context),
    case member(BoundBinding, Rel) of
        true -> {true, []};
        false -> {false, [#{kind => member_of_false, relation => get_name(Rel), binding => BoundBinding}]}
    end;
evaluate_constraint_with_diagnostics(Database, {'and', Constraints}, Context) ->
    evaluate_constraint_list(Database, Constraints, Context);
evaluate_constraint_with_diagnostics(Database, {'or', Constraints}, Context) ->
    {AnyTrue, DiagnosticsRev} = lists:foldl(
        fun(ConstraintTerm, {AccTrue, AccDiags}) ->
            {Result, Diags} = evaluate_constraint_with_diagnostics(Database, ConstraintTerm, Context),
            {AccTrue orelse Result, lists:reverse(Diags) ++ AccDiags}
        end,
        {false, []},
        Constraints
    ),
    {AnyTrue, lists:reverse(DiagnosticsRev)};
evaluate_constraint_with_diagnostics(Database, {'not', Constraint}, Context) ->
    {Result, Diagnostics} = evaluate_constraint_with_diagnostics(Database, Constraint, Context),
    {not Result, Diagnostics};
evaluate_constraint_with_diagnostics(Database, {'not', Constraint, _Universe}, Context) ->
    {Result, Diagnostics} = evaluate_constraint_with_diagnostics(Database, Constraint, Context),
    {not Result, Diagnostics};
evaluate_constraint_with_diagnostics(Database, {exists, VarName, QuantifierRel, Constraint}, Context)
  when is_atom(VarName), is_atom(QuantifierRel) ->
    evaluate_quantifier_with_diagnostics(Database, exists, VarName, QuantifierRel, Constraint, Context);
evaluate_constraint_with_diagnostics(Database, {forall, VarName, QuantifierRel, Constraint}, Context)
  when is_atom(VarName), is_atom(QuantifierRel) ->
    evaluate_quantifier_with_diagnostics(Database, forall, VarName, QuantifierRel, Constraint, Context);
evaluate_constraint_with_diagnostics(_Database, Constraint, _Context) ->
    {false, [#{kind => unsupported_constraint, constraint => Constraint}]}.

evaluate_quantifier_with_diagnostics(Database, Quantifier, VarName, QuantifierRel, Constraint, Context) ->
    case get_domain_from_db(Database, QuantifierRel) of
        {error, not_found} ->
            {false, [#{kind => quantifier_relation_not_found,
                       quantifier => Quantifier,
                       variable => VarName,
                       relation => QuantifierRel}]};
        {ok, Relation} ->
            case Relation#relation.cardinality of
                {finite, _} ->
                    evaluate_finite_quantifier(Database, Quantifier, VarName, QuantifierRel, Constraint, Context);
                _ ->
                    {false, [#{kind => unbounded_quantifier,
                               quantifier => Quantifier,
                               variable => VarName,
                               relation => QuantifierRel}]}
            end
    end.

evaluate_finite_quantifier(Database, Quantifier, VarName, QuantifierRel, Constraint, Context) ->
    Iterator = operations:get_tuples_iterator(Database, QuantifierRel),
    case operations:collect_all(Iterator) of
        {error, Reason, _Partial} ->
            {false, [#{kind => quantifier_iteration_error,
                       quantifier => Quantifier,
                       variable => VarName,
                       relation => QuantifierRel,
                       reason => Reason}]};
        Tuples when is_list(Tuples) ->
            evaluate_quantifier_tuples(Database, Quantifier, VarName, Constraint, Context, Tuples)
    end.

evaluate_quantifier_tuples(_Database, exists, _VarName, _Constraint, _Context, []) ->
    {false, []};
evaluate_quantifier_tuples(_Database, forall, _VarName, _Constraint, _Context, []) ->
    {true, []};
evaluate_quantifier_tuples(Database, Quantifier, VarName, Constraint, Context, Tuples) ->
    {ResultsRev, DiagnosticsRev} = lists:foldl(
        fun(TupleValueMap, {AccResults, AccDiags}) ->
            case extract_quantifier_value(TupleValueMap) of
                {ok, QuantifiedValue} ->
                    ScopedContext = Context#{VarName => QuantifiedValue},
                    {Result, Diags} = evaluate_constraint_with_diagnostics(Database, Constraint, ScopedContext),
                    {[Result | AccResults], lists:reverse(Diags) ++ AccDiags};
                {error, Reason} ->
                    {[false | AccResults], [#{kind => invalid_quantifier_tuple,
                                              variable => VarName,
                                              tuple => TupleValueMap,
                                              reason => Reason} | AccDiags]}
            end
        end,
        {[], []},
        Tuples
    ),
    Results = lists:reverse(ResultsRev),
    Diagnostics = lists:reverse(DiagnosticsRev),
    case Quantifier of
        exists -> {lists:any(fun(X) -> X end, Results), Diagnostics};
        forall -> {lists:all(fun(X) -> X end, Results), Diagnostics}
    end.

extract_quantifier_value(TupleValueMap) when is_map(TupleValueMap) ->
    case maps:to_list(TupleValueMap) of
        [{_Attr, Value}] ->
            {ok, Value};
        [] ->
            {error, empty_tuple};
        _ ->
            {error, non_unary_quantifier_relation}
    end.

-spec build_membership_criteria(#database_state{}, map()) -> map().
build_membership_criteria(Database, Schema) ->
    maps:fold(
        fun(AttrName, DomainName, Acc) ->
            case get_domain_from_db(Database, DomainName) of
                {ok, Domain} ->
                    Test = case Domain#relation.membership_criteria of
                        #{test := TestFun} -> TestFun;
                        _ -> fun(_) -> true end
                    end,
                    Acc#{AttrName => #{domain => DomainName, test => Test}};
                {error, not_found} ->
                    Acc#{AttrName => #{domain => DomainName, test => fun(_) -> true end}}
            end
        end,
        #{},
        Schema
    ).

-spec lt(term(), term()) -> relational_constraint().
lt(Left, Right) ->
    {member_of, less_than, #{left => Left, right => Right}}.

-spec lte(term(), term()) -> relational_constraint().
lte(Left, Right) ->
    {member_of, less_than_or_equal, #{left => Left, right => Right}}.

-spec gt(term(), term()) -> relational_constraint().
gt(Left, Right) ->
    {member_of, greater_than, #{left => Left, right => Right}}.

-spec gte(term(), term()) -> relational_constraint().
gte(Left, Right) ->
    {member_of, greater_than_or_equal, #{left => Left, right => Right}}.

-spec eq(term(), term()) -> relational_constraint().
eq(Left, Right) ->
    {member_of, equal, #{left => Left, right => Right}}.

-spec neq(term(), term()) -> relational_constraint().
neq(Left, Right) ->
    {member_of, not_equal, #{left => Left, right => Right}}.

-spec between(term(), term()) -> relational_constraint().
between(Min, Max) ->
    {'and', [gte({var, value}, Min), lt({var, value}, Max)]}.

-spec empty_constraints() -> #relation_constraints{}.
empty_constraints() ->
    #relation_constraints{constraints = []}.

-spec example_supertype_exclusive_constraints() -> #{atom() => #relation_constraints{}}.
example_supertype_exclusive_constraints() ->
    #{
        b => #relation_constraints{constraints = [
            {b_requires_a, {member_of, a, #{id => {var, id}}}},
            {b_excludes_c, {'not', {member_of, c, #{id => {var, id}}}}}
        ]},
        c => #relation_constraints{constraints = [
            {c_requires_a, {member_of, a, #{id => {var, id}}}},
            {c_excludes_b, {'not', {member_of, b, #{id => {var, id}}}}}
        ]}
    }.

-spec example_order_item_references_constraints() -> #relation_constraints{}.
example_order_item_references_constraints() ->
    #relation_constraints{constraints = [
        {order_item_order_fk, {member_of, order, #{id => {var, order_id}}}},
        {order_item_product_fk, {member_of, product, #{id => {var, product_id}}}}
    ]}.

-spec example_manager_constraints() -> #relation_constraints{}.
example_manager_constraints() ->
    #relation_constraints{constraints = [
        {managed_employee_exists, {member_of, employee, #{id => {var, employee_id}}}},
        {manager_exists, {member_of, employee, #{id => {var, manager_id}}}},
        {no_self_management, neq({var, employee_id}, {var, manager_id})}
    ]}.

-spec example_ticket_status_exclusive_constraints() -> #{atom() => #relation_constraints{}}.
example_ticket_status_exclusive_constraints() ->
    #{
        open_ticket => #relation_constraints{constraints = [
            {open_excludes_closed, {'not', {member_of, closed_ticket, #{ticket_id => {var, ticket_id}}}}}
        ]},
        closed_ticket => #relation_constraints{constraints = [
            {closed_excludes_open, {'not', {member_of, open_ticket, #{ticket_id => {var, ticket_id}}}}}
        ]}
    }.

-spec example_weak_entity_dependency_constraints() -> #relation_constraints{}.
example_weak_entity_dependency_constraints() ->
    #relation_constraints{constraints = [
        {dependent_person_fk, {member_of, person, #{id => {var, person_id}}}}
    ]}.

-spec example_department_has_employee_constraints() -> #relation_constraints{}.
example_department_has_employee_constraints() ->
    #relation_constraints{constraints = [
        {department_has_employee, {member_of, employee_dept, #{dept_id => {var, id}}}}
    ]}.

-spec example_symmetry_antisymmetry_constraints() -> #{atom() => #relation_constraints{}}.
example_symmetry_antisymmetry_constraints() ->
    #{
        married_to => #relation_constraints{constraints = [
            {marriage_is_symmetric, {member_of, married_to, #{person_a => {var, person_b}, person_b => {var, person_a}}}}
        ]},
        reports_to => #relation_constraints{constraints = [
            {reports_to_no_self, neq({var, employee_id}, {var, manager_id})},
            {reports_to_antisymmetric,
             {'or', [
                 eq({var, employee_id}, {var, manager_id}),
                 {'not', {member_of, reports_to,
                          #{employee_id => {var, manager_id}, manager_id => {var, employee_id}}}}
             ]}}
        ]}
    }.

-spec run_example_supertype_exclusive() -> map().
run_example_supertype_exclusive() ->
    main:setup(),
    DB0 = operations:create_database(example_supertype_exclusive),
    {DB1, _} = operations:create_relation(DB0, a, #{id => integer}),
    {DB2, _} = operations:create_relation(DB1, b, #{id => integer}),
    {DB3, _} = operations:create_relation(DB2, c, #{id => integer}),
    ConstraintsByRel = example_supertype_exclusive_constraints(),
    {DB4, _} = operations:update_relation_constraints(DB3, b, maps:get(b, ConstraintsByRel)),
    {DB5, _} = operations:update_relation_constraints(DB4, c, maps:get(c, ConstraintsByRel)),
    {DB6, _} = operations:create_tuple(DB5, a, #{id => 1}),
    InB = operations:create_tuple(DB6, b, #{id => 1}),
    DBAfterB = db_after(InB),
    InCWithB = operations:create_tuple(DBAfterB, c, #{id => 1}),
    {DB7, _} = operations:create_tuple(DBAfterB, a, #{id => 3}),
    InCWithAAndNoB = operations:create_tuple(DB7, c, #{id => 3}),
    InCWithoutA = operations:create_tuple(DBAfterB, c, #{id => 2}),
    #{accepted_in_b => InB,
      accepted_in_c_with_a_only => InCWithAAndNoB,
      rejected_c_overlap_b => InCWithB,
      rejected_c_missing_a => InCWithoutA}.

-spec run_example_order_item_references() -> map().
run_example_order_item_references() ->
    main:setup(),
    DB0 = operations:create_database(example_order_item_references),
    {DB1, _} = operations:create_relation(DB0, order, #{id => integer}),
    {DB2, _} = operations:create_relation(DB1, product, #{id => integer}),
    {DB3, _} = operations:create_relation(DB2, order_item, #{order_id => integer, product_id => integer}),
    {DB4, _} = operations:update_relation_constraints(DB3, order_item, example_order_item_references_constraints()),
    {DB5, _} = operations:create_tuple(DB4, order, #{id => 10}),
    {DB6, _} = operations:create_tuple(DB5, product, #{id => 20}),
    Valid = operations:create_tuple(DB6, order_item, #{order_id => 10, product_id => 20}),
    DBAfterValid = db_after(Valid),
    MissingOrder = operations:create_tuple(DBAfterValid, order_item, #{order_id => 999, product_id => 20}),
    MissingProduct = operations:create_tuple(DBAfterValid, order_item, #{order_id => 10, product_id => 999}),
    #{accepted_valid_order_item => Valid,
      rejected_missing_order => MissingOrder,
      rejected_missing_product => MissingProduct}.

-spec run_example_manager_constraints() -> map().
run_example_manager_constraints() ->
    main:setup(),
    DB0 = operations:create_database(example_manager_constraints),
    {DB1, _} = operations:create_relation(DB0, employee, #{id => integer}),
    {DB2, _} = operations:create_relation(DB1, manager_of, #{employee_id => integer, manager_id => integer}),
    {DB3, _} = operations:update_relation_constraints(DB2, manager_of, example_manager_constraints()),
    {DB4, _} = operations:create_tuple(DB3, employee, #{id => 1}),
    {DB5, _} = operations:create_tuple(DB4, employee, #{id => 2}),
    Valid = operations:create_tuple(DB5, manager_of, #{employee_id => 1, manager_id => 2}),
    DBAfterValid = db_after(Valid),
    Self = operations:create_tuple(DBAfterValid, manager_of, #{employee_id => 1, manager_id => 1}),
    MissingEmployee = operations:create_tuple(DBAfterValid, manager_of, #{employee_id => 3, manager_id => 2}),
    MissingManager = operations:create_tuple(DBAfterValid, manager_of, #{employee_id => 1, manager_id => 3}),
    #{accepted_valid_manager_pair => Valid,
      rejected_self_management => Self,
      rejected_missing_employee => MissingEmployee,
      rejected_missing_manager => MissingManager}.

-spec run_example_ticket_status_exclusive() -> map().
run_example_ticket_status_exclusive() ->
    main:setup(),
    DB0 = operations:create_database(example_ticket_status_exclusive),
    {DB1, _} = operations:create_relation(DB0, open_ticket, #{ticket_id => integer}),
    {DB2, _} = operations:create_relation(DB1, closed_ticket, #{ticket_id => integer}),
    ConstraintsByRel = example_ticket_status_exclusive_constraints(),
    {DB3, _} = operations:update_relation_constraints(DB2, open_ticket, maps:get(open_ticket, ConstraintsByRel)),
    {DB4, _} = operations:update_relation_constraints(DB3, closed_ticket, maps:get(closed_ticket, ConstraintsByRel)),
    Open7 = operations:create_tuple(DB4, open_ticket, #{ticket_id => 7}),
    DBAfterOpen7 = db_after(Open7),
    Closed7 = operations:create_tuple(DBAfterOpen7, closed_ticket, #{ticket_id => 7}),
    Closed8 = operations:create_tuple(DBAfterOpen7, closed_ticket, #{ticket_id => 8}),
    DBAfterClosed8 = db_after(Closed8),
    Open8 = operations:create_tuple(DBAfterClosed8, open_ticket, #{ticket_id => 8}),
    #{accepted_open_7 => Open7,
      rejected_closed_7 => Closed7,
      accepted_closed_8 => Closed8,
      rejected_open_8 => Open8}.

-spec run_example_weak_entity_dependency() -> map().
run_example_weak_entity_dependency() ->
    main:setup(),
    DB0 = operations:create_database(example_weak_entity_dependency),
    {DB1, _} = operations:create_relation(DB0, person, #{id => integer}),
    {DB2, _} = operations:create_relation(DB1, dependent, #{person_id => integer, dep_name => string}),
    {DB3, _} = operations:update_relation_constraints(DB2, dependent, example_weak_entity_dependency_constraints()),
    {DB4, _} = operations:create_tuple(DB3, person, #{id => 1}),
    Valid = operations:create_tuple(DB4, dependent, #{person_id => 1, dep_name => "kid"}),
    DBAfterValid = db_after(Valid),
    MissingParent = operations:create_tuple(DBAfterValid, dependent, #{person_id => 2, dep_name => "kid2"}),
    #{accepted_existing_parent => Valid,
      rejected_missing_parent => MissingParent}.

-spec run_example_department_has_employee() -> map().
run_example_department_has_employee() ->
    main:setup(),
    DB0 = operations:create_database(example_department_has_employee),
    {DB1, _} = operations:create_relation(DB0, employee_dept, #{dept_id => integer, employee_id => integer}),
    {DB2, _} = operations:create_relation(DB1, department, #{id => integer}),
    {DB3, _} = operations:update_relation_constraints(DB2, department, example_department_has_employee_constraints()),
    {DB4, _} = operations:create_tuple(DB3, employee_dept, #{dept_id => 1, employee_id => 99}),
    Dept1 = operations:create_tuple(DB4, department, #{id => 1}),
    DBAfterDept1 = db_after(Dept1),
    Dept2 = operations:create_tuple(DBAfterDept1, department, #{id => 2}),
    #{accepted_department_with_employee => Dept1,
      rejected_department_without_employee => Dept2}.

-spec run_example_symmetry_antisymmetry() -> map().
run_example_symmetry_antisymmetry() ->
    main:setup(),
    DB0 = operations:create_database(example_symmetry_antisymmetry),
    {DB1, _} = operations:create_relation(DB0, married_to, #{person_a => integer, person_b => integer}),
    {DB2, _} = operations:create_relation(DB1, reports_to, #{employee_id => integer, manager_id => integer}),
    {DB3, _} = operations:create_tuple(DB2, married_to, #{person_a => 2, person_b => 1}),
    {DB4, _} = operations:create_tuple(DB3, married_to, #{person_a => 1, person_b => 2}),
    ConstraintsByRel = example_symmetry_antisymmetry_constraints(),
    {DB5, _} = operations:update_relation_constraints(DB4, married_to, maps:get(married_to, ConstraintsByRel)),
    {DB6, _} = operations:update_relation_constraints(DB5, reports_to, maps:get(reports_to, ConstraintsByRel)),
    MissingReverseMarriage = operations:create_tuple(DB6, married_to, #{person_a => 3, person_b => 4}),
    AcceptedWithReverse = operations:create_tuple(DB6, married_to, #{person_a => 2, person_b => 1}),
    Reports12 = operations:create_tuple(DB6, reports_to, #{employee_id => 1, manager_id => 2}),
    DBAfterReports12 = db_after(Reports12),
    Reports21 = operations:create_tuple(DBAfterReports12, reports_to, #{employee_id => 2, manager_id => 1}),
    Reports33 = operations:create_tuple(DBAfterReports12, reports_to, #{employee_id => 3, manager_id => 3}),
    #{rejected_missing_reverse_marriage => MissingReverseMarriage,
      accepted_with_existing_reverse_marriage => AcceptedWithReverse,
      accepted_reports_1_to_2 => Reports12,
      rejected_reports_2_to_1 => Reports21,
      rejected_reports_3_to_3 => Reports33}.

-spec run_all_examples() -> map().
run_all_examples() ->
    #{
        supertype_exclusive => run_example_supertype_exclusive(),
        order_item_references => run_example_order_item_references(),
        manager_constraints => run_example_manager_constraints(),
        ticket_status_exclusive => run_example_ticket_status_exclusive(),
        weak_entity_dependency => run_example_weak_entity_dependency(),
        department_has_employee => run_example_department_has_employee(),
        symmetry_antisymmetry => run_example_symmetry_antisymmetry()
    }.

db_after({DB, _Relation}) -> DB.

-spec merge_constraints(#relation_constraints{} | undefined, #relation_constraints{} | undefined) -> #relation_constraints{}.
merge_constraints(C1, C2) ->
    N1 = normalize_constraints(C1),
    N2 = normalize_constraints(C2),
    #relation_constraints{constraints = N1#relation_constraints.constraints ++ N2#relation_constraints.constraints}.

-spec filter_constraints(#relation_constraints{} | undefined, [atom()]) -> #relation_constraints{}.
filter_constraints(Constraints, Attrs) ->
    Normalized = normalize_constraints(Constraints),
    Keep = [
        {Name, Formula} || {Name, Formula} <- Normalized#relation_constraints.constraints,
             lists:all(fun(V) -> lists:member(V, Attrs) end, vars_in_constraint(Formula))
    ],
    #relation_constraints{constraints = Keep}.

-spec rename_constraint_attrs(#relation_constraints{} | undefined, map()) -> #relation_constraints{}.
rename_constraint_attrs(Constraints, RenameMappings) ->
    Normalized = normalize_constraints(Constraints),
    Renamed = [{Name, rename_vars_in_constraint(Formula, RenameMappings)} ||
               {Name, Formula} <- Normalized#relation_constraints.constraints],
    #relation_constraints{constraints = Renamed}.

-spec infer_constraints_from_pred(term(), map()) -> #relation_constraints{}.
infer_constraints_from_pred(Predicate, _Schema) when is_function(Predicate) ->
    empty_constraints();
infer_constraints_from_pred({attr, AttrName, Op, Value}, _Schema) ->
    Name = list_to_atom(atom_to_list(AttrName) ++ "_" ++ atom_to_list(Op)),
    #relation_constraints{constraints = [{Name, op_to_constraint(Op, AttrName, Value)}]};
infer_constraints_from_pred({'and', Predicates}, Schema) ->
    lists:foldl(
        fun(Pred, Acc) ->
            merge_constraints(Acc, infer_constraints_from_pred(Pred, Schema))
        end,
        empty_constraints(),
        Predicates
    );
infer_constraints_from_pred({'or', Predicates}, Schema) ->
    OrParts = [
        constraint_from_predicate(P, Schema) || P <- Predicates,
        constraint_from_predicate(P, Schema) =/= undefined
    ],
    case OrParts of
        [] -> empty_constraints();
        _ -> #relation_constraints{constraints = [{inferred_or, {'or', OrParts}}]}
    end;
infer_constraints_from_pred(_, _Schema) ->
    empty_constraints().

constraint_from_predicate({attr, AttrName, Op, Value}, _Schema) ->
    op_to_constraint(Op, AttrName, Value);
constraint_from_predicate({'and', Predicates}, Schema) ->
    {'and', [C || C <- [constraint_from_predicate(P, Schema) || P <- Predicates], C =/= undefined]};
constraint_from_predicate({'or', Predicates}, Schema) ->
    {'or', [C || C <- [constraint_from_predicate(P, Schema) || P <- Predicates], C =/= undefined]};
constraint_from_predicate(_, _Schema) ->
    undefined.

op_to_constraint(lt, Attr, Value) -> lt({var, Attr}, Value);
op_to_constraint(lte, Attr, Value) -> lte({var, Attr}, Value);
op_to_constraint(gt, Attr, Value) -> gt({var, Attr}, Value);
op_to_constraint(gte, Attr, Value) -> gte({var, Attr}, Value);
op_to_constraint(eq, Attr, Value) -> eq({var, Attr}, Value);
op_to_constraint(neq, Attr, Value) -> neq({var, Attr}, Value);
op_to_constraint(between, Attr, {Min, Max}) -> {'and', [gte({var, Attr}, Min), lt({var, Attr}, Max)]};
op_to_constraint(_, _Attr, _Value) -> {'and', []}.

normalize_constraints(undefined) ->
    empty_constraints();
normalize_constraints(#relation_constraints{} = Constraints) ->
    NormalizedEntries = normalize_named_constraints(Constraints#relation_constraints.constraints),
    assert_unique_constraint_names(NormalizedEntries),
    Constraints#relation_constraints{constraints = NormalizedEntries};
normalize_constraints(_) ->
    empty_constraints().

normalize_named_constraints(Entries) ->
    [normalize_named_constraint(Entry) || Entry <- Entries].

normalize_named_constraint({Name, Formula}) when is_atom(Name) ->
    {Name, Formula};
normalize_named_constraint(InvalidEntry) ->
    error({invalid_constraint_entry, InvalidEntry}).

assert_unique_constraint_names(NamedConstraints) ->
    Names = [Name || {Name, _Formula} <- NamedConstraints],
    case has_duplicates(Names) of
        true ->
            error({duplicate_constraint_names, Names});
        false ->
            ok
    end.

has_duplicates([]) ->
    false;
has_duplicates([Name | Rest]) ->
    lists:member(Name, Rest) orelse has_duplicates(Rest).

vars_in_constraint({member_of, _Rel, Binding}) when is_map(Binding) ->
    maps:fold(
      fun(_K, V, Acc) ->
          Acc ++ vars_in_term(V)
      end,
      [],
      Binding);
vars_in_constraint({'and', Cs}) -> lists:usort(lists:flatmap(fun vars_in_constraint/1, Cs));
vars_in_constraint({'or', Cs}) -> lists:usort(lists:flatmap(fun vars_in_constraint/1, Cs));
vars_in_constraint({'not', C}) -> vars_in_constraint(C);
vars_in_constraint({'not', C, _}) -> vars_in_constraint(C);
vars_in_constraint({exists, Var, _Rel, C}) -> lists:delete(Var, vars_in_constraint(C));
vars_in_constraint({forall, Var, _Rel, C}) -> lists:delete(Var, vars_in_constraint(C));
vars_in_constraint(_) -> [].

vars_in_term({var, Name}) when is_atom(Name) -> [Name];
vars_in_term({const, _}) -> [];
vars_in_term(T) when is_tuple(T) ->
    lists:flatmap(fun vars_in_term/1, tuple_to_list(T));
vars_in_term(T) when is_list(T) ->
    lists:flatmap(fun vars_in_term/1, T);
vars_in_term(T) when is_map(T) ->
    maps:fold(fun(_K, V, Acc) -> Acc ++ vars_in_term(V) end, [], T);
vars_in_term(_) -> [].

rename_vars_in_constraint({member_of, RelName, Binding}, Mappings) ->
    RenamedBinding = maps:map(
        fun(_K, {var, VarName}) ->
            {var, maps:get(VarName, Mappings, VarName)};
           (_K, V) ->
            V
        end,
        Binding
    ),
    {member_of, RelName, RenamedBinding};
rename_vars_in_constraint({'and', Constraints}, Mappings) ->
    {'and', [rename_vars_in_constraint(C, Mappings) || C <- Constraints]};
rename_vars_in_constraint({'or', Constraints}, Mappings) ->
    {'or', [rename_vars_in_constraint(C, Mappings) || C <- Constraints]};
rename_vars_in_constraint({'not', Constraint}, Mappings) ->
    {'not', rename_vars_in_constraint(Constraint, Mappings)};
rename_vars_in_constraint({'not', Constraint, Universe}, Mappings) ->
    {'not', rename_vars_in_constraint(Constraint, Mappings), Universe};
rename_vars_in_constraint({exists, Var, Rel, Constraint}, Mappings) ->
    {exists, maps:get(Var, Mappings, Var), Rel, rename_vars_in_constraint(Constraint, Mappings)};
rename_vars_in_constraint({forall, Var, Rel, Constraint}, Mappings) ->
    {forall, maps:get(Var, Mappings, Var), Rel, rename_vars_in_constraint(Constraint, Mappings)};
rename_vars_in_constraint(Other, _Mappings) ->
    Other.

validate_tuple_against_criteria(Tuple, Criteria) when is_map(Criteria) ->
    maps:fold(
        fun(Attr, Constraint, Acc) ->
            Acc andalso validate_attribute(maps:get(Attr, Tuple, undefined), Constraint)
        end,
        true,
        Criteria
    ).

validate_attribute(undefined, _) -> false;
validate_attribute(Value, #{test := Test}) -> Test(#{value => Value});
validate_attribute(Value, Constraint) when is_function(Constraint, 1) -> Constraint(Value);
validate_attribute(_, _) -> true.

apply_substitution(Binding, Substitution) ->
    maps:map(
        fun(_K, {var, VarName}) ->
            maps:get(VarName, Substitution, {var, VarName});
           (_K, V) ->
            V
        end,
        Binding
    ).

find_variables(Binding) ->
    maps:fold(
        fun(_K, {var, VarName}, Acc) -> [VarName | Acc];
           (_K, _, Acc) -> Acc
        end,
        [],
        Binding
    ).

get_schema(#domain{schema = S}) -> S;
get_schema(#relation{schema = S}) -> S.

get_name(#domain{name = N}) -> N;
get_name(#relation{name = N}) -> N.

get_lineage(#relation{lineage = L}) -> L;
get_lineage(#domain{name = N}) -> {base, N}.

schemas_compatible(S1, S2) ->
    maps:keys(S1) =:= maps:keys(S2).

merge_schemas(S1, _S2) ->
    S1.

get_cardinality(#domain{cardinality = C}) -> C;
get_cardinality(#relation{cardinality = C}) -> C.

complement_cardinality(R, Universe) ->
    RC = get_cardinality(R),
    UC = get_cardinality(Universe),
    case {RC, UC} of
        {{finite, N}, {finite, M}} -> {finite, max(0, M - N)};
        {{finite, _}, Infinite} -> Infinite;
        _ -> UC
    end.

intersection_cardinality(R1, R2) ->
    C1 = get_cardinality(R1),
    C2 = get_cardinality(R2),
    case {C1, C2} of
        {{finite, N}, _} -> {finite, N};
        {_, {finite, M}} -> {finite, M};
        _ -> min_cardinality(C1, C2)
    end.

union_cardinality(R1, R2) ->
    C1 = get_cardinality(R1),
    C2 = get_cardinality(R2),
    case {C1, C2} of
        {{finite, N}, {finite, M}} -> {finite, N + M};
        _ -> max_cardinality(C1, C2)
    end.

min_cardinality({finite, N}, {finite, M}) -> {finite, min(N, M)};
min_cardinality({finite, N}, _) -> {finite, N};
min_cardinality(_, {finite, M}) -> {finite, M};
min_cardinality(aleph_zero, _) -> aleph_zero;
min_cardinality(_, aleph_zero) -> aleph_zero;
min_cardinality(continuum, continuum) -> continuum.

max_cardinality({finite, N}, {finite, M}) -> {finite, max(N, M)};
max_cardinality(continuum, _) -> continuum;
max_cardinality(_, continuum) -> continuum;
max_cardinality(aleph_zero, _) -> aleph_zero;
max_cardinality(_, aleph_zero) -> aleph_zero.

evaluate_constraint(Database, {member_of, RelName, Binding}, Context) when is_atom(RelName) ->
    BoundBinding = bind_variables(Binding, Context),
    case get_domain_from_db(Database, RelName) of
        {ok, Relation} ->
            {ok, member(BoundBinding, Relation)};
        {error, not_found} ->
            {error, {relation_not_found, RelName}}
    end;
evaluate_constraint(_Database, {member_of, #relation{} = Rel, Binding}, Context) ->
    BoundBinding = bind_variables(Binding, Context),
    {ok, member(BoundBinding, Rel)};
evaluate_constraint(_Database, {member_of, #domain{} = Rel, Binding}, Context) ->
    BoundBinding = bind_variables(Binding, Context),
    {ok, member(BoundBinding, Rel)};
evaluate_constraint(Database, {'not', Constraint, _Universe}, Context) ->
    case evaluate_constraint(Database, Constraint, Context) of
        {ok, Bool} -> {ok, not Bool};
        Error -> Error
    end;
evaluate_constraint(Database, {'and', Constraints}, Context) ->
    Results = [evaluate_constraint(Database, C, Context) || C <- Constraints],
    case lists:all(fun({ok, true}) -> true; (_) -> false end, Results) of
        true -> {ok, true};
        false -> {ok, false}
    end;
evaluate_constraint(Database, {'or', Constraints}, Context) ->
    Results = [evaluate_constraint(Database, C, Context) || C <- Constraints],
    case lists:any(fun({ok, true}) -> true; (_) -> false end, Results) of
        true -> {ok, true};
        false -> {ok, false}
    end;
evaluate_constraint(Database, {'not', Constraint}, Context) ->
    {Result, _Diagnostics} = evaluate_constraint_with_diagnostics(Database, {'not', Constraint}, Context),
    {ok, Result};
evaluate_constraint(Database, {exists, VarName, QuantifierRel, Constraint}, Context) ->
    {Result, _Diagnostics} = evaluate_constraint_with_diagnostics(Database, {exists, VarName, QuantifierRel, Constraint}, Context),
    {ok, Result};
evaluate_constraint(Database, {forall, VarName, QuantifierRel, Constraint}, Context) ->
    {Result, _Diagnostics} = evaluate_constraint_with_diagnostics(Database, {forall, VarName, QuantifierRel, Constraint}, Context),
    {ok, Result};
evaluate_constraint(_Database, _, _) ->
    {error, unsupported_constraint}.

bind_variables(Binding, Context) ->
    maps:map(
        fun(_K, {var, VarName}) ->
            maps:get(VarName, Context, maps:get(value, Context, undefined));
           (_K, {const, Value}) ->
            Value;
           (_K, V) ->
            V
        end,
        Binding
    ).

-spec get_domain_from_db(#database_state{}, atom()) -> {ok, #relation{}} | {error, not_found}.
get_domain_from_db(Database, DomainName) ->
    case maps:find(DomainName, Database#database_state.relations) of
        {ok, DomainHash} ->
            [Domain] = mnesia:dirty_read(relation, DomainHash),
            {ok, Domain};
        error ->
            {error, not_found}
    end.
