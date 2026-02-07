-module(constraint).

-include("../include/operations.hrl").

-export([
    example_1op/0,
    example_2op/0,
    less_than/2,
    less_than_or_equal/2,
    greater_than/2,
    greater_than_or_equal/2,
    equal/2,
    not_equal/2,
    member/2,
    member/3,
    'not'/2,
    'and'/2,
    'or'/2,
    create_1op/4,
    derive_1op/2,
    declare_1opic/3,
    validate_1opic/4,
    resolve/4,
    validate_tuple/3,
    validate_tuple_against_schema/3,
    build_membership_criteria/2,
    lt/2,       % value < X
    lte/2,      % value <= X
    gt/2,       % value > X
    gte/2,      % value >= X
    eq/2,       % value = X
    neq/2,      % value != X
    between/2,  % Min <= value < Max
    create_2op/2,
    add_tuple_constraint/2,
    %% Constraint Propagation
    empty_constraints/0,           % Create empty #relation_constraints{}
    merge_constraints/2,           % Merge two constraint records (AND semantics)
    filter_constraints/2,          % Keep only constraints for given attributes
    rename_constraint_attrs/2,     % Update attribute names in constraints
    infer_constraints_from_pred/2, % Extract constraints from predicate
    add_attribute_constraint/3,    % Add an attribute constraint to constraints
    get_domain_from_db/2
]).

-type relational_constraint() ::
    {member_of, relation_name(), binding()}     % (x, y, ...) in R
  | {'not', relational_constraint(), domain()}  % NOT P within universe D
  | {'and', [relational_constraint()]}          % P1 AND P2 AND ...
  | {'or', [relational_constraint()]}           % P1 OR P2 OR ...
  | {exists, atom(), relational_constraint()}   % EXISTS x: P(x)
  | {forall, atom(), domain(), relational_constraint()}. % FORALL x in D: P(x)

-type relation_name() :: atom().
-type domain() :: atom() | #domain{}.
-type binding() :: #{atom() => term() | {var, atom()}}.

-export_type([relational_constraint/0, binding/0]).

-spec less_than(atom(), cardinality()) -> #relation{}.
less_than(DomainName, Cardinality) ->
    #relation{
        hash = undefined,  % Computed lazily if needed
        name = less_than,
        tree = undefined,  % Generator-based, not stored
        schema = #{left => DomainName, right => DomainName},
        constraints = #{},
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
        constraints = #{},
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
        constraints = #{},
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
        constraints = #{},
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
        constraints = #{},
        %% The diagonal has the same cardinality as the domain itself
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
        constraints = #{},
        cardinality = Cardinality,
        generator = {comparison, '/=', DomainName},
        membership_criteria = #{
            intension => fun(#{left := L, right := R}) -> L /= R end
        },
        provenance = undefined,
        lineage = {base, not_equal}
    }.

-spec member(map(), #domain{} | #relation{}) -> boolean().
member(Tuple, #relation{membership_criteria = #{intension := Intension}}) ->
    %% Generator-based relation with intension predicate (e.g., less_than)
    Intension(Tuple);
member(Tuple, #relation{membership_criteria = #{test := TestFun}}) ->
    %% Domain relation with test function (e.g., integer, string, atom domains)
    TestFun(Tuple);
member(_Tuple, #relation{tree = undefined, generator = Generator}) when Generator =/= undefined ->
    %% Generator-based relation without explicit intension - would need enumeration
    %% This is a fallback; ideally all generator relations have an intension
    {error, {no_intension, requires_enumeration}};
member(Tuple, #relation{} = Relation) ->
    %% Stored relation - check actual membership in the tree
    relation_contains(Tuple, Relation);
member(Tuple, #domain{membership_criteria = #{test := TestFun}}) ->
    %% Domain (1OP) with a test function
    TestFun(Tuple);
member(Tuple, #domain{membership_criteria = Criteria}) when is_map(Criteria) ->
    %% Domain with attribute-level criteria
    validate_tuple_against_criteria(Tuple, Criteria).

-spec member(binding(), #domain{} | #relation{}, map()) ->
    {true, map()} | false.
member(Binding, Relation, Substitution) ->
    BoundTuple = apply_substitution(Binding, Substitution),
    %% We also gotta look at th remaining variables
    case find_variables(BoundTuple) of
        [] ->
            %% For the fully bound, apply the direct membership test
            case member(BoundTuple, Relation) of
                true -> {true, Substitution};
                false -> false
            end;
        Variables ->
            %% In the scenario we have actual variables, we need to search for satisfying values
            %% which requires enumeration through the generator
            find_satisfying_substitution(BoundTuple, Variables, Relation, Substitution)
    end.

-spec 'not'(#relation{}, #relation{}) -> #relation{}.
'not'(R, Universe) ->
    RName = get_name(R),
    UniverseName = get_name(Universe),
    ResultName = list_to_atom("not_" ++ atom_to_list(RName) ++
                              "_in_" ++ atom_to_list(UniverseName)),
    #relation{
        hash = undefined,
        name = ResultName,
        tree = undefined,
        schema = get_schema(R),
        constraints = #{},
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
            ResultName = list_to_atom(atom_to_list(R1Name) ++
                                      "_and_" ++ atom_to_list(R2Name)),
            #relation{
                hash = undefined,
                name = ResultName,
                tree = undefined,
                schema = merge_schemas(Schema1, Schema2),
                constraints = #{},
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

%% @doc Union of two relations (logical OR).
%%
%% Returns a relation containing tuples that are in R1 OR R2 (or both).
%% Requires compatible schemas.
%%
%% @param R1 First relation
%% @param R2 Second relation
%% @returns A new relation representing the union
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
            ResultName = list_to_atom(atom_to_list(R1Name) ++
                                      "_or_" ++ atom_to_list(R2Name)),
            #relation{
                hash = undefined,
                name = ResultName,
                tree = undefined,
                schema = merge_schemas(Schema1, Schema2),
                constraints = #{},
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

%%% 1OP Constraint Operations (Attribute Constraints)

%% @doc Create a constraint for a 1st Order Property (attribute constraint).
%%
%% A 1OP is a property (attribute) of an entity. This function creates a CONSTRAINT
%% for that 1OP which:
%% 1. Specifies which domain (or relation) the attribute draws values from
%% 2. Applies additional constraints beyond domain membership
%%
%% The validation process for a 1OP constraint:
%% 1. Get the attribute value from a tuple
%% 2. Check if value ∈ domain (using domain's membership criteria)
%% 3. Apply additional constraints specified here
-spec create_1op(atom(), atom(), atom(), [relational_constraint()]) -> #attribute_constraint{}.
create_1op(Name, AttributeName, Domain, Constraints) ->
    #attribute_constraint{
        name = Name,
        attribute = AttributeName,
        domain = Domain,
        constraints = Constraints
    }.

%% @doc Create a tuple-level constraint (2OP) as a conjunction list.
%%
%% Tuple constraints are evaluated in addition to all attribute constraints.
%% Each entry in this list is a formula term that must hold for the tuple.
-spec create_2op(atom(), [term()]) -> #tuple_constraint{}.
create_2op(Name, Constraints) when is_list(Constraints) ->
    #tuple_constraint{name = Name, constraints = Constraints}.

%% @doc Derive a 1OP constraint from another 1OP constraint by adding constraints.
%%
%% Creates a new attribute constraint by adding additional constraints to an
%% existing 1OP constraint. The derived constraint applies all parent constraints
%% plus the additional ones.
-spec derive_1op(#attribute_constraint{}, [relational_constraint()]) -> #attribute_constraint{}.
derive_1op(#attribute_constraint{} = Parent1OPConstraint, AdditionalConstraints) ->
    AllConstraints = Parent1OPConstraint#attribute_constraint.constraints ++ AdditionalConstraints,
    #attribute_constraint{
        name = Parent1OPConstraint#attribute_constraint.name,
        attribute = Parent1OPConstraint#attribute_constraint.attribute,
        domain = Parent1OPConstraint#attribute_constraint.domain,
        constraints = AllConstraints
    }.

%%% 1OPiC (1st Order Property in Context) Operations
%%%
%%% A 1OPiC is an attribute - a 1OP that assumes a specific meaning within
%%% the context of a particular relation (entity group).
%%%
%%% Key insight: The SAME 1OP (domain) can manifest as MULTIPLE 1OPiCs with
%%% DIFFERENT meanings in the same or different relations.
%%%
%%% Example:
%%%   - 1OP: `compensation` (the abstract property of monetary compensation)
%%%   - 1OPiC: `salary` in employees (base pay - one meaning of compensation)
%%%   - 1OPiC: `commission` in employees (variable pay - another meaning)
%%%
%%% Both `salary` and `commission` draw values from `compensation`, but they
%%% have distinct semantic meanings in the context of the employees relation.
%%%
%%% At the logical level:
%%%   - 1OP = Domain (value set)
%%%   - 1OPiC = Attribute (contextualized appearance of a domain)
%%%   - Schema entry `#{salary => compensation}` declares the 1OPiC relationship

%% @doc Declare a 1OPiC (attribute) in a relation schema.
%%
%% A 1OPiC is a 1OP in a specific context - an attribute that:
%% 1. Draws its values from a specific domain (1OP)
%% 2. Has a particular meaning in this relation's context
%%
%% Multiple attributes can draw from the same domain but represent
%%% different meanings (e.g., salary and commission both from compensation).
-spec declare_1opic(atom(), atom() | #domain{}, map()) -> map().
declare_1opic(AttributeName, Domain1OP, Schema) when is_atom(Domain1OP) ->
    Schema#{AttributeName => Domain1OP};
declare_1opic(AttributeName, #domain{name = DomainName}, Schema) ->
    Schema#{AttributeName => DomainName}.

%% @doc Validate that a value satisfies its 1OPiC's domain constraints.
%%
%% Checks that the value is a member of the domain (1OP) that the
%% attribute (1OPiC) draws from.
-spec validate_1opic(#database_state{}, atom(), term(), map()) -> true | {false, term()}.
validate_1opic(Database, AttributeName, Value, Schema) ->
    case maps:get(AttributeName, Schema, undefined) of
        undefined ->
            {false, {unknown_attribute, AttributeName}};
        DomainName ->
            %% Get the domain from database using version hash
            case get_domain_from_db(Database, DomainName) of
                {ok, Domain} ->
                    Tuple = #{value => Value},
                    case member(Tuple, Domain) of
                        true -> true;
                        false -> {false, {not_in_domain, Value, DomainName}}
                    end;
                {error, not_found} ->
                    {false, {domain_not_found, DomainName}}
            end
    end.

%%% Constraint Resolution

%% @doc Resolve constraints for a relation against given values.
%%
%% @param Database The database state for version-aware constraint evaluation
%% @param Type The type of resolution (domain, relation, etc.)
%% @param RelationOrDomain The relation or domain
%% @param Values The values to check
%% @returns Resolution result
-spec resolve(#database_state{}, atom(), #domain{} | #relation{}, map()) ->
    {ok, boolean()} | {error, term()}.
resolve(_Database, domain, #domain{} = Domain, Values) ->
    Result = member(Values, Domain),
    {ok, Result};
resolve(_Database, relation, #relation{} = Relation, Values) ->
    Result = member(Values, Relation),
    {ok, Result};
resolve(Database, constraint, Constraint, Binding) ->
    evaluate_constraint(Database, Constraint, Binding).

%% @doc Validate a tuple against a relation's schema and constraints.
%%
%% Checks that:
%% 1. All required attributes are present
%% 2. Each attribute value is a member of its declared domain
%% 3. Each attribute value satisfies its 1OP constraints (if any)
%%
%% @param Database The database state for version-aware domain lookups
%% @param Tuple The tuple map to validate
%% @param Relation The relation with schema and constraints
%% @returns ok | {error, Reason}
-spec validate_tuple(#database_state{}, map(), #relation{}) -> ok | {error, term()}.
validate_tuple(Database, Tuple, #relation{schema = Schema, constraints = Constraints}) ->
    case validate_tuple_against_schema(Database, Tuple, Schema) of
        ok ->
            %% Also validate against attribute constraints (1OPs)
            validate_tuple_against_constraints(Database, Tuple, Constraints);
        Error ->
            Error
    end.

-spec validate_tuple_against_schema(#database_state{}, map(), map()) -> ok | {error, term()}.
validate_tuple_against_schema(Database, Tuple, Schema) ->
    %% Check all schema attributes are present and valid
    SchemaAttrs = maps:keys(Schema),
    TupleAttrs = maps:keys(Tuple),
    %% Check for missing attributes
    Missing = SchemaAttrs -- TupleAttrs,
    case Missing of
        [] ->
            %% Check for extra attributes not in schema
            Extra = TupleAttrs -- SchemaAttrs,
            case Extra of
                [] ->
                    %% Validate each attribute against its domain
                    validate_all_attributes(Database, Tuple, Schema);
                _ ->
                    {error, {extra_attributes, Extra}}
            end;
        _ ->
            {error, {missing_attributes, Missing}}
    end.

-spec validate_all_attributes(#database_state{}, map(), map()) -> ok | {error, term()}.
validate_all_attributes(Database, Tuple, Schema) ->
    Results = maps:fold(
        fun(AttrName, Value, Acc) ->
            case Acc of
                {error, _} = Err -> Err;
                ok ->
                    case validate_1opic(Database, AttrName, Value, Schema) of
                        true -> ok;
                        {false, Reason} -> {error, {invalid_attribute, AttrName, Reason}}
                    end
            end
        end,
        ok,
        Tuple
    ),
    Results.

-spec validate_tuple_against_constraints(#database_state{}, map(), #relation_constraints{} | undefined) -> ok | {error, term()}.
validate_tuple_against_constraints(_Database, _Tuple, undefined) ->
    %% No constraints defined
    ok;
validate_tuple_against_constraints(Database, Tuple, #relation_constraints{attribute_constraints = AttrConstraints}) ->
    %% Validate each attribute against its constraint
    validate_attribute_constraints(Database, Tuple, AttrConstraints).

-spec validate_attribute_constraints(#database_state{}, map(), #{atom() => #attribute_constraint{}}) -> ok | {error, term()}.
validate_attribute_constraints(Database, Tuple, AttrConstraints) when is_map(AttrConstraints) ->
    maps:fold(
        fun(AttrName, #attribute_constraint{domain = Domain, constraints = Constraints}, Acc) ->
            case Acc of
                {error, _} = Err -> Err;
                ok ->
                    case maps:get(AttrName, Tuple, undefined) of
                        undefined ->
                            %% Attribute not in tuple - schema validation should catch this
                            ok;
                        Value ->
                            %% Validate: value ∈ domain AND additional constraints
                            validate_attribute_constraint(Database, AttrName, Value, Domain, Constraints)
                    end
            end
        end,
        ok,
        AttrConstraints
    ).

-spec validate_attribute_constraint(#database_state{}, atom(), term(), atom(), [relational_constraint()]) -> ok | {error, term()}.
validate_attribute_constraint(Database, AttrName, Value, Domain, Constraints) ->
    %% Step 1: Check domain membership
    case get_domain_from_db(Database, Domain) of
        {ok, DomainRec} ->
            ValueTuple = #{value => Value},
            case member(ValueTuple, DomainRec) of
                true ->
                    %% Step 2: Apply additional constraints
                    validate_additional_constraints(Database, AttrName, Value, Constraints);
                false ->
                    {error, {not_in_domain, AttrName, Value, Domain}}
            end;
        {error, not_found} ->
            {error, {unknown_domain, AttrName, Domain}}
    end.

-spec validate_additional_constraints(#database_state{}, atom(), term(), [relational_constraint()]) -> ok | {error, term()}.
validate_additional_constraints(_Database, _AttrName, _Value, []) ->
    %% No additional constraints
    ok;
validate_additional_constraints(Database, AttrName, Value, Constraints) ->
    %% Evaluate each constraint
    Context = #{value => Value},
    Results = [evaluate_constraint(Database, C, Context) || C <- Constraints],
    case lists:all(fun({ok, true}) -> true; (_) -> false end, Results) of
        true -> ok;
        false -> {error, {constraint_violation, AttrName, Value, Constraints}}
    end.

-spec build_membership_criteria(#database_state{}, map()) -> map().
build_membership_criteria(Database, Schema) ->
    maps:fold(
        fun(AttrName, DomainName, Acc) ->
	    %% Get domain from DB using version hash
	    %% Domains are stored in the DB through operations:create_immutable_relation (see operations.erl:531)
	    case get_domain_from_db(Database, DomainName) of
                {ok, Domain} ->
                    %% Domains are stored as #relation{} records, not #domain{}
                    Test = case Domain#relation.membership_criteria of
                        #{test := TestFun} -> TestFun;
                        _ -> fun(_) -> true end
                    end,
                    Acc#{AttrName => #{domain => DomainName, test => Test}};
                {error, not_found} ->
                    %% Unknown domain, default to permissive test
                    Acc#{AttrName => #{domain => DomainName, test => fun(_) -> true end}}
            end
        end,
        #{},
        Schema
    ).

%%% Constraint Builders
%%%
%%% Convenience functions for building relational constraints.
%%% These create constraints in the form expected by create_1op/3.

%% @doc Create a "less than" constraint: value &lt; X
%%
%% Expressed relationally as: (value, X) ∈ less_than
-spec lt(term(), term()) -> relational_constraint().
lt(Left, Right) ->
    {member_of, less_than, #{left => Left, right => Right}}.

%% @doc Create a "less than or equal" constraint: value &lt;= X
-spec lte(term(), term()) -> relational_constraint().
lte(Left, Right) ->
    {member_of, less_than_or_equal, #{left => Left, right => Right}}.

%% @doc Create a "greater than" constraint: value &gt; X
%%
%% Expressed relationally as: (X, value) ∈ greater_than
-spec gt(term(), term()) -> relational_constraint().
gt(Left, Right) ->
    {member_of, greater_than, #{left => Left, right => Right}}.

%% @doc Create a "greater than or equal" constraint: value &gt;= X
-spec gte(term(), term()) -> relational_constraint().
gte(Left, Right) ->
    {member_of, greater_than_or_equal, #{left => Left, right => Right}}.

%% @doc Create an "equal" constraint: value = X
-spec eq(term(), term()) -> relational_constraint().
eq(Left, Right) ->
    {member_of, equal, #{left => Left, right => Right}}.

%% @doc Create a "not equal" constraint: value != X
-spec neq(term(), term()) -> relational_constraint().
neq(Left, Right) ->
    {member_of, not_equal, #{left => Left, right => Right}}.

%% @doc Create a range constraint: Min &lt;= value &lt; Max
%%
%% This is a conjunction of two relational constraints:
%% (Min, value) ∈ greater_than_or_equal AND (value, Max) ∈ less_than
-spec between(term(), term()) -> relational_constraint().
between(Min, Max) ->
    {'and', [gte({var,value}, Min), lt({var, value}, Max)]}.

%%% Constraint Propagation
%%%
%%% These functions support propagating constraints through relational operations.
%%% Constraints are stored in #relation_constraints{} records.

%% @doc Create an empty constraints record.
%%
%% @returns Empty #relation_constraints{} with no constraints
-spec empty_constraints() -> #relation_constraints{}.
empty_constraints() ->
    #relation_constraints{
        attribute_constraints = #{},
        tuple_constraints = [],
        multi_tuple_constraints = []
    }.

%% @doc Add an attribute constraint to a constraints record.
%%
%% @param Constraints The #relation_constraints{} record or undefined
%% @param AttrName The attribute name
%% @param AttrConstraint The #attribute_constraint{} to add
%% @returns Updated #relation_constraints{}
-spec add_attribute_constraint(#relation_constraints{} | undefined, atom(), #attribute_constraint{}) -> #relation_constraints{}.
add_attribute_constraint(undefined, AttrName, #attribute_constraint{name = Name} = AttrConstraint) when is_atom(Name) ->
    #relation_constraints{
        attribute_constraints = #{AttrName => AttrConstraint},
        tuple_constraints = [],
        multi_tuple_constraints = []
    };
add_attribute_constraint(#relation_constraints{attribute_constraints = AttrConstraints} = Constraints,
                         AttrName,
                         #attribute_constraint{name = Name} = AttrConstraint) when is_atom(Name) ->
    Constraints#relation_constraints{
        attribute_constraints = AttrConstraints#{AttrName => AttrConstraint}
    }.

%% @doc Add a tuple constraint to a constraints record.
%%
%% Tuple constraints are kept separate for lifecycle management and are
%% evaluated as an implicit conjunction over the whole list.
-spec add_tuple_constraint(#relation_constraints{} | undefined, #tuple_constraint{}) -> #relation_constraints{}.
add_tuple_constraint(undefined, #tuple_constraint{name = Name} = TupleConstraint) when is_atom(Name) ->
    #relation_constraints{
        attribute_constraints = #{},
        tuple_constraints = [TupleConstraint],
        multi_tuple_constraints = []
    };
add_tuple_constraint(#relation_constraints{tuple_constraints = TupleConstraints} = Constraints,
                     #tuple_constraint{name = Name} = TupleConstraint) when is_atom(Name) ->
    Constraints#relation_constraints{
        tuple_constraints = [TupleConstraint | TupleConstraints]
    }.

%% @doc Merge two constraint records with AND semantics.
%%
%% Merges attribute constraints from both records. For attributes that appear
%% in both, combines constraints using derive_1op (conjunction).
%%
%% @param C1 First #relation_constraints{} or undefined
%% @param C2 Second #relation_constraints{} or undefined
%% @returns Merged #relation_constraints{}
-spec merge_constraints(#relation_constraints{} | undefined, #relation_constraints{} | undefined) -> #relation_constraints{}.
merge_constraints(undefined, undefined) ->
    empty_constraints();
merge_constraints(undefined, C2) when is_record(C2, relation_constraints) ->
    C2;
merge_constraints(C1, undefined) when is_record(C1, relation_constraints) ->
    C1;
merge_constraints(#relation_constraints{attribute_constraints = AttrConstraints1,
                                        tuple_constraints = TupleConstraints1,
                                        multi_tuple_constraints = MultiTupleConstraints1},
                  #relation_constraints{attribute_constraints = AttrConstraints2,
                                        tuple_constraints = TupleConstraints2,
                                        multi_tuple_constraints = MultiTupleConstraints2}) ->
    %% Merge attribute constraints
    MergedAttrConstraints = maps:fold(
        fun(Attr, Constraint2, Acc) ->
            case maps:get(Attr, Acc, undefined) of
                undefined ->
                    %% Attribute only in C2
                    Acc#{Attr => Constraint2};
                Constraint1 ->
                    %% Attribute in both - merge constraints
                    MergedConstraint = derive_1op(Constraint1, Constraint2#attribute_constraint.constraints),
                    Acc#{Attr => MergedConstraint}
            end
        end,
        AttrConstraints1,
        AttrConstraints2
    ),
    #relation_constraints{
        attribute_constraints = MergedAttrConstraints,
        tuple_constraints = TupleConstraints1 ++ TupleConstraints2,
        multi_tuple_constraints = MultiTupleConstraints1 ++ MultiTupleConstraints2
    }.

%% @doc Filter constraints to only keep those for specified attributes.
%%
%% Used by PROJECT to drop constraints for attributes that are projected away.
%%
%% @param Constraints The #relation_constraints{} or undefined
%% @param Attrs List of attribute names to keep
%% @returns Filtered #relation_constraints{}
-spec filter_constraints(#relation_constraints{} | undefined, [atom()]) -> #relation_constraints{}.
filter_constraints(undefined, _Attrs) ->
    empty_constraints();
filter_constraints(#relation_constraints{attribute_constraints = AttrConstraints} = Constraints, Attrs) ->
    FilteredAttrConstraints = maps:with(Attrs, AttrConstraints),
    Constraints#relation_constraints{
        attribute_constraints = FilteredAttrConstraints
    }.

%% @doc Rename attribute names in constraints according to a mapping.
%%
%% Updates both the attribute field in #attribute_constraint{} and any
%% {var, AttrName} references inside constraint terms. Used by RENAME operator.
%%
%% @param Constraints The #relation_constraints{} or undefined
%% @param RenameMappings Map of #{old_name => new_name}
%% @returns #relation_constraints{} with renamed attributes
-spec rename_constraint_attrs(#relation_constraints{} | undefined, map()) -> #relation_constraints{}.
rename_constraint_attrs(undefined, _RenameMappings) ->
    empty_constraints();
rename_constraint_attrs(#relation_constraints{attribute_constraints = AttrConstraints} = Constraints, RenameMappings) ->
    RenamedAttrConstraints = maps:fold(
        fun(OldAttr, #attribute_constraint{constraints = Cs} = AttrConstraint, Acc) ->
            NewAttr = maps:get(OldAttr, RenameMappings, OldAttr),
            %% Also rename variable references inside constraint terms
            RenamedConstraints = [rename_vars_in_constraint(C, RenameMappings) || C <- Cs],
            RenamedConstraint = AttrConstraint#attribute_constraint{
                attribute = NewAttr,
                constraints = RenamedConstraints
            },
            Acc#{NewAttr => RenamedConstraint}
        end,
        #{},
        AttrConstraints
    ),
    Constraints#relation_constraints{
        attribute_constraints = RenamedAttrConstraints
    }.

%% @doc Infer constraints from a predicate.
%%
%% For opaque function predicates, returns empty constraints (cannot analyze).
%% For declarative predicate terms, extracts constraint structure and creates
%% #attribute_constraint{} records using domain information from schema.
%%
%% Declarative predicate format:
%%   {attr, AttrName, Op, Value} - e.g., {attr, age, lt, 50}
%%   {'and', [Pred1, Pred2, ...]}
%%   {'or', [Pred1, Pred2, ...]}
%% @param Predicate The predicate (function or declarative term)
%% @param Schema The relation schema (for context, maps attr => domain)
%% @returns Inferred #relation_constraints{}
-spec infer_constraints_from_pred(term(), map()) -> #relation_constraints{}.
infer_constraints_from_pred(Predicate, _Schema) when is_function(Predicate) ->
    %% Opaque function - cannot extract constraints
    empty_constraints();
infer_constraints_from_pred({attr, AttrName, Op, Value}, Schema) ->
    %% Declarative constraint on single attribute
    Constraint = op_to_constraint(Op, Value),
    Domain = maps:get(AttrName, Schema, undefined),
    case Domain of
        undefined ->
            %% Attribute not in schema - cannot create constraint
            empty_constraints();
        _ ->
            AttrConstraint = #attribute_constraint{
                name = AttrName,
                attribute = AttrName,
                domain = Domain,
                constraints = [Constraint]
            },
            #relation_constraints{
                attribute_constraints = #{AttrName => AttrConstraint},
                tuple_constraints = [],
                multi_tuple_constraints = []
            }
    end;
infer_constraints_from_pred({'and', Predicates}, Schema) ->
    %% Conjunction - merge all constraints
    lists:foldl(
        fun(Pred, Acc) ->
            PredConstraints = infer_constraints_from_pred(Pred, Schema),
            merge_constraints(Acc, PredConstraints)
        end,
        empty_constraints(),
        Predicates
    );
infer_constraints_from_pred({'or', _Predicates}, _Schema) ->
    %% Disjunction - cannot easily propagate as attribute-level constraints
    %% Would need to track as full formula, not per-attribute
    empty_constraints();
infer_constraints_from_pred(_, _Schema) ->
    %% Unknown format
    empty_constraints().

%% Helper: rename variable references inside a constraint term
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
rename_vars_in_constraint({'not', Constraint, Universe}, Mappings) ->
    {'not', rename_vars_in_constraint(Constraint, Mappings), Universe};
rename_vars_in_constraint(Other, _Mappings) ->
    Other.

op_to_constraint(lt, Value) -> lt({var, value}, Value);
op_to_constraint(lte, Value) -> lte({var, value}, Value);
op_to_constraint(gt, Value) -> gt({var, value}, Value);
op_to_constraint(gte, Value) -> gte({var, value}, Value);
op_to_constraint(eq, Value) -> eq({var, value}, Value);
op_to_constraint(neq, Value) -> neq({var, value}, Value);
op_to_constraint(between, {Min, Max}) -> between(Min, Max);
op_to_constraint(_, _Value) -> {'and', []}.  % No-op constraint

%% Validate tuple against criteria map
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
validate_attribute(Value, Constraint) when is_function(Constraint, 1) ->
    Constraint(Value);
validate_attribute(_, _) -> true.

%% Check if tuple exists in a mutable relation
relation_contains(_Tuple, #relation{tree = undefined}) ->
    false;
relation_contains(Tuple, #relation{tree = Tree}) ->
    %% Would need to hash the tuple and check the Merkle tree
    %% This is a placeholder - actual implementation depends on storage
    TupleHash = erlang:phash2(Tuple),
    gb_trees:is_defined(TupleHash, Tree).

%% Apply variable substitution to a binding
apply_substitution(Binding, Substitution) ->
    maps:map(
        fun(_K, {var, VarName}) ->
            maps:get(VarName, Substitution, {var, VarName});
           (_K, V) ->
            V
        end,
        Binding
    ).

%% Find variables in a binding
find_variables(Binding) ->
    maps:fold(
        fun(_K, {var, VarName}, Acc) -> [VarName | Acc];
           (_K, _, Acc) -> Acc
        end,
        [],
        Binding
    ).

%% Find a substitution that satisfies membership
find_satisfying_substitution(_Tuple, _Variables, _Relation, _Substitution) ->
    %% This would require enumeration via generators
    %% Placeholder for now - full implementation needs generator integration
    false.

%% Get schema from domain or relation
get_schema(#domain{schema = S}) -> S;
get_schema(#relation{schema = S}) -> S.

%% Get name from domain or relation
get_name(#domain{name = N}) -> N;
get_name(#relation{name = N}) -> N.

%% Get lineage from relation
get_lineage(#relation{lineage = L}) -> L;
get_lineage(#domain{name = N}) -> {base, N}.

%% Check if schemas are compatible (same attributes with compatible domains)
schemas_compatible(S1, S2) ->
    %% Schemas are compatible if they have the same structure
    maps:keys(S1) =:= maps:keys(S2).

%% Merge two compatible schemas
merge_schemas(S1, _S2) ->
    %% For now, just use the first schema (they should be identical)
    S1.

%% Get cardinality from domain or relation
get_cardinality(#domain{cardinality = C}) -> C;
get_cardinality(#relation{cardinality = C}) -> C.

%% Compute cardinality of complement
complement_cardinality(R, Universe) ->
    RC = get_cardinality(R),
    UC = get_cardinality(Universe),
    case {RC, UC} of
        {{finite, N}, {finite, M}} -> {finite, max(0, M - N)};
        {{finite, _}, Infinite} -> Infinite;
        _ -> UC
    end.

%% Compute cardinality of intersection
intersection_cardinality(R1, R2) ->
    C1 = get_cardinality(R1),
    C2 = get_cardinality(R2),
    case {C1, C2} of
        {{finite, N}, _} -> {finite, N};  % Upper bound
        {_, {finite, M}} -> {finite, M};  % Upper bound
        _ -> min_cardinality(C1, C2)
    end.

%% Compute cardinality of union
union_cardinality(R1, R2) ->
    C1 = get_cardinality(R1),
    C2 = get_cardinality(R2),
    case {C1, C2} of
        {{finite, N}, {finite, M}} -> {finite, N + M};  % Upper bound
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

%% Evaluate a relational constraint
evaluate_constraint(Database, {member_of, RelName, Binding}, Context) when is_atom(RelName) ->
    %% Get the relation from the database using version hash
    BoundBinding = bind_variables(Binding, Context),
    case get_domain_from_db(Database, RelName) of
        {ok, Relation} ->
            {ok, member(BoundBinding, Relation)};
        {error, not_found} ->
            {error, {relation_not_found, RelName}}
    end;
evaluate_constraint(_Database, {member_of, #domain{} = Rel, Binding}, Context) ->
    %% Inline domain record - no database lookup needed
    BoundBinding = bind_variables(Binding, Context),
    {ok, member(BoundBinding, Rel)};
evaluate_constraint(Database, {'not', Constraint, Universe}, Context) ->
    case evaluate_constraint(Database, Constraint, Context) of
        {ok, true} -> {ok, false};
        {ok, false} ->
            %% Must also be in universe
            case evaluate_constraint(Database, {member_of, Universe, Context}, Context) of
                {ok, true} -> {ok, true};
                _ -> {ok, false}
            end;
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
evaluate_constraint(_Database, _, _) ->
    {error, unsupported_constraint}.

bind_variables(Binding, Context) ->
    maps:map(
        fun(_K, {var, VarName}) ->
            maps:get(VarName, Context, maps:get(value, Context, undefined));
           (_K, V) ->
            V
        end,
        Binding
    ).

%% @doc Get domain/relation from database by name, using version hash
%% This replaces the old get_domain/1 function with version-aware lookup
-spec get_domain_from_db(#database_state{}, atom()) ->
    {ok, #relation{}} | {error, not_found}.
get_domain_from_db(Database, DomainName) ->
    case maps:find(DomainName, Database#database_state.relations) of
        {ok, DomainHash} ->
	    io:format("~p~n", [DomainName]),
            [Domain] = mnesia:dirty_read(relation, DomainHash),
            {ok, Domain};
        error ->
            {error, not_found}
    end.

example_1op() ->
    main:setup(),
    DB = operations:create_database(test_db),
    maps:keys(DB#database_state.relations),
    {DB1, _} = operations:create_relation(DB, employees, #{id => integer, name => string, age => integer, salary => integer}),
    IdConstraint = constraint:create_1op(id_positive, id, integer, [constraint:gt({var, value}, 0)]),
    AgeConstraint = constraint:create_1op(age_range, age, integer, [constraint:gte({var, value}, 18), constraint:lt({var,value}, 70)]),
    SalaryConstraint = constraint:create_1op(salary_range, salary, integer, [constraint:between(30000, 200000)]),
    C1 = constraint:add_attribute_constraint(undefined, id, IdConstraint),
    C2 = constraint:add_attribute_constraint(C1, age, AgeConstraint),
    C3 = constraint:add_attribute_constraint(C2, salary, SalaryConstraint),
    {DB2, _UpdatedRel} = operations:update_relation_constraints(DB1, employees, C3),
    {DB3, _} = operations:create_tuple(DB2, employees, #{id => 1, name => "Alice", age => 30, salary => 75000}),
    {DB4, _} = operations:create_tuple(DB3, employees, #{id => -1, name => "Alice", age => 30, salary => 75000}),
    {_DB5, _} = operations:create_tuple(DB4, employees, #{id => 1, name => "Alice", age => 30, salary => 3005000}).

example_2op() ->
    create_2op(tuple_gt_sum,
    [
        {exists, s,
         {'and', [
             {member_of, plus, #{a => {var, b}, b => {var, c}, sum => {var, s}}},
             {member_of, greater_than, #{left => {var, a}, right => {var, s}}}
         ]}}
    ]).
