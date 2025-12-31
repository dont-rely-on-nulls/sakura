%%% @doc Pure Relational Operators
%%%
%%% This module provides pure relational algebra operators that take relations
%%% as input and return new ephemeral relations as output. No database state
%%% is modified. Each operator returns a relation with a generator closure that
%%% can spawn fresh iterators on demand, allowing reproducible query results.
%%%
%%% == Architecture ==
%%%
%%% Relational Operators (Pure Functions):
%%% - Input: One or more #relation{} records
%%% - Output: New ephemeral #relation{} with generator closure
%%% - No side effects, no DB modification
%%% - Generator can be called multiple times for same results
%%%
%%% Iterator Operators (Data Transport):
%%% - Input: Iterator PIDs
%%% - Output: Iterator PID
%%% - Used inside generator closures
%%% - Examples: select_iterator/2, project_iterator/2, take_iterator/2
%%%
%%% @author Nekoma Team
%%% @copyright 2025

-module(relational_operators).

-include("../include/operations.hrl").

-export([
    take/2,
    select/2,
    project/2,
    join/3,
    theta_join/3,
    rename/2,
    sort/2
]).

%%% ============================================================================
%%% Internal Helper Functions
%%% ============================================================================

%% @private
%% Generate a random name for ephemeral relations.
generate_ephemeral_name() ->
    {UUID, _} = uuid:get_v1(uuid:new(self(), erlang)),
    [Base64UUID] = io_lib:format("~s", [base64:encode(UUID)]),
    Base64UUID.

%%% ============================================================================
%%% Relational Operators
%%% ============================================================================

%% @doc Take operator (τ): Returns ephemeral relation with first N tuples.
%%
%% Creates an ephemeral relation containing N arbitrary elements from the
%% source relation. The generator closure captures the source relation and N,
%% spawning fresh iterators on each call.
%%
%% == Example ==
%% <pre>
%% NaturalRel = get_relation(DB, natural),
%% Some100 = relational_operators:take(NaturalRel, 100),
%%
%% %% Can call generator multiple times
%% Iter1 = operations:get_iterator_from_generator(ephemeral, Some100#relation.generator),
%% Results1 = operations:collect_all(Iter1),
%%
%% Iter2 = operations:get_iterator_from_generator(ephemeral, Some100#relation.generator),
%% Results2 = operations:collect_all(Iter2),
%% %% Results1 =:= Results2
%% </pre>
%%
%% @param SourceRelation Source #relation{} record
%% @param N Number of tuples to take
%% @returns Ephemeral #relation{} with generator
-spec take(#relation{}, pos_integer()) -> #relation{}.
take(SourceRelation, N) when is_record(SourceRelation, relation), is_integer(N), N > 0 ->
    %% Compute result cardinality
    ResultCardinality = case SourceRelation#relation.cardinality of
        {finite, M} when M < N -> {finite, M};
        {finite, _} -> {finite, N};
        _ -> {finite, N}
    end,
    Name = generate_ephemeral_name(),
    SourceGen = SourceRelation#relation.generator,
    GeneratorFun = fun() ->
        SourceIter = SourceGen(),
        operations:take_iterator(SourceIter, N)
    end,
    #relation{
        hash = <<>>,
        name = Name,
        tree = undefined,
        schema = SourceRelation#relation.schema,
        constraints = SourceRelation#relation.constraints,
        cardinality = ResultCardinality,
        generator = GeneratorFun,
        membership_criteria = #{},
        provenance = SourceRelation#relation.provenance,
        lineage = {take, N, SourceRelation#relation.lineage}
    }.

%% @doc Select operator (σ): Returns ephemeral relation filtered by predicate.
%%
%% Creates an ephemeral relation containing only tuples that satisfy the
%% predicate. The generator closure captures the source relation and predicate.
%%
%% Constraints are propagated: source constraints are preserved, and new
%% constraints may be inferred from declarative predicates.
%%
%% Predicate formats:
%% - fun((map()) -> boolean()) - opaque function, no inference
%% - {attr, AttrName, Op, Value} - declarative, infers constraints
%% - {'and', [Pred1, ...]} - conjunction of declarative predicates
%%
%% @param SourceRelation Source #relation{} record
%% @param Predicate Function or declarative term
%% @returns Ephemeral #relation{} with generator and propagated constraints
-spec select(#relation{}, fun((map()) -> boolean()) | term()) -> #relation{}.
select(SourceRelation, Predicate) when is_record(SourceRelation, relation) ->
    Name = generate_ephemeral_name(),

    %% Build actual filter function from predicate
    FilterFun = predicate_to_function(Predicate),

    SourceGen = SourceRelation#relation.generator,
    GeneratorFun = fun() ->
        SourceIter = SourceGen(),
        operations:select_iterator(SourceIter, FilterFun)
    end,

    %% Infer constraints from predicate and merge with source constraints
    Schema = SourceRelation#relation.schema,
    InferredConstraints = constraint:infer_constraints_from_pred(Predicate, Schema),
    MergedConstraints = constraint:merge_constraints(
        SourceRelation#relation.constraints,
        InferredConstraints
    ),

    #relation{
        hash = <<>>,
        name = Name,
        tree = undefined,
        schema = Schema,
        constraints = MergedConstraints,
        cardinality = unknown,  % Cannot determine without evaluation
        generator = GeneratorFun,
        membership_criteria = #{},
        provenance = SourceRelation#relation.provenance,
        lineage = {select, Predicate, SourceRelation#relation.lineage}
    }.

%% @doc Project operator (π): Returns ephemeral relation with selected attributes.
%%
%% Creates an ephemeral relation containing only the specified attributes.
%% The generator closure captures the source relation and attribute list.
%%
%% Constraints are filtered to only include those for projected attributes.
%% Constraints on attributes that are projected away are dropped.
%%
%% @param SourceRelation Source #relation{} record
%% @param Attributes List of attribute names to keep
%% @returns Ephemeral #relation{} with generator and filtered constraints
-spec project(#relation{}, [atom()]) -> #relation{}.
project(SourceRelation, Attributes) when is_record(SourceRelation, relation), is_list(Attributes) ->
    Name = generate_ephemeral_name(),

    %% Compute projected schema
    ProjectedSchema = maps:with(Attributes, SourceRelation#relation.schema),

    %% Filter constraints to only projected attributes
    FilteredConstraints = constraint:filter_constraints(
        SourceRelation#relation.constraints,
        Attributes
    ),

    SourceGen = SourceRelation#relation.generator,
    GeneratorFun = fun() ->
        SourceIter = SourceGen(),
        operations:project_iterator(SourceIter, Attributes)
    end,

    #relation{
        hash = <<>>,
        name = Name,
        tree = undefined,
        schema = ProjectedSchema,
        constraints = FilteredConstraints,
        cardinality = SourceRelation#relation.cardinality,
        generator = GeneratorFun,
        membership_criteria = #{},
        provenance = SourceRelation#relation.provenance,
        lineage = {project, Attributes, SourceRelation#relation.lineage}
    }.

%% @doc Join operator (⋈): Returns ephemeral relation from equijoin.
%%
%% Creates an ephemeral relation by joining two relations on a common attribute.
%% The generator closure captures both source relations and the join attribute.
%%
%% Constraints from both relations are merged with AND semantics.
%% If both relations have constraints on the same attribute, both are kept.
%%
%% @param LeftRelation Left #relation{} record
%% @param RightRelation Right #relation{} record
%% @param JoinAttribute Attribute name to join on
%% @returns Ephemeral #relation{} with generator and merged constraints
-spec join(#relation{}, #relation{}, atom()) -> #relation{}.
join(LeftRelation, RightRelation, JoinAttribute)
    when is_record(LeftRelation, relation),
         is_record(RightRelation, relation),
         is_atom(JoinAttribute) ->

    Name = list_to_atom(
        "join_" ++ atom_to_list(LeftRelation#relation.name) ++ "_" ++
        atom_to_list(RightRelation#relation.name) ++ "_" ++
        integer_to_list(erlang:unique_integer([positive]))
    ),

    %% Merge schemas
    MergedSchema = maps:merge(LeftRelation#relation.schema, RightRelation#relation.schema),

    %% Merge constraints from both relations (AND semantics)
    MergedConstraints = constraint:merge_constraints(
        LeftRelation#relation.constraints,
        RightRelation#relation.constraints
    ),

    LeftGen = LeftRelation#relation.generator,
    RightGen = RightRelation#relation.generator,
    GeneratorFun = fun() ->
        LeftIter = LeftGen(),
        RightIter = RightGen(),
        operations:equijoin_iterator(LeftIter, RightIter, JoinAttribute)
    end,

    #relation{
        hash = <<>>,
        name = Name,
        tree = undefined,
        schema = MergedSchema,
        constraints = MergedConstraints,
        cardinality = unknown,
        generator = GeneratorFun,
        membership_criteria = #{},
        provenance = {join, LeftRelation#relation.provenance, RightRelation#relation.provenance},
        lineage = {join, JoinAttribute, LeftRelation#relation.lineage, RightRelation#relation.lineage}
    }.

%% @doc Theta Join operator (⋈θ): Returns ephemeral relation from theta join.
%%
%% Creates an ephemeral relation by joining two relations with an arbitrary
%% predicate. The generator closure captures both source relations and predicate.
%%
%% Constraints are merged from both relations, and additional constraints may
%% be inferred from declarative predicates.
%%
%% @param LeftRelation Left #relation{} record
%% @param RightRelation Right #relation{} record
%% @param Predicate Function or declarative term
%% @returns Ephemeral #relation{} with generator and merged constraints
-spec theta_join(#relation{}, #relation{}, fun((map(), map()) -> boolean()) | term()) -> #relation{}.
theta_join(LeftRelation, RightRelation, Predicate)
    when is_record(LeftRelation, relation),
         is_record(RightRelation, relation) ->

    Name = list_to_atom(
        "theta_join_" ++ atom_to_list(LeftRelation#relation.name) ++ "_" ++
        atom_to_list(RightRelation#relation.name) ++ "_" ++
        integer_to_list(erlang:unique_integer([positive]))
    ),

    MergedSchema = maps:merge(LeftRelation#relation.schema, RightRelation#relation.schema),

    %% Merge constraints from both relations
    MergedConstraints = constraint:merge_constraints(
        LeftRelation#relation.constraints,
        RightRelation#relation.constraints
    ),

    %% Infer additional constraints from predicate
    InferredConstraints = constraint:infer_constraints_from_pred(Predicate, MergedSchema),
    FinalConstraints = constraint:merge_constraints(MergedConstraints, InferredConstraints),

    %% Build filter function from predicate (for theta join, needs 2-arity)
    %% The theta_join_iterator expects a 2-arity function
    FilterFun = theta_predicate_to_function(Predicate),

    LeftGen = LeftRelation#relation.generator,
    RightGen = RightRelation#relation.generator,
    GeneratorFun = fun() ->
        LeftIter = LeftGen(),
        RightIter = RightGen(),
        operations:theta_join_iterator(LeftIter, RightIter, FilterFun)
    end,

    #relation{
        hash = <<>>,
        name = Name,
        tree = undefined,
        schema = MergedSchema,
        constraints = FinalConstraints,
        cardinality = unknown,
        generator = GeneratorFun,
        membership_criteria = #{},
        provenance = {join, LeftRelation#relation.provenance, RightRelation#relation.provenance},
        lineage = {theta_join, Predicate, LeftRelation#relation.lineage, RightRelation#relation.lineage}
    }.

%% @doc Rename operator (ρ): Returns ephemeral relation with renamed attributes.
%%
%% Creates an ephemeral relation with attributes renamed according to the mapping.
%% The generator closure captures the source relation and rename mappings.
%%
%% Constraints are updated: both the map keys and any {var, AttrName} references
%% inside constraint terms are renamed according to the mapping.
%%
%% @param SourceRelation Source #relation{} record
%% @param RenameMappings Map of old_name => new_name (atoms or strings)
%% @returns Ephemeral #relation{} with generator and renamed constraints
-spec rename(#relation{}, #{atom() => atom()}) -> #relation{}.
rename(SourceRelation, RenameMappings) when is_record(SourceRelation, relation), is_map(RenameMappings) ->
    Name = generate_ephemeral_name(),

    %% Compute renamed schema
    RenamedSchema = maps:fold(
        fun(OldAttr, NewAttr, SchemaAcc) ->
            case maps:get(OldAttr, SchemaAcc, undefined) of
                undefined -> SchemaAcc;
                Type ->
                    SchemaAcc1 = maps:remove(OldAttr, SchemaAcc),
                    maps:put(NewAttr, Type, SchemaAcc1)
            end
        end,
        SourceRelation#relation.schema,
        RenameMappings
    ),

    %% Rename constraint attributes (both keys and internal variable references)
    RenamedConstraints = constraint:rename_constraint_attrs(
        SourceRelation#relation.constraints,
        RenameMappings
    ),

    SourceGen = SourceRelation#relation.generator,
    GeneratorFun = fun() ->
        SourceIter = SourceGen(),
        rename_iterator(SourceIter, RenameMappings)
    end,

    #relation{
        hash = <<>>,
        name = Name,
        tree = undefined,
        schema = RenamedSchema,
        constraints = RenamedConstraints,
        cardinality = SourceRelation#relation.cardinality,
        generator = GeneratorFun,
        membership_criteria = #{},
        provenance = SourceRelation#relation.provenance,
        lineage = {rename, RenameMappings, SourceRelation#relation.lineage}
    }.

%% @doc Sort operator: Returns ephemeral relation with sorted tuples.
%%
%% Creates an ephemeral relation with tuples sorted by the comparator.
%% WARNING: This is a blocking operation - it materializes all tuples.
%%
%% @param SourceRelation Source #relation{} record
%% @param Comparator Function that compares two tuples
%% @returns Ephemeral #relation{} with generator
-spec sort(#relation{}, fun((map(), map()) -> boolean())) -> #relation{}.
sort(SourceRelation, Comparator) when is_record(SourceRelation, relation), is_function(Comparator, 2) ->
    Name = generate_ephemeral_name(),

    SourceGen = SourceRelation#relation.generator,
    GeneratorFun = fun() ->
        SourceIter = SourceGen(),
        operations:sort_iterator(SourceIter, Comparator)
    end,

    #relation{
        hash = <<>>,
        name = Name,
        tree = undefined,
        schema = SourceRelation#relation.schema,
        constraints = SourceRelation#relation.constraints,
        cardinality = SourceRelation#relation.cardinality,
        generator = GeneratorFun,
        membership_criteria = #{},
        provenance = SourceRelation#relation.provenance,
        lineage = {sort, Comparator, SourceRelation#relation.lineage}
    }.

%%% Helper Functions

%% @private
%% @doc Rename iterator - applies rename mapping to each tuple
rename_iterator(SourceIter, RenameMappings) ->
    spawn(fun() -> rename_loop(SourceIter, RenameMappings) end).

rename_loop(SourceIter, RenameMappings) ->
    receive
        {next, Caller} ->
            case operations:next_tuple(SourceIter) of
                done ->
                    Caller ! done;
                {ok, Tuple} ->
                    RenamedTuple = maps:fold(
                        fun(OldAttr, NewAttr, TupleAcc) ->
                            case maps:get(OldAttr, TupleAcc, undefined) of
                                undefined -> TupleAcc;
                                Value ->
                                    TupleAcc1 = maps:remove(OldAttr, TupleAcc),
                                    maps:put(NewAttr, Value, TupleAcc1)
                            end
                        end,
                        Tuple,
                        RenameMappings
                    ),
                    Caller ! {tuple, RenamedTuple},
                    rename_loop(SourceIter, RenameMappings)
            end;
        {close, Caller} ->
            operations:close_iterator(SourceIter),
            Caller ! ok
    end.

%% @private
%% @doc Convert a predicate (function or declarative term) to a filter function.
%%
%% Supports:
%% - fun((map()) -> boolean()) - returns as-is
%% - {attr, AttrName, Op, Value} - single attribute comparison
%% - {'and', [Pred1, ...]} - conjunction
%% - {'or', [Pred1, ...]} - disjunction
predicate_to_function(Predicate) when is_function(Predicate, 1) ->
    Predicate;
predicate_to_function({attr, AttrName, Op, Value}) ->
    fun(Tuple) ->
        AttrValue = maps:get(AttrName, Tuple, undefined),
        apply_comparison(Op, AttrValue, Value)
    end;
predicate_to_function({'and', Predicates}) ->
    Funs = [predicate_to_function(P) || P <- Predicates],
    fun(Tuple) ->
        lists:all(fun(F) -> F(Tuple) end, Funs)
    end;
predicate_to_function({'or', Predicates}) ->
    Funs = [predicate_to_function(P) || P <- Predicates],
    fun(Tuple) ->
        lists:any(fun(F) -> F(Tuple) end, Funs)
    end;
predicate_to_function(_) ->
    %% Unknown format - accept all
    fun(_) -> true end.

%% @private
%% @doc Apply a comparison operation.
apply_comparison(lt, A, B) -> A < B;
apply_comparison(lte, A, B) -> A =< B;
apply_comparison(gt, A, B) -> A > B;
apply_comparison(gte, A, B) -> A >= B;
apply_comparison(eq, A, B) -> A =:= B;
apply_comparison(neq, A, B) -> A =/= B;
apply_comparison(between, A, {Min, Max}) -> A >= Min andalso A < Max;
apply_comparison(_, _, _) -> true.

%% @private
%% @doc Convert a theta predicate to a 2-arity filter function.
%%
%% Theta join predicates operate on merged tuples from both relations.
%% For declarative predicates, we convert to a function that works on
%% the merged tuple (left and right tuples combined).
theta_predicate_to_function(Predicate) when is_function(Predicate, 2) ->
    Predicate;
theta_predicate_to_function(Predicate) when is_function(Predicate, 1) ->
    %% Convert 1-arity function to 2-arity by merging tuples
    fun(LeftTuple, RightTuple) ->
        MergedTuple = maps:merge(LeftTuple, RightTuple),
        Predicate(MergedTuple)
    end;
theta_predicate_to_function(DeclarativePred) ->
    %% Convert declarative predicate to 1-arity first, then wrap
    OneTupleFun = predicate_to_function(DeclarativePred),
    fun(LeftTuple, RightTuple) ->
        MergedTuple = maps:merge(LeftTuple, RightTuple),
        OneTupleFun(MergedTuple)
    end.
