-module(provenance_test).
-include_lib("eunit/include/eunit.hrl").
-include("operations.hrl").

%%% Provenance Tracking Test Suite
%%%
%%% Tests for attribute-level provenance tracking in relational operations.
%%% Provenance tracks the origin of each attribute through query pipelines.
%%%
%%% NOTE ON TUPLE ORDERING:
%%% These tests observe tuples in a specific order (e.g., Alice before Bob) because
%%% collect_all/1 currently returns results in Merkle tree traversal order for finite
%%% relations. This ordering is an IMPLEMENTATION DETAIL and is NOT guaranteed.
%%% Relations are unordered sets - the current order is based on how tuples were
%%% inserted and hashed into the Merkle tree. Future implementations may return tuples
%%% in different orders. Tests use lists:all/2 to verify properties hold for ALL tuples
%%% rather than relying on specific ordering.

provenance_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_base_provenance/1,
      fun test_join_provenance/1,
      fun test_join_with_conflicts_provenance/1,
      fun test_equijoin_preserves_both_sources/1
     ]}.

%%% Setup and Cleanup

setup() ->
    operations:setup(),
    DB = operations:create_database(test_db),

    % Create employees relation
    {DB1, _} = operations:create_relation(DB, employees, #{
        id => integer,
        name => string,
        dept_id => integer
    }),

    {DB2, _} = operations:create_tuple(DB1, employees, #{id => 1, name => "Alice", dept_id => 10}),
    {DB3, _} = operations:create_tuple(DB2, employees, #{id => 2, name => "Bob", dept_id => 20}),

    % Create departments relation
    {DB4, _} = operations:create_relation(DB3, departments, #{
        dept_id => integer,
        dept_name => string,
        budget => integer
    }),

    {DB5, _} = operations:create_tuple(DB4, departments, #{dept_id => 10, dept_name => "Engineering", budget => 100000}),
    {DB6, _} = operations:create_tuple(DB5, departments, #{dept_id => 20, dept_name => "Sales", budget => 80000}),

    DB6.

cleanup(_DB) ->
    ok.

%%% Base Provenance Tests

test_base_provenance(DB) ->
    % Query with provenance enabled
    Iter = operations:get_tuples_iterator(DB, employees, #{'_provenance' => true}),
    Results = operations:collect_all(Iter),

    % All tuples should have correct provenance structure
    [
     ?_assertEqual(2, length(Results)),
     ?_assert(lists:all(fun(T) ->
                           maps:is_key(meta, T) andalso
                           begin
                               Meta = maps:get(meta, T),
                               maps:is_key(provenance, Meta)
                           end
                       end, Results)),
     % Check ALL tuples have correct provenance mapping
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Prov = maps:get(provenance, Meta),
                           % All attributes should map to {employees, attr_name}
                           maps:get(id, Prov) =:= {employees, id} andalso
                           maps:get(name, Prov) =:= {employees, name} andalso
                           maps:get(dept_id, Prov) =:= {employees, dept_id}
                       end, Results))
    ].

%%% Join Provenance Tests

test_join_provenance(DB) ->
    % Create iterators with provenance enabled
    EmpIter = operations:get_tuples_iterator(DB, employees, #{'_provenance' => true}),
    DeptIter = operations:get_tuples_iterator(DB, departments, #{'_provenance' => true}),

    % Join them
    JoinIter = operations:equijoin_iterator(EmpIter, DeptIter, dept_id),
    Results = operations:collect_all(JoinIter),

    % All results should have metadata with provenance
    [
     ?_assertEqual(2, length(Results)),
     ?_assert(lists:all(fun(T) -> maps:is_key(meta, T) end, Results)),
     % Check that ALL tuples have correct provenance structure
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Prov = maps:get(provenance, Meta),
                           % Employee attributes should come from employees
                           maps:get(id, Prov) =:= {employees, id} andalso
                           maps:get(name, Prov) =:= {employees, name} andalso
                           % Department attributes should come from departments
                           maps:get(dept_name, Prov) =:= {departments, dept_name} andalso
                           maps:get(budget, Prov) =:= {departments, budget}
                       end, Results))
    ].

test_join_with_conflicts_provenance(DB) ->
    % Create relations with conflicting attribute names
    {DB1, _} = operations:create_relation(DB, table_a, #{
        id => integer,
        value => integer
    }),
    {DB2, _} = operations:create_tuple(DB1, table_a, #{id => 1, value => 100}),

    {DB3, _} = operations:create_relation(DB2, table_b, #{
        id => integer,
        value => integer  % Conflict!
    }),
    {DB4, _} = operations:create_tuple(DB3, table_b, #{id => 1, value => 200}),

    % Create iterators with provenance
    AIter = operations:get_tuples_iterator(DB4, table_a, #{'_provenance' => true}),
    BIter = operations:get_tuples_iterator(DB4, table_b, #{'_provenance' => true}),

    JoinIter = operations:equijoin_iterator(AIter, BIter, id),
    Results = operations:collect_all(JoinIter),

    % Should be exactly one result tuple
    ?_assertEqual(1, length(Results)),

    % Check provenance handles conflicts correctly
    [Result] = Results,
    Meta = maps:get(meta, Result),
    Prov = maps:get(provenance, Meta),

    [
     % Left 'value' provenance
     ?_assertEqual({table_a, value}, maps:get(value, Prov)),
     % Right 'value' provenance (prefixed)
     ?_assertEqual({table_b, value}, maps:get(right_value, Prov)),
     % Join attribute from left side
     ?_assertEqual({table_a, id}, maps:get(id, Prov)),
     % Join attribute from right side (prefixed)
     ?_assertEqual({table_b, id}, maps:get(right_id, Prov))
    ].

test_equijoin_preserves_both_sources(DB) ->
    % Even though equijoin only keeps one dept_id value,
    % provenance should track both sources
    EmpIter = operations:get_tuples_iterator(DB, employees, #{'_provenance' => true}),
    DeptIter = operations:get_tuples_iterator(DB, departments, #{'_provenance' => true}),

    JoinIter = operations:equijoin_iterator(EmpIter, DeptIter, dept_id),
    Results = operations:collect_all(JoinIter),

    % Check that ALL tuples have both dept_id sources in provenance
    [
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Prov = maps:get(provenance, Meta),
                           % Left side dept_id
                           maps:is_key(dept_id, Prov) andalso
                           % Right side dept_id (prefixed)
                           maps:is_key(right_dept_id, Prov)
                       end, Results)),
     % Verify the sources are correct for ALL tuples
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Prov = maps:get(provenance, Meta),
                           maps:get(dept_id, Prov) =:= {employees, dept_id} andalso
                           maps:get(right_dept_id, Prov) =:= {departments, dept_id}
                       end, Results))
    ].
