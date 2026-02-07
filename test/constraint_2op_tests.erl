-module(constraint_2op_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/operations.hrl").

exists_quantifier_inference_plus_test() ->
    main:setup(),
    DB0 = operations:create_database(test_db),
    {DB1, _} = operations:create_relation(DB0, employees, #{a => integer, b => integer, c => integer}),

    TupleConstraint = constraint:create_2op(a_gt_b_plus_c,
    [
        {exists, s, integer,
         {'and', [
             {member_of, plus, #{a => {var, b}, b => {var, c}, sum => {var, s}}},
             {member_of, greater_than, #{left => {var, a}, right => {var, s}}}
         ]}}
    ]),

    Constraints = constraint:add_tuple_constraint(undefined, TupleConstraint),
    {DB2, _} = operations:update_relation_constraints(DB1, employees, Constraints),

    {DB3, _} = operations:create_tuple(DB2, employees, #{a => 10, b => 3, c => 4}),
    Rejected = operations:create_tuple(DB3, employees, #{a => 6, b => 3, c => 4}),

    ?assertMatch({error, {constraint_violation, {tuple_constraint_violations, [_ | _]}}}, Rejected),

    {error, {constraint_violation, {tuple_constraint_violations, [Violation | _]}}} = Rejected,
    Diagnostics = maps:get(diagnostics, Violation, []),

    ?assert(lists:any(fun(D) -> maps:get(kind, D, undefined) =:= quantified_by_inference end, Diagnostics)),
    ?assert(lists:any(fun(D) -> maps:get(kind, D, undefined) =:= member_of_false end, Diagnostics)).

forall_infinite_domain_rejected_test() ->
    main:setup(),
    DB0 = operations:create_database(test_db),
    {DB1, _} = operations:create_relation(DB0, employees, #{a => integer, b => integer, c => integer}),

    TupleConstraint = constraint:create_2op(forall_integer_rejected,
    [
        {forall, s, integer,
         {member_of, greater_than, #{left => {var, a}, right => {var, s}}}}
    ]),

    Constraints = constraint:add_tuple_constraint(undefined, TupleConstraint),
    {DB2, _} = operations:update_relation_constraints(DB1, employees, Constraints),
    Rejected = operations:create_tuple(DB2, employees, #{a => 10, b => 3, c => 4}),

    ?assertMatch({error, {constraint_violation, {tuple_constraint_violations, [_ | _]}}}, Rejected),
    {error, {constraint_violation, {tuple_constraint_violations, [Violation | _]}}} = Rejected,
    Diagnostics = maps:get(diagnostics, Violation, []),
    ?assert(lists:any(fun(D) -> maps:get(kind, D, undefined) =:= unbounded_quantifier end, Diagnostics)).
