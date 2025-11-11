-module(operations).
-export([select/2, project/2, cartesian_product/2]).

%% @doc Selection - filter tuples based on a predicate
%% `Predicate` is a function that takes a tuple and returns true/false
select(Relation, Predicate) ->
    sets:filter(Predicate, Relation).

%% @doc Projection - select specific attributes from a relation
%% Note that `AttributeNames` can have duplicate keys, which are ignored
project(Relation, AttributeNames) when is_list(AttributeNames) ->
    sets:map(fun (Tuple) -> maps:with(AttributeNames, Tuple) end, Relation).

cartesian_product(Relation1, Relation2) ->
    sets:from_list([maps:merge(Tuple1, Tuple2) || Tuple1 <- sets:to_list(Relation1), Tuple2 <- sets:to_list(Relation2)], [{version, 2}]).
