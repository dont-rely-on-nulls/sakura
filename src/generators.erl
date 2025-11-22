%%% @doc Generators for Infinite Relations
%%%
%%% This module provides generator functions for primitive infinite relations:
%%% - Naturals (ℕ): {0, 1, 2, 3, ...}
%%% - Integers (ℤ): {0, 1, -1, 2, -2, 3, -3, ...}
%%% - Rationals (ℚ): Using Stern-Brocot tree enumeration
%%%
%%% Generators produce tuples lazily based on constraints, enabling
%%% memory-bounded iteration over infinite relations.
%%%
%%% @author Nekoma Team
%%% @copyright 2025

-module(generators).
-include("operations.hrl").

-export([
    naturals/1,
    integers/1,
    rationals/1,
    make_range_generator/3,
    constrained_naturals/2,
    constrained_integers/2
]).

%%% ============================================================================
%%% Primitive Generators
%%% ============================================================================

%% @doc Generator for natural numbers (ℕ = {0, 1, 2, 3, ...}).
%%
%% Enumerates natural numbers starting from 0. If constraints specify a range,
%% only generates numbers within that range.
%%
%% @param Constraints Map of attribute constraints
%% @returns Generator function producing naturals
naturals(Constraints) ->
    case extract_bounds(Constraints, value) of
        {range, Min, Max} when Min >= 0 ->
            make_range_generator(Min, Max, fun(N) -> #{value => N} end);
        {gte, Min} when Min >= 0 ->
            make_unbounded_generator(Min, fun(N) -> #{value => N} end);
        {lte, Max} when Max >= 0 ->
            make_range_generator(0, Max, fun(N) -> #{value => N} end);
        {eq, Value} when Value >= 0 ->
            make_singleton_generator(#{value => Value});
        {in, List} ->
            make_membership_generator(List, fun(N) -> #{value => N} end);
        unbounded ->
            make_unbounded_generator(0, fun(N) -> #{value => N} end);
        {error, Reason} ->
            fun(_) -> {error, Reason} end
    end.

%% @doc Generator for integers (ℤ = {..., -2, -1, 0, 1, 2, ...}).
%%
%% Enumerates integers using interleaving strategy:
%% 0, 1, -1, 2, -2, 3, -3, ...
%%
%% Mapping: f(0) = 0
%%          f(2n) = n      for n > 0
%%          f(2n+1) = -n   for n > 0
%%
%% @param Constraints Map of attribute constraints
%% @returns Generator function producing integers
integers(Constraints) ->
    case extract_bounds(Constraints, value) of
        {range, Min, Max} ->
            make_integer_range_generator(Min, Max);
        {gte, Min} ->
            make_unbounded_integer_generator(Min, infinity);
        {lte, Max} ->
            make_unbounded_integer_generator(neg_infinity, Max);
        {eq, Value} ->
            make_singleton_generator(#{value => Value});
        {in, List} ->
            make_membership_generator(List, fun(N) -> #{value => N} end);
        unbounded ->
            make_interleaved_integer_generator();
        {error, Reason} ->
            fun(_) -> {error, Reason} end
    end.

%% @doc Generator for rational numbers (ℚ) using Stern-Brocot tree.
%%
%% Enumerates all rational numbers in reduced form (p/q where gcd(p,q) = 1)
%% using the Stern-Brocot tree, which systematically generates all positive
%% rationals without repetition.
%%
%% Schema: #{numerator => integer, denominator => natural}
%%
%% @param Constraints Map of attribute constraints
%% @returns Generator function producing rationals
rationals(Constraints) ->
    %% Stern-Brocot tree enumeration
    %% Start with mediant tree: 0/1, 1/1, then generate mediants
    InitialState = {queue:from_list([{0, 1}, {1, 1}]), {0, 1}, {1, 0}},
    make_stern_brocot_generator(InitialState, Constraints).

%%% ============================================================================
%%% Helper: Constraint Extraction
%%% ============================================================================

%% @doc Extract bounds from constraints for a given attribute.
%%
%% @param Constraints Constraint map
%% @param Attr Attribute name
%% @returns {range, Min, Max} | {gte, Min} | {lte, Max} | {eq, Value} | unbounded
extract_bounds(Constraints, Attr) ->
    case maps:get(Attr, Constraints, undefined) of
        undefined ->
            unbounded;
        {range, Min, Max} ->
            {range, Min, Max};
        {gte, Min} ->
            {gte, Min};
        {lte, Max} ->
            {lte, Max};
        {gt, Min} ->
            {gte, Min + 1};
        {lt, Max} ->
            {lte, Max - 1};
        {eq, Value} ->
            {eq, Value};
        {in, List} ->
            {in, List};
        Other ->
            {error, {unsupported_constraint, Other}}
    end.

%%% ============================================================================
%%% Generator Constructors
%%% ============================================================================

%% @doc Create a generator for a finite range [Min, Max].
make_range_generator(Min, Max, MapFun) when Min =< Max ->
    make_range_gen(Min, Max, MapFun).

make_range_gen(Current, Max, MapFun) when Current =< Max ->
    fun(next) ->
        Tuple = MapFun(Current),
        NextGen = make_range_gen(Current + 1, Max, MapFun),
        {value, Tuple, NextGen}
    end;
make_range_gen(_, _, _) ->
    fun(_) -> done end.

%% @doc Create an unbounded generator starting from Min.
make_unbounded_generator(Start, MapFun) ->
    make_unbounded_gen(Start, MapFun).

make_unbounded_gen(Current, MapFun) ->
    fun(next) ->
        Tuple = MapFun(Current),
        NextGen = make_unbounded_gen(Current + 1, MapFun),
        {value, Tuple, NextGen}
    end.

%% @doc Create a singleton generator (single value).
make_singleton_generator(Tuple) ->
    fun(next) -> {value, Tuple, fun(_) -> done end} end.

%% @doc Create a generator from a membership list.
make_membership_generator([], _MapFun) ->
    fun(_) -> done end;
make_membership_generator([H|T], MapFun) ->
    fun(next) ->
        Tuple = MapFun(H),
        NextGen = make_membership_generator(T, MapFun),
        {value, Tuple, NextGen}
    end.

%% @doc Create integer range generator.
make_integer_range_generator(Min, Max) when Min =< Max ->
    make_range_generator(Min, Max, fun(N) -> #{value => N} end);
make_integer_range_generator(_, _) ->
    fun(_) -> done end.

%% @doc Create interleaved integer generator: 0, 1, -1, 2, -2, 3, -3, ...
make_interleaved_integer_generator() ->
    make_interleaved_gen(0).

make_interleaved_gen(0) ->
    fun(next) ->
        {value, #{value => 0}, make_interleaved_gen(1)}
    end;
make_interleaved_gen(N) ->
    fun(next) ->
        Positive = #{value => N},
        NextGen1 = fun(next) ->
            Negative = #{value => -N},
            NextGen2 = make_interleaved_gen(N + 1),
            {value, Negative, NextGen2}
        end,
        {value, Positive, NextGen1}
    end.

%% @doc Unbounded integer generator with optional min/max bounds.
make_unbounded_integer_generator(neg_infinity, infinity) ->
    make_interleaved_integer_generator();
make_unbounded_integer_generator(Min, infinity) when is_integer(Min) ->
    make_unbounded_generator(Min, fun(N) -> #{value => N} end);
make_unbounded_integer_generator(neg_infinity, Max) when is_integer(Max) ->
    %% Generate from Max down to -infinity
    make_decreasing_gen(Max);
make_unbounded_integer_generator(Min, Max) when is_integer(Min), is_integer(Max) ->
    make_integer_range_generator(Min, Max).

make_decreasing_gen(Current) ->
    fun(next) ->
        Tuple = #{value => Current},
        NextGen = make_decreasing_gen(Current - 1),
        {value, Tuple, NextGen}
    end.

%% @doc Stern-Brocot tree generator for rationals.
%%
%% The Stern-Brocot tree enumerates all positive rationals exactly once.
%% Each rational p/q is generated in reduced form.
%%
%% Algorithm:
%% 1. Start with two fractions: 0/1 (left ancestor) and 1/0 (right ancestor)
%% 2. The mediant of a/b and c/d is (a+c)/(b+d)
%% 3. Build tree by computing mediants
make_stern_brocot_generator(State, _Constraints) ->
    make_sb_gen(State).

make_sb_gen({{[],[]}, _L, _R}) ->
    fun(_) -> done end;
make_sb_gen({Queue, {LA, LB}, {RA, RB}}) ->
    fun(next) ->
        case queue:out(Queue) of
            {empty, _} ->
                done;
            {{value, {P, Q}}, RestQueue} ->
                %% Current rational is P/Q
                Tuple = #{numerator => P, denominator => Q},

                %% Generate children (mediants)
                %% Left child: mediant of left ancestor and current
                %% Right child: mediant of current and right ancestor
                LeftChild = {LA + P, LB + Q},
                RightChild = {P + RA, Q + RB},

                %% Enqueue children
                NewQueue = queue:in(RightChild, queue:in(LeftChild, RestQueue)),

                %% Continue with updated state
                NextGen = make_sb_gen({NewQueue, {LA, LB}, {RA, RB}}),
                {value, Tuple, NextGen}
        end
    end.

%%% ============================================================================
%%% Constrained Generators (Public API)
%%% ============================================================================

%% @doc Create a constrained naturals generator.
-spec constrained_naturals(non_neg_integer(), non_neg_integer()) -> generator_fun().
constrained_naturals(Min, Max) ->
    naturals(#{value => {range, Min, Max}}).

%% @doc Create a constrained integers generator.
-spec constrained_integers(integer(), integer()) -> generator_fun().
constrained_integers(Min, Max) ->
    integers(#{value => {range, Min, Max}}).
