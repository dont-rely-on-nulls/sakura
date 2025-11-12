-module(main).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    %% Start Mnesia (or other children) here
    io:format("Starting app...~n"),

    %% Ensure Mnesia is started
    mnesia:start(),

    {ok, self()}.

stop(_State) ->
    io:format("Stopping app...~n"),
    ok.

%% setup() ->
%%     mnesia:create_table(attributes, [{type, set},
%% 				 {attributes, [name, value, type]},
%% 				 {ram_copies, [node()]},
%% 				 {index, [name]}
%% 				]).


