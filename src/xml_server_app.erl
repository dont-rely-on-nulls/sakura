%%% @doc XML Server Application Starter
%%%
%%% Convenient module for starting the XML server with all dependencies.

-module(xml_server_app).
-export([start/0, start/1]).

%% @doc Start XML server on default port 8080.
start() ->
    start(8080).

%% @doc Start XML server on specified port.
start(Port) ->
    % Initialize Sakura
    main:setup(),
    io:format("~nSakura initialized~n"),

    % Create example database
    DB = repl:example_db(),

    % Start XML server
    xml_server:start(Port, DB),

    io:format("~nXML Server ready!~n"),
    io:format("Connect with: nc localhost ~p~n", [Port]),
    io:format("Or run Emacs client: M-x sakura-run-examples~n~n"),

    ok.
