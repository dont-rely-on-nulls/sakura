-module(poc).
-export([test/0, setup/0]).

test() ->
    io:format("~nVolcano Iterator~n~n"),
    %% Setup in-memory "database" (simulating Mnesia)
    io:format("Setting up content-addressed storage...~n"),
    Storage = setup(),
    %% Insert data with automatic deduplication
    io:format("Inserting products (note 'Soap' is stored only once)...~n"),
    {Storage2, SoapHash} = store_value(Storage, "Soap"),
    {Storage3, Price1Hash} = store_value(Storage2, 1.0),
    {Storage4, BazookaHash} = store_value(Storage3, "Bazooka"),
    {Storage5, Price2Hash} = store_value(Storage4, 413.0),
    {Storage6, _SoapHash2} = store_value(Storage5, "Soap"),  % Duplicate on purpose
    {Storage7, Price3Hash} = store_value(Storage6, 50.0),
    io:format("Unique values stored: ~p~n", [maps:size(maps:get(values, Storage7))]),
    io:format("(Expected: 5, not 6 becase 'Soap' got deduplicated)~n~n"),

    Tuple1 = #{name => SoapHash, price => Price1Hash},
    Tuple2 = #{name => BazookaHash, price => Price2Hash},
    Tuple3 = #{name => SoapHash, price => Price3Hash},
    Tuples = [Tuple1, Tuple2, Tuple3],

    io:format("Created 3 product tuples (referencing hashes)~n"),
    io:format("Tuple1: #{name => ~s, price => ~s}~n",
              [short_hash(SoapHash), short_hash(Price1Hash)]),
    io:format("Tuple2: #{name => ~s, price => ~s}~n",
              [short_hash(BazookaHash), short_hash(Price2Hash)]),
    io:format("Tuple3: #{name => ~s, price => ~s}~n~n",
              [short_hash(SoapHash), short_hash(Price3Hash)]),

    io:format("Building query pipeline (Volcano Model):~n"),
    io:format("Scan -> SELECT(price =< 100) -> Project(name)~n~n"),

    ScanPid = spawn(fun() -> scan_iterator(Tuples, Storage7) end),

    SelectPid = spawn(fun() -> 
        select_iterator(ScanPid, Storage7, fun(#{price := P}) -> P =< 100.0 end)
    end),

    ProjectPid = spawn(fun() ->
        project_iterator(SelectPid, [name])
    end),

    io:format("Pulling results (on-demand, lazy evaluation):~n"),
    Results = pull_all(ProjectPid),

    io:format("~n"),
    lists:foreach(fun(#{name := Name}) ->
        io:format("-> #{name => ~p}~n", [Name])
    end, Results),
    ok.

setup() ->
    #{values => #{}}.  % Hash => Value map

store_value(Storage, Value) ->
    Hash = value_hash(Value),
    Values = maps:get(values, Storage),
    case maps:is_key(Hash, Values) of
        true ->
            io:format("[DEDUP] Value ~p already exists (hash: ~s)~n",
                      [Value, short_hash(Hash)]),
            {Storage, Hash};
        false ->
            io:format("[STORE] Value ~p (hash: ~s)~n",
                      [Value, short_hash(Hash)]),
            NewValues = maps:put(Hash, Value, Values),
            {Storage#{values => NewValues}, Hash}
    end.

get_value(Storage, Hash) ->
    Values = maps:get(values, Storage),
    maps:get(Hash, Values).

value_hash(Value) ->
    %% Simple hash for demo (in production: crypto:hash(sha256, term_to_binary(Value)))
    erlang:phash2(Value).

short_hash(Hash) ->
    lists:flatten(io_lib:format("~8.16.0B", [Hash])).

scan_iterator([], _Storage) ->
    receive
        {next, Caller} -> Caller ! done
    end;
scan_iterator([TupleWithHashes | Rest], Storage) ->
    receive
        {next, Caller} ->
            %% Unpacks all the hashes from each tuple
	    %% later we gotta use this on the merkle tree
	    %% for now it's just silly and inneficient
            ResolvedTuple = maps:map(fun(_Attr, Hash) ->
                get_value(Storage, Hash)
            end, TupleWithHashes),
            io:format("[SCAN] Emitting tuple: ~p~n", [ResolvedTuple]),
            Caller ! {tuple, ResolvedTuple},
            scan_iterator(Rest, Storage)
    end.

select_iterator(SourcePid, Storage, Predicate) ->
    receive
        {next, Caller} ->
            SourcePid ! {next, self()},
            receive
                done ->
                    Caller ! done;
                {tuple, Tuple} ->
                    case Predicate(Tuple) of
                        true ->
                            io:format("[SELECT] Pass: ~p~n", [Tuple]),
                            Caller ! {tuple, Tuple},
                            select_iterator(SourcePid, Storage, Predicate);
                        false ->
                            io:format("[SELECT] Reject: ~p~n", [Tuple]),
                            %% Ignore this dude, and lock the process again
                            select_iterator_loop(SourcePid, Storage, Predicate, Caller)
                    end
            end
    end.

select_iterator_loop(SourcePid, Storage, Predicate, Caller) ->
    SourcePid ! {next, self()},
    receive
        done ->
            Caller ! done;
        {tuple, Tuple} ->
            case Predicate(Tuple) of
                true ->
                    io:format("[SELECT] Pass: ~p~n", [Tuple]),
                    Caller ! {tuple, Tuple},
                    select_iterator(SourcePid, Storage, Predicate);
                false ->
                    io:format("[SELECT] Reject: ~p~n", [Tuple]),
                    select_iterator_loop(SourcePid, Storage, Predicate, Caller)
            end
    end.

project_iterator(SourcePid, Attributes) ->
    receive
        {next, Caller} ->
            SourcePid ! {next, self()},
            receive
                done ->
                    Caller ! done;
                {tuple, Tuple} ->
                    ProjectedTuple = maps:with(Attributes, Tuple),
                    io:format("[PROJECT] ~p -> ~p~n", [Tuple, ProjectedTuple]),
                    Caller ! {tuple, ProjectedTuple},
                    project_iterator(SourcePid, Attributes)
            end
    end.

pull_all(IteratorPid) ->
    pull_all(IteratorPid, []).

pull_all(IteratorPid, Acc) ->
    IteratorPid ! {next, self()},
    receive
        done ->
            lists:reverse(Acc);
        {tuple, Tuple} ->
            pull_all(IteratorPid, [Tuple | Acc])
    after 1000 ->
        {error, timeout}
    end.
