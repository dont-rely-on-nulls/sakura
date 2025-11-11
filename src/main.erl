-module(main).
-export([fibonacci/1]).

fibonacci(0) -> 0;
fibonacci(1) -> 1;
fibonacci(N) ->
    fibonacci(N-2) + fibonacci(N-1).
