%% Regression probe against the compiler-bundled, published BEAM Async runtime.
%% Equivalent F#: async { try do! Async.Sleep 1000 finally raise cleanupDefect }.
%% The Actor CE owns its own finalizers; this probes plain Async inside an adapter.
-module(beam_async_finalizer_probe).
-export([run/0, run_using/0]).

run() ->
    Token = fable_cancellation:create(),
    fable_cancellation:cancel(Token),
    put(cleanups, 0),
    Work = fable_async_builder:try_finally(
        fable_async:sleep(1000),
        fun(_) ->
            put(cleanups, get(cleanups) + 1),
            erlang:error(#{message => <<"cleanup defect">>})
        end
    ),
    Result = try fable_async:run_synchronously(Work, Token) of
        Value -> {unexpected_success, Value}
    catch
        _:Error -> Error
    end,
    Count = erase(cleanups),
    io:format("Plain BEAM Async finalizer: ~p invocations, result ~p~n", [Count, Result]),
    case {Count, Result} of
        {1, #{message := <<"cleanup defect">>}} -> ok;
        _ -> erlang:error({async_finalizer_contract_failed, Count, Result})
    end.

%% Equivalent F#: async { use resource = disposable; return 42 }.
run_using() ->
    put(disposals, 0),
    Resource = #{dispose => fun() -> put(disposals, get(disposals) + 1), ok end},
    Work = fable_async_builder:using(Resource, fun(_) -> fable_async_builder:return(42) end),
    Result = try fable_async:run_synchronously(Work) of
        Value -> {returned, Value}
    catch
        _:Error -> Error
    end,
    Count = erase(disposals),
    io:format("Plain BEAM Async Using: ~p disposals, result ~p~n", [Count, Result]),
    case {Count, Result} of
        {1, {returned, 42}} -> ok;
        _ -> erlang:error({async_using_contract_failed, Count, Result})
    end.
