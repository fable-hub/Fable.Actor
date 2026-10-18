-module(interop_demo).
-export([run/0]).

%% Run the compiled F# and Erlang peers together with `just test-interop`.

run() ->
    io:format("~n=== F# <-> Erlang Actor Interop ===~n~n"),

    %% --- Start Joe (native Erlang process) ---
    %% Joe is a simple receive loop that forwards replies to the demo process.
    Self = self(),
    JoePid = spawn(fun() -> joe_loop(Self) end),

    %% --- Start Dag (F# actor) ---
    DagPid = interop_interop_example:start_dag_pid(JoePid),

    %% === Demo 1: Fire-and-forget (Erlang → F#) ===
    %%
    %% Actor.send wraps messages as {fable_actor_msg, Msg}, so Erlang must do the same.
    %% Dag receives a (DagMsg, ReplyChannel) pair even when the reply is unused.
    io:format("  [Joe/Erlang] Saying hello to Dag...~n"),
    DagPid ! {fable_actor_msg, {{hello_from, <<"Joe">>}, #{reply => fun(_) -> ok end}}},

    %% Wait for Dag's raw reply (no envelope — just a binary)
    receive
        {joe_said, <<"Hei Joe! Dag her. Hyggelig å møte deg!"/utf8>> = Reply} ->
            io:format("  [Joe/Erlang] Got reply: ~ts~n~n", [Reply]);
        {joe_said, Reply} ->
            error({unexpected_greeting, Reply})
    after 5000 ->
        error(greeting_timed_out)
    end,

    %% === Demo 2: Call pattern (Erlang → F#) ===
    %%
    %% F# ReplyChannel<string> compiles to: #{reply => fun(V) -> ... end}
    %% Erlang can construct this map manually to call F# actors.
    io:format("  [Joe/Erlang] Asking Dag his full name (call pattern)...~n"),
    %% decision: capture the caller before constructing the callback because it executes in Dag's process
    %% invariant: every reply carries the unique ref used by the matching selective receive
    Ref = make_ref(),
    Me = self(),  %% Capture caller pid BEFORE the lambda (self() in lambda runs in callee's process!)
    Rc = #{reply => fun(V) -> Me ! {fable_actor_reply, Ref, V} end},
    DagPid ! {fable_actor_msg, {ask_name, Rc}},

    receive
        {fable_actor_reply, Ref, <<"Dag Brattli">> = Name} ->
            io:format("  [Joe/Erlang] Dag says his name is: ~s~n", [Name]);
        {fable_actor_reply, Ref, Name} ->
            error({unexpected_name, Name})
    after 5000 ->
        error(name_timed_out)
    end,

    %% === Demo 3: F# call through Actor.fromPid to a native Erlang actor ===
    NativePid = spawn(fun native_actor_loop/0),
    interop_interop_example:start_native_joe_call(NativePid, self()),

    receive
        <<"Native Joe">> ->
            io:format("  [Dag/F#]     Native Joe replied through Actor.fromPid~n");
        Unexpected ->
            error({unexpected_native_reply, Unexpected})
    after 5000 ->
        error(native_call_timed_out)
    end,

    exit(DagPid, kill),
    io:format("~nDone!~n"),
    ok.

%% Joe's receive loop — forwards any message back to the demo process.
joe_loop(Parent) ->
    receive
        Msg -> Parent ! {joe_said, Msg}
    end.

native_actor_loop() ->
    receive
        {fable_actor_msg, {ask_name, ReplyChannel}} ->
            (maps:get(reply, ReplyChannel))(<<"Native Joe">>);
        Unexpected ->
            error({unexpected_actor_message, Unexpected})
    end.
