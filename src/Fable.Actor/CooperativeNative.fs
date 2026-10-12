module internal Fable.Actor.CooperativeNative

#if FABLE_COMPILER_BEAM
open Fable.Core
open Fable.Core.BeamInterop

// decision: keep the opt-in lifetime protocol separate from native links and their force-kill contract
// invariant: publication follows monitored body death and every registered descendant's terminal publication
// invariant: ownership acknowledgement precedes child startup; no child joins an already published lifetime
// decision: monitor each waiting caller so caller death cannot retain an abandoned private observer
// tradeoff: one control process retains an exit until release or creator death; noncooperative work can outlive a deadline
let spawn (parent: obj) (body: obj -> unit) : obj =
    emitErlExpr
        (parent, body)
        """
    (fun() ->
        Creator = self(), Ready = alias([explicit_unalias]),
        Guard = spawn(fun() ->
            CM = monitor(process, Creator),
            Token = fable_cancellation:create(),
            {Worker, WM} = spawn_monitor(fun() ->
                receive {begin_cooperative, Handle} ->
                    put('$fable_actor_cooperative', {element(2, Handle), Token}),
                    Exit = try
                        fable_cancellation:throw_if_cancellation_requested(Token),
                        $1(Handle),
                        normal
                    catch
                        error:operation_cancelled ->
                            case fable_cancellation:is_cancellation_requested(Token) of
                                true -> cancelled; false -> {failed, #{exn_type => exception, message => <<"operation_cancelled">>}}
                            end;
                        _:Failure -> {failed, fable_async:wrap_error(Failure)}
                    end,
                    exit({cooperative_body_exit, Exit})
                end
            end),
            Handle = {Worker, self(), Token},
            Stop = fun(S) ->
                fable_cancellation:cancel(Token),
                Worker ! cooperative_cancel,
                maps:foreach(fun(Child, ChildHandle) ->
                    fable_cancellation:cancel(element(3, ChildHandle)),
                    element(1, ChildHandle) ! cooperative_cancel, Child ! cooperative_stop
                end, maps:get(children, S)),
                S#{stopping := true}
            end,
            Loop = fun Again(S) ->
                BodyExit = maps:get(body_exit, S),
                Children = maps:get(children, S),
                Exit = case {BodyExit, map_size(Children), maps:get(failure, S)} of
                    {undefined, _, _} -> undefined;
                    {_, N, _} when N > 0 -> undefined;
                    {{failed, _} = Failed, 0, _} -> Failed;
                    {_, 0, {failed, _} = Failed} -> Failed;
                    {Done, 0, undefined} -> Done
                end,
                S1 = case {Exit, maps:get(published, S)} of
                    {undefined, _} -> S;
                    {_, true} -> S;
                    {_, false} ->
                        maps:foreach(fun(Alias, M) ->
                            Alias ! {cooperative_stopped, Alias, Exit}, demonitor(M, [flush])
                        end, maps:get(waiters, S)),
                        case $0 of
                            undefined -> ok;
                            {ParentWorker, ParentGuard, _} ->
                                ParentGuard ! {cooperative_child_exit, self(), Worker, Exit},
                                case Exit of
                                    {failed, Error} -> ParentWorker ! {fable_actor_msg, #{pid => Worker, reason => Error}};
                                    _ -> ok
                                end
                        end,
                        S#{published := true, waiters := #{}}
                end,
                case Exit =/= undefined andalso maps:get(release, S1) of
                    true -> ok;
                    false -> receive
                        cooperative_begin -> Worker ! {begin_cooperative, Handle}, Again(S1);
                        cooperative_stop -> Again(Stop(S1));
                        {cooperative_wait, Alias, Caller} ->
                            case Exit of
                                undefined ->
                                    M = monitor(process, Caller),
                                    Again(Stop(S1#{waiters := (maps:get(waiters, S1))#{Alias => M}}));
                                _ -> Alias ! {cooperative_stopped, Alias, Exit}, Again(S1)
                            end;
                        {cooperative_unwait, Alias} ->
                            Waiters = maps:get(waiters, S1),
                            case maps:find(Alias, Waiters) of {ok, M} -> demonitor(M, [flush]); error -> ok end,
                            Again(S1#{waiters := maps:remove(Alias, Waiters)});
                        {cooperative_own, Alias, Child, ChildHandle} ->
                            case maps:get(stopping, S1) orelse Exit =/= undefined of
                                true -> fable_cancellation:cancel(element(3, ChildHandle)), element(1, ChildHandle) ! cooperative_cancel, Child ! cooperative_stop;
                                false -> ok
                            end,
                            Alias ! {cooperative_owned, Alias, Exit =:= undefined},
                            case Exit of
                                undefined -> Again(S1#{children := Children#{Child => ChildHandle}});
                                _ -> Again(S1)
                            end;
                        {cooperative_child_exit, Child, _, ChildExit} ->
                            case maps:is_key(Child, Children) of
                                false -> Again(S1);
                                true ->
                                    Failure = case {maps:get(failure, S1), ChildExit} of
                                        {undefined, {failed, _}} -> ChildExit;
                                        {Previous, _} -> Previous
                                    end,
                                    Again(S1#{children := maps:remove(Child, Children), failure := Failure})
                            end;
                        {'DOWN', WM, process, Worker, Reason} ->
                            Actual = case Reason of
                                {cooperative_body_exit, Result} -> Result;
                                Other -> {failed, #{exn_type => exception, message => iolist_to_binary(io_lib:format("~p", [Other]))}}
                            end,
                            Again(Stop(S1#{body_exit := Actual}));
                        {'DOWN', CM, process, Creator, _} ->
                            Stopped = Stop(S1#{release := true}),
                            Worker ! {begin_cooperative, Handle},
                            Again(Stopped);
                        {'DOWN', M, process, _, _} ->
                            Again(S1#{waiters := maps:filter(fun(_, Monitor) -> Monitor =/= M end, maps:get(waiters, S1))});
                        cooperative_release_on_exit -> Again(S1#{release := true});
                        {cooperative_release, Alias} ->
                            Released = Exit =/= undefined,
                            Alias ! {cooperative_released, Alias, Released},
                            case Released of true -> ok; false -> Again(S1) end
                    end
                end
            end,
            Ready ! {Ready, Handle},
            Loop(#{body_exit => undefined, children => #{}, failure => undefined,
                stopping => false, published => false, release => false, waiters => #{}})
        end),
        M = monitor(process, Guard),
        Handle = receive {Ready, H} -> H; {'DOWN', M, process, Guard, Reason} -> erlang:error(Reason) end,
        unalias(Ready),
        case $0 of
            undefined -> ok;
            {_, ParentGuard, _} ->
                Alias = alias([explicit_unalias]), PM = monitor(process, ParentGuard),
                ParentGuard ! {cooperative_own, Alias, Guard, Handle},
                Owned = receive
                    {cooperative_owned, Alias, Accepted} -> Accepted;
                    {'DOWN', PM, process, ParentGuard, _} -> false
                end,
                unalias(Alias), demonitor(PM, [flush]),
                case Owned of
                    true -> ok;
                    false ->
                        Guard ! cooperative_release_on_exit, Guard ! cooperative_stop, Guard ! cooperative_begin,
                        demonitor(M, [flush]), erlang:error(#{exn_type => invalid_operation, message => <<"Cooperative parent has already exited">>})
                end
        end,
        Guard ! cooperative_begin,
        demonitor(M, [flush]), Handle
    end)()
    """

// invariant: finalizers run without ambient cancellation checkpoints, and their defects remain observable
let finalize (compensation: unit -> unit) : unit =
    emitErlExpr
        compensation
        """
    (fun() ->
        Previous = put('$fable_actor_cooperative_cleanup', true),
        try $0(ok)
        after case Previous of undefined -> erase('$fable_actor_cooperative_cleanup'); _ -> put('$fable_actor_cooperative_cleanup', Previous) end end
    end)()
    """

let checkpoint () : unit =
    emitErlExpr
        ()
        "case {get('$fable_actor_cooperative_cleanup'), get('$fable_actor_cooperative')} of {true, _} -> ok; {_, undefined} -> ok; {_, {_, Token}} -> fable_cancellation:throw_if_cancellation_requested(Token) end"

// invariant: resource-producing continuations install Using before the next cancellation checkpoint
let checkpointValue (value: obj) : unit =
    let resource: bool =
        emitErlExpr
            value
            "(fun() -> Value = case is_reference($0) of true -> get($0); false -> $0 end, is_map(Value) andalso is_map_key(dispose, Value) end)()"

    if not resource then
        checkpoint ()

let token () : System.Threading.CancellationToken =
    emitErlExpr
        ()
        "case {get('$fable_actor_cooperative_cleanup'), get('$fable_actor_cooperative')} of {true, _} -> undefined; {_, undefined} -> undefined; {_, {_, Token}} -> Token end"

let runAsync (operation: Async<'Value>) : 'Value =
    emitErlExpr
        operation
        "fable_async:run_synchronously($0, case {get('$fable_actor_cooperative_cleanup'), get('$fable_actor_cooperative')} of {true, _} -> undefined; {_, undefined} -> undefined; {_, {_, Token}} -> Token end)"

// invariant: cancellation wakes an idle receive without becoming an application message
let receive (cont: obj -> unit) : unit =
    emitErlExpr
        cont
        """
    (fun() ->
        Check = fun() -> case {get('$fable_actor_cooperative_cleanup'), get('$fable_actor_cooperative')} of {true, _} -> ok; {_, undefined} -> ok; {_, {_, Token}} -> fable_cancellation:throw_if_cancellation_requested(Token) end end,
        Loop = fun Again() ->
            Check(),
            receive
                {fable_actor_msg, Value} -> Check(), $0(Value);
                cooperative_cancel -> Check(), Again();
                {fable_actor_reply, _, _} -> Again();
                {'EXIT', _, normal} -> Again();
                {'EXIT', Pid, Reason} -> $0(#{pid => Pid, reason => Reason})
            end
        end,
        Loop()
    end)()
    """

let stop (handle: obj) (deadline: int) : obj option =
    emitErlExpr
        (handle, deadline)
        """
    (fun() ->
        Guard = element(2, $0), Alias = alias([explicit_unalias]), M = monitor(process, Guard),
        Guard ! {cooperative_wait, Alias, self()},
        try receive
            {cooperative_stopped, Alias, Exit} -> {some, Exit};
            {'DOWN', M, process, Guard, _} -> erlang:error(cooperative_observer_released)
        after $1 -> undefined end
        after Guard ! {cooperative_unwait, Alias}, unalias(Alias), demonitor(M, [flush]),
            (fun Flush() -> receive {cooperative_stopped, Alias, _} -> Flush() after 0 -> ok end end)()
        end
    end)()
    """

let releaseWhenExited (handle: obj) : unit =
    emitErlExpr handle "element(2, $0) ! cooperative_release_on_exit, ok"

let release (handle: obj) : bool =
    emitErlExpr
        handle
        """
    (fun() ->
        Guard = element(2, $0), Alias = alias([explicit_unalias]), M = monitor(process, Guard),
        Guard ! {cooperative_release, Alias},
        Result = receive {cooperative_released, Alias, Done} -> Done; {'DOWN', M, process, Guard, _} -> true end,
        unalias(Alias), demonitor(M, [flush]), Result
    end)()
    """
#endif
