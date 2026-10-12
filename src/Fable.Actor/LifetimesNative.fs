/// Native cooperative lifecycle implementation.
///
/// decision: keep the opt-in lifetime protocol separate from native links and their force-kill contract
/// decision: express lifecycle policy in F# and use Fable.Beam for OTP primitives
/// invariant: publication follows monitored body death and every registered descendant's terminal publication
/// invariant: ownership acknowledgement precedes child startup; no child joins an already published lifetime
/// decision: monitor each waiting caller so caller death cannot retain an abandoned private observer
/// tradeoff: one control process retains an exit until release or creator death; noncooperative work can outlive a deadline
module internal Fable.Actor.LifetimesNative

#if FABLE_COMPILER_BEAM
open System.Threading
open Fable.Core
open Fable.Core.BeamInterop
open Fable.Beam
open Fable.Actor.Types

type private Handle = Pid<ExecutionNative.WorkerMessage> * Pid<ControlMessage> * CancellationTokenSource

and private ControlMessage =
    | [<CompiledName("cooperative_begin")>] Begin
    | [<CompiledName("cooperative_stop")>] Stop
    | [<CompiledName("cooperative_wait")>] Wait of Alias<ControlReply> * Pid<obj>
    | [<CompiledName("cooperative_unwait")>] Unwait of Alias<ControlReply>
    | [<CompiledName("cooperative_own")>] Own of Alias<ControlReply> * Pid<ControlMessage> * Handle
    | [<CompiledName("cooperative_child_exit")>] ChildExit of Pid<ControlMessage> * Pid<ExecutionNative.WorkerMessage> * ActorExit
    | [<CompiledName("DOWN")>] Down of Ref<Pid<obj>> * Atom * Pid<obj> * obj
    | [<CompiledName("cooperative_release_on_exit")>] ReleaseOnExit
    | [<CompiledName("cooperative_release")>] Release of Alias<ControlReply>

and private ControlReply =
    | [<CompiledName("cooperative_ready")>] Ready of Alias<ControlReply> * Handle
    | [<CompiledName("cooperative_owned")>] Owned of Alias<ControlReply> * bool
    | [<CompiledName("cooperative_stopped")>] Stopped of Alias<ControlReply> * ActorExit
    | [<CompiledName("cooperative_released")>] Released of Alias<ControlReply> * bool

type private StartupMessage = | [<CompiledName("begin_cooperative")>] BeginBody of Handle

type private BodyResult = | [<CompiledName("cooperative_body_exit")>] BodyExit of ActorExit

// decision: native immutable maps accept opaque PID and alias keys without inventing an F# ordering for them
type private State = {
    BodyExit: ActorExit option
    Children: Maps.BeamMap<Pid<ControlMessage>, Handle>
    Failure: exn option
    Stopping: bool
    Published: bool
    Release: bool
    Waiters: Maps.BeamMap<Alias<ControlReply>, Ref<Pid<obj>>>
}

let private decodeBodyExit (reason: obj) : ActorExit option =
    emitErlExpr reason "case $0 of {cooperative_body_exit, Result} -> Result; _ -> undefined end"

// invariant: a suspended worker selects only startup authorization, leaving earlier cancellation wakes queued
let private beginBody (handle: Handle) =
    let worker, _, _ = handle
    Erlang.send (unbox<Pid<StartupMessage>>(box worker)) (BeginBody handle)

let private cancel ((worker, guard, cts): Handle) =
    cts.Cancel()
    Erlang.send worker ExecutionNative.Cancel
    Erlang.send guard Stop

let private requestStop ((worker, _, cts): Handle) state =
    cts.Cancel()
    Erlang.send worker ExecutionNative.Cancel

    state.Children
    |> Maps.fold (fun _ child () -> cancel child) ()

    { state with Stopping = true }

let private terminal state =
    match state.BodyExit with
    | Some exit when Maps.size state.Children = 0 ->
        match exit, state.Failure with
        | ActorExit.Failed _, _ -> Some exit
        | _, Some error -> Some(ActorExit.Failed error)
        | _, None -> Some exit
    | _ -> None

let private publish parent ((worker, guard, _): Handle) state =
    match terminal state with
    | Some exit when not state.Published ->
        state.Waiters
        |> Maps.fold
            (fun alias monitor () ->
                Erlang.sendAlias alias (Stopped(alias, exit))
                Erlang.demonitorFlush monitor)
            ()

        parent
        |> Option.iter (fun (parentWorker, parentGuard, _) ->
            Erlang.send parentGuard (ChildExit(guard, worker, exit))

            match exit with
            | ActorExit.Failed error ->
                Erlang.send parentWorker (ExecutionNative.Message(box ({ Pid = box worker; Reason = box error }: ChildExited)))
            | _ -> ())

        {
            state with
                Published = true
                Waiters = Maps.empty ()
        }
    | _ -> state

let rec private controlLoop parent handle workerMonitor creatorMonitor state =
    let state = publish parent handle state
    let exit = terminal state

    if exit.IsNone || not state.Release then
        let next =
            match Erlang.receive<ControlMessage>() with
            | Begin ->
                beginBody handle
                Some state
            | Stop -> Some(requestStop handle state)
            | Wait(alias, caller) ->
                match exit with
                | Some result ->
                    Erlang.sendAlias alias (Stopped(alias, result))
                    Some state
                | None ->
                    let monitor = Erlang.monitor caller

                    Some(
                        requestStop handle {
                            state with
                                Waiters = Maps.put alias monitor state.Waiters
                        }
                    )
            | Unwait alias ->
                Maps.tryFind alias state.Waiters
                |> Option.iter Erlang.demonitorFlush

                Some {
                    state with
                        Waiters = Maps.remove alias state.Waiters
                }
            | Own(alias, child, childHandle) ->
                if state.Stopping || exit.IsSome then
                    cancel childHandle

                Erlang.sendAlias alias (Owned(alias, exit.IsNone))

                Some(
                    if exit.IsNone then
                        {
                            state with
                                Children = Maps.put child childHandle state.Children
                        }
                    else
                        state
                )
            | ChildExit(child, _, childExit) ->
                if Maps.containsKey child state.Children then
                    let failure =
                        match state.Failure, childExit with
                        | None, ActorExit.Failed error -> Some error
                        | previous, _ -> previous

                    Some {
                        state with
                            Children = Maps.remove child state.Children
                            Failure = failure
                    }
                else
                    Some state
            | Down(monitor, _, _, reason) when Erlang.exactEquals (box monitor) (box workerMonitor) ->
                let actual =
                    decodeBodyExit reason
                    |> Option.defaultWith (fun () -> ActorExit.Failed(System.Exception(Erlang.formatTerm reason)))

                Some(requestStop handle { state with BodyExit = Some actual })
            | Down(monitor, _, _, _) when Erlang.exactEquals monitor creatorMonitor ->
                let stopped = requestStop handle { state with Release = true }
                beginBody handle
                Some stopped
            | Down(monitor, _, _, _) ->
                Some {
                    state with
                        Waiters = Maps.filter (fun _ current -> not (Erlang.exactEquals current monitor)) state.Waiters
                }
            | ReleaseOnExit -> Some { state with Release = true }
            | Release alias ->
                Erlang.sendAlias alias (Released(alias, exit.IsSome))
                if exit.IsSome then None else Some state

        match next with
        | Some next -> controlLoop parent handle workerMonitor creatorMonitor next
        | None -> ()

// decision: bound-reference receives stay at the interop boundary because matching after receive would consume other callers' replies
let private awaitReady (alias: Alias<ControlReply>) monitor guard : Handle =
    emitErlExpr
        (alias, monitor, guard)
        "receive {cooperative_ready, $0, Handle} -> Handle; {'DOWN', $1, process, $2, Reason} -> erlang:error(Reason) end"

let private awaitOwned (alias: Alias<ControlReply>) monitor guard : bool =
    emitErlExpr (alias, monitor, guard) "receive {cooperative_owned, $0, Accepted} -> Accepted; {'DOWN', $1, process, $2, _} -> false end"

let spawn (parent: obj) (body: obj -> unit) : obj =
    let parent: Handle option = unbox parent
    let creator = Erlang.self<obj>()
    let ready = Erlang.alias<ControlReply>()

    let guard, monitor =
        Erlang.spawnMonitor<ControlMessage>(fun () ->
            let creatorMonitor = Erlang.monitor creator
            let cts = new CancellationTokenSource()

            let worker, workerMonitor =
                Erlang.spawnMonitor<ExecutionNative.WorkerMessage>(fun () ->
                    match Erlang.receive<StartupMessage>() with
                    | BeginBody handle ->
                        let _, guard, _ = handle
                        let exit = ExecutionNative.run (box guard) cts.Token (fun () -> body (box handle))

                        Erlang.exit (BodyExit exit))

            let handle = worker, Erlang.self<ControlMessage>(), cts
            Erlang.sendAlias ready (Ready(ready, handle))

            controlLoop parent handle workerMonitor creatorMonitor {
                BodyExit = None
                Children = Maps.empty ()
                Failure = None
                Stopping = false
                Published = false
                Release = false
                Waiters = Maps.empty ()
            })

    try
        let handle = awaitReady ready monitor guard

        parent
        |> Option.iter (fun (_, parentGuard, _) ->
            let alias = Erlang.alias<ControlReply>()
            let parentMonitor = Erlang.monitor parentGuard

            let owned =
                try
                    Erlang.send parentGuard (Own(alias, guard, handle))
                    awaitOwned alias parentMonitor parentGuard
                finally
                    Erlang.unalias alias |> ignore
                    Erlang.demonitorFlush parentMonitor

            if not owned then
                Erlang.send guard ReleaseOnExit
                cancel handle
                Erlang.send guard Begin
                invalidOp "Cooperative parent has already exited")

        Erlang.send guard Begin
        box handle
    finally
        Erlang.unalias ready |> ignore
        Erlang.demonitorFlush monitor

let private awaitStopped (alias: Alias<ControlReply>) monitor guard deadline : obj option =
    emitErlExpr
        (alias, monitor, guard, deadline)
        "receive {cooperative_stopped, $0, Exit} -> {some, Exit}; {'DOWN', $1, process, $2, _} -> erlang:error(cooperative_observer_released) after $3 -> undefined end"

let private flushStopped (alias: Alias<ControlReply>) : unit =
    emitErlExpr alias "(fun Flush() -> receive {cooperative_stopped, $0, _} -> Flush() after 0 -> ok end end)()"

let stop (handle: obj) deadline =
    let _, guard, _ = unbox<Handle> handle
    let alias = Erlang.alias<ControlReply>()
    let monitor = Erlang.monitor guard

    try
        Erlang.send guard (Wait(alias, Erlang.self<obj>()))
        awaitStopped alias monitor guard deadline
    finally
        Erlang.send guard (Unwait alias)
        Erlang.unalias alias |> ignore
        Erlang.demonitorFlush monitor
        flushStopped alias

let releaseWhenExited (handle: obj) =
    let _, guard, _ = unbox<Handle> handle
    Erlang.send guard ReleaseOnExit

let private awaitReleased (alias: Alias<ControlReply>) monitor guard : bool =
    emitErlExpr (alias, monitor, guard) "receive {cooperative_released, $0, Done} -> Done; {'DOWN', $1, process, $2, _} -> true end"

let release (handle: obj) =
    let _, guard, _ = unbox<Handle> handle
    let alias = Erlang.alias<ControlReply>()
    let monitor = Erlang.monitor guard

    try
        Erlang.send guard (Release alias)
        awaitReleased alias monitor guard
    finally
        Erlang.unalias alias |> ignore
        Erlang.demonitorFlush monitor
#endif
