/// Platform primitives for BEAM target.
///
/// Delegates to Fable.Beam.Erlang for standard BIFs and keeps only
/// actor-specific protocol Emits (tagged messages, selective receive).
///
/// decision: uses typed Fable.Beam bindings except where selective receive requires bound Erlang variables
/// invariant: public actor messages and replies retain their established Erlang envelope tags
module Fable.Actor.Platform

#if FABLE_COMPILER_BEAM

open Fable.Core
open Fable.Core.BeamInterop
open Fable.Actor.Types
open Fable.Beam

// ============================================================================
// Atom literals
// ============================================================================

let private atomKill: Atom = Erlang.binaryToAtom "kill"
let private atomNormal: Atom = Erlang.binaryToAtom "normal"

// ============================================================================
// Process helpers (use Fable.Beam.Erlang with actor-specific atoms)
// ============================================================================

let killProcess (pid: Pid<'Msg>) : unit = Erlang.exitPid pid atomKill
let trapExits () : unit = Erlang.trapExit () |> ignore
let formatReason (reason: obj) : string = Erlang.formatTerm reason

// ============================================================================
// Internal message protocol
// ============================================================================

/// Tagged-tuple envelopes used by the BEAM wire protocol.
///
/// decision: models envelopes as a DU so send and receive derive the same tags from one typed definition
/// invariant: CompiledName values match the tags consumed by native Erlang interoperability code
type InternalMsg =
    | [<CompiledName("fable_actor_msg")>] ActorMsg of payload: obj
    | [<CompiledName("fable_actor_reply")>] Reply of ref: Ref<obj> * value: obj
    | [<CompiledName("EXIT")>] Exit of pid: Pid<obj> * reason: obj

// ============================================================================
// Message passing
// ============================================================================

/// Send a tagged user message: Pid ! {fable_actor_msg, Msg}.
/// The envelope tag comes from InternalMsg.ActorMsg's CompiledName.
let sendMsg (pid: Pid<'Msg>) (msg: 'Msg) : unit =
    Erlang.send (unbox<Pid<InternalMsg>> pid) (ActorMsg(box msg))

/// Send a tagged reply: Pid ! {fable_actor_reply, Ref, Value}.
/// The envelope tag comes from InternalMsg.Reply's CompiledName.
let sendReply (pid: Pid<'Caller>) (ref: Ref<'Reply>) (value: 'Reply) : unit =
    Erlang.send (unbox<Pid<InternalMsg>> pid) (Reply(unbox<Ref<obj>> ref, box value))

/// Block until a user message or abnormal child exit is available.
///
/// decision: drops stale replies and normal exits because neither is an application message
/// invariant: abnormal EXIT signals reach the actor body as ChildExited values
let rec receiveMsg (cont: obj -> unit) : unit =
    match Erlang.receive<InternalMsg>() with
    | ActorMsg payload -> cont payload
    | Reply _ -> receiveMsg cont // stray reply (ref already timed out); drop and keep waiting
    | Exit(_, reason) when Erlang.exactEquals reason atomNormal -> receiveMsg cont
    | Exit(pid, reason) -> cont (box ({ Pid = box pid; Reason = reason }: ChildExited))

/// Block until the reply matching a specific ref arrives.
///
/// decision: emits the receive expression directly to preserve Erlang bound-variable matching semantics
/// invariant: replies with other refs remain in the mailbox
let recvReply (ref: Ref<'Reply>) : 'Reply =
    emitErlExpr ref "receive {fable_actor_reply, $0, FableReply} -> FableReply end"

/// Selectively receive a reply or return None after the timeout.
///
/// invariant: timing out does not consume a late or unrelated reply
let recvReplyWithTimeout (ref: Ref<'Reply>) (timeout: int) : 'Reply option =
    emitErlExpr (ref, timeout) "receive {fable_actor_reply, $0, FableReply} -> {some, FableReply} after $1 -> undefined end"

// ============================================================================
// Child exit detection
// ============================================================================

[<Emit("is_map($0) andalso is_map_key(pid, $0) andalso is_map_key(reason, $0)")>]
let isChildExited (msg: obj) : bool = nativeOnly

// ============================================================================
// Timer
// ============================================================================

type private TimerControl = | [<CompiledName("cancel")>] Cancel

/// Schedule a callback after ms milliseconds and return its process for cancellation.
///
/// decision: gives each timer a process so cancellation uses ordinary BEAM messaging
/// tradeoff: allocates one lightweight process per timer to avoid a shared timer registry
let timerSchedule (ms: int) (callback: unit -> unit) : obj =
    let pid: Pid<TimerControl> =
        Erlang.spawn (fun () ->
            match Erlang.receive<TimerControl> ms with
            | Some Cancel -> ()
            | None -> callback ())

    box pid

/// Cancel a scheduled timer by sending the cancel atom to its process.
let timerCancel (timer: obj) : unit =
    Erlang.send (unbox<Pid<TimerControl>> timer) Cancel

#endif

#if FABLE_COMPILER_BEAM

/// Link to the supplied parent and watch its normal exit as well as abnormal exit.
///
/// decision: uses an ownership watcher because native links alone leave children alive after normal parent exit
/// invariant: the watcher exits when either endpoint dies and kills the child on every parent death
let spawnOwnedProcess (parent: Pid<'Parent>) (body: unit -> unit) : Pid<'Msg> =
    emitErlExpr
        (parent, body)
        """
    (fun() ->
        Parent = $0,
        Child = spawn(fun() -> link(Parent), $1(ok) end),
        spawn(fun() ->
            PM = monitor(process, Parent), CM = monitor(process, Child),
            receive
                {'DOWN', PM, process, Parent, _} -> exit(Child, kill);
                {'DOWN', CM, process, Child, _} -> ok
            end
        end),
        Child
    end)()
    """

let waitProcessDeath (monitor: Ref<Pid<'Msg>>) (pid: Pid<'Msg>) : unit =
    emitErlExpr (monitor, pid) "receive {'DOWN', $0, process, $1, _} -> ok end"

/// Kill and observe process death. Monitoring precedes the exit signal to close the death race.
///
/// tradeoff: native kill skips user finalizers but provides process death even for noncooperative work
let stopProcess (pid: Pid<'Msg>) (deadline: int) : bool =
    emitErlExpr
        (pid, deadline)
        """
    (fun() ->
        M = monitor(process, $0), exit($0, kill),
        receive {'DOWN', M, process, $0, _} -> true
        after $1 -> demonitor(M, [flush]), false end
    end)()
    """

#endif

#if FABLE_COMPILER_PYTHON

open Fable.Core

// TODO(upstream): https://github.com/fable-compiler/Fable/pull/5036
// Replace addCancellationListener/removeCancellationListener/cancellationGate with the
// public disposable token.Register API when Lifetime.cancellation retires its Python branch.
// decision: uses native registration handles because fable-library 5.19 Register omits its return value
// decision: replaces the listener dictionary on removal so disposal during Cancel does not mutate its live iterator
// invariant: removing an actor registration leaves other token listeners installed
[<Emit("$0.add_listener($1)")>]
let addCancellationListener (token: System.Threading.CancellationToken) (callback: unit -> unit) : int = nativeOnly

[<Emit("setattr($0, 'listeners', {k: v for k, v in $0.listeners.items() if k != $1})")>]
let removeCancellationListener (token: System.Threading.CancellationToken) (id: int) : unit = nativeOnly

// TODO(upstream): https://github.com/fable-compiler/Fable/pull/5035
// Delete newGate/enterGate/leaveGate when Lifetime.synchronize uses the fixed runtime lock.
[<Emit("__import__('threading').RLock()")>]
let newGate () : obj = nativeOnly

[<Emit("$0.acquire()")>]
let enterGate (gate: obj) : unit = nativeOnly

[<Emit("$0.release()")>]
let leaveGate (gate: obj) : unit = nativeOnly

[<Emit("$0.lock")>]
let cancellationGate (token: System.Threading.CancellationToken) : obj = nativeOnly

// TODO(upstream): https://github.com/fable-compiler/Fable/pull/5038
// Delete startDeadline/cancelDeadline after Lifetime.deadline uses fixed Async.Sleep;
// retain positive deadlines and disposal of the actor-owned timer on every settlement.
// decision: retains the asyncio timer handle so settlement cancels the timer rather than only suppressing its callback
[<Emit("__import__('asyncio').get_running_loop().call_later($0 / 1000, $1)")>]
let startDeadline (ms: int) (callback: unit -> unit) : obj = nativeOnly

[<Emit("$0.cancel()")>]
let cancelDeadline (handle: obj) : unit = nativeOnly

#endif

#if FABLE_COMPILER_BEAM

let exitReason (reason: obj) : ActorExit =
    if Erlang.exactEquals reason atomNormal then
        ActorExit.Normal
    else
        ActorExit.Failed(ProcessExitException(formatReason reason))

let monotonicMilliseconds () : int64 =
    emitErlExpr () "erlang:monotonic_time(millisecond)"

let beginCall (pid: Pid<'Msg>) : Ref<obj> * Ref<Pid<'Msg>> =
    emitErlExpr pid "{erlang:alias([explicit_unalias]), erlang:monitor(process, $0)}"

let sendAliasReply (alias: Ref<obj>) (value: 'Reply) : unit =
    emitErlExpr (alias, value) "$0 ! {fable_actor_reply, $0, $1}, ok"

/// Receive only this request's reply or target monitor signal.
///
/// invariant: unrelated application messages, links, and monitors remain untouched
let recvCall (alias: Ref<obj>) (monitor: Ref<Pid<'Msg>>) (slice: int) : int * obj =
    emitErlExpr
        (alias, monitor, slice)
        """
    receive
        {fable_actor_reply, $0, Value} -> {0, Value};
        {'DOWN', $1, process, _, Reason} -> {1, Reason}
    after $2 -> {2, undefined} end
    """

/// Revoke first, then flush, so no reply can arrive after the flush.
let endCall (alias: Ref<obj>) (monitor: Ref<Pid<'Msg>>) : unit =
    emitErlExpr
        (alias, monitor)
        """
    (fun() ->
        unalias($0), demonitor($1, [flush]),
        Flush = fun Loop() ->
            receive {fable_actor_reply, $0, _} -> Loop() after 0 -> ok end
        end,
        Flush()
    end)()
    """

#endif

#if FABLE_COMPILER_PYTHON

/// Build a context-aware Async without the upstream cancellation fall-through wrapper.
///
/// TODO(upstream): https://github.com/fable-compiler/Fable/pull/5037
/// and https://github.com/fable-compiler/Fable/pull/5038
/// Delete this raw-context adapter when Lifetime.withContext uses standard Async on Python.
///
/// decision: hands cancellation to the actor settlement gate because fable-library 5.19 protected_cont continues after on_cancel
[<Fable.Core.Emit("lambda ctx: $0(((ctx.on_success, ctx.on_error, ctx.on_cancel), ctx.cancel_token))")>]
let fromContext
    (body: (('T -> unit) * (exn -> unit) * (System.OperationCanceledException -> unit)) * System.Threading.CancellationToken -> unit)
    : Async<'T> =
    nativeOnly

#endif
