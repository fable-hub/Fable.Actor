/// Platform primitives for BEAM target.
///
/// Delegates to Fable.Beam.Erlang for standard BIFs and keeps only
/// actor-specific protocol Emits (tagged messages, selective receive).
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

/// DU mapping the tagged-tuple envelope used on the wire.
/// Each case's CompiledName matches the Erlang atom tag, so a typed
/// `Erlang.receive<InternalMsg>()` compiles to the selective receive
/// `receive {fable_actor_msg, ...}; {'EXIT', ...} end`.
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

/// CPS receive: blocks until a user message arrives, passing EXIT signals
/// through transparently as ChildExited (a normal exit is ignored).
let rec receiveMsg (cont: obj -> unit) : unit =
    match Erlang.receive<InternalMsg> () with
    | ActorMsg payload -> cont payload
    | Reply _ -> receiveMsg cont // stray reply (ref already timed out); drop and keep waiting
    | Exit(_, reason) when Erlang.exactEquals reason atomNormal -> receiveMsg cont
    | Exit(pid, reason) -> cont (box ({ Pid = box pid; Reason = reason }: ChildExited))

/// Blocking selective receive for a reply matching a specific ref.
/// Uses emitErlExpr to preserve Erlang's bound-variable semantics —
/// only the message with the matching ref is consumed from the mailbox.
let recvReply (ref: Ref<'Reply>) : 'Reply =
    emitErlExpr ref "receive {fable_actor_reply, $0, FableReply} -> FableReply end"

/// Selective receive for a reply with timeout.
/// Returns Some(reply) or None on timeout.
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

/// Schedule a callback after ms milliseconds.
/// Returns the timer process pid (boxed) for cancellation.
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
