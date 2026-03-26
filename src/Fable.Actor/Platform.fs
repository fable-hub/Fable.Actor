/// Platform primitives for BEAM target.
///
/// Delegates to Fable.Beam.Erlang for standard BIFs and keeps only
/// actor-specific protocol Emits (tagged messages, selective receive).
module Fable.Actor.Platform

#if FABLE_COMPILER_BEAM

open Fable.Core
open Fable.Core.BeamInterop
open Fable.Actor.Types
open Fable.Beam.Erlang

// ============================================================================
// Atom literals
// ============================================================================

let private atomKill: Atom = binaryToAtom "kill"
let private atomNormal: Atom = binaryToAtom "normal"

// ============================================================================
// Process helpers (use Fable.Beam.Erlang with actor-specific atoms)
// ============================================================================

let killProcess (pid: Pid) : unit = exitPid pid atomKill
let exitNormal () : unit = exit atomNormal
let trapExits () : unit = trapExit () |> ignore
let formatReason (reason: obj) : string = formatTerm reason

// ============================================================================
// Internal message protocol
// ============================================================================

/// DU mapping the tagged-tuple envelope used on the wire.
/// Each case's CompiledName matches the Erlang atom tag.
type InternalMsg =
    | [<CompiledName("fable_actor_msg")>] ActorMsg of payload: obj
    | [<CompiledName("fable_actor_timer")>] ActorTimer of ref: obj * callback: (obj -> unit)
    | [<CompiledName("EXIT")>] Exit of pid: Pid * reason: obj

// ============================================================================
// Message passing
// ============================================================================

/// Send a tagged user message: Pid ! {fable_actor_msg, Msg}
[<Emit("$0 ! {fable_actor_msg, $1}, ok")>]
let sendMsg (pid: Pid) (msg: obj) : unit = nativeOnly

/// Send a tagged reply: Pid ! {fable_actor_reply, Ref, Value}
[<Emit("$0 ! {fable_actor_reply, $1, $2}, ok")>]
let sendReply (pid: Pid) (ref: Ref) (value: obj) : unit = nativeOnly

/// CPS receive: blocks until a user message arrives.
/// Dispatches timer callbacks and EXIT signals transparently.
/// Uses emitErlExpr for the blocking receive to avoid overload resolution
/// issues with Erlang.receive<'T>() vs Erlang.receive<'T>(timeout).
let rec receiveMsg (cont: obj -> unit) : unit =
    let msg: InternalMsg =
        emitErlExpr
            ()
            "receive {fable_actor_msg, P__} -> {fable_actor_msg, P__}; {fable_actor_timer, R__, C__} -> {fable_actor_timer, R__, C__}; {'EXIT', P__, R__} -> {'EXIT', P__, R__} end"

    match msg with
    | ActorMsg payload -> cont payload
    | ActorTimer(_, callback) ->
        callback (box ())
        receiveMsg cont
    | Exit(_, reason) when exactEquals reason atomNormal -> receiveMsg cont
    | Exit(pid, reason) -> cont (box ({ Pid = box pid; Reason = reason }: ChildExited))

/// Blocking selective receive for a reply matching a specific ref.
/// Uses emitErlExpr to preserve Erlang's bound-variable semantics —
/// only the message with the matching ref is consumed from the mailbox.
let recvReply (ref: Ref) : obj =
    emitErlExpr ref "receive {fable_actor_reply, $0, FableReply} -> FableReply end"

/// Selective receive for a reply with timeout.
/// Returns Some(reply) or None on timeout.
let recvReplyWithTimeout (ref: Ref) (timeout: int) : obj option =
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
/// Returns the timer process pid for cancellation.
let timerSchedule (ms: int) (callback: unit -> unit) : Pid =
    spawn (fun () ->
        match Erlang.receive<TimerControl> ms with
        | Some Cancel -> ()
        | None -> callback ())

/// Cancel a scheduled timer by sending the cancel atom to its process.
let timerCancel (timer: Pid) : unit = Fable.Beam.Erlang.send timer (box Cancel)

#endif
