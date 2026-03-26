/// Core types for Fable.Actor — cross-platform actor primitives.
///
/// No platform code, no dependencies beyond Fable.Core.
module Fable.Actor.Types

open Fable.Core

/// A reply channel that the receiver calls to send a response back to the caller.
type ReplyChannel<'Reply> = { Reply: 'Reply -> unit }

/// Opaque handle for a scheduled timer (erased to the platform-native handle).
[<Erase>]
type TimerHandle = TimerHandle of obj

/// What the actor should do after handling a message.
type Next<'State> =
    | Continue of 'State
    | Stop
    | StopAbnormal of exn

/// Notification when a child actor dies.
type ChildExited = { Pid: obj; Reason: obj }

exception ProcessExitException of string

/// What the supervisor should do when a child crashes.
[<RequireQualifiedAccess>]
type Directive =
    | Restart
    | Stop
    | Escalate

/// Supervision strategy — consulted when a child crashes.
type Strategy = OneForOne of decider: (exn -> Directive)
