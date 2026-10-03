/// Core types for Fable.Actor — cross-platform actor primitives.
///
/// No platform code, no dependencies beyond Fable.Core.
module Fable.Actor.Types

open Fable.Core

/// A reply channel that the receiver calls to send a response back to the caller.
///
/// decision: uses a callback record instead of AsyncReplyChannel so the call protocol compiles to every target
type ReplyChannel<'Reply> = { Reply: 'Reply -> unit }

/// Opaque handle for a scheduled timer.
///
/// decision: erases the wrapper so each target can retain its native cancellation handle without exposing it
[<Erase>]
type TimerHandle = TimerHandle of obj

/// What the actor should do after handling a message.
///
/// decision: makes normal and abnormal termination explicit handler results so supervision can distinguish them
/// invariant: StopAbnormal raises its exception from the actor loop and therefore triggers linked supervision
type Next<'State> =
    | Continue of 'State
    | Stop
    | StopAbnormal of exn

/// Notification when a child actor dies.
///
/// decision: keeps pid and reason opaque because their runtime representations differ across targets
type ChildExited = { Pid: obj; Reason: obj }

/// Observed terminal state of an actor workflow or native process.
[<RequireQualifiedAccess>]
type ActorExit =
    | Normal
    | Cancelled
    | Failed of exn

/// A stop deadline bounds observation, not arbitrary user work.
[<RequireQualifiedAccess>]
type StopResult =
    | Completed of ActorExit
    | TimedOut

exception ProcessExitException of string

/// What the supervisor should do when a child crashes.
[<RequireQualifiedAccess>]
type Directive =
    | Restart
    | Stop
    | Escalate

/// Supervision strategy — consulted when a child crashes.
///
/// decision: starts with one-for-one supervision because each SupervisedChild owns one independently restartable body
type Strategy = OneForOne of decider: (exn -> Directive)
