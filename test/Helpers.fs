namespace Fable.Actor.Tests

open Fable.Actor
open Fable.Actor.Types
open Fable.Core

// Shared plumbing for the cross-target behavioral suite. Assertions and the runner come from
// Scriptorium (Nib + Quill); what is left here is the glue the suite needs to drive actors —
// a per-target sleep, the bridge that hands an `ActorOp` to Quill (which speaks `Async`), and a
// reporter actor for observing state that has to cross a process boundary on BEAM.
//
// decision: keeps one behavioral suite for every target so platform branches must satisfy the same API contract
// invariant: target-specific test plumbing remains contained in this module
[<AutoOpen>]
module Helpers =

    /// Counter protocol, shared by the call/reply and callAsync suites.
    type CounterMsg =
        | Increment
        | Decrement
        | GetCount

#if FABLE_COMPILER_BEAM
    open Fable.Core

    [<Emit("timer:sleep($0)")>]
    let private sleepMs (ms: int) : unit = nativeOnly

    [<Emit("element(2, process_info(self(), message_queue_len))")>]
    let queueLength () : int = nativeOnly

    [<Emit("length(element(2, process_info(self(), monitors)))")>]
    let monitorCount () : int = nativeOnly

    [<Emit("(fun() -> ObservedPid = $0, M = monitor(process, ObservedPid), receive {'DOWN', M, process, ObservedPid, _} -> true after 2000 -> demonitor(M, [flush]), false end end)()")>]
    let private observedDeath (pid: Fable.Beam.Pid<'Msg>) : bool = nativeOnly

    /// Observe process death without issuing another kill.
    let observeExit (target: Actor<'Msg>) : ActorOp<unit> =
        actor {
            if not (observedDeath target.Pid) then
                failwith "Worker exit was not observed"
        }

    /// Suspend the current actor for `ms` milliseconds.
    let sleep (ms: int) : ActorOp<unit> =
        actor {
            sleepMs ms
            return ()
        }

    /// Bridge an ActorOp into the Async that Quill expects.
    ///
    /// decision: runs the CPS continuation inline because BEAM Async is erased to synchronous callbacks
    /// assumption: the operation completes synchronously on BEAM before the returned Async completes
    let toAsync (op: ActorOp<unit>) : Async<unit> = async { op.Run(fun () -> ()) }

#else

#if FABLE_COMPILER_PYTHON
    [<Emit("len($0.listeners)")>]
    let tokenListenerCount (token: System.Threading.CancellationToken) : int = nativeOnly
#else
#if FABLE_COMPILER_JAVASCRIPT
    [<Emit("$0._listeners.size")>]
    let tokenListenerCount (token: System.Threading.CancellationToken) : int = nativeOnly
#endif
#endif

    /// Observe workflow and descendant exit without requesting shutdown.
    let observeExit (target: Actor<'Msg>) : ActorOp<unit> =
        actor {
            let! _ =
                Lifetime.withContext (fun ((ok, error, _), _) ->
                    let pending =
                        Lifetime.Pending(fun result ->
                            match result with
                            | Some ex -> error ex
                            | None -> ok 1)

                    pending.Add(
                        Lifetime.deadline 2000 (fun () -> pending.Settle(Some(System.TimeoutException "Worker exit was not observed")))
                    )

                    pending.Add(target.Lifetime.Observe(fun _ -> pending.Settle None)))

            return ()
        }

    /// Suspend the current actor for `ms` milliseconds (yields to the event loop).
    let sleep (ms: int) : ActorOp<unit> = Async.Sleep ms

    /// On every target but BEAM `ActorOp` *is* `Async`, so the bridge is the identity.
    let toAsync (op: ActorOp<unit>) : Async<unit> = op

#endif

    /// A one-cell actor for observing state across a process boundary.
    ///
    /// decision: reports observations through messages because BEAM actors copy captured mutable values
    /// invariant: Some replaces the cell and None replies with its latest value
    let reporter initial =
        Actor.start initial (fun state (msg, rc) ->
            match msg with
            | Some v -> Continue v
            | None ->
                rc.Reply state
                Continue state)
