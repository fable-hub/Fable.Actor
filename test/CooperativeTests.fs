module Fable.Actor.Tests.CooperativeTests

open System.Threading
open Fable.Actor
open Fable.Actor.Types
open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test
open LifecycleTests

let private deadline = 2000
let private shortDeadline = 20

let private noncooperative events =
#if FABLE_COMPILER_BEAM
    // decision: an opaque host wait has no CE checkpoints; release explicitly transfers its result back to the worker
    {
        Run =
            fun cont ->
                Async.RunSynchronously(Actor.callAsync events (Await 1))
                |> ignore

                cont ()
    }
#else
    // invariant: this fixture ignores its worker token until the independently owned release barrier replies
    Async.FromContinuations(fun (ok, error, _) -> Async.StartWithContinuations(await 1 events, ok, error, error, CancellationToken.None))
#endif

let private idleAndRepeated () =
    actor {
        let started, cleaned = barrier (), barrier ()

        let owner: CooperativeActor<int> =
            Cooperative.spawn (fun owner ->
                actor {
                    try
                        Actor.cast started Signal
                        let! _ = (Cooperative.reference owner).Receive()
                        return ()
                    finally
                        Actor.cast cleaned Signal
                })

        do! await 1 started
        assertThat (Cooperative.release owner) (isEqualTo LifetimeRelease.Running)
        let! result = Cooperative.stop deadline owner
        assertThat result (isEqualTo (StopResult.Completed ActorExit.Cancelled))
        do! await 1 cleaned
        let! repeated = Cooperative.stop deadline owner
        assertThat repeated (isEqualTo result)
        assertThat (Cooperative.release owner) (isEqualTo LifetimeRelease.Released)
    }

let private timeoutAndRetry () =
    actor {
        let started, cleaned, released = barrier (), barrier (), barrier ()

        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun _ ->
                actor {
                    try
                        Actor.cast started Signal
                        do! noncooperative released
                    finally
                        Actor.cast cleaned Signal
                })

        do! await 1 started
        let! pending = Cooperative.stop shortDeadline owner
        assertThat pending (isEqualTo StopResult.TimedOut)
        let! cleanupCount = Actor.callWithTimeout deadline cleaned Read
        assertThat cleanupCount (isEqualTo 0)
        Actor.cast released Signal
        let! stopped = Cooperative.stop deadline owner
        assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Cancelled))
        do! await 1 cleaned
        assertThat (Cooperative.release owner) (isEqualTo LifetimeRelease.Released)
    }

let private normalAndOwnedCleanup () =
    actor {
        let started, cleaned, released, finalized =
            barrier (), barrier (), barrier (), barrier ()

        let parent: CooperativeActor<unit> =
            Cooperative.spawn (fun parent ->
                actor {
                    try
                        let _child: CooperativeActor<unit> =
                            Cooperative.spawnLinked parent (fun _ ->
                                actor {
                                    try
                                        Actor.cast started Signal
                                        do! noncooperative released
                                    finally
                                        Actor.cast cleaned Signal
                                })

                        let! _ = (Cooperative.reference parent).Receive()
                        return ()
                    finally
                        Actor.cast finalized Signal
                })

        do! await 1 started
        Actor.send (Cooperative.reference parent) ()
        do! await 1 finalized
        let! pending = Cooperative.stop shortDeadline parent
        assertThat pending (isEqualTo StopResult.TimedOut)
        Actor.cast released Signal
        let! stopped = Cooperative.stop deadline parent
        assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Normal))
        do! await 1 cleaned
        let! repeated = Cooperative.stop deadline parent
        assertThat repeated (isEqualTo stopped)
        assertThat (Cooperative.release parent) (isEqualTo LifetimeRelease.Released)
    }

let private cleanupDefect () =
    actor {
        let started = barrier ()
        let original = System.Exception "cooperative cleanup defect"

        let owner: CooperativeActor<int> =
            Cooperative.spawn (fun owner ->
                actor {
                    try
                        Actor.cast started Signal
                        let! _ = (Cooperative.reference owner).Receive()
                        return ()
                    finally
                        raise original
                })

        do! await 1 started
        let! stopped = Cooperative.stop deadline owner

        match stopped with
        | StopResult.Completed(ActorExit.Failed error) -> assertThat error (isEqualTo original)
        | other -> failwithf "Cleanup failure was lost: %A" other

        assertThat (Cooperative.release owner) (isEqualTo LifetimeRelease.Released)
    }

let private sleepAdapter () =
    actor {
        let started, cleaned = barrier (), barrier ()

        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun _ ->
                actor {
                    try
                        Actor.cast started Signal
                        do! Async.Sleep 100000
                    finally
                        Actor.cast cleaned Signal
                })

        do! await 1 started
        let! stopped = Cooperative.stop deadline owner
        assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Cancelled))
        do! await 1 cleaned
        assertThat (Cooperative.release owner) (isEqualTo LifetimeRelease.Released)
    }

let private acquireWith started released dispose : ActorOp<System.IDisposable> =
    let resource () = {
        new System.IDisposable with
            member _.Dispose() = dispose ()
    }
#if FABLE_COMPILER_BEAM
    {
        Run =
            fun cont ->
                Actor.cast started Signal
                (noncooperative released).Run(fun () -> cont (resource ()))
    }
#else
    Async.FromContinuations(fun (ok, error, _) ->
        Actor.cast started Signal
        Async.StartWithContinuations(noncooperative released, (fun () -> ok (resource ())), error, error, CancellationToken.None))
#endif

let private acquire started released disposed =
    acquireWith started released (fun () -> Actor.cast disposed Signal)

let private cancelledAcquisition () =
    actor {
        let started, released, disposed = barrier (), barrier (), barrier ()

        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun _ ->
                actor {
                    use! _resource = acquire started released disposed
                    return ()
                })

        do! await 1 started
        let! pending = Cooperative.stop shortDeadline owner
        assertThat pending (isEqualTo StopResult.TimedOut)
        Actor.cast released Signal
        let! stopped = Cooperative.stop deadline owner
        assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Cancelled))
        let! count = Actor.callWithTimeout deadline disposed Read
        assertThat count (isEqualTo 1)
        assertThat (Cooperative.release owner) (isEqualTo LifetimeRelease.Released)
    }

let private busyLoop () =
    actor {
        let started, cleaned = barrier (), barrier ()

        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun _ ->
                actor {
                    try
                        Actor.cast started Signal

                        while true do
                            ()
                    finally
                        Actor.cast cleaned Signal
                })

        do! await 1 started
        let! stopped = Cooperative.stop deadline owner
        assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Cancelled))
        do! await 1 cleaned
        assertThat (Cooperative.release owner) (isEqualTo LifetimeRelease.Released)
    }

let private requestCancellation () =
    actor {
        let started, cleaned = barrier (), barrier ()

        let service: Actor<unit * ReplyChannel<int>> =
            Actor.start () (fun () (_, _) ->
                Actor.cast started Signal
                Continue())

        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun _ ->
                actor {
                    try
                        let! _ = Actor.callResult 100000 CancellationToken.None service ()
                        failwith "Owned call continued after cancellation"
                    finally
                        Actor.cast cleaned Signal
                })

        do! await 1 started
        let! stopped = Cooperative.stop deadline owner
        assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Cancelled))
        do! await 1 cleaned
        let! _ = Actor.stop deadline service
        assertThat (Cooperative.release owner) (isEqualTo LifetimeRelease.Released)
    }

#if FABLE_COMPILER_BEAM
let private normalCleanup () =
    actor {
        let finalized, released = barrier (), barrier ()

        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun _ ->
                actor {
                    try
                        return ()
                    finally
                        Actor.cast finalized Signal
                        (Actor.callWithTimeout deadline released (Await 1)).Run ignore
                })

        do! await 1 finalized
        let! pending = Cooperative.stop shortDeadline owner
        assertThat pending (isEqualTo StopResult.TimedOut)
        Actor.cast released Signal
        let! stopped = Cooperative.stop deadline owner
        assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Normal))
        assertThat (Cooperative.release owner) (isEqualTo LifetimeRelease.Released)
    }
#endif

let private requestVariants () =
    actor {
        for kind in 0..3 do
            let started, cleaned = barrier (), barrier ()

            let service: Actor<unit * ReplyChannel<int>> =
                Actor.start () (fun () (_, _) ->
                    Actor.cast started Signal
                    Continue())

            let owner: CooperativeActor<int> =
                Cooperative.spawn (fun owner ->
                    actor {
                        try
                            match kind with
                            | 0 -> let! _ = Actor.call service () in return ()
                            | 1 -> let! _ = Actor.callWithTimeout 100000 service () in return ()
                            | 2 -> let! _ = Actor.callAsync service () in return ()
                            | _ -> let! _ = Actor.callResultAsync 100000 CancellationToken.None service () in return ()

                            failwith "Cancelled request resumed user code"
                        finally
                            Actor.cast cleaned Signal
                    })

            do! await 1 started
            let! stopped = Cooperative.stop deadline owner
            assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Cancelled))
            let! count = Actor.callWithTimeout deadline cleaned Read
            assertThat count (isEqualTo 1)
            Cooperative.release owner |> ignore
            let! _ = Actor.stop deadline service
            let! _ = Actor.stop deadline started
            let! _ = Actor.stop deadline cleaned
            ()
    }

let private retainedFailure () =
    actor {
        let exited = barrier ()
        let original = System.Exception "original cooperative body defect"

        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun _ ->
                actor {
                    try
                        raise original
                    finally
                        Actor.cast exited Signal
                })

        do! await 1 exited

        for _ in 1..3 do
            let! stopped = Cooperative.stop deadline owner

            match stopped with
            | StopResult.Completed(ActorExit.Failed error) -> assertThat error (isEqualTo original)
            | other -> failwithf "Original body defect was lost: %A" other

        Cooperative.release owner |> ignore
        let! _ = Actor.stop deadline exited
        ()
    }

let private descendantDefect () =
    actor {
        let started, exited, cleaned = barrier (), barrier (), barrier ()
        let original = System.Exception "original owner defect"
        let childDefect = System.Exception "descendant cleanup defect"

        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun owner ->
                actor {
                    try
                        let child: CooperativeActor<int> =
                            Cooperative.spawnLinked owner (fun child ->
                                actor {
                                    try
                                        Actor.cast started Signal
                                        let! _ = (Cooperative.reference child).Receive()
                                        return ()
                                    finally
                                        Actor.cast cleaned Signal
                                        raise childDefect
                                })

                        Cooperative.releaseWhenExited child
                        let! _ = (Cooperative.reference owner).Receive()
                        raise original
                    finally
                        Actor.cast exited Signal
                })

        do! await 1 started
        Actor.send (Cooperative.reference owner) ()
        do! await 1 exited

        for _ in 1..2 do
            let! stopped = Cooperative.stop deadline owner

            match stopped with
            | StopResult.Completed(ActorExit.Failed error) -> assertThat error (isEqualTo original)
            | other -> failwithf "Descendant replaced original failure: %A" other

        let! count = Actor.callWithTimeout deadline cleaned Read
        assertThat count (isEqualTo 1)
        Cooperative.release owner |> ignore

        for events in [ started; exited; cleaned ] do
            let! _ = Actor.stop deadline events
            ()
    }

let private transitiveOwnership () =
    actor {
        let started, released, cleaned = barrier (), barrier (), barrier ()

        let owner: CooperativeActor<int> =
            Cooperative.spawn (fun owner ->
                actor {
                    let child: CooperativeActor<int> =
                        Cooperative.spawnLinked owner (fun child ->
                            actor {
                                let leaf: CooperativeActor<int> =
                                    Cooperative.spawnLinked child (fun _ ->
                                        actor {
                                            try
                                                Actor.cast started Signal
                                                do! noncooperative released
                                            finally
                                                Actor.cast cleaned Signal
                                        })

                                Cooperative.releaseWhenExited leaf
                                let! _ = (Cooperative.reference child).Receive()
                                return ()
                            })

                    Cooperative.releaseWhenExited child
                    let! _ = (Cooperative.reference owner).Receive()
                    return ()
                })

        do! await 1 started
        let! pending = Cooperative.stop shortDeadline owner
        assertThat pending (isEqualTo StopResult.TimedOut)
        Actor.cast released Signal
        let! stopped = Cooperative.stop deadline owner
        assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Cancelled))
        let! count = Actor.callWithTimeout deadline cleaned Read
        assertThat count (isEqualTo 1)
        Cooperative.release owner |> ignore

        for events in [ started; released; cleaned ] do
            let! _ = Actor.stop deadline events
            ()
    }

let private invalidDeadline () =
    actor {
        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun owner -> actor { let! _ = (Cooperative.reference owner).Receive() in return () })

        for ms in [ 0; -1 ] do
            let mutable rejected = false

            try
                Cooperative.stop ms owner |> ignore
            with error ->
                assertThat (error.Message.Contains "Cleanup deadline must be positive") isTrue
                rejected <- true

            assertThat rejected isTrue

        assertThat (Cooperative.release owner) (isEqualTo LifetimeRelease.Running)
        let! _ = Cooperative.stop deadline owner
        Cooperative.release owner |> ignore
    }

let private terminalAttachment () =
    actor {
        let entered = barrier ()
        let owner: CooperativeActor<unit> = Cooperative.spawn (fun _ -> actor { return () })
        let! stopped = Cooperative.stop deadline owner
        // The body can race an immediate cancellation; either result is terminal.
        assertThat (completed stopped) isTrue
        let mutable rejected = false

        try
            let child: CooperativeActor<unit> =
                Cooperative.spawnLinked owner (fun _ -> actor { Actor.cast entered Signal })

            Cooperative.releaseWhenExited child
        with _ ->
            rejected <- true

        assertThat rejected isTrue
        let! count = Actor.callWithTimeout deadline entered Read
        assertThat count (isEqualTo 0)
        Cooperative.release owner |> ignore
        let! _ = Actor.stop deadline entered
        ()
    }

#if FABLE_COMPILER_BEAM
open Fable.Core.BeamInterop

let private nativeGuard (CooperativeActor(_, handle)) : obj = emitErlExpr handle "element(2, $0)"

let private nativeMonitors guard : int =
    emitErlExpr guard "length(element(2, process_info($0, monitors)))"

let private monitorNative guard : obj =
    emitErlExpr guard "monitor(process, $0)"

let private nativeDown monitor : bool =
    emitErlExpr (monitor, deadline) "receive {'DOWN', $0, process, _, _} -> true after $1 -> demonitor($0, [flush]), false end"

let private retention () =
    actor {
        let started, released = barrier (), barrier ()

        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun _ ->
                actor {
                    Actor.cast started Signal
                    do! noncooperative released
                })

        do! await 1 started
        let guard = nativeGuard owner
        let baseline = nativeMonitors guard
        let callerMonitors = monitorCount ()
        let messages = queueLength ()

        for _ in 1..20 do
            let! stopped = Cooperative.stop 1 owner
            assertThat stopped (isEqualTo StopResult.TimedOut)
            // This acknowledged request follows unwait from the same sender.
            assertThat (Cooperative.release owner) (isEqualTo LifetimeRelease.Running)
            assertThat (nativeMonitors guard) (isEqualTo baseline)

        assertThat (monitorCount ()) (isEqualTo callerMonitors)
        assertThat (queueLength ()) (isEqualTo messages)
        Actor.cast released Signal
        let! _ = Cooperative.stop deadline owner
        let observed = monitorNative guard
        Cooperative.release owner |> ignore
        assertThat (nativeDown observed) isTrue

        for events in [ started; released ] do
            let! _ = Actor.stop deadline events
            ()
    }

let private creatorDeath () =
    actor {
        let started, cleaned = barrier (), barrier ()
        let saved = reporter None

        let creator: Actor<unit> =
            Actor.spawn (fun creator ->
                actor {
                    let owner: CooperativeActor<int> =
                        Cooperative.spawn (fun owner ->
                            actor {
                                try
                                    Actor.cast started Signal
                                    let! _ = (Cooperative.reference owner).Receive()
                                    return ()
                                finally
                                    Actor.cast cleaned Signal
                            })

                    Actor.cast saved (Some(Some owner))
                    let! _ = creator.Receive()
                    return ()
                })

        do! await 1 started
        let! owner = Actor.callWithTimeout deadline saved None
        let owner = Option.get owner
        let observed = monitorNative (nativeGuard owner)
        let! _ = Actor.stop deadline creator
        do! await 1 cleaned
        assertThat (nativeDown observed) isTrue
        let! _ = Actor.stop deadline saved
        let! _ = Actor.stop deadline started
        let! _ = Actor.stop deadline cleaned
        ()
    }

let private shieldedCleanup () =
    actor {
        let started, finalizing, released, cleaned =
            barrier (), barrier (), barrier (), barrier ()

        let owner: CooperativeActor<int> =
            Cooperative.spawn (fun owner ->
                actor {
                    try
                        Actor.cast started Signal
                        let! _ = (Cooperative.reference owner).Receive()
                        return ()
                    finally
                        Actor.cast finalizing Signal

                        (actor {
                            do! Async.Sleep 1
                            do! await 1 released
                            Actor.cast cleaned Signal
                        })
                            .Run
                            ignore
                })

        do! await 1 started
        let! pending = Cooperative.stop shortDeadline owner
        assertThat pending (isEqualTo StopResult.TimedOut)
        do! await 1 finalizing
        let! count = Actor.callWithTimeout deadline cleaned Read
        assertThat count (isEqualTo 0)
        Actor.cast released Signal
        let! stopped = Cooperative.stop deadline owner
        assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Cancelled))
        let! count = Actor.callWithTimeout deadline cleaned Read
        assertThat count (isEqualTo 1)
        Cooperative.release owner |> ignore

        for events in [ started; finalizing; released; cleaned ] do
            let! _ = Actor.stop deadline events
            ()
    }
#endif

let private lateCleanupDefect () =
    actor {
        let started, released, disposed = barrier (), barrier (), barrier ()
        let original = System.Exception "late acquisition cleanup defect"

        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun _ ->
                actor {
                    use! _resource =
                        acquireWith started released (fun () ->
                            Actor.cast disposed Signal
                            raise original)

                    return ()
                })

        do! await 1 started
        let! pending = Cooperative.stop shortDeadline owner
        assertThat pending (isEqualTo StopResult.TimedOut)
        Actor.cast released Signal
        let! stopped = Cooperative.stop deadline owner

        match stopped with
        | StopResult.Completed(ActorExit.Failed error) -> assertThat error (isEqualTo original)
        | other -> failwithf "Late resource defect was lost: %A" other

        let! count = Actor.callWithTimeout deadline disposed Read
        assertThat count (isEqualTo 1)
        Cooperative.release owner |> ignore

        for events in [ started; released; disposed ] do
            let! _ = Actor.stop deadline events
            ()
    }

let private cancellationRace () =
    actor {
        let cleaned = barrier ()

        for iteration in 1..40 do
            let entered = barrier ()

            let owner: CooperativeActor<unit> =
                Cooperative.spawn (fun owner ->
                    actor {
                        try
                            Actor.cast entered Signal
                            let! _ = (Cooperative.reference owner).Receive()
                            return ()
                        finally
                            Actor.cast cleaned Signal
                    })

            do! await 1 entered
            Actor.send (Cooperative.reference owner) ()
            let! first = Cooperative.stop deadline owner

            match first with
            | StopResult.Completed ActorExit.Normal
            | StopResult.Completed ActorExit.Cancelled -> ()
            | other -> failwithf "Receive/cancel race did not settle: %A" other

            let! repeated = Cooperative.stop deadline owner
            assertThat repeated (isEqualTo first)
            let! count = Actor.callWithTimeout deadline cleaned Read
            assertThat count (isEqualTo iteration)
            Cooperative.release owner |> ignore
            let! _ = Actor.stop deadline entered
            ()

        let! _ = Actor.stop deadline cleaned
        ()
    }

let private failedText () =
    actor {
        let exited = barrier ()

        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun _ ->
                actor {
                    try
                        failwith "body text defect"
                    finally
                        Actor.cast exited Signal
                })

        do! await 1 exited
        let! stopped = Cooperative.stop deadline owner

        match stopped with
        | StopResult.Completed(ActorExit.Failed error) -> assertThat error.Message (isEqualTo "body text defect")
        | other -> failwithf "Text defect was lost: %A" other

        Cooperative.release owner |> ignore
        let! _ = Actor.stop deadline exited
        ()
    }

#if FABLE_COMPILER_BEAM
let private abandonedObserver () =
    actor {
        let started, released = barrier (), barrier ()

        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun _ ->
                actor {
                    Actor.cast started Signal
                    do! noncooperative released
                })

        do! await 1 started
        let guard = nativeGuard owner
        let baseline = nativeMonitors guard

        let waiter: Actor<unit> =
            Actor.spawn (fun _ -> actor { let! _ = Cooperative.stop 100000 owner in return () })

        let waitForCount expected : bool =
            emitErlExpr
                (guard, expected, deadline)
                """
            (fun() ->
                Until = erlang:monotonic_time(millisecond) + $2,
                Check = fun Again() ->
                    case length(element(2, process_info($0, monitors))) of
                        $1 -> true;
                        _ -> case erlang:monotonic_time(millisecond) >= Until of
                            true -> false; false -> erlang:yield(), Again() end
                    end
                end,
                Check()
            end)()
            """

        assertThat (waitForCount (baseline + 1)) isTrue
        let! _ = Actor.stop deadline waiter
        assertThat (waitForCount baseline) isTrue
        Actor.cast released Signal
        let! _ = Cooperative.stop deadline owner
        Cooperative.release owner |> ignore

        for events in [ started; released ] do
            let! _ = Actor.stop deadline events
            ()
    }
#endif

#if FABLE_COMPILER_BEAM
let private deferredRelease () =
    actor {
        let started, released, cleaned = barrier (), barrier (), barrier ()

        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun _ ->
                actor {
                    try
                        Actor.cast started Signal
                        do! noncooperative released
                    finally
                        Actor.cast cleaned Signal
                })

        do! await 1 started
        let guard = nativeGuard owner
        let observed = monitorNative guard
        Cooperative.releaseWhenExited owner
        let running: bool = emitErlExpr guard "is_process_alive($0)"
        assertThat running isTrue
        let! count = Actor.callWithTimeout deadline cleaned Read
        assertThat count (isEqualTo 0)
        Actor.cast released Signal
        assertThat (nativeDown observed) isTrue
        let! count = Actor.callWithTimeout deadline cleaned Read
        assertThat count (isEqualTo 1)

        for events in [ started; released; cleaned ] do
            let! _ = Actor.stop deadline events
            ()
    }
#endif

let private preconstructedRequest () =
    actor {
        let admitted, cleaned = barrier (), barrier ()

        let service: Actor<unit * ReplyChannel<int>> =
            Actor.start () (fun () (_, _) ->
                Actor.cast admitted Signal
                Continue())
        // invariant: construction precedes both owners; each execution must resolve its own ambient token
        let operation = Actor.callResult 2000 CancellationToken.None service ()

        for n in 1..2 do
            let owner: CooperativeActor<unit> =
                Cooperative.spawn (fun _ ->
                    actor {
                        try
                            let! _ = operation
                            return ()
                        finally
                            Actor.cast cleaned Signal
                    })

            do! await n admitted
            let! stopped = Cooperative.stop 500 owner
            assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Cancelled))
            do! await n cleaned
            Cooperative.release owner |> ignore

        for events in [ admitted; cleaned ] do
            let! _ = Actor.stop deadline events
            ()

        let! _ = Actor.stop deadline service
        ()
    }

let private cancellationHandler () =
    actor {
        let started, handled, cleaned = barrier (), barrier (), barrier ()

        let owner: CooperativeActor<unit> =
            Cooperative.spawn (fun owner ->
                actor {
                    try
                        try
                            Actor.cast started Signal
                            let! _ = (Cooperative.reference owner).Receive()
                            return ()
                        with error ->
                            Actor.cast handled Signal
                            raise error
                    finally
                        Actor.cast cleaned Signal
                })

        do! await 1 started
        let! stopped = Cooperative.stop deadline owner
        assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Cancelled))
        do! await 1 cleaned
        let! count = Actor.callWithTimeout deadline handled Read
        assertThat count (isEqualTo 0)
        Cooperative.release owner |> ignore

        for events in [ started; handled; cleaned ] do
            let! _ = Actor.stop deadline events
            ()
    }

let private earlierDescendantFailure () =
    actor {
        let started, handled, checkedEvents = barrier (), barrier (), barrier ()
        let handles = reporter None
        let original = System.Exception "earlier descendant cleanup defect"

        let owner: CooperativeActor<obj> =
            Cooperative.spawn (fun owner ->
                actor {
                    let child: CooperativeActor<unit> =
                        Cooperative.spawnLinked owner (fun child ->
                            actor {
                                try
                                    Actor.cast started Signal
                                    let! _ = (Cooperative.reference child).Receive()
                                    return ()
                                finally
                                    raise original
                            })

                    Actor.cast handles (Some(Some child))

                    let rec loop () =
                        actor {
                            let! message = (Cooperative.reference owner).Receive()

                            match Actor.tryAsChildExited message with
                            | Some exited ->
                                assertThat (unbox<exn> exited.Reason) (isEqualTo original)
                                Actor.cast handled Signal
                                return! loop ()
                            | None ->
                                Actor.cast checkedEvents Signal
                                return ()
                        }

                    return! loop ()
                })

        do! await 1 started
        let! child = Actor.callWithTimeout deadline handles None
        let child = Option.get child
        Actor.send (Cooperative.reference child) ()
        do! await 1 handled
        // decision: a subsequent parent-handled message proves the child failed while its owner stayed live
        Actor.send (Cooperative.reference owner) (box "checkpoint")
        do! await 1 checkedEvents
        do! observeExit (Cooperative.reference owner)

        for _ in 1..2 do
            let! stopped = Cooperative.stop deadline owner

            match stopped with
            | StopResult.Completed(ActorExit.Failed error) -> assertThat error (isEqualTo original)
            | other -> failwithf "Earlier descendant failure was lost: %A" other

        Cooperative.release child |> ignore
        Cooperative.release owner |> ignore

        for events in [ started; handled; checkedEvents ] do
            let! _ = Actor.stop deadline events
            ()

        let! _ = Actor.stop deadline handles
        ()
    }

let tests =
    testList (
        "Cooperative",
        [
            testAsync (
                "preconstructed and reused requests inherit the executing owner token",
                timeout 5000,
                fun _ -> toAsync (preconstructedRequest ())
            )
            testAsync ("cancellation bypasses logging and rethrow error handlers", timeout 5000, fun _ -> toAsync (cancellationHandler ()))
            testAsync (
                "earlier handled descendant cleanup failure remains terminal",
                timeout 5000,
                fun _ -> toAsync (earlierDescendantFailure ())
            )
            testAsync ("late acquisition cleanup defects remain observable", timeout 5000, fun _ -> toAsync (lateCleanupDefect ()))
            testAsync ("receive/cancellation races retain one exit and one cleanup", timeout 10000, fun _ -> toAsync (cancellationRace ()))
            testAsync ("text failures preserve their exception message", timeout 5000, fun _ -> toAsync (failedText ()))
#if FABLE_COMPILER_BEAM
            testAsync ("releaseWhenExited waits for actual exit", timeout 5000, fun _ -> toAsync (deferredRelease ()))
            testAsync ("dead stop callers release private observers", timeout 5000, fun _ -> toAsync (abandonedObserver ()))
#endif
            testAsync ("all typed request adapters inherit cancellation", timeout 10000, fun _ -> toAsync (requestVariants ()))
            testAsync ("original failed exit is retained", timeout 5000, fun _ -> toAsync (retainedFailure ()))
            testAsync ("body failure precedes descendant cleanup defects", timeout 5000, fun _ -> toAsync (descendantDefect ()))
            testAsync ("stop includes transitive descendant cleanup", timeout 5000, fun _ -> toAsync (transitiveOwnership ()))
            testAsync ("invalid deadline leaves work owned", timeout 5000, fun _ -> toAsync (invalidDeadline ()))
            testAsync ("terminal parent rejects new child ownership", timeout 5000, fun _ -> toAsync (terminalAttachment ()))
#if FABLE_COMPILER_BEAM
            testAsync ("timeouts detach aliases and private observers", timeout 5000, fun _ -> toAsync (retention ()))
            testAsync ("creator death reclaims observation after cleanup", timeout 5000, fun _ -> toAsync (creatorDeath ()))
            testAsync ("finalizer adapters are shielded until acknowledged completion", timeout 5000, fun _ -> toAsync (shieldedCleanup ()))
#endif
            testAsync ("typed request wait inherits owned cancellation", timeout 5000, fun _ -> toAsync (requestCancellation ()))
#if FABLE_COMPILER_BEAM
            testAsync ("normal completion is retained through delayed finalization", timeout 5000, fun _ -> toAsync (normalCleanup ()))
#endif
            testAsync ("cancelled acquisition disposes exactly once", timeout 5000, fun _ -> toAsync (cancelledAcquisition ()))
            testAsync ("actor loop cooperates between iterations", timeout 5000, fun _ -> toAsync (busyLoop ()))
            testAsync ("idle cancellation, retained exit and explicit release", timeout 5000, fun _ -> toAsync (idleAndRepeated ()))
            testAsync ("noncooperative work remains owned after timeout", timeout 5000, fun _ -> toAsync (timeoutAndRetry ()))
            testAsync ("normal exit includes owned descendant cleanup", timeout 5000, fun _ -> toAsync (normalAndOwnedCleanup ()))
            testAsync ("finalizer failure survives cancellation", timeout 5000, fun _ -> toAsync (cleanupDefect ()))
            testAsync ("sequential Async adapter receives the owned token", timeout 5000, fun _ -> toAsync (sleepAdapter ()))
        ]
    )
