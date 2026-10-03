module Fable.Actor.Tests.LifecycleTests

open System.Threading
open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test
open Fable.Actor
open Fable.Actor.Types

// decision: observations cross process boundaries so BEAM tests never assert a copied mutable capture
// invariant: startup and cleanup assertions wait for an observed event rather than a scheduling delay
type BarrierMsg =
    | Signal
    | Await of int
    | Read

let barrier () =
    Actor.start (0, []) (fun (count, waiters) (msg, rc) ->
        match msg with
        | Signal ->
            let count = count + 1

            let ready, pending =
                waiters
                |> List.partition (fun (n, _) -> n <= count)

            for _, reply in ready do
                reply.Reply count

            Continue(count, pending)
        | Await n when n > count -> Continue(count, (n, rc) :: waiters)
        | _ ->
            rc.Reply count
            Continue(count, waiters))

let await n events =
    actor { let! _ = Actor.callWithTimeout 2000 events (Await n) in return () }

let completed result =
    match result with
    | StopResult.Completed _ -> true
    | _ -> false

let idle events cleanup : Actor<int> =
    Actor.spawn (fun inbox ->
        actor {
            try
                Actor.cast events Signal
                let! _ = inbox.Receive()
                return ()
            finally
                Actor.cast cleanup Signal
        })

let tests =
    testList (
        "Lifecycle",
        [
            testAsync (
                "idle stop observes exit and repeated stop is safe",
                fun _ ->
                    toAsync (
                        actor {
                            let started, cleaned = barrier (), barrier ()
                            let worker = idle started cleaned
                            do! await 1 started
                            let! stopped = Actor.stop 2000 worker
                            assertThat (completed stopped) isTrue
#if FABLE_COMPILER_BEAM
                            assertThat (Fable.Beam.Erlang.isProcessAlive worker.Pid) isFalse
#else
                            do! await 1 cleaned
                            let! count = Actor.call cleaned Read
                            assertThat count (isEqualTo 1)
#endif
                            let! again = Actor.stop 2000 worker
                            assertThat (completed again) isTrue
                            let! _ = Actor.stop 2000 started
                            let! _ = Actor.stop 2000 cleaned
                            return ()
                        }
                    )
            )
            testAsync (
                "busy cooperative work exits on stop",
                fun _ ->
                    toAsync (
                        actor {
                            let started, cleaned = barrier (), barrier ()

                            let worker: Actor<int> =
                                Actor.spawn (fun _ ->
                                    actor {
                                        try
                                            Actor.cast started Signal
                                            do! sleep 60000
                                        finally
                                            Actor.cast cleaned Signal
                                    })

                            do! await 1 started
                            let! stopped = Actor.stop 2000 worker
                            assertThat (completed stopped) isTrue
#if FABLE_COMPILER_BEAM
                            assertThat (Fable.Beam.Erlang.isProcessAlive worker.Pid) isFalse
#else
                            do! await 1 cleaned
#endif
                            let! _ = Actor.stop 2000 started
                            let! _ = Actor.stop 2000 cleaned
                            return ()
                        }
                    )
            )
            testAsync (
                "external cancellation wakes an idle worker",
                fun _ ->
                    toAsync (
                        actor {
                            use cts = new CancellationTokenSource()
                            let started = barrier ()

                            let worker: Actor<int> =
                                Actor.spawnWithToken cts.Token (fun inbox ->
                                    actor {
                                        Actor.cast started Signal
                                        let! _ = inbox.Receive()
                                        return ()
                                    })

                            do! await 1 started
                            cts.Cancel()
                            let! stopped = Actor.stop 2000 worker
                            assertThat (completed stopped) isTrue
#if FABLE_COMPILER_BEAM
                            assertThat (Fable.Beam.Erlang.isProcessAlive worker.Pid) isFalse
#endif
                            let! _ = Actor.stop 2000 started
                            return ()
                        }
                    )
            )
            testAsync (
                "concurrent stop requests share one cleanup",
                fun _ ->
                    toAsync (
                        actor {
                            let started, cleaned, doneEvents = barrier (), barrier (), barrier ()
                            let worker = idle started cleaned
                            do! await 1 started

                            for _ in 1..8 do
                                let _: Actor<int> =
                                    Actor.spawn (fun _ ->
                                        actor {
                                            let! result = Actor.stop 2000 worker

                                            if completed result then
                                                Actor.cast doneEvents Signal
                                        })

                                ()

                            do! await 8 doneEvents
#if FABLE_COMPILER_BEAM
                            assertThat (Fable.Beam.Erlang.isProcessAlive worker.Pid) isFalse
#else
                            do! await 1 cleaned
                            let! count = Actor.call cleaned Read
                            assertThat count (isEqualTo 1)
#endif
                            let! _ = Actor.stop 2000 started
                            let! _ = Actor.stop 2000 cleaned
                            let! _ = Actor.stop 2000 doneEvents
                            return ()
                        }
                    )
            )
            testAsync (
                "normal parent completion shuts down owned child",
                fun _ ->
                    toAsync (
                        actor {
                            let started, cleaned = barrier (), barrier ()
                            let handle = reporter None

                            let parent: Actor<int> =
                                Actor.spawn (fun inbox ->
                                    actor {
                                        Actor.trapExits ()

                                        let child =
                                            Actor.spawnLinked inbox (fun child ->
                                                actor {
                                                    try
                                                        Actor.cast started Signal
                                                        let! _ = child.Receive()
                                                        return ()
                                                    finally
                                                        Actor.cast cleaned Signal
                                                })

                                        Actor.cast handle (Some(Some child))
                                        let! _ = inbox.Receive()
                                        return ()
                                    })

                            do! await 1 started
                            let! child = Actor.call handle None
                            Actor.send parent 1
                            // The child eventually rejects work even after normal parent exit.
                            match child with
                            | None -> failwith "Missing child handle"
                            | Some child ->
#if FABLE_COMPILER_BEAM
                                // Monitoring is the worker-exit barrier; this does not issue another kill.
                                let monitor = Fable.Beam.Erlang.monitor child.Pid
                                Fable.Actor.Platform.waitProcessDeath monitor child.Pid
                                assertThat (Fable.Beam.Erlang.isProcessAlive child.Pid) isFalse
#else
                                do! await 1 cleaned
                                let! stopped = Actor.stop 2000 parent
                                assertThat (completed stopped) isTrue
                                let! stoppedChild = Actor.stop 2000 child
                                assertThat (completed stoppedChild) isTrue
#endif
                            let! _ = Actor.stop 2000 parent
                            let! _ = Actor.stop 2000 handle
                            let! _ = Actor.stop 2000 started
                            let! _ = Actor.stop 2000 cleaned
                            return ()
                        }
                    )
            )
            testAsync (
                "restart replacement remains owned during parent shutdown",
                fun _ ->
                    toAsync (
                        actor {
                            let generations, ready, cleaned = barrier (), barrier (), barrier ()
                            let handle = reporter None

                            let parent: Actor<obj> =
                                Actor.spawn (fun inbox ->
                                    actor {
                                        Actor.trapExits ()

                                        let body (child: Actor<string>) =
                                            actor {
                                                try
                                                    Actor.cast generations Signal
                                                    let! msg = child.Receive()

                                                    if msg = "crash" then
                                                        failwith "restart me"
                                                finally
                                                    Actor.cast cleaned Signal
                                            }

                                        let first = Actor.spawnSupervised inbox (OneForOne(fun _ -> Directive.Restart)) body
                                        Actor.cast handle (Some(Some first.Actor))
                                        Actor.cast ready Signal
                                        let! msg = inbox.Receive()

                                        match Actor.tryAsChildExited msg with
                                        | None -> failwith "Missing child exit"
                                        | Some exit ->
                                            match Actor.handleChildExit inbox first exit with
                                            | ChildExitResult.Stopped -> failwith "Missing replacement"
                                            | ChildExitResult.Restarted replacement ->
                                                Actor.cast handle (Some(Some replacement.Actor))
                                                Actor.cast ready Signal
                                                let! _ = inbox.Receive()
                                                return ()
                                    })

                            do! await 1 ready
                            do! await 1 generations
                            let! first = Actor.call handle None
                            Actor.send (Option.get first) "crash"
                            do! await 2 ready
                            do! await 2 generations
                            let! replacement = Actor.call handle None
                            let! result = Actor.stop 2000 parent
                            assertThat (completed result) isTrue
#if FABLE_COMPILER_BEAM
                            let replacement = Option.get replacement
                            let monitor = Fable.Beam.Erlang.monitor replacement.Pid
                            Fable.Actor.Platform.waitProcessDeath monitor replacement.Pid
                            assertThat (Fable.Beam.Erlang.isProcessAlive replacement.Pid) isFalse
#else
                            do! await 2 cleaned
#endif
                            let! _ = Actor.stop 2000 handle
                            let! _ = Actor.stop 2000 generations
                            let! _ = Actor.stop 2000 ready
                            let! _ = Actor.stop 2000 cleaned
                            return ()
                        }
                    )
            )
            testAsync (
                "invalid cleanup deadline does not stop the target",
                fun _ ->
                    toAsync (
                        actor {
                            let worker = reporter 7
                            let mutable rejected = false

                            try
                                Actor.stop 0 worker |> ignore
                            with _ ->
                                rejected <- true

                            assertThat rejected isTrue
                            let! value = Actor.call worker None
                            assertThat value (isEqualTo 7)
                            let! _ = Actor.stop 2000 worker
                            return ()
                        }
                    )
            )
#if !FABLE_COMPILER_BEAM
            testAsync (
                "noncooperative work times out and remains owned until release",
                fun _ ->
                    toAsync (
                        actor {
                            let started, cleaned = barrier (), barrier ()
                            let releaseHandle = reporter None

                            let worker: Actor<int> =
                                Actor.spawn (fun _ ->
                                    actor {
                                        try
                                            let! _ =
                                                Async.FromContinuations(fun (ok, _, _) ->
                                                    Actor.cast releaseHandle (Some(Some(fun () -> ok 1)))
                                                    Actor.cast started Signal)

                                            return ()
                                        finally
                                            Actor.cast cleaned Signal
                                    })

                            do! await 1 started
                            let! result = Actor.stop 10 worker
                            assertThat result (isEqualTo StopResult.TimedOut)
                            let! cleanupCount = Actor.call cleaned Read
                            assertThat cleanupCount (isEqualTo 0)
                            let! release = Actor.call releaseHandle None
                            release |> Option.get |> fun f -> f ()
                            let! stopped = Actor.stop 2000 worker
                            assertThat (completed stopped) isTrue
                            do! await 1 cleaned
                            let! _ = Actor.stop 2000 releaseHandle
                            let! _ = Actor.stop 2000 started
                            let! _ = Actor.stop 2000 cleaned
                            return ()
                        }
                    )
            )
            testAsync (
                "external registration is released on normal exit",
                fun _ ->
                    toAsync (
                        actor {
                            use cts = new CancellationTokenSource()

                            let worker: Actor<int> =
                                Actor.spawnWithToken cts.Token (fun _ -> actor { return () })

                            let! stopped = Actor.stop 2000 worker
                            assertThat (completed stopped) isTrue
                            cts.Cancel()
                            return ()
                        }
                    )
            )
            testAsync (
                "cleanup exception is reported",
                fun _ ->
                    toAsync (
                        actor {
                            let started = barrier ()

                            let worker: Actor<int> =
                                Actor.spawn (fun inbox ->
                                    actor {
                                        try
                                            Actor.cast started Signal
                                            let! _ = inbox.Receive()
                                            return ()
                                        finally
                                            failwith "cleanup failed"
                                    })

                            do! await 1 started
                            let! result = Actor.stop 2000 worker

                            match result with
                            | StopResult.Completed(ActorExit.Failed ex) -> assertThat ex.Message (isEqualTo "cleanup failed")
                            | other -> failwithf "Cleanup failure was lost: %A" other

                            let! _ = Actor.stop 2000 started
                            return ()
                        }
                    )
            )
#endif
        ]
    )
