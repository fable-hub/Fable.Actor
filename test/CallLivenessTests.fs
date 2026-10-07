module Fable.Actor.Tests.CallLivenessTests

open System.Threading
open Fable.Core
open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test
open Fable.Actor
open Fable.Actor.Types
open Fable.Actor.Tests.LifecycleTests

type Request =
    | Echo of int
    | Hold
    | Crash
    | Finish

let worker delivered held =
    Actor.spawn (fun inbox ->
        let rec loop () =
            actor {
                let! msg, rc = inbox.Receive()
                Actor.cast delivered Signal

                match msg with
                | Echo n ->
                    rc.Reply n
                    rc.Reply -1
                    return! loop ()
                | Hold ->
                    Actor.cast held (Some(Some rc))
                    return! loop ()
                | Crash -> failwith "handler crashed"
                | Finish -> return ()
            }

        loop ())

let terminated =
    function
    | CallResult.TargetTerminated _ -> true
    | _ -> false

let tests =
    testList (
        "Call liveness",
        [
            testAsync (
                "reply settles once and duplicates cannot affect the next call",
                fun _ ->
                    toAsync (
                        actor {
                            let delivered, held = barrier (), reporter None
                            let target = worker delivered held

                            for n in 1..10 do
                                let! result = Actor.callResult 2000 CancellationToken.None target (Echo n)

                                match result with
                                | CallResult.Reply value -> assertThat value (isEqualTo n)
                                | _ -> failwith "Missing reply"
#if !FABLE_COMPILER_BEAM
                            assertThat target.Lifetime.ObserverCount (isEqualTo 0)
#endif
                            let! _ = Actor.stop 2000 target
                            let! _ = Actor.stop 2000 delivered
                            let! _ = Actor.stop 2000 held
                            return ()
                        }
                    )
            )
            testAsync (
                "unit replies are successful replies",
                fun _ ->
                    toAsync (
                        actor {
                            let target =
                                Actor.start () (fun state (msg, rc) ->
                                    rc.Reply()
                                    Continue state)

                            let! result = Actor.callResult 2000 CancellationToken.None target ()

                            match result with
                            | CallResult.Reply() -> ()
                            | _ -> failwith "Unit reply lost"

                            let! _ = Actor.stop 2000 target
                            return ()
                        }
                    )
            )
            testAsync (
                "handler crashes settle rather than hanging",
                fun _ ->
                    toAsync (
                        actor {
                            let delivered, held = barrier (), reporter None
                            let target = worker delivered held
                            let! result = Actor.callResult 2000 CancellationToken.None target Crash
                            assertThat (terminated result) isTrue
                            do! await 1 delivered
                            let! _ = Actor.stop 2000 target
                            let! _ = Actor.stop 2000 delivered
                            let! _ = Actor.stop 2000 held
                            return ()
                        }
                    )
            )
            testAsync (
                "unbounded callAsync reports handler death",
                fun _ ->
                    toAsync (
                        actor {
                            let delivered, held = barrier (), reporter None
                            let target = worker delivered held
                            let mutable failed = false

                            try
                                let! _ = Actor.callAsync target Crash
                                ()
                            with _ ->
                                failed <- true

                            assertThat failed isTrue
                            do! await 1 delivered
                            let! _ = Actor.stop 2000 target
                            let! _ = Actor.stop 2000 delivered
                            let! _ = Actor.stop 2000 held
                            return ()
                        }
                    )
            )
            testAsync (
                "normal exit without a reply settles the call",
                fun _ ->
                    toAsync (
                        actor {
                            let delivered, held = barrier (), reporter None
                            let target = worker delivered held
                            let! result = Actor.callResult 2000 CancellationToken.None target Finish
                            assertThat (terminated result) isTrue
                            do! await 1 delivered
                            let! _ = Actor.stop 2000 target
                            let! _ = Actor.stop 2000 delivered
                            let! _ = Actor.stop 2000 held
                            return ()
                        }
                    )
            )
            testAsync (
                "a stopped target rejects calls promptly",
                fun _ ->
                    toAsync (
                        actor {
                            let target = reporter 1
                            let! _ = Actor.stop 2000 target
                            let! result = Actor.callResult 2000 CancellationToken.None target None
                            assertThat (terminated result) isTrue
                        }
                    )
            )
            testAsync (
                "invalid reply deadlines do not deliver requests",
                fun _ ->
                    toAsync (
                        actor {
                            let delivered, held = barrier (), reporter None
                            let target = worker delivered held

                            for ms in [ 0; -1 ] do
                                let mutable rejected = false

                                try
                                    Actor.callResult ms CancellationToken.None target (Echo 99)
                                    |> ignore
                                with _ ->
                                    rejected <- true

                                assertThat rejected isTrue

                                try
                                    Actor.callWithTimeout ms target (Echo 99)
                                    |> ignore
                                with _ ->
                                    ()

                            let! n = Actor.call delivered Read
                            assertThat n (isEqualTo 0)
                            let! _ = Actor.stop 2000 target
                            let! _ = Actor.stop 2000 delivered
                            let! _ = Actor.stop 2000 held
                            return ()
                        }
                    )
            )
            testAsync (
                "timeout does not undo delivery and late replies leave no abandoned mailbox entries",
                fun _ ->
                    toAsync (
                        actor {
                            let delivered, held = barrier (), reporter None
                            let target = worker delivered held
                            use timeoutCts = new CancellationTokenSource()
#if FABLE_COMPILER_BEAM
                            let beforeQueue = queueLength ()
                            let beforeMonitors = monitorCount ()
#endif
                            for n in 1..20 do
                                let! result = Actor.callResult 10 timeoutCts.Token target Hold
                                assertThat result (isEqualTo CallResult.TimedOut)
                                do! await (2 * n - 1) delivered
                                let! oldReply = Actor.call held None
                                Option.get oldReply |> fun rc -> rc.Reply 999
                                let! fresh = Actor.callAsyncWithTimeout 2000 target (Echo n)
                                assertThat fresh (isEqualTo n)
                                Option.get oldReply |> fun rc -> rc.Reply 888
#if FABLE_COMPILER_BEAM
                            assertThat (queueLength ()) (isEqualTo beforeQueue)
                            assertThat (monitorCount ()) (isEqualTo beforeMonitors)
#else
                            assertThat target.Lifetime.ObserverCount (isEqualTo 0)
#endif
#if FABLE_COMPILER_PYTHON || FABLE_COMPILER_JAVASCRIPT
                            assertThat (tokenListenerCount timeoutCts.Token) (isEqualTo 0)
#endif
                            let! _ = Actor.stop 2000 target
                            let! _ = Actor.stop 2000 delivered
                            let! _ = Actor.stop 2000 held
                            return ()
                        }
                    )
            )
            testAsync (
                "pre-cancelled callers do not deliver",
                fun _ ->
                    toAsync (
                        actor {
                            use cts = new CancellationTokenSource()
                            cts.Cancel()
                            let delivered, held = barrier (), reporter None
                            let target = worker delivered held
                            let! result = Actor.callResult 2000 cts.Token target Hold
                            assertThat result (isEqualTo CallResult.Cancelled)
                            let! n = Actor.call delivered Read
                            assertThat n (isEqualTo 0)
                            let! _ = Actor.stop 2000 target
                            let! _ = Actor.stop 2000 delivered
                            let! _ = Actor.stop 2000 held
                            return ()
                        }
                    )
            )
            testAsync (
                "subscription cancellation settles a delivered pending call and preserves later calls",
                fun _ ->
                    toAsync (
                        actor {
                            use cts = new CancellationTokenSource()
                            let delivered, held, doneEvents = barrier (), reporter None, barrier ()
                            let outcome = reporter None
                            let target = worker delivered held

                            let caller: Actor<int> =
                                Actor.spawn (fun _ ->
                                    actor {
                                        let! result = Actor.callResult 2000 cts.Token target Hold
                                        Actor.cast outcome (Some(Some result))
                                        Actor.cast doneEvents Signal
                                    })

                            do! await 1 delivered
                            cts.Cancel()
                            do! await 1 doneEvents
                            let! result = Actor.call outcome None
                            assertThat (Option.get result) (isEqualTo CallResult.Cancelled)
                            let! oldReply = Actor.call held None
                            Option.get oldReply |> fun rc -> rc.Reply 42
                            let! value = Actor.callWithTimeout 2000 target (Echo 7)
                            assertThat value (isEqualTo 7)
                            let! _ = Actor.stop 2000 caller
                            let! _ = Actor.stop 2000 target
                            let! _ = Actor.stop 2000 delivered
                            let! _ = Actor.stop 2000 held
                            let! _ = Actor.stop 2000 doneEvents
                            let! _ = Actor.stop 2000 outcome
                            return ()
                        }
                    )
            )
            testAsync (
                "ambient pre-cancellation invokes one cancellation and delivers nothing",
                fun _ ->
                    toAsync (
                        actor {
                            use cts = new CancellationTokenSource()
                            cts.Cancel()
                            let delivered, held, cancelledEvents = barrier (), reporter None, barrier ()
                            let target = worker delivered held
                            let computation = Actor.callAsync target Hold

                            let start () =
                                Async.StartWithContinuations(
                                    computation,
                                    (fun value ->
                                        ignore value
                                        failwith "Unexpected reply"),
                                    (fun ex ->
                                        ignore ex
                                        failwith "Expected cancellation"),
                                    (fun ex ->
                                        ignore ex
                                        Actor.cast cancelledEvents Signal),
                                    cts.Token
                                )
#if FABLE_COMPILER_BEAM
                            Fable.Beam.Erlang.spawn start |> ignore
#else
                            start ()
#endif
                            do! await 1 cancelledEvents
                            let! cancellations = Actor.call cancelledEvents Read
                            let! requests = Actor.call delivered Read
                            assertThat cancellations (isEqualTo 1)
                            assertThat requests (isEqualTo 0)
                            let! _ = Actor.stop 2000 target
                            let! _ = Actor.stop 2000 delivered
                            let! _ = Actor.stop 2000 held
                            let! _ = Actor.stop 2000 cancelledEvents
                            return ()
                        }
                    )
            )
            testAsync (
                "ambient cancellation settles the unbounded Async call",
                fun _ ->
                    toAsync (
                        actor {
                            use cts = new CancellationTokenSource()
                            let delivered, held, doneEvents = barrier (), reporter None, barrier ()
                            let outcome = reporter "waiting"
                            let target = worker delivered held

                            let computation =
                                async {
                                    let! _ = Actor.callAsync target Hold
                                    return 1
                                }

                            let start () =
                                Async.StartWithContinuations(
                                    computation,
                                    (fun (value: int) ->
                                        ignore value
                                        Actor.cast outcome (Some "reply")
                                        Actor.cast doneEvents Signal),
                                    (fun ex ->
                                        ignore ex
                                        Actor.cast outcome (Some "error")
                                        Actor.cast doneEvents Signal),
                                    (fun ex ->
                                        ignore ex
                                        Actor.cast outcome (Some "cancelled")
                                        Actor.cast doneEvents Signal),
                                    cts.Token
                                )
#if FABLE_COMPILER_BEAM
                            Fable.Beam.Erlang.spawn start |> ignore
#else
                            start ()
#endif
                            do! await 1 delivered
                            cts.Cancel()
                            do! await 1 doneEvents
                            let! result = Actor.call outcome None
                            assertThat result (isEqualTo "cancelled")
#if FABLE_COMPILER_PYTHON || FABLE_COMPILER_JAVASCRIPT
                            assertThat (tokenListenerCount cts.Token) (isEqualTo 0)
#endif
                            let! oldReply = Actor.call held None
                            Option.get oldReply |> fun rc -> rc.Reply 88
                            let! value = Actor.callAsyncWithTimeout 2000 target (Echo 3)
                            assertThat value (isEqualTo 3)
                            let! _ = Actor.stop 2000 target
                            let! _ = Actor.stop 2000 delivered
                            let! _ = Actor.stop 2000 held
                            let! _ = Actor.stop 2000 doneEvents
                            let! _ = Actor.stop 2000 outcome
                            return ()
                        }
                    )
            )
            testAsync (
                "target stop settles already delivered pending calls",
                fun _ ->
                    toAsync (
                        actor {
                            let delivered, held, doneEvents = barrier (), reporter None, barrier ()
                            let outcome = reporter None
                            let target = worker delivered held

                            let caller: Actor<int> =
                                Actor.spawn (fun _ ->
                                    actor {
                                        let! result = Actor.callResult 2000 CancellationToken.None target Hold
                                        Actor.cast outcome (Some(Some result))
                                        Actor.cast doneEvents Signal
                                    })

                            do! await 1 delivered
                            let! stopped = Actor.stop 2000 target
                            assertThat (completed stopped) isTrue
                            do! await 1 doneEvents
                            let! result = Actor.call outcome None
                            assertThat (terminated (Option.get result)) isTrue
                            let! _ = Actor.stop 2000 caller
                            let! _ = Actor.stop 2000 delivered
                            let! _ = Actor.stop 2000 held
                            let! _ = Actor.stop 2000 doneEvents
                            let! _ = Actor.stop 2000 outcome
                            return ()
                        }
                    )
            )
#if !FABLE_COMPILER_BEAM
            testAsync (
                "settlement disposes resources once across concurrent results and late acquisition",
                fun _ ->
                    async {
                        let gate = Lifetime.newGate ()
                        let mutable notifications, disposed = 0, 0

                        let pending =
                            Lifetime.Pending(fun (_: int) -> Lifetime.synchronize gate (fun () -> notifications <- notifications + 1))

                        let resource () =
                            Lifetime.disposable (fun () -> Lifetime.synchronize gate (fun () -> disposed <- disposed + 1))

                        pending.Add(resource ())

                        do!
                            [| for n in 1..20 -> async { pending.Settle n } |]
                            |> Async.Parallel
                            |> Async.Ignore

                        pending.Add(resource ())
                        assertThat notifications (isEqualTo 1)
                        assertThat disposed (isEqualTo 2)
                    }
            )
#endif
        ]
    )
