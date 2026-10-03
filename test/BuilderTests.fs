module Fable.Actor.Tests.BuilderTests

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Actor
open Fable.Actor.Types

type TimeoutMsg =
    | Slow
    | Fast

// ============================================================================
// computation-expression control flow
// ============================================================================

let private controlFlowTests =
    testList (
        "actor { }",
        [
            testAsync (
                "for..in runs the body for each element, in order",
                fun _ ->
                    toAsync (
                        actor {
                            let mutable acc = []

                            for x in [ 1; 2; 3 ] do
                                do! sleep 1
                                acc <- acc @ [ x * 10 ]

                            assertThat acc (isEqualTo [ 10; 20; 30 ])
                        }
                    )
            )

            testAsync (
                "while repeats the body until the guard is false",
                fun _ ->
                    toAsync (
                        actor {
                            let mutable i = 0
                            let mutable sum = 0

                            while i < 5 do
                                do! sleep 1
                                sum <- sum + i
                                i <- i + 1

                            assertThat sum (isEqualTo 10)
                        }
                    )
            )

            testAsync (
                "use disposes the resource when the scope ends",
                fun _ ->
                    toAsync (
                        actor {
                            let mutable disposed = false

                            do!
                                actor {
                                    use _r = {
                                        new System.IDisposable with
                                            member _.Dispose() = disposed <- true
                                    }

                                    do! sleep 1
                                }

                            assertThat disposed isTrue
                        }
                    )
            )

            testAsync (
                "try/finally runs the compensation on normal completion",
                fun _ ->
                    toAsync (
                        actor {
                            let mutable cleaned = false

                            do!
                                actor {
                                    try
                                        do! sleep 1
                                    finally
                                        cleaned <- true
                                }

                            assertThat cleaned isTrue
                        }
                    )
            )

            testAsync (
                "try/finally runs the compensation on a throwing body, and re-raises",
                fun _ ->
                    toAsync (
                        actor {
                            let mutable cleaned = false
                            let mutable caught = false

                            try
                                do!
                                    actor {
                                        try
                                            do! sleep 1
                                            failwith "boom"
                                        finally
                                            cleaned <- true
                                    }
                            with _ ->
                                caught <- true

                            assertThat cleaned isTrue
                            assertThat caught isTrue
                        }
                    )
            )

            testAsync (
                "an Async can be bound inside an actor body",
                // On BEAM this exercises the Async -> ActorOp bridge (Async.RunSynchronously).
                fun _ ->
                    toAsync (
                        actor {
                            let mutable ran = false
                            do! async { ran <- true }
                            let! v = async { return 42 }
                            assertThat ran isTrue
                            assertThat v (isEqualTo 42)
                        }
                    )
            )
        ]
    )

// ============================================================================
// callWithTimeout
// ============================================================================

let private timeoutTests =
    testList (
        "callWithTimeout",
        [
            testAsync (
                "succeeds when the reply arrives within the timeout",
                fun _ ->
                    toAsync (
                        actor {
                            let worker =
                                Actor.start () (fun _state (msg, rc) ->
                                    match msg with
                                    | Fast ->
                                        rc.Reply "fast"
                                        Continue()
                                    | Slow ->
                                        rc.Reply "slow"
                                        Continue())

                            let! result = Actor.callWithTimeout 1000 worker Fast
                            assertThat result (isEqualTo "fast")
                        }
                    )
            )

            testAsync (
                "raises TimeoutException when the reply takes too long",
                fun _ ->
                    toAsync (
                        actor {
                            let worker: Actor<unit * ReplyChannel<string>> =
                                Actor.spawn (fun inbox ->
                                    let rec loop () =
                                        actor {
                                            let! (), _rc = inbox.Receive()
                                            // Never reply — let it time out
                                            do! sleep 5000
                                            return! loop ()
                                        }

                                    loop ())

                            let mutable timedOut = false

                            try
                                let! _result = Actor.callWithTimeout 50 worker ()
                                ()
                            with :? System.TimeoutException ->
                                timedOut <- true

                            assertThat timedOut isTrue
                        }
                    )
            )
        ]
    )

// ============================================================================
// callAsync
// ============================================================================

let private callAsyncTests =
    testList (
        "callAsync",
        [
            testAsync (
                "a reply can be let!-bound from inside an async { } block",
                // Regression target for Fable.Reactive's mapActor: on BEAM `call` is CPS and cannot
                // be let!-bound in async { }, but callAsync can. The caller (this process) and the
                // server (a distinct process from Actor.start) must not deadlock.
                fun _ ->
                    toAsync (
                        actor {
                            let server =
                                Actor.start 0 (fun count (msg, rc) ->
                                    match msg with
                                    | Increment -> Continue(count + 1)
                                    | Decrement -> Continue(count - 1)
                                    | GetCount ->
                                        rc.Reply count
                                        Continue count)

                            Actor.cast server Increment
                            Actor.cast server Increment
                            do! sleep 10

                            let! count =
                                async {
                                    let! c = Actor.callAsync server GetCount
                                    return c
                                }

                            assertThat count (isEqualTo 2)
                        }
                    )
            )
        ]
    )

let tests = testList ("Builder", [ controlFlowTests; timeoutTests; callAsyncTests ])
