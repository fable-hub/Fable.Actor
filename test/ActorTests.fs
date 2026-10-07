module Fable.Actor.Tests.ActorTests

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Actor
open Fable.Actor.Types

type SingleMsg =
    | SetValue of string
    | GetValue

type CollectIntMsg =
    | AddItem of int
    | GetItems

type PostMsg =
    | SetInt of int
    | GetInt

type Command =
    | Add of int
    | Done

type CollectorMsg<'T> =
    | Collect of 'T
    | GetResults of expectedCount: int

// decision: holds collector replies until the expected messages arrive so independent senders do not rely on scheduler timing
let private collector () =
    Actor.start ([], []) (fun (results, waiters) (msg, rc) ->
        match msg with
        | Collect x ->
            let results = results @ [ x ]
            let count = List.length results

            let ready, pending =
                waiters
                |> List.partition (fun (expected, _) -> expected <= count)

            for _, reply in ready do
                reply.Reply results

            Continue(results, pending)
        | GetResults expected when expected > List.length results -> Continue(results, (expected, rc) :: waiters)
        | GetResults _ ->
            rc.Reply results
            Continue(results, waiters))

type TimerMsg =
    | Tick
    | GetTicks

// ============================================================================
// spawn
// ============================================================================

let private spawnTests =
    testList (
        "spawn",
        [
            testAsync (
                "an actor that does nothing does not crash",
                fun _ ->
                    toAsync (
                        actor {
                            let _a: Actor<string> = Actor.spawn (fun _inbox -> actor { return () })

                            do! sleep 10
                            assertThat true isTrue
                        }
                    )
            )

            testAsync (
                "a spawned actor can send to other actors",
                fun _ ->
                    toAsync (
                        actor {
                            let collector = collector ()

                            let _worker: Actor<string> =
                                Actor.spawn (fun _inbox ->
                                    Actor.cast collector (Collect "hello")
                                    Actor.cast collector (Collect "world")
                                    actor { return () })

                            let! results = Actor.callWithTimeout 2000 collector (GetResults 2)
                            assertThat results (isEqualTo [ "hello"; "world" ])
                            let! _ = Actor.stop 2000 collector
                            return ()
                        }
                    )
            )

            testAsync (
                "a spawned actor can receive, transform and forward",
                fun _ ->
                    toAsync (
                        actor {
                            let collector = collector ()

                            let doubler: Actor<int> =
                                Actor.spawn (fun inbox ->
                                    let rec loop () =
                                        actor {
                                            let! n = inbox.Receive()
                                            Actor.cast collector (Collect(n * 2))
                                            return! loop ()
                                        }

                                    loop ())

                            Actor.send doubler 1
                            Actor.send doubler 2
                            Actor.send doubler 3

                            let! results = Actor.callWithTimeout 2000 collector (GetResults 3)
                            assertThat results (isEqualTo [ 2; 4; 6 ])
                            let! _ = Actor.stop 2000 doubler
                            let! _ = Actor.stop 2000 collector
                            return ()
                        }
                    )
            )

            testAsync (
                "an actor can do async work before replying",
                fun _ ->
                    toAsync (
                        actor {
                            let worker: Actor<unit * ReplyChannel<string>> =
                                Actor.spawn (fun inbox ->
                                    let rec loop () =
                                        actor {
                                            let! (), rc = inbox.Receive()
                                            do! sleep 10
                                            rc.Reply "done"
                                            return! loop ()
                                        }

                                    loop ())

                            let! result = Actor.call worker ()
                            assertThat result (isEqualTo "done")
                        }
                    )
            )
        ]
    )

// ============================================================================
// receive
// ============================================================================

let private receiveTests =
    testList (
        "receive",
        [
            testAsync (
                "a single message is received",
                fun _ ->
                    toAsync (
                        actor {
                            let a =
                                Actor.start "" (fun state (msg, rc) ->
                                    match msg with
                                    | SetValue s -> Continue s
                                    | GetValue ->
                                        rc.Reply state
                                        Continue state)

                            Actor.cast a (SetValue "hello")
                            do! sleep 10
                            let! got = Actor.call a GetValue
                            assertThat got (isEqualTo "hello")
                        }
                    )
            )

            testAsync (
                "multiple messages are received in order",
                fun _ ->
                    toAsync (
                        actor {
                            let a =
                                Actor.start [] (fun items (msg, rc) ->
                                    match msg with
                                    | AddItem n -> Continue(items @ [ n ])
                                    | GetItems ->
                                        rc.Reply items
                                        Continue items)

                            Actor.cast a (AddItem 1)
                            Actor.cast a (AddItem 2)
                            Actor.cast a (AddItem 3)
                            do! sleep 10
                            let! items = Actor.call a GetItems
                            assertThat items (isEqualTo [ 1; 2; 3 ])
                        }
                    )
            )

            testAsync (
                "Post is equivalent to send",
                fun _ ->
                    toAsync (
                        actor {
                            let a =
                                Actor.start 0 (fun state (msg, rc) ->
                                    match msg with
                                    | SetInt n -> Continue n
                                    | GetInt ->
                                        rc.Reply state
                                        Continue state)

                            a.Post((SetInt 42, { Reply = fun _ -> () }))
                            do! sleep 10
                            let! got = Actor.call a GetInt
                            assertThat got (isEqualTo 42)
                        }
                    )
            )
        ]
    )

// ============================================================================
// start (stateful actor)
// ============================================================================

let private startTests =
    testList (
        "start",
        [
            testAsync (
                "state is threaded through the handler",
                fun _ ->
                    toAsync (
                        actor {
                            let counter =
                                Actor.start 0 (fun count (msg, rc) ->
                                    match msg with
                                    | Increment -> Continue(count + 1)
                                    | Decrement -> Continue(count - 1)
                                    | GetCount ->
                                        rc.Reply count
                                        Continue count)

                            Actor.cast counter Increment
                            Actor.cast counter Increment
                            Actor.cast counter Increment
                            Actor.cast counter Decrement

                            do! sleep 10

                            let! count = Actor.call counter GetCount
                            assertThat count (isEqualTo 2)
                        }
                    )
            )

            testAsync (
                "Stop terminates the handler loop",
                fun _ ->
                    toAsync (
                        actor {
                            let counter =
                                Actor.start 0 (fun count (msg, rc) ->
                                    match msg with
                                    | Add _ -> Continue(count + 1)
                                    | Done ->
                                        rc.Reply count
                                        Stop)

                            Actor.cast counter (Add 1)
                            Actor.cast counter (Add 1)
                            Actor.cast counter (Add 1)

                            do! sleep 10

                            let! count = Actor.call counter Done
                            assertThat count (isEqualTo 3)
                        }
                    )
            )
        ]
    )

// ============================================================================
// call
// ============================================================================

let private callTests =
    testList (
        "call",
        [
            testAsync (
                "a call awaits the reply",
                fun _ ->
                    toAsync (
                        actor {
                            let counter =
                                Actor.start 0 (fun count (msg, rc) ->
                                    match msg with
                                    | Increment -> Continue(count + 1)
                                    | Decrement -> Continue(count - 1)
                                    | GetCount ->
                                        rc.Reply count
                                        Continue count)

                            Actor.cast counter Increment
                            Actor.cast counter Increment
                            Actor.cast counter Increment

                            do! sleep 10

                            let! count = Actor.call counter GetCount
                            assertThat count (isEqualTo 3)
                        }
                    )
            )

            testAsync (
                "sequential calls return the value at the time of the call",
                fun _ ->
                    toAsync (
                        actor {
                            let counter =
                                Actor.start 0 (fun count (msg, rc) ->
                                    match msg with
                                    | Increment -> Continue(count + 1)
                                    | Decrement -> Continue(count - 1)
                                    | GetCount ->
                                        rc.Reply count
                                        Continue count)

                            Actor.cast counter Increment
                            do! sleep 10
                            let! c1 = Actor.call counter GetCount
                            assertThat c1 (isEqualTo 1)

                            Actor.cast counter Increment
                            Actor.cast counter Increment
                            do! sleep 10
                            let! c2 = Actor.call counter GetCount
                            assertThat c2 (isEqualTo 3)
                        }
                    )
            )
        ]
    )

// ============================================================================
// schedule
// ============================================================================

let private scheduleTests =
    testList (
        "schedule",
        [
            testAsync (
                "callbacks fire after their delay",
                fun _ ->
                    toAsync (
                        actor {
                            let ticker =
                                Actor.start 0 (fun count (msg, rc) ->
                                    match msg with
                                    | Tick -> Continue(count + 1)
                                    | GetTicks ->
                                        rc.Reply count
                                        Continue count)

                            Actor.schedule 10 (fun () -> Actor.cast ticker Tick)
                            |> ignore

                            Actor.schedule 20 (fun () -> Actor.cast ticker Tick)
                            |> ignore

                            Actor.schedule 30 (fun () -> Actor.cast ticker Tick)
                            |> ignore

                            do! sleep 100

                            let! ticks = Actor.call ticker GetTicks
                            assertThat ticks (isEqualTo 3)
                        }
                    )
            )
        ]
    )

// ============================================================================
// kill
// ============================================================================

let private killTests =
    testList (
        "kill",
        [
            testAsync (
                "a killed actor receives no further messages",
                fun _ ->
                    toAsync (
                        actor {
                            let mutable received = false

                            let target: Actor<string> =
                                Actor.spawn (fun inbox ->
                                    let rec loop () =
                                        actor {
                                            let! _msg = inbox.Receive()
                                            received <- true
                                            return! loop ()
                                        }

                                    loop ())

                            Actor.kill target
                            Actor.send target "should not arrive"
                            do! sleep 50

                            assertThat received isFalse
                        }
                    )
            )
        ]
    )

let tests =
    testList ("Actor", [ spawnTests; receiveTests; startTests; callTests; scheduleTests; killTests ])
