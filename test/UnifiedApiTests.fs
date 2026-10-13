module Fable.Actor.Tests.UnifiedApiTests

open System.Threading
open Fable.Actor
open Fable.Actor.Types
open Fable.Actor.Tests.LifecycleTests
open Fable.Actor.Tests.CommandObservations
open Fable.Actor.Tests.CommandTestSupport
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

let private commonReferences () =
    actor {
        let loop: Actor<CounterMsg * ReplyChannel<int>> =
            Actor.spawn (fun inbox ->
                let rec receive count =
                    actor {
                        let! message, reply = inbox.Receive()

                        match message with
                        | Increment -> return! receive (count + 1)
                        | Decrement -> return! receive (count - 1)
                        | GetCount ->
                            reply.Reply count
                            return! receive count
                    }

                receive 0)

        let update count (message, reply) =
            match message with
            | Increment -> count + 1
            | Decrement -> count - 1
            | GetCount ->
                reply.Reply count
                count

        let state = Actor.spawn (0, update)

        let explicitState =
            Actor.spawn (0, fun count message -> Continue(update count message))

        let commands =
            Actor.spawn (0, fun count message -> update count message, Command.none)
        // invariant: the same typed call/cast protocol traverses every factory's send adapter
        let references: Actor<CounterMsg * ReplyChannel<int>> list = [ loop; state; explicitState; commands ]

        for target in references do
            Actor.cast target Increment
            let! value = Actor.callWithTimeout deadline target GetCount
            assertThat value (isEqualTo 1)
            let! _ = Actor.stop deadline target
            ()
    }

let private allStateFactories () =
    actor {
        let update count (message, reply) =
            match message with
            | Increment -> count + 1
            | Decrement -> count - 1
            | GetCount ->
                reply.Reply count
                count

        let cooperative: CooperativeActor<CounterMsg * ReplyChannel<int>> list = [
            Actor.spawnCooperative (0, update)
            Actor.spawnCooperative (0, fun state message -> Continue(update state message))
            Actor.spawnCooperative (0, fun state message -> update state message, Command.none)
        ]

        for capability in cooperative do
            let reference = Lifetimes.reference capability
            Actor.cast reference Increment
            let! count = Actor.callWithTimeout deadline reference GetCount
            assertThat count (isEqualTo 1)
            let! result = Lifetimes.stop deadline capability
            assertThat result (isEqualTo (StopResult.Completed ActorExit.Cancelled))
            Lifetimes.release capability |> ignore
    }

let private linkedStateFactories () =
    actor {
        let update count (message, reply) =
            match message with
            | Increment -> count + 1
            | Decrement -> count - 1
            | GetCount ->
                reply.Reply count
                count

        let parent: Actor<unit> =
            Actor.spawn (fun inbox -> actor { let! _ = inbox.Receive() in return () })

        let children: Actor<CounterMsg * ReplyChannel<int>> list = [
            Actor.spawnLinked (parent, 0, update)
            Actor.spawnLinked (parent, 0, fun state message -> Continue(update state message))
            Actor.spawnLinked (parent, 0, fun state message -> update state message, Command.none)
        ]

        for child in children do
            Actor.cast child Increment
            let! count = Actor.callWithTimeout deadline child GetCount
            assertThat count (isEqualTo 1)

        Actor.send parent ()
        do! observeExit parent

        for child in children do
            do! observeExit child

        let owner: CooperativeActor<unit> =
            Actor.spawnCooperative (fun inbox -> actor { let! _ = inbox.Receive() in return () })

        let descendants: CooperativeActor<CounterMsg * ReplyChannel<int>> list = [
            Actor.spawnLinked (owner, 0, update)
            Actor.spawnLinked (owner, 0, fun state message -> Continue(update state message))
            Actor.spawnLinked (owner, 0, fun state message -> update state message, Command.none)
        ]

        for child in descendants do
            Actor.cast (Lifetimes.reference child) Increment
            let! count = Actor.callWithTimeout deadline (Lifetimes.reference child) GetCount
            assertThat count (isEqualTo 1)

        let! result = Lifetimes.stop deadline owner
        assertThat result (isEqualTo (StopResult.Completed ActorExit.Cancelled))

        for child in descendants do
            let! result = Lifetimes.stop deadline child
            assertThat result (isEqualTo (StopResult.Completed ActorExit.Cancelled))
            Lifetimes.release child |> ignore

        Lifetimes.release owner |> ignore
    }

let private asyncAndMessageConstructors () =
    actor {
        let events, factories = journal (), journal ()

        let operation value =
            record factories value
            async { return Ok(value + 1) }

        let command = Command.ofAsync operation 6 Completed
        let! inert = take 0 factories
        assertThat inert (isEqualTo [])
        let target = owner events
        Actor.send target (Begin command)
        let! called = take 1 factories
        assertThat called (isEqualTo [ 6 ])
        let! completed = take 1 events
        assertThat completed (isEqualTo [ Outcome(Ok 7) ])
        Actor.send target (Begin(Command.ofMsg (Completed(Ok 3))))
        let! both = take 2 events
        assertThat both (isEqualTo [ Outcome(Ok 7); Outcome(Ok 3) ])
        do! stop target
        let! _ = Actor.stop deadline factories
        let! _ = Actor.stop deadline events
        return ()
    }

let private adaptedParentFailures () =
    actor {
        let failures = journal ()
        let parent, handles = supervised failures (fun state _ -> state, Command.none)
        let! created = take 1 handles
        let owner = List.head created
        // invariant: runtime failures bypass a command reference's application-message envelope
        let child: Actor<unit> =
            Actor.spawnLinked (owner, fun _ -> actor { return failwith "linked-reference-defect" })

        let! errors = take 1 failures
        assertThat ((List.head errors).Contains "linked-reference-defect") isTrue
        do! observeExit child
        do! observeExit owner
        do! observeExit parent
        let! _ = Actor.stop deadline failures
        let! _ = Actor.stop deadline handles
        return ()
    }

let private closeBeforeUpdate () =
    actor {
        let events = journal ()

        let target =
            Actor.spawn (
                0,
                (fun state message ->
                    match message with
                    | Close -> failwith "Terminating input must not run update"
                    | Inspect ->
                        record events (State state)
                        state, Command.none
                    | _ -> state + 1, Command.none),
                shouldStop =
                    (fun state message ->
                        match message with
                        | Close -> state = 1
                        | _ -> false)
            )

        Actor.send target (Begin Command.none)
        Actor.send target Inspect
        let! accepted = take 1 events
        assertThat accepted (isEqualTo [ State 1 ])
        Actor.send target Close
        do! observeExit target
        let! result = Actor.stop deadline target
#if !FABLE_COMPILER_BEAM
        assertThat result (isEqualTo (StopResult.Completed ActorExit.Normal))
#else
        assertThat (completed result) isTrue
#endif
        let! _ = Actor.stop deadline events
        return ()
    }

let private noncooperative started release =
#if FABLE_COMPILER_BEAM
    // decision: an opaque wait has no actor checkpoints, so only the explicit barrier can release it
    {
        Run =
            fun continuation ->
                Actor.cast started Signal

                Async.RunSynchronously(Actor.asyncCall release (Await 1))
                |> ignore

                continuation ()
    }
#else
    // invariant: admission is signalled inside the noncancellable callback, not before an actor cancellation checkpoint
    Async.FromContinuations(fun (ok, error, _) ->
        Actor.cast started Signal
        Async.StartWithContinuations(await 1 release, ok, error, error, CancellationToken.None))
#endif

let private cooperativeCleanup delayedCleanup =
    actor {
        let events = journal ()
        let started, cleaned, released = barrier (), barrier (), barrier ()
        let target = Actor.spawnCooperative (0, update events, shouldStop = shouldStop)

        let operation () =
            actor {
                try
                    if delayedCleanup then
                        do! noncooperative started released
                    else
                        Actor.cast started Signal
                        do! Async.Sleep 100000

                    return Ok 1
                finally
                    Actor.cast cleaned Signal
            }

        Actor.send (Lifetimes.reference target) (Begin(Command.ofActor operation () Completed))
        do! await 1 started
        let! first = Lifetimes.stop (if delayedCleanup then 20 else deadline) target

        if delayedCleanup then
            assertThat first (isEqualTo StopResult.TimedOut)
            assertThat (Lifetimes.release target) (isEqualTo LifetimeRelease.Running)
            let! count = Actor.callWithTimeout deadline cleaned Read
            assertThat count (isEqualTo 0)
            Actor.cast released Signal
        else
            assertThat (completed first) isTrue

        let! stopped = Lifetimes.stop deadline target
        assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Cancelled))
        do! await 1 cleaned
        let! count = Actor.callWithTimeout deadline cleaned Read
        assertThat count (isEqualTo 1)
        let! repeated = Lifetimes.stop deadline target
        assertThat repeated (isEqualTo stopped)
        let! outcomes = take 0 events
        assertThat outcomes (isEqualTo [])
        assertThat (Lifetimes.release target) (isEqualTo LifetimeRelease.Released)

        for observation in [ started; cleaned; released ] do
            let! _ = Actor.stop deadline observation
            ()

        let! _ = Actor.stop deadline events
        return ()
    }

let private cooperativeNormalExit () =
    actor {
        let events = journal ()
        let started, cleaned = barrier (), barrier ()
        let target = Actor.spawnCooperative (0, update events, shouldStop = shouldStop)

        let operation () =
            actor {
                try
                    Actor.cast started Signal
                    do! Async.Sleep 100000
                    return Ok 1
                finally
                    Actor.cast cleaned Signal
            }

        Actor.send (Lifetimes.reference target) (Begin(Command.ofActor operation () Completed))
        do! await 1 started
        Actor.send (Lifetimes.reference target) Close
        // Worker cleanup is an acknowledgement that normal owner completion has closed admission.
        do! await 1 cleaned
        let! stopped = Lifetimes.stop deadline target
        assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Normal))
        Lifetimes.release target |> ignore
        let! _ = Actor.stop deadline events
        let! _ = Actor.stop deadline started
        let! _ = Actor.stop deadline cleaned
        return ()
    }

let private cooperativeDefect () =
    actor {
        let events, started = journal (), barrier ()
        let target = Actor.spawnCooperative (0, update events)

        let work =
            Command.ofAsync
                (fun () ->
                    async {
                        Actor.cast started Signal
                        return failwith "async-command-defect"
                    })
                ()
                Completed

        Actor.send (Lifetimes.reference target) (Begin work)
        do! await 1 started
        // A parent-owned child failure stays retained even when stop races the failure notification.
        let! stopped = Lifetimes.stop deadline target

        match stopped with
        | StopResult.Completed(ActorExit.Failed error) -> assertThat (error.Message.Contains "async-command-defect") isTrue
        | other -> failwithf "Command defect was lost: %A" other

        Lifetimes.release target |> ignore
        let! _ = Actor.stop deadline started
        let! _ = Actor.stop deadline events
        return ()
    }

let private cooperativeTree () =
    actor {
        let events, handles = journal (), journal ()
        let started, cleaned = barrier (), barrier ()

        let parent: CooperativeActor<unit> =
            Actor.spawnCooperative (fun inbox ->
                actor {
                    let child = Actor.spawnLinked (inbox.Owner, 0, update events)

                    let work =
                        Command.ofActor
                            (fun () ->
                                actor {
                                    try
                                        Actor.cast started Signal
                                        do! Async.Sleep 100000
                                        return Ok 1
                                    finally
                                        Actor.cast cleaned Signal
                                })
                            ()
                            Completed

                    Actor.send (Lifetimes.reference child) (Begin work)
                    record handles (Lifetimes.reference child)
                    Lifetimes.releaseWhenExited child
                    let! _ = inbox.Receive()
                    return ()
                })

        let! references = take 1 handles
        do! await 1 started
        let! stopped = Lifetimes.stop deadline parent
        assertThat stopped (isEqualTo (StopResult.Completed ActorExit.Cancelled))
        do! await 1 cleaned
        do! observeExit (List.head references)
        Lifetimes.release parent |> ignore
        let! _ = Actor.stop deadline events
        let! _ = Actor.stop deadline handles
        let! _ = Actor.stop deadline started
        let! _ = Actor.stop deadline cleaned
        return ()
    }

let tests =
    testList (
        "Unified actor API",
        [
            case "every authoring form shares a typed call and cast reference" commonReferences
            case "cooperative state and command factories share typed messaging" allStateFactories
            case "linked factory overloads retain ordinary and cooperative ownership" linkedStateFactories
            case "Async factories are inert and immediate commands return typed messages" asyncAndMessageConstructors
            case "linked failures bypass adapted application-message references" adaptedParentFailures
            case "termination uses accepted state and skips the matching update" closeBeforeUpdate
            case "cooperative command stop awaits worker finalizers and retains exit" (fun () -> cooperativeCleanup false)
            case "cooperative command timeout leaves workers owned until actual exit" (fun () -> cooperativeCleanup true)
            case "normal command owner completion awaits cooperative worker cleanup" cooperativeNormalExit
            case "Async command defects remain retained terminal failures" cooperativeDefect
            case "linked cooperative command owners join transitive cleanup" cooperativeTree
        ]
    )
