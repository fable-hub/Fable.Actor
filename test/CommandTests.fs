module Fable.Actor.Tests.CommandTests

open System.Threading
open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test
open Fable.Actor
open Fable.Actor.Types
open Fable.Actor.Commands
open Fable.Actor.Tests.LifecycleTests
open Fable.Actor.Tests.CommandTestSupport
open Fable.Actor.Tests.CommandObservations

let private constructorTests = [
    case "completion updates observe the state accepted with their command" (fun () ->
        actor {
            let events = journal ()

            let target =
                CommandActor.start 0 (fun state message ->
                    match message with
                    | Begin work -> CommandNext.Continue(42, work)
                    | Completed _ ->
                        record events (State state)
                        CommandNext.Continue(state, Command.none)
                    | _ -> CommandNext.Stop)

            CommandActor.send target (Begin(Command.ofOperation (fun () -> actor { return Ok 1 }) Completed))
            let! values = take 1 events
            assertThat values (isEqualTo [ State 42 ])
            do! stop target
            let! _ = Actor.stop deadline events
            return ()
        })
    case "construction, mapping, and batching are inert" (fun () ->
        actor {
            let factories = journal ()

            let operation () =
                record factories "factory"
                actor { return 1 }

            let command =
                Command.ofOperation operation id
                |> Command.map ((+) 1)

            let _ = Command.batch [ Command.none; command; command ]
            let! values = take 0 factories
            assertThat values (isEqualTo [])
            let! _ = Actor.stop deadline factories
            return ()
        })
    case "none preserves state and starts no work" (fun () ->
        actor {
            let events = journal ()
            let target = owner events
            CommandActor.send target (Begin Command.none)
            CommandActor.send target Inspect
            let! values = take 1 events
            assertThat values (isEqualTo [ State 0 ])
            do! stop target
            let! _ = Actor.stop deadline events
            return ()
        })
    case "success and domain failure arrive unchanged as messages" (fun () ->
        actor {
            let events = journal ()
            let target = owner events
            CommandActor.send target (Begin(Command.ofOperation (fun () -> actor { return Ok 7 }) Completed))
            let! first = take 1 events
            assertThat first (isEqualTo [ Outcome(Ok 7) ])
            CommandActor.send target (Begin(Command.ofOperation (fun () -> actor { return Error "unavailable" }) Completed))
            let! second = take 2 events
            assertThat second (isEqualTo [ Outcome(Ok 7); Outcome(Error "unavailable") ])
            CommandActor.send target Inspect
            let! states = take 3 events
            assertThat (List.last states) (isEqualTo (State 7))
            do! stop target
            let! _ = Actor.stop deadline events
            return ()
        })
    case "mapping runs inside work and nested batches preserve every completion" (fun () ->
        actor {
            let events = journal ()
            let target = owner events

            let command value =
                Command.ofOperation (fun () -> actor { return value }) id
                |> Command.map (Ok >> Completed)

            let batch =
                Command.batch [ command 1; Command.batch [ Command.none; command 2 ]; command 3 ]

            CommandActor.send target (Begin batch)
            let! values = take 3 events
            sameEvents [ Outcome(Ok 1); Outcome(Ok 2); Outcome(Ok 3) ] values
            CommandActor.send target Inspect
            let! states = take 4 events
            assertThat (List.last states) (isEqualTo (State 6))
            do! stop target
            let! _ = Actor.stop deadline events
            return ()
        })
]

let private requestTests = [
    case "pre-cancelled requests perform no service admission" (fun () ->
        actor {
            let events, admitted = journal (), journal ()
            let service = gate admitted
            let target = owner events
            let token = new CancellationTokenSource()
            token.Cancel()
            CommandActor.send target (Begin(request deadline token.Token service 1))
            let! values = take 1 events
            assertThat values (isEqualTo [ RequestOutcome CallResult.Cancelled ])
            let! started = take 0 admitted
            assertThat started (isEqualTo [])
            do! stop target
            let! _ = Actor.stop deadline service
            let! _ = Actor.stop deadline admitted
            let! _ = Actor.stop deadline events
            return ()
        })
    case "two active commands and none leave the owner responsive" (fun () ->
        actor {
            let events, admitted = journal (), journal ()
            let service = gate admitted
            let target = owner events
            CommandActor.send target (Begin(request deadline CancellationToken.None service 1))
            CommandActor.send target (Begin(request deadline CancellationToken.None service 2))
            let! started = take 2 admitted
            assertThat (List.sort started) (isEqualTo [ 1; 2 ])
            CommandActor.send target (Begin Command.none)
            CommandActor.send target Inspect
            let! responsive = take 1 events
            assertThat responsive (isEqualTo [ State 0 ])
            Actor.cast service Release
            let! values = take 3 events

            sameEvents
                [
                    State 0
                    RequestOutcome(CallResult.Reply(Ok 1))
                    RequestOutcome(CallResult.Reply(Ok 2))
                ]
                values

            do! stop target
            let! _ = Actor.stop deadline service
            let! _ = Actor.stop deadline admitted
            let! _ = Actor.stop deadline events
            return ()
        })
    case "request timeout remains an explicit outcome" (fun () ->
        actor {
            let events, admitted = journal (), journal ()
            let service = gate admitted
            let target = owner events
            CommandActor.send target (Begin(request replyDeadline CancellationToken.None service 1))
            let! values = take 1 events
            assertThat values (isEqualTo [ RequestOutcome CallResult.TimedOut ])
            Actor.cast service Release
            CommandActor.send target Inspect
            let! settled = take 2 events
            assertThat settled (isEqualTo [ RequestOutcome CallResult.TimedOut; State 0 ])
            do! stop target
            let! _ = Actor.stop deadline service
            let! _ = Actor.stop deadline admitted
            let! _ = Actor.stop deadline events
            return ()
        })
    case "explicit cancellation settles an admitted request as data" (fun () ->
        actor {
            let events, admitted = journal (), journal ()
            let service = gate admitted
            let target = owner events
            let token = new CancellationTokenSource()
            CommandActor.send target (Begin(request deadline token.Token service 1))
            let! _ = take 1 admitted
            token.Cancel()
            let! values = take 1 events
            assertThat values (isEqualTo [ RequestOutcome CallResult.Cancelled ])
            Actor.cast service Release
            do! stop target
            let! _ = Actor.stop deadline service
            let! _ = Actor.stop deadline admitted
            let! _ = Actor.stop deadline events
            return ()
        })
    case "service termination remains distinct from timeout" (fun () ->
        actor {
            let events, admitted = journal (), journal ()
            let service = gate admitted
            let target = owner events
            CommandActor.send target (Begin(request deadline CancellationToken.None service 1))
            let! _ = take 1 admitted
            Actor.cast service Finish
            let! values = take 1 events

            match values with
            | [ RequestOutcome(CallResult.TargetTerminated _) ] -> ()
            | _ -> failwith "Missing termination outcome"

            do! stop target
            let! _ = Actor.stop deadline admitted
            let! _ = Actor.stop deadline events
            return ()
        })
]

let private defectCase name command (marker: string) =
    case name (fun () ->
        actor {
            let failures = journal ()

            let parent, handles =
                supervised failures (fun state message ->
                    match message with
                    | Begin work -> CommandNext.Continue(state, work)
                    | _ -> failwith "Unexpected completion")

            let! created = take 1 handles
            let child = List.head created
            CommandActor.send child (Begin command)
            let! errors = take 1 failures
            assertThat ((List.head errors).Contains marker) isTrue
            do! observeExit (runtime child)
            do! observeExit parent
            let! _ = Actor.stop deadline failures
            let! _ = Actor.stop deadline handles
            return ()
        })

let private defectTests = [
    defectCase
        "operation body defects fail the linked owner"
        (Command.ofOperation (fun () -> actor { return failwith "body-defect" }) Completed)
        "body-defect"
    defectCase
        "operation defects fail the linked owner"
        (Command.ofOperation (fun () -> failwith "factory-defect") Completed)
        "factory-defect"
    defectCase
        "completion defects fail the linked owner"
        (Command.ofOperation (fun () -> actor { return 1 }) (fun _ -> failwith "completion-defect"))
        "completion-defect"
    defectCase
        "map defects fail the linked owner"
        (Command.ofOperation (fun () -> actor { return 1 }) id
         |> Command.map (fun _ -> failwith "map-defect"))
        "map-defect"
    case "an update defect does not execute a constructed command" (fun () ->
        actor {
            let failures, executions = journal (), journal ()

            let work =
                Command.ofOperation
                    (fun () ->
                        record executions "started"
                        actor { return Ok 1 })
                    Completed

            let parent, handles =
                supervised failures (fun _ _ ->
                    let _ = work
                    failwith "update-defect")

            let! created = take 1 handles
            let child = List.head created
            CommandActor.send child Defect
            let! errors = take 1 failures
            assertThat ((List.head errors).Contains "update-defect") isTrue
            let! started = take 0 executions
            assertThat started (isEqualTo [])
            do! observeExit parent
            let! _ = Actor.stop deadline failures
            let! _ = Actor.stop deadline executions
            let! _ = Actor.stop deadline handles
            return ()
        })
]

let private ownershipCase closeNormally =
    case
        (if closeNormally then
             "normal owner completion stops active work"
         else
             "external owner stop stops active work")
        (fun () ->
            actor {
                let events, admitted, cleanup = journal (), journal (), barrier ()
                let service = gate admitted
                let target = owner events
#if FABLE_COMPILER_BEAM
                let workers = journal ()
#endif
                let operation () =
                    actor {
#if FABLE_COMPILER_BEAM
                        record workers (Actor.self<unit>())
#endif
                        try
                            return! Actor.callResult deadline CancellationToken.None service (Enter 1)
                        finally
                            Actor.cast cleanup Signal
                    }

                CommandActor.send target (Begin(Command.ofOperation operation RequestFinished))
                let! _ = take 1 admitted

                if closeNormally then
                    CommandActor.send target Close
                    do! observeExit (runtime target)
                else
                    do! stop target
#if FABLE_COMPILER_BEAM
                let! children = take 1 workers
                do! observeExit (List.head children)
                let! count = Actor.callWithTimeout deadline cleanup Read
                assertThat count (isEqualTo 0)
                let! _ = Actor.stop deadline workers
#else
                do! await 1 cleanup
#endif
                let! outcomes = take 0 events
                assertThat outcomes (isEqualTo [])
                Actor.cast service Release
                let! _ = Actor.stop deadline service
                let! _ = Actor.stop deadline admitted
                let! _ = Actor.stop deadline cleanup
                let! _ = Actor.stop deadline events
                return ()
            })

let tests =
    testList (
        "Commands",
        constructorTests
        @ requestTests
        @ defectTests
        @ [ ownershipCase true; ownershipCase false ]
    )
