module Fable.Actor.Tests.CommandTestSupport

open System.Threading
open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test
open Fable.Actor
open Fable.Actor.Types
open Fable.Actor.Commands
open Fable.Actor.Tests.LifecycleTests
open Fable.Actor.Tests.CommandObservations

type CommandNext<'State, 'Message> = Fable.Actor.Commands.Next<'State, 'Message>

let deadline = 2000
let replyDeadline = 50

type Message =
    | Begin of Command<Message>
    | Completed of Result<int, string>
    | RequestFinished of CallResult<Result<int, string>>
    | Inspect
    | Close
    | Defect

type Event =
    | State of int
    | Outcome of Result<int, string>
    | RequestOutcome of CallResult<Result<int, string>>

let sameEvents expected actual =
    assertThat (List.length actual) (isEqualTo (List.length expected))

    for event in expected do
        assertThat (List.contains event actual) isTrue

let update events state =
    function
    | Begin command -> CommandNext.Continue(state, command)
    | Completed result ->
        record events (Outcome result)

        let next =
            match result with
            | Ok value -> state + value
            | Error _ -> state

        CommandNext.Continue(next, Command.none)
    | RequestFinished result ->
        record events (RequestOutcome result)
        CommandNext.Continue(state, Command.none)
    | Inspect ->
        record events (State state)
        CommandNext.Continue(state, Command.none)
    | Close -> CommandNext.Stop
    | Defect -> failwith "update-defect"

let owner events = CommandActor.start 0 (update events)
let runtime (CommandActor target) = target

let stop target =
    actor {
        let! result = CommandActor.stop deadline target
        assertThat (completed result) isTrue
    }

type GateMessage =
    | Enter of int
    | Release
    | Finish

let gate admitted =
    Actor.start [] (fun waiters (message, reply) ->
        match message with
        | Enter value ->
            record admitted value
            Continue((value, reply) :: waiters)
        | Release ->
            for value, waiter in waiters do
                waiter.Reply(Ok value)

            Continue []
        | Finish -> Stop)

let request timeout token service value =
    Command.ofOperation (fun () -> Actor.callResult timeout token service (Enter value)) RequestFinished

let case name operation =
    testAsync (name, fun _ -> toAsync (operation ()))

let reasonText (reason: obj) =
#if FABLE_COMPILER_BEAM
    Actor.formatReason reason
#else
    match reason with
    | :? exn as error -> error.Message
    | other -> sprintf "%A" other
#endif

let supervised events update =
    let handles = journal ()

    let parent =
        Actor.spawn (fun inbox ->
            actor {
                Actor.trapExits ()
                let child = CommandActor.startLinked inbox 0 update
                record handles child
                let! message = inbox.Receive()

                match Actor.tryAsChildExited message with
                | Some exited -> record events (reasonText exited.Reason)
                | None -> failwith "Expected linked owner defect"
            })

    parent, handles
