module CommandPackageConsumer

open Fable.Actor
open Fable.Actor.Types
open Fable.Actor.Commands
open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test
open type Scriptorium.Quill.Runner

type Message =
    | Start
    | Added of int

type Observation =
    | Accepted of int
    | AwaitCompletions

let private completionCount = 2
let private deadline = 2000

let private observations () =
    Actor.start (0, 0, []) (fun (latest, count, waiters) (message, reply) ->
        match message with
        | Accepted value ->
            let nextCount = count + 1

            if nextCount = completionCount then
                for waiter in waiters do
                    waiter.Reply value

                Continue(value, nextCount, [])
            else
                Continue(value, nextCount, waiters)
        | AwaitCompletions when count < completionCount -> Continue(latest, count, reply :: waiters)
        | AwaitCompletions ->
            reply.Reply latest
            Continue(latest, count, waiters))

let private scenario () =
    actor {
        let observed = observations ()

        let command =
            Command.batch [
                Command.ofOperation (fun () -> actor { return 1 }) id
                |> Command.map Added
                Command.ofOperation (fun () -> actor { return 2 }) Added
            ]

        let owner =
            CommandActor.start 10 (fun state message ->
                match message with
                | Start -> Fable.Actor.Commands.Next.Continue(state, command)
                | Added value ->
                    let next = state + value
                    Actor.cast observed (Accepted next)
                    Fable.Actor.Commands.Next.Continue(next, Command.none))

        CommandActor.send owner Start
        let! latest = Actor.callWithTimeout deadline observed AwaitCompletions
        assertThat latest (isEqualTo 13)
        let! stopped = CommandActor.stopAsync deadline owner

        match stopped with
        | StopResult.Completed _ -> ()
        | StopResult.TimedOut -> failwith "Command owner did not stop"

        let! _ = Actor.stop deadline observed
        return ()
    }

let private toAsync (operation: ActorOp<unit>) : Async<unit> =
#if FABLE_COMPILER_BEAM
    async { operation.Run(fun () -> ()) }
#else
    operation
#endif

[<EntryPoint>]
let main _ =
    runTests [
        testAsync ("packed commands preserve accepted state and deliver concurrent completions", fun _ -> toAsync (scenario ()))
    ]
