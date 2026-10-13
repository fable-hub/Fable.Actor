module CommandPackageConsumer

open Fable.Actor
open Fable.Actor.Types
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
    Actor.spawn (
        (0, 0, []),
        (fun (latest, count, waiters) (message, reply) ->
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
    )

let private scenario cooperative =
    actor {
        let observed = observations ()

        let command =
            Command.batch [
                Command.ofActor (fun () -> actor { return 1 }) () id
                |> Command.map Added
                Command.ofAsync (fun value -> async { return value }) 2 Added
            ]

        let update state message =
            match message with
            | Start -> state, command
            | Added value ->
                let next = state + value
                Actor.cast observed (Accepted next)
                next, Command.none

        let owner, close, release =
            if cooperative then
                let lifetime = Actor.spawnCooperative (10, update)

                Lifetimes.reference lifetime,
                (fun () -> Lifetimes.asyncStop deadline lifetime),
                (fun () -> Lifetimes.release lifetime |> ignore)
            else
                let reference: Actor<Message> = Actor.spawn (10, update)
                reference, (fun () -> Actor.asyncStop deadline reference), ignore

        Actor.send owner Start
        let! latest = Actor.callWithTimeout deadline observed AwaitCompletions
        assertThat latest (isEqualTo 13)
        let! stopped = close ()

        match stopped with
        | StopResult.Completed _ -> release ()
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
        testAsync ("packed commands preserve accepted state and deliver concurrent completions", fun _ -> toAsync (scenario false))
        testAsync ("packed cooperative command owners share the typed message reference", fun _ -> toAsync (scenario true))
    ]
