module OrdinaryOverhead.Bench

open Fable.Actor
open Fable.Actor.Types
open Fable.Core
open Fable.Core.BeamInterop

#if FABLE_COMPILER_BEAM
// decision: measure within a fresh ordinary worker so spawning, message preparation and reporting stay outside the timed region
// invariant: every sample checks its result and reports reductions from the same process that executes the actor expression
[<Emit("{erlang:monotonic_time(microsecond), element(2, process_info(self(), reductions))}")>]
let private counters () : int64 * int = nativeOnly

type Measurement = {
    Case: string
    Iterations: int
    Microseconds: int64
    Reductions: int
    Checksum: int64
}

let private immediate value : ActorOp<int64> = { Run = fun cont -> cont value }

let rec private binds remaining sum =
    actor {
        if remaining = 0 then
            return sum
        else
            let! value = immediate 1L
            return! binds (remaining - 1) (sum + value)
    }

let rec private asyncBinds remaining sum =
    actor {
        if remaining = 0 then
            return sum
        else
            let! value = async { return 1L }
            return! asyncBinds (remaining - 1) (sum + value)
    }

let rec private receives (inbox: ActorInbox<int>) remaining sum =
    actor {
        if remaining = 0 then
            return sum
        else
            let! value = inbox.Receive()
            return! receives inbox (remaining - 1) (sum + int64 value)
    }

let rec private finalizers remaining sum cleanup =
    actor {
        if remaining = 0 then
            return sum
        else
            let! value =
                actor {
                    try
                        return 1L
                    finally
                        cleanup ()
                }

            return! finalizers (remaining - 1) (sum + value) cleanup
    }

let rec private calls service remaining sum =
    actor {
        if remaining = 0 then
            return sum
        else
            let! value = Actor.call service ()
            return! calls service (remaining - 1) (sum + int64 value)
    }

let private report (recipient: Actor<Measurement>) name iterations (startTime, startReductions) sum =
    let endTime, endReductions = counters ()

    if sum <> int64 iterations then
        failwithf "Invalid %s checksum: %d" name sum

    Actor.send recipient {
        Case = name
        Iterations = iterations
        Microseconds = endTime - startTime
        Reductions = endReductions - startReductions
        Checksum = sum
    }

let private sample name iterations =
    let recipient = Actor.self<Measurement>()

    match name with
    | "bind" ->
        Actor.spawn (fun (_: ActorInbox<unit>) ->
            actor {
                let before = counters ()
                let! sum = binds iterations 0L
                report recipient name iterations before sum
            })
        |> ignore
    | "async" ->
        Actor.spawn (fun (_: ActorInbox<unit>) ->
            actor {
                let before = counters ()
                let! sum = asyncBinds iterations 0L
                report recipient name iterations before sum
            })
        |> ignore
    | "receive" ->
        Actor.spawn (fun inbox ->
            actor {
                for _ in 1..iterations do
                    Actor.send inbox.Reference 1

                let before = counters ()
                let! sum = receives inbox iterations 0L
                report recipient name iterations before sum
            })
        |> ignore
    | "finally" ->
        Actor.spawn (fun (_: ActorInbox<unit>) ->
            actor {
                let mutable cleaned = 0
                let before = counters ()
                let! sum = finalizers iterations 0L (fun () -> cleaned <- cleaned + 1)
                let after = counters ()

                if cleaned <> iterations then
                    failwithf "Wrong cleanup count: %d" cleaned

                if sum <> int64 iterations then
                    failwithf "Wrong finalizer checksum: %d" sum

                Actor.send recipient {
                    Case = name
                    Iterations = iterations
                    Microseconds = fst after - fst before
                    Reductions = snd after - snd before
                    Checksum = sum
                }
            })
        |> ignore
    | "call" ->
        Actor.spawn (fun (_: ActorInbox<unit>) ->
            actor {
                let service: Actor<unit * ReplyChannel<int>> =
                    Actor.spawn (
                        0,
                        (fun state (_, reply) ->
                            reply.Reply 1
                            Continue(state + 1))
                    )

                let before = counters ()
                let! sum = calls service iterations 0L
                report recipient name iterations before sum
                Actor.kill service
            })
        |> ignore
    | _ -> invalidArg "name" "Unknown workload"

    let mutable result = Unchecked.defaultof<Measurement>
    (Actor.receive<Measurement>()).Run(fun value -> result <- value)
    result

[<EntryPoint>]
let main args =
    let iterations, rounds = int args[0], int args[1]

    for round in 0..rounds do
        let order =
            if round % 2 = 0 then
                [ "bind"; "async"; "receive"; "finally"; "call" ]
            else
                [ "call"; "finally"; "receive"; "async"; "bind" ]

        for name in order do
            let count = if name = "call" then iterations / 20 else iterations
            let result = sample name count

            printfn
                "{\"round\":%d,\"case\":\"%s\",\"iterations\":%d,\"microseconds\":%d,\"reductions\":%d,\"checksum\":%d}"
                round
                result.Case
                result.Iterations
                result.Microseconds
                result.Reductions
                result.Checksum

    0
#else
[<EntryPoint>]
let main _ =
    failwith "This experiment measures native BEAM execution"
#endif
