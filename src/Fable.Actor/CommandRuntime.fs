namespace Fable.Actor

open Fable.Actor.Types
open Fable.Core

type private Input<'Message> = Input of 'Message

/// State and finite-work execution over the shared actor runtime.
///
/// decision: each owner's workers share one immutable completion reference so updates allocate no send adapters
[<RequireQualifiedAccess>]
module internal CommandRuntime =
    // decision: keep the owner mailbox object-typed so runtime child exits never masquerade as application union cases
    // invariant: only an Input envelope can reach the application update
    let private reference (owner: Actor<obj>) : Actor<'Message> =
        owner.MapInput(fun message -> box (Input message))

#if FABLE_COMPILER_BEAM
    [<Emit("case $0 of {CommandException__, CommandStack__} when is_map(CommandException__), is_list(CommandStack__) -> CommandException__; CommandReason__ -> CommandReason__ end")>]
    let private exceptionReason (reason: obj) : obj = nativeOnly
#else
    let private exceptionReason reason = reason
#endif

    let private failChild (exited: ChildExited) =
        match exceptionReason exited.Reason with
        | :? exn as error -> raise error
        | reason -> raise (ProcessExitException(sprintf "%A" reason))

    let private operationBody (owner: Actor<'Message>) (operation: Operation<'Message>) _ =
        actor {
            let! message = operation.Create()
            owner.Post message
        }

    let private execute (owner: Actor<obj>) target (Command operations) =
        for operation in operations do
            // decision: every operation uses an owned linked actor rather than another scheduler or a single-command scope
            // invariant: operation and completion-mapping defects enter the owner's linked failure path
            ActorRuntime.spawnLinked owner (operationBody target operation)
            |> ignore

    let private executeCooperative owner target (Command operations) =
        for operation in operations do
            // invariant: every command worker joins the cooperative owner's cleanup tree before its factory runs
            let worker = LifetimesRuntime.spawnLinked owner (operationBody target operation)
            // decision: the owner observes child cleanup, so individual worker capabilities relinquish retained observation
            LifetimesRuntime.releaseWhenExited worker

    // decision: empty-work branching stays outside the actor CE so skipping dispatch and enumeration adds no workflow bind
    let private dispatch execute (Command operations as command) =
        match operations with
        | [] -> ()
        | _ -> execute command

    let private body
        (initial: 'State)
        (update: 'State -> 'Message -> 'State * Command<'Message>)
        (shouldStop: ('State -> 'Message -> bool) option)
        (receive: unit -> ActorOp<obj>)
        execute
        =
        let rec loop state =
            actor {
                let! message = receive ()

                match ActorRuntime.tryAsChildExited message with
                | Some exited -> return failChild exited
                | None ->
                    let (Input input) = unbox<Input<'Message>> message
                    // decision: termination is a host predicate over accepted state and input, preserving tuple-only updates
                    // decision: an absent predicate skips callback dispatch on the default update path
                    let terminate =
                        match shouldStop with
                        | Some predicate -> predicate state input
                        | None -> false

                    if terminate then
                        return ()
                    else
                        let nextState, command = update state input
                        // invariant: an update that defects starts none of its returned work
                        dispatch execute command
                        return! loop nextState
            }

        actor {
            // decision: receive native child defects as messages so the owner follows its normal runtime failure and cleanup path
            ActorRuntime.trapExits ()
            return! loop initial
        }

    let private ordinaryBody initial update shouldStop (inbox: ActorInbox<obj>) =
        let target = reference inbox.Reference
        body initial update shouldStop inbox.Receive (execute inbox.Reference target)

    let private cooperativeBody initial update shouldStop (inbox: CooperativeInbox<obj>) =
        let target = reference inbox.Reference
        body initial update shouldStop inbox.Receive (executeCooperative inbox.Owner target)

    let spawn initial update shouldStop =
        ActorRuntime.spawn (ordinaryBody initial update shouldStop)
        |> fun inbox -> reference inbox.Reference

    let spawnLinked parent initial update shouldStop =
        ActorRuntime.spawnLinked parent (ordinaryBody initial update shouldStop)
        |> fun inbox -> reference inbox.Reference

    let private adapt (CooperativeActor(target, handle)) =
        CooperativeActor(reference target, handle)

    let spawnCooperative initial update shouldStop =
        LifetimesRuntime.spawn (cooperativeBody initial update shouldStop)
        |> adapt

    let spawnCooperativeLinked parent initial update shouldStop =
        LifetimesRuntime.spawnLinked parent (cooperativeBody initial update shouldStop)
        |> adapt
