namespace Fable.Actor.Commands

open Fable.Actor
open Fable.Actor.Types
open Fable.Core

/// A typed command-owner handle over the existing actor runtime.
/// Its internal mailbox also receives linked-worker exit notifications.
type CommandActor<'Message> = internal CommandActor of Actor<obj>

type private Input<'Message> = Input of 'Message

/// Optional synchronous state updates with linked finite-work execution.
[<RequireQualifiedAccess>]
module CommandActor =
    // decision: keep the owner mailbox object-typed so runtime child exits never masquerade as application union cases
    // invariant: only an Input envelope can reach the application update
    let private post owner message = Actor.send owner (box (Input message))

#if FABLE_COMPILER_BEAM
    [<Fable.Core.Emit("case $0 of {CommandException__, CommandStack__} when is_map(CommandException__), is_list(CommandStack__) -> CommandException__; CommandReason__ -> CommandReason__ end")>]
    let private exceptionReason (reason: obj) : obj = nativeOnly
#else
    let private exceptionReason reason = reason
#endif

    let private failChild (exited: ChildExited) =
        match exceptionReason exited.Reason with
        | :? exn as error -> raise error
        | reason -> raise (ProcessExitException(sprintf "%A" reason))

    let private execute owner (Command operations) =
        for operation in operations do
            // decision: every operation uses an owned linked actor rather than another scheduler or a single-command scope
            // invariant: operation and completion-mapping defects enter the owner's linked failure path
            Actor.spawnLinked owner (fun _ ->
                actor {
                    let! message = operation.Create()
                    post owner message
                })
            |> ignore

    let private body initial update (inbox: Actor<obj>) =
        let rec loop state =
            actor {
                let! message = inbox.Receive()

                match Actor.tryAsChildExited message with
                | Some exited -> return failChild exited
                | None ->
                    let (Input input) = unbox<Input<'Message>> message

                    match update state input with
                    | Fable.Actor.Commands.Next.Stop -> return ()
                    | Fable.Actor.Commands.Next.Continue(nextState, command) ->
                        // invariant: an update that defects starts none of its returned work
                        execute inbox command
                        return! loop nextState
            }

        actor {
            // decision: receive native child defects as messages so the owner follows its normal runtime failure and cleanup path
            Actor.trapExits ()
            return! loop initial
        }

    /// Start an owner with synchronous serialized updates and concurrent finite commands.
    /// The state-only Actor.start and explicit Actor.spawn APIs remain independent.
    let start (initial: 'State) (update: 'State -> 'Message -> Next<'State, 'Message>) : CommandActor<'Message> =
        CommandActor(Actor.spawn (body initial update))

    /// Start a command owner linked to a supervising object-mailbox actor.
    /// Its failures follow the existing spawnLinked notification and ownership contract.
    let startLinked (parent: Actor<obj>) (initial: 'State) (update: 'State -> 'Message -> Next<'State, 'Message>) : CommandActor<'Message> =
        CommandActor(Actor.spawnLinked parent (body initial update))

    /// Queue an application message without acknowledging delivery or processing.
    let send (CommandActor owner: CommandActor<'Message>) (message: 'Message) = post owner message

    /// Request stop and observe exit within a positive deadline, using the existing runtime.
    /// BEAM kill skips finalizers and does not acknowledge descendant cleanup.
    let stop deadline (CommandActor owner) : ActorOp<StopResult> = Actor.stop deadline owner

    /// Async-host form of stop with the same deadline and target-specific semantics.
    let stopAsync deadline (CommandActor owner) : Async<StopResult> = Actor.stopAsync deadline owner
