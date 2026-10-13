namespace Fable.Actor

open Fable.Actor

// decision: command descriptions are optional factory inputs so explicit loops and state-only actors need no command execution policy

// decision: retain factories in records so command mapping constructs data and preserves callback arity across targets
type internal Operation<'Message> = { Create: unit -> ActorOp<'Message> }

/// An inert description of finite actor-owned work that produces application messages.
/// Constructing, mapping, or batching commands does not run their operation factories.
type Command<'Message> = internal Command of Operation<'Message> list

/// Optional finite-work descriptions; actor loops and state-only handlers do not require them.
[<RequireQualifiedAccess>]
module Command =
    /// Start no new work. Previously started work is unaffected.
    let none: Command<'Message> = Command []

    /// Describe an operation whose value becomes one application message.
    /// Both the factory and the completion function execute inside a linked worker.
    /// Defects fail the owner; Result and CallResult values remain ordinary message data.
    let ofActor (operation: 'Argument -> ActorOp<'Value>) (argument: 'Argument) (completed: 'Value -> 'Message) : Command<'Message> =
        Command [
            {
                Create =
                    fun () ->
                        actor {
                            let! value = operation argument
                            return completed value
                        }
            }
        ]

    /// Describe an Async workflow that runs through the actor builder inside its owned worker.
    /// Defects follow actor supervision; Result values remain application data.
    ///
    /// decision: bridges Async through the shared builder so cooperative cancellation follows the executing worker's lifetime
    let ofAsync (operation: 'Argument -> Async<'Value>) (argument: 'Argument) (completed: 'Value -> 'Message) : Command<'Message> =
        ofActor (fun input -> actor { let! value = operation input in return value }) argument completed

    /// Queue a message after the current update, through the same owned-work execution path.
    let ofMsg (message: 'Message) : Command<'Message> =
        ofActor (fun () -> actor { return message }) () id

    /// Group commands that may run concurrently. Completion order is unspecified.
    /// This does not add throttling, replacement, or a limit on active work.
    let batch (commands: Command<'Message> list) : Command<'Message> =
        commands
        |> List.collect (fun (Command operations) -> operations)
        |> Command

    /// Change completion messages without starting work.
    let map (mapping: 'Original -> 'Message) (Command operations: Command<'Original>) : Command<'Message> =
        operations
        |> List.map (fun operation -> {
            Create =
                fun () ->
                    actor {
                        let! message = operation.Create()
                        return mapping message
                    }
        })
        |> Command
