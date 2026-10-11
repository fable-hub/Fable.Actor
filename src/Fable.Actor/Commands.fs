namespace Fable.Actor.Commands

open Fable.Actor

// decision: keep work descriptions in an optional namespace so loop and state-only actors need no command concepts

// decision: retain factories in records so command mapping constructs data and preserves callback arity across targets
type internal Operation<'Message> = { Create: unit -> ActorOp<'Message> }

/// An inert description of finite actor-owned work that produces application messages.
/// Constructing, mapping, or batching commands does not run their operation factories.
type Command<'Message> = internal Command of Operation<'Message> list

/// The next private state and work, or normal owner completion.
[<RequireQualifiedAccess>]
type Next<'State, 'Message> =
    /// Accept the new state before starting the described work.
    | Continue of 'State * Command<'Message>
    /// Finish the owner; linked workers follow the runtime's shutdown contract.
    | Stop

/// Optional finite-work descriptions; actor loops and state-only handlers do not require them.
[<RequireQualifiedAccess>]
module Command =
    /// Start no new work. Previously started work is unaffected.
    let none: Command<'Message> = Command []

    /// Describe an operation whose value becomes one application message.
    /// Both the factory and the completion function execute inside a linked worker.
    /// Defects fail the owner; Result and CallResult values remain ordinary message data.
    let ofOperation (operation: unit -> ActorOp<'Value>) (completed: 'Value -> 'Message) : Command<'Message> =
        Command [
            {
                Create =
                    fun () ->
                        actor {
                            let! value = operation ()
                            return completed value
                        }
            }
        ]

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
