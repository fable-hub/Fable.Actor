# One actor API

Choose how to write an actor's behavior through `Actor`. The factory is an
erased static F# type; it groups overloads without a runtime factory object.
The generic `Actor<Message>` is the message-reference type returned by ordinary
factories and by `Lifetimes.reference` for cooperative capabilities.

| Behavior | Factory | Application code |
| --- | --- | --- |
| Explicit loop | `Actor.spawn body` | Receive messages inside `actor { }` |
| State-only | `Actor.spawn(initialState, update)` | Return the new state |
| State with explicit termination | `Actor.spawn(initialState, update)` | Return `Continue state`, `Stop`, or `StopAbnormal error` |
| State plus commands | `Actor.spawn(initialState, update)` | Return `nextState, command` |

The handler's type selects the overload. Commands do not introduce another
public actor-handle type or require command-specific messaging functions.

```fsharp
open Fable.Actor

let counter =
    Actor.spawn(0, fun count increment -> count + increment)

Actor.send counter 1
```

A command update can return `Loading, Command.ofAsync fetchData () DataLoaded`
or `state, Command.none`. The optional `shouldStop = predicate` on pure state
and command factories finishes the owner before the matching update runs.
See [Commands](commands.md) for the full execution contract.

## References and inboxes

Share an `Actor<Message>` for messaging. It supports `Post` and the standard
`Actor.send`, `cast`, and call APIs. It identifies the same underlying actor
regardless of authoring form. It does not expose a receive operation.

An explicit body receives `ActorInbox<Message>`, which adds `Receive` and
provides `Reference` for sending to itself or attaching ordinary children. A caller cannot steal messages from the actor's inbox through the
reference returned by spawn.

## Lifetime policy

Use `Actor.spawnCooperative` with a loop, state handler, or command
update when shutdown must observe registered cleanup across targets. Construction
returns a `CooperativeActor<Message>` lifetime capability. `Lifetimes.reference`
provides its ordinary message reference; `Lifetimes.stop`, `asyncStop`, `release`
and `releaseWhenExited` govern observation.

A cooperative loop receives `CooperativeInbox<Message>`. It supplies `Receive`,
`Reference` for messaging, and `Owner` for cooperative child attachment:
`Actor.spawnLinked(inbox.Owner, childBody)`.

`Actor.spawnLinked` chooses ordinary or cooperative ownership from its parent
argument's type. Both support loops, state handlers and command updates.
Ordinary factories remain the default. Native `Actor.stop` through a message
reference remains a kill-based escape hatch. See [Lifetimes](lifetimes.md).

## Migration from the earlier API

This branch changes source APIs; it is not a compatibility-only facade.

| Earlier spelling | Unified API |
| --- | --- |
| `Actor.start state handler` | `Actor.spawn(state, handler)` |
| `Actor.spawnLinked parent body` | `Actor.spawnLinked(parent, body)` |
| `Actor.spawnWithToken token body` | `Actor.spawn(body, cancellationToken = token)` |
| `Lifetimes.spawn body` | `Actor.spawnCooperative body` |
| `Lifetimes.spawnLinked parent body` | `Actor.spawnLinked(parent, body)` |
| `(Lifetimes.reference owner).Receive()` inside a cooperative body | `inbox.Receive()`; its capability is `inbox.Owner` |
| `CommandActor.start state update` | `Actor.spawn(state, update)` |
| `Commands.Next.Continue(state, command)` | `state, command` |
| `Commands.Next.Stop` | A factory `shouldStop` predicate, or external shutdown |
| `CommandActor.send/stop/stopAsync` | `Actor.send/stop/asyncStop` |
| `Command.ofOperation factory completed` | `Command.ofActor factory () completed` |
| `open Fable.Actor.Commands` | `open Fable.Actor` |

Typed body annotations change from `Actor<Message>` to `ActorInbox<Message>`
or `CooperativeInbox<Message>`. References remain `Actor<Message>`.
