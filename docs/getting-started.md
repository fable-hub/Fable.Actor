# Getting started

When several parts of an application need to update the same state, an actor
gives one message loop responsibility for it. Callers send messages, and the
actor handles them in sequence. This keeps state changes together and makes
coordination explicit.

Fable.Actor lets you use this model in F# on .NET, JavaScript, Python and BEAM.
Start here with a simple message loop; later guides add replies, recovery and
cooperative shutdown as you need them.

## Install

Add the package to an F# project:

```sh
dotnet add package Fable.Actor --prerelease
```

On .NET, build and run your project normally. For Fable targets, compile it with
Fable and run the generated output in the target runtime. See
[Platform notes](platforms.md) and the repository's
[build instructions](development.md).

## Write a message loop

```fsharp
open Fable.Actor

let greeter =
    Actor.spawn (fun inbox ->
        let rec loop () = actor {
            let! name = inbox.Receive()
            printfn "Hello, %s!" name
            return! loop ()
        }
        loop ())

Actor.send greeter "World"
```

`Actor.spawn` starts an actor and returns its message handle. Its body receives
an `ActorInbox<Message>` with a receive operation; the shared message reference
does not expose `Receive`. `actor { }` describes the body: `let!` awaits an operation,
and `return!` continues the loop. `Actor.send` posts a message without waiting
for it to be processed. `inbox.Post` is the equivalent instance method.

Returning from the body completes the actor. To stop it from elsewhere, await
`Actor.stop` inside another actor expression:

```fsharp
open Fable.Actor.Types

let closeGreeter () = actor {
    let! result = Actor.stop 2000 greeter
    match result with
    | StopResult.Completed exit -> printfn "Stopped: %A" exit
    | StopResult.TimedOut -> printfn "Exit is not yet observed"
}
```

The deadline is in milliseconds and must be positive. On BEAM, ordinary stop
uses force kill and skips finalizers. Choose [Lifetimes](lifetimes.md) when you
need cooperative cleanup across targets.

## Choose an authoring form

The overloads on `Actor.spawn` accept an explicit loop, a state-only update, or
an update returning `nextState, command`. All return the same typed message
reference. See [One actor API](actors.md) and [Commands](commands.md).

## Keep state inside the actor

Pass state through the loop or use `Actor.spawn(initialState, update)`, introduced in the next guide.
Share message handles, not mutable variables: BEAM processes have separate heaps,
so captured mutable values do not form shared state between actors.

Next: [Messaging](messaging.md).
