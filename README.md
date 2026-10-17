# Fable.Actor

> Fable.Actor requires Fable v5.22 (or higher).

Fable.Actor is a cross-platform actor library for F# on .NET, JavaScript, Python
and BEAM. It gives explicit message loops, state handlers and handlers that start
finite work the same typed actor reference and messaging API. It also provides
request/reply with explicit timeout and termination outcomes, linked-child
supervision, and optional cooperative lifetimes that observe owned cleanup.

On .NET, JavaScript and Python, `MailboxProcessor` supplies the underlying
mailbox; Fable.Actor adds those actor-level APIs and lifecycle rules around it.
On BEAM, actors run as native Erlang processes. Write portable bodies with
`actor { }`, keep state inside each actor, and communicate through typed messages.
Shutdown details differ by target; see [Platform notes](docs/platforms.md).

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

Start with [Getting started](docs/getting-started.md), then follow the
[documentation guide](docs/README.md):

- [One actor API](docs/actors.md): choose a loop, state handler or command handler.
- [Messaging](docs/messaging.md): state, requests, deadlines and timers.
- [Supervision](docs/supervision.md): linked children and restart policies.
- [Commands](docs/commands.md): optional state handlers with linked finite work.
- [Lifetimes](docs/lifetimes.md): optional cooperative shutdown and owned cleanup,
  using the same builder and NuGet package.
- [Platform notes](docs/platforms.md): portability and runtime limits.
- [Development](docs/development.md): build, test and run the examples.

Use messages rather than shared mutable state: on BEAM, actors have separate
heaps and captured values are copied.

MIT licensed. Built with [Fable](https://github.com/fable-compiler/Fable) and
[Fable.Beam](https://github.com/fable-compiler/Fable.Beam).
