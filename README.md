# Fable.Actor

> Experimental / work in progress.

Fable.Actor is an actor library for F# on .NET, JavaScript, Python and BEAM.
Write message loops with `actor { }`, keep state inside each actor, and communicate
through typed messages. BEAM uses native processes; other targets use
`MailboxProcessor`.

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
