# Fable.Actor

> **Warning: Experimental / Work in Progress**

Fable.Actor is a cross-platform actor library for F#, compiled via [Fable](https://github.com/fable-compiler/Fable) to BEAM (Erlang), Python, and JavaScript. It's a `MailboxProcessor` replacement that works across Fable targets, with BEAM-native supervision via process links.

**Key difference from MailboxProcessor:** actors do not assume shared memory. On BEAM, each actor runs in an isolated process — captured closures and mutable globals are copied, not shared. Code that relies on closing over mutable variables or sharing state through module-level references will not work correctly on BEAM. All communication must go through message passing (`send`/`receive`/`call`).

## Build

Requires .NET SDK 10+ and the [Fable](https://github.com/fable-compiler/Fable) compiler.

```sh
just check    # Type-check F# with dotnet build
just build    # Compile F# to Erlang via Fable
just format   # Format source with Fantomas
```

## Test

One behavioral suite in `test/` runs on every target — the same project, compiled by Fable to each
language. Tests are written with [Scriptorium](https://github.com/fable-hub/Scriptorium) — Quill for
the runner, Nib for assertions.

```sh
just test-native   # Run on .NET (MailboxProcessor implementation)
just test-python   # Compile to Python via Fable, then run
just test-js       # Compile to JavaScript via Fable, then run under Node
just test-beam     # Compile to Erlang via Fable, then run on the BEAM VM
just test          # All four
```

## Quick Start

### Stateful Actor

```fsharp
open Fable.Actor.Types
open Fable.Actor

type CounterMsg =
    | Increment
    | GetCount

let counter = start 0 (fun count (msg, rc) ->
    match msg with
    | Increment -> Continue (count + 1)
    | GetCount ->
        rc.Reply count
        Continue count)

cast counter Increment
cast counter Increment
let! count = call counter GetCount
// count = 2
```

### Actor with Computation Expression

The `actor { }` CE maps to each platform's concurrency primitive — `MailboxProcessor` on .NET/Python/JS, CPS-based blocking receive on BEAM.

```fsharp
open Fable.Actor

let greeter = spawn (fun inbox ->
    let rec loop () = actor {
        let! msg = inbox.Receive()
        printfn "Hello, %s!" msg
        return! loop ()
    }
    loop ())

send greeter "World"
```

#### `spawn` vs `actor`

If you know `MailboxProcessor`, the mapping is one-to-one:

```fsharp
MailboxProcessor.Start (fun inbox -> async { ... })   // MailboxProcessor
spawn                  (fun inbox -> actor { ... })   // Fable.Actor
```

- **`spawn`** launches the actor and returns its handle: `MailboxProcessor` on .NET/Python/JS, a native process on BEAM.
- **`actor { }`** describes its body. Operations use `Async` on .NET/Python/JS, with actor cancellation and cleanup guards; BEAM uses a CPS computation with native blocking receive.

### Supervision

`spawnSupervised` creates a child actor with a supervision strategy. When the child crashes, the strategy decides what to do: `Restart`, `Stop`, or `Escalate`.

```fsharp
let supervisor = spawn (fun inbox ->
    trapExits ()

    let child =
        spawnSupervised inbox
            (OneForOne (fun ex ->
                match ex with
                | :? System.TimeoutException -> Directive.Restart
                | _ -> Directive.Stop))
            (fun childInbox ->
                let rec loop () = actor {
                    let! msg = childInbox.Receive()
                    // process msg... might crash
                    return! loop ()
                }
                loop ())

    // Send messages directly to the child
    send child.Actor "work"

    let rec loop currentChild = actor {
        let! msg = inbox.Receive()
        match tryAsChildExited msg with
        | Some exited ->
            match handleChildExit inbox currentChild exited with
            | ChildExitResult.Restarted replacement ->
                return! loop replacement
            | ChildExitResult.Stopped ->
                printfn "Child stopped permanently"
        | None -> return! loop currentChild
    }
    loop child)
```

For lower-level control, `spawnLinked` + `trapExits` gives you raw EXIT signals without automatic restart.

### Timers

```fsharp
let ticker = start 0 (fun count (msg, _rc) ->
    match msg with
    | "tick" ->
        printfn "tick %d" count
        Continue (count + 1)
    | _ -> Continue count)

schedule 1000 (fun () -> cast ticker "tick") |> ignore
```

## Architecture

```text
src/Fable.Actor/
  Types.fs      — Messages, supervision, call and shutdown results
  Platform.fs   — Native BEAM protocol primitives and Python runtime adapters
  Lifecycle.fs  — Emulated actor ownership and pending-operation settlement
  Actor.fs      — Actor computation expression and public APIs
```

### Platform Strategy

| Platform |        Actor wraps         |     Concurrency model      |
| -------- | -------------------------- | -------------------------- |
| .NET     | `MailboxProcessor`         | Async + threads            |
| Python   | `MailboxProcessor` (Fable) | asyncio                    |
| JS       | `MailboxProcessor` (Fable) | Promises                   |
| BEAM     | Native process             | Erlang processes + mailbox |

Non-BEAM actors use `MailboxProcessor` with an owned cooperative lifetime.
BEAM actors use native processes, links, and monitors.

### API

| Function                                   | Description                                          |
| ------------------------------------------ | ---------------------------------------------------- |
| `spawn body`                               | Spawn an actor: `spawn (fun inbox -> actor { ... })` |
| `spawnWithToken token body`                | Spawn with an external cancellation token            |
| `spawnLinked parent body`                  | Spawn a linked child actor (EXIT on crash)           |
| `spawnSupervised parent strategy body`     | Spawn a child with supervision (auto-restart)        |
| `handleChildExit parent supervised exited` | Return a replacement child or `Stopped`              |
| `tryAsChildExited msg`                     | Check if a message is a `ChildExited` notification   |
| `start state handler`                      | Stateful actor with message handler loop             |
| `send actor msg`                           | Fire-and-forget message send                         |
| `cast actor msg`                           | Fire-and-forget to a call-capable actor              |
| `call actor msg`                           | Async request-response (returns `ActorOp<'Reply>`)   |
| `callAsync actor msg`                      | Request-response from an Async expression            |
| `callResult ms token actor msg`            | Bounded call with an explicit result                 |
| `callResultAsync ms token actor msg`       | Bounded result from an Async expression              |
| `callWithTimeout ms actor msg`             | Like `call` but raises `TimeoutException` on expiry  |
| `callAsyncWithTimeout ms actor msg`        | Bounded reply from an Async expression               |
| `kill actor`                               | Request actor shutdown                               |
| `stop ms actor` / `stopAsync ms actor`     | Request shutdown and await observed exit             |
| `trapExits ()`                             | Enable supervision (EXIT signals become messages)    |
| `schedule ms callback`                     | Schedule a timer callback                            |
| `cancelTimer timer`                        | Cancel a scheduled timer                             |

### Shutdown and bounded calls

Deadlines are positive milliseconds, validated before sending a request or
starting shutdown.

`stop`/`stopAsync` return `Completed exit` or `TimedOut`; cleanup failures appear
as `ActorExit.Failed`. On .NET, JS, and Python, cancellation wakes idle receives
and stops cooperative work. Completion observes workflow, actor-expression
cleanup, and owned-child exit. Repeated stops share cleanup; a timeout leaves
unfinished work owned. On BEAM, stop confirms native process death and skips
finalizers. Await each current worker if disposal must confirm the whole tree
has exited.

Linked children stop on any parent exit, including normal completion. Child
crashes notify emulated parents; BEAM retains native link/trap-exit behavior.
Keep replacement children returned by `handleChildExit`.

`callResult`/`callResultAsync` return `Reply`, `TimedOut`, `TargetTerminated`, or
`Cancelled`. Existing `call`/`callAsync` deadlines remain unbounded, but target
termination and caller cancellation now settle their waits. Calls settle once
and ignore late replies. Timeout and cancellation do not undo delivered work;
requests are never automatically retried. Await replies to preserve backpressure.

For subscription disposal, close upstream admission, cancel the token used by
pending calls, then await stop and inspect its result. Emulated calls settle
when shutdown closes admission; that notification alone does not confirm exit.

**Compatibility:** emulated handles now come from spawn APIs rather than record
literals. Timeout calls deliver when run. Arbitrary supplied Async keeps its
runtime semantics: Python 5.19 Async.Sleep can retain cancelled callbacks until
their deadline. Source `TODO(upstream)` comments identify removable adapters.

### Design Principles

- **Actor is the only abstraction** — no Observable, Observer, or Rx types
- **No shared memory** — actors communicate only via messages (critical for BEAM)
- **`actor { }` CE is the composition mechanism** — `async { }` on non-BEAM, CPS on BEAM
- **MailboxProcessor-compatible** — same `inbox.Receive()` / `actor.Post()` API
- **Supervision via links** — `spawnLinked` + `trapExits` for fault tolerance
- **Rx composition lives elsewhere** — use [AsyncRx](https://github.com/dbrattli/AsyncRx) with `actor { }` instead of `MailboxProcessor`

## Why?

`MailboxProcessor` assumes shared memory — closures can capture mutable state, and multiple agents can reference the same objects. On BEAM, each actor is an isolated process with its own heap, so shared mutable references silently break. Fable.Actor provides a clean actor abstraction where all communication goes through message passing (`send`/`receive`/`call`), making it safe to compile to native processes on BEAM while also working on Python and .NET.

## Examples

### Timeflies

The classic Rx "time flies like an arrow" demo — each letter follows your mouse with an increasing delay, creating a trailing snake effect. One actor per letter, a distributor fans out mouse events.

| Target |             Run             |           UI            |
| ------ | --------------------------- | ----------------------- |
| BEAM   | `just run-timeflies`        | Cowboy WebSocket server |
| Python | `just run-timeflies-python` | tkinter                 |
| JS     | `just run-timeflies-js`     | React (Feliz) + Vite    |

## License

MIT

## Related Projects

- [FSharp.Control.AsyncRx](https://github.com/dbrattli/AsyncRx) — Async Reactive Extensions for F#
- [Fable](https://github.com/fable-compiler/Fable) — F# to JS/Python/BEAM compiler
- [Fable.Beam](https://github.com/fable-compiler/Fable.Beam) — F# bindings (FFI) for BEAM/Erlang
