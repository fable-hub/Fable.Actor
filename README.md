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

```sh
just test-native   # Run .NET tests
just test-python   # Compile to Python via Fable, then run
just test-beam     # Compile to Erlang via Fable, then run
just test          # Run .NET + Python tests
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

- **`spawn`** *launches* an actor — it starts a long-running process with a mailbox and returns an `Actor<'Msg>` handle. It is the equivalent of `MailboxProcessor.Start`. On .NET/Python/JS it wraps `MailboxProcessor.Start`; on BEAM it wraps `Erlang.spawn`.
- **`actor { }`** *describes the body* — the receive loop you pass to `spawn`. It is the equivalent of `async { }`. On .NET/Python/JS it **is** `async` (`ActorOp<'T> = Async<'T>`, and every CE member delegates straight to the `async` builder); on BEAM it compiles to a CPS-based blocking receive instead, since the BEAM has no async runtime.

In short: `actor` is to `spawn` what `async` is to `MailboxProcessor.Start`. The only reason `actor` exists rather than reusing `async` is BEAM — on the other three targets it's a transparent passthrough.

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

    let rec loop () = actor {
        let! msg = inbox.Receive()
        match tryAsChildExited msg with
        | Some exited ->
            let restarted = handleChildExit inbox child exited
            if not restarted then
                printfn "Child stopped permanently"
        | None -> ()
        return! loop ()
    }
    loop ())
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
  Types.fs      — ReplyChannel, Next<'State>, ChildExited, Directive, Strategy
  Platform.fs   — BEAM: IActorPlatform + [<ImportAll("fable_actor_platform")>]
                  Non-BEAM: empty (uses MailboxProcessor directly)
  Actor.fs      — actor { }, spawn, spawnLinked, start, send, call, kill, schedule
  erl/          — BEAM platform implementation (native processes)
```

### Platform Strategy

| Platform |        Actor wraps         |     Concurrency model      |
| -------- | -------------------------- | -------------------------- |
| .NET     | `MailboxProcessor`         | Async + threads            |
| Python   | `MailboxProcessor` (Fable) | asyncio                    |
| JS       | `MailboxProcessor` (Fable) | Promises                   |
| BEAM     | Native process             | Erlang processes + mailbox |

On non-BEAM targets, `Actor<'Msg>` is a thin wrapper around `MailboxProcessor<'Msg>`. No platform-specific runtime needed — Fable's built-in `MailboxProcessor` handles everything. On BEAM, actors map to real Erlang processes with native supervision.

### API

|                  Function                  |                     Description                      |
| ------------------------------------------ | ---------------------------------------------------- |
| `spawn body`                               | Spawn an actor: `spawn (fun inbox -> actor { ... })` |
| `spawnLinked parent body`                  | Spawn a linked child actor (EXIT on crash)           |
| `spawnSupervised parent strategy body`     | Spawn a child with supervision (auto-restart)        |
| `handleChildExit parent supervised exited` | Apply strategy to a crashed child                    |
| `tryAsChildExited msg`                     | Check if a message is a `ChildExited` notification   |
| `start state handler`                      | Stateful actor with message handler loop             |
| `send actor msg`                           | Fire-and-forget message send                         |
| `cast actor msg`                           | Fire-and-forget to a call-capable actor              |
| `call actor msg`                           | Async request-response (returns `ActorOp<'Reply>`)   |
| `callWithTimeout ms actor msg`             | Like `call` but raises `TimeoutException` on expiry  |
| `kill actor`                               | Kill an actor immediately                            |
| `trapExits ()`                             | Enable supervision (EXIT signals become messages)    |
| `schedule ms callback`                     | Schedule a timer callback                            |
| `cancelTimer timer`                        | Cancel a scheduled timer                             |

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
