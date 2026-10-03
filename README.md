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
| `handleChildExit parent supervised exited` | Return a replacement child or `Stopped`             |
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

## Owned shutdown

`Actor.kill` requests shutdown. `Actor.stop cleanupDeadline actor` (or
`stopAsync`) requests it and returns `StopResult.Completed exit` or `TimedOut`.
The deadline must be a positive number of milliseconds and is validated before
shutdown begins. Repeated and concurrent requests share the same lifecycle.

On .NET, JavaScript, and Python, spawn, spawnLinked, and spawnWithToken run with
an owned cancellation token. Idle receives wake, cooperative Async work cancels,
and actor-expression `finally`/`use` cleanup runs once. Completion observes the
workflow and its owned linked descendants exiting. Cleanup failures are returned
as `ActorExit.Failed`; a timeout leaves unfinished work owned and does not force
it to exit. Synchronous user code can block the target scheduler and delay both
cancellation and deadline observation. Finalizers inside separately supplied
Async computations retain that runtime's semantics.

On BEAM, stop monitors native process death after kill. It does not promise
finalizers, graceful cleanup, or completed descendant shutdown at the instant
that the parent dies. An external token requests native kill. A process watcher
releases its registration after death. Use an application shutdown message for
graceful cleanup before native stop, or OTP supervision for shutdown escalation.

`spawnLinked` now establishes parent ownership on every target: parent shutdown,
normal completion, and failure all shut down the child. Native BEAM links also
retain their usual symmetric abnormal-exit behavior: an abnormal or killed child
terminates a parent that is not trapping exits; a trapping parent receives
`ChildExited`. Normal child exits are suppressed by the actor receive protocol.
Emulated links preserve their supervision default: only abnormal child exit
emits `ChildExited`, and it does not automatically terminate the parent. Normal
and cooperative cancelled child exits are silent. The supplied BEAM parent is
now the link endpoint even when spawnLinked is invoked by another process.

This ownership is stronger than a raw BEAM link, which alone does not terminate
children on normal parent exit. Keep replacement children returned by
`handleChildExit` (the rc.12 API) so later shutdown addresses the current
generation. Actor handles on emulated targets are now constructed by spawn APIs
rather than public record literals; Mb and Cts remain available for existing
interop, but lifecycle operations must go through Actor APIs.

## Bounded calls and subscription disposal

`Actor.callResult replyDeadline cancellationToken actor message` returns
`CallResult.Reply value`, `TimedOut`, `TargetTerminated exit`, or `Cancelled`.
`callResultAsync` provides the same protocol from an Async expression.
`callWithTimeout` and the new `callAsyncWithTimeout` return the reply directly;
they raise `TimeoutException`, `TargetTerminatedException`, or cancellation on
failure. Deadlines are positive milliseconds, validated before request delivery.

`call` and `callAsync` retain rc.11's unbounded reply deadline. They now settle
on target shutdown/death and ambient caller cancellation instead of hanging on
those paths. The timeout APIs now deliver requests when the operation runs,
rather than when an emulated call operation is merely constructed.

On emulated targets, shutdown closes admission and settles pending calls at the
stop request, even if noncooperative work has not exited yet. The reported reason
is `Cancelled` in that case; normal workflow completion and crashes report their
observed reason when they close admission. This notification is not confirmation
of completed cleanup: await `stop` for that guarantee. On BEAM, monitors observe
actual process death. A call to an already dead PID reports the native `noproc`
reason, not its historical exit reason. Native stop likewise confirms death but
does not retain a previous exit reason; it reports `Completed Cancelled` for the
issued kill operation.

A call settles once and detaches its timer, target observer/monitor, and caller
registrations. BEAM aliases revoke the reply destination and flush queued
matching replies; subsequent late replies never enter the mailbox. Emulated
reply callbacks become inert and retain no caller continuation or reply value.
Neither timeout nor cancellation retracts a delivered request or proves that
processing was undone. There is no automatic retry. Await each reply before
submitting the next item when request/reply pacing supplies your backpressure;
fire-and-forget sends retain their existing unbounded queue behavior.

A downstream subscription can own a parent actor and create its workers with
`spawnLinked` or `spawnSupervised`. Keep each replacement returned by
`handleChildExit`. Give pending `callResultAsync` operations the subscription's
cancellation token. During disposal, close upstream admission, cancel that token
to settle the outstanding calls, then await `stopAsync` on the parent and check
for timeout or cleanup failure. On BEAM, explicitly await each current worker's
stop as well if disposal must confirm the entire tree is dead; parent death alone
does not confirm completed descendant shutdown. A synchronous IDisposable can
request cancellation; an async disposal path must own the await and report its
result. Notification suppression alone is not worker termination.

Actor-owned Python cancellation registrations, locks, and deadline timers use
small platform adapters for gaps in fable-library 5.19.0. Actor binders check
cancellation before resuming user work, and actor-expression cleanup ignores
duplicate terminal callbacks. Separately supplied Async computations keep their
runtime behavior: in particular, Python Async.Sleep cancellation still leaves
its underlying scheduled callback until its due time. Actor-owned call/stop
**deadline timers** are cancelled physically; arbitrary user Async resources
cannot receive that same guarantee from this library. The upstream work is
captured in [the Fable runtime fix prompt](docs/fable-runtime-fix-prompt.md).
