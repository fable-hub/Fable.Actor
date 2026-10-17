# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Repository-wide contributor and Agent Decision Comment instructions live in
`AGENTS.md`. Read and follow that file before modifying code.

## Project Overview

Fable.Actor is a cross-platform actor library written in F# and compiled via [Fable](https://github.com/fable-compiler/Fable) to BEAM (Erlang), Python, and JavaScript. It provides typed actors with supervision, designed to be the foundation that Rx libraries (like AsyncRx) can build on.

## Build Commands

```sh
just check                       # Type-check library + test projects
just build                       # F# -> Erlang, then rebar3 compile
just format                      # dotnet fantomas src test
just test                        # Shared suite on .NET + Python + JS + BEAM
just test-native                 # .NET only (fastest feedback loop)
just test-beam                   # BEAM only
```

## Tests

One suite, one project (`test/Fable.Actor.Tests.fsproj`), compiled to each target:

```
test/Helpers.fs  ActorTests.fs  SupervisionTests.fs  BuilderTests.fs  Main.fs
```

Assertions come from [Scriptorium](https://github.com/fable-hub/Scriptorium) — Nib for
`assertThat x (isEqualTo y)`, Quill for the runner (`runTests [ ... ]`).

Do **not** split this into a project per target. Fable.Giraffe does, because it has a src project
per target that each test project must reference; Fable.Actor has one library project with
`#if FABLE_COMPILER_BEAM` inside, and `Fable.Beam` arrives transitively through the project
reference. One project compiles cleanly to all four targets from a shared `obj/`, no `--noCache`
needed. Split only if a target ever needs its own package or rebar dependency.

Notes:

- **Quill speaks `Async`, the library speaks `ActorOp`.** `Helpers.toAsync` bridges them: the
  identity on Python/JS/.NET (where `ActorOp = Async`), and on BEAM a `Run` of the CPS chain
  (where `Async` is erased to synchronous callbacks anyway). Tests read
  `testAsync("name", fun _ -> toAsync (actor { ... }))`.
- **`reporter`** (in `Helpers.fs`) is how a test observes state that crosses a process boundary:
  on BEAM a `let mutable` captured by a spawned actor is a copy, so the value has to be published
  with `Actor.cast` and read back with `Actor.call`.
- **BEAM prints `Error in process <0.x.0> with exit value:` reports.** That is the VM logging the
  children the supervision tests deliberately crash — expected, not a failure. The exit code and
  Quill's summary line are what matter.
- The BEAM run is self-contained: Fable compiles the suite *and* the `Fable.Actor` sources it
  references into `build/tests-beam`, and generates the `rebar.config` for it.

## Architecture

One F# project: `src/Fable.Actor/`

### Core Files

1. **Types.fs** — `Actor<'Msg>`, `Next<'State>`, `ReplyChannel<'Reply>`, `ChildExited`
2. **Platform.fs** — `IActorPlatform` erased interface, `[<ImportAll("fable_actor_platform")>]`
3. **Actor.fs** — `actor { }` CE, `spawn`, `spawnLinked`, `start`, `send`, `call`, `receive`, `kill`, `trapExits`

### Core Types

- **Actor<'Msg>**: Typed wrapper around a platform-specific process identifier
- **ActorOp<'T>**: CPS-based computation — blocking receive on BEAM, async/await on Python, promise on JS
- **Next<'State>**: `Continue of 'State` | `Stop` — actor handler return type
- **ReplyChannel<'Reply>**: Callback for synchronous request-response via `call`
- **ChildExited**: Notification when a linked child actor dies

### Platform Interface

Each target provides a native `fable_actor_platform` module implementing `IActorPlatform`:
- BEAM: `fable_actor_platform.erl` (processes, mailbox, selective receive)
- Python: `fable_actor_platform.py` (asyncio tasks)
- JS: TBD

### Design Principles

- **Actor is the only abstraction** — no Observable, Observer, or Rx types
- **`actor { }` CE is the composition mechanism** — maps to platform concurrency (BEAM process, asyncio task, JS promise)
- **Supervision via links** — `spawnLinked` + `trapExits` delivers EXIT signals as messages
- **Rx composition lives elsewhere** — AsyncRx uses `actor { }` instead of `MailboxProcessor`

## Dependencies

- .NET SDK 10+
- Fable 5.22 (local tool, see `.config/dotnet-tools.json`) — 5.11 emits Fable package
  sub-namespaces into nested `Sinks/src/` directories that rebar3 never compiles, which breaks
  Scriptorium.Parchment on BEAM
- Fable.Core 5.0.0 (library) / 5.2.0 (test projects, required by Scriptorium)
- fable-library 5.22+ (Python target)
- Scriptorium.Quill 0.5.1 + Scriptorium.Nib 0.4.1 (test projects only)
