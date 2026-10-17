# Repository Guidelines

Guidance for humans and coding agents working in this repository. Read the
Agent Decision Comment rules before modifying code; they are enforced in review
before the code itself is reviewed.

## Project Overview

Fable.Actor is a cross-platform actor library written in F# and compiled via
[Fable](https://github.com/fable-compiler/Fable) to .NET, JavaScript, Python and
BEAM (Erlang). It provides typed actors with request/reply, linked-child
supervision and opt-in cooperative lifetimes, and is designed as the foundation
that Rx libraries (like AsyncRx) can build on.

## Build Commands

```sh
just check                       # Type-check library + test projects
just build                       # F# -> Erlang, then rebar3 compile
just format                      # Format every hand-written F# directory
just format-check                # The formatting gate CI runs
just test                        # Shared suite on .NET + Python + JS + BEAM
just test-native                 # .NET only (fastest feedback loop)
just test-beam                   # BEAM only
just build-interop               # Compile the native Erlang interop example
just test-command-package        # BEAM command/lifetime regression suite
just build-timeflies             # BEAM Timeflies example (library included)
just build-timeflies-python      # Python Timeflies example
just build-timeflies-js          # JavaScript Timeflies example
```

CI enforces `just format-check`, a warning-free library build
(`dotnet build src/Fable.Actor -warnaserror`), the suite on all four targets,
`just test-command-package` and a compile of every example. Run those locally
before pushing.

## Agent Decision Comments

This repository uses Agent Decision Comments (ADCs).
Follow specification 0.1.0 at <https://github.com/dbrattli/adc>, currently
pinned to commit `51310dbb60ba960890f431fe06ed701873eadb8b` because no upstream release or
tag exists yet.

ADCs are source-level comments using four directives:

- `decision:` records a deliberate choice and its reason.
- `invariant:` records a falsifiable property the code must preserve.
- `assumption:` records an external belief the code does not guarantee.
- `tradeoff:` records a cost accepted for a concrete benefit.

Place ADCs in the nearest comment or documentation comment attached to the code
they govern. Use one directive per line, present tense, and describe rationale
that is not already evident from the implementation, types, or tests. In
documentation comments, write normal API prose first and leave a blank line
before the directives. Do not add ADCs mechanically to trivial code.

Before modifying code, read all active ADCs in the affected scope.
Preserve them or update them explicitly.
Add comments for non-obvious decisions, invariants, assumptions, and tradeoffs
introduced by a change.

A change that introduces a non-obvious engineering decision or constraint is
incomplete until its ADCs are present and consistent with the implementation.
Reviewers evaluate the comments before reviewing the code.

In `justfile` recipes the ADC block goes above the recipe's doc comment, because
`just --list` shows only the last comment line before a recipe.

## Tests

One suite, one project (`test/Fable.Actor.Tests.fsproj`), compiled to each target:

```
test/Helpers.fs            ActorTests.fs          SupervisionTests.fs
BuilderTests.fs            LifecycleTests.fs      CallLivenessTests.fs
CooperativeTests.fs        CommandObservations.fs CommandTestSupport.fs
CommandTests.fs            UnifiedApiTests.fs     TimefliesExampleTests.fs
Main.fs
```

Assertions come from [Scriptorium](https://github.com/fable-hub/Scriptorium) — Nib for
`assertThat x (isEqualTo y)`, Quill for the runner (`runTests [ ... ]`).

Do **not** split this into a project per target. Fable.Giraffe does, because it has a src project
per target that each test project must reference; Fable.Actor has one library project with
`#if FABLE_COMPILER_BEAM` inside, and `Fable.Beam` arrives transitively through the project
reference. One project compiles cleanly to all four targets from a shared `obj/`, no `--noCache`
needed. Split only if a target ever needs its own package or rebar dependency.

The rule bounds the library's API contract, not the number of subjects the suite
may exercise: `TimefliesExampleTests.fs` drives the shared example core through
its own `sendFn` sink, so the demo ships no test seam. Keep example coverage in
this project rather than adding a second suite.

Notes:

- **Quill speaks `Async`, the library speaks `ActorOp`.** `Helpers.toAsync` bridges them: the
  identity on Python/JS/.NET (where `ActorOp = Async`), and on BEAM a `Run` of the CPS chain
  (where `Async` is erased to synchronous callbacks anyway). Tests read
  `testAsync("name", fun _ -> toAsync (actor { ... }))`, or `case "name" operation` from
  `CommandTestSupport.fs`. Quill's default per-test timeout is 5s; pass `timeout ms` for the
  timing-sensitive example cases.
- **`reporter`** (in `Helpers.fs`) is how a test observes state that crosses a process boundary:
  on BEAM a `let mutable` captured by a spawned actor is a copy, so the value has to be published
  with `Actor.cast` and read back with `Actor.call`. `CommandObservations.journal` is the
  barrier-style variant: `record` appends, and a `Take n` call waits until n values arrived.
- **BEAM prints `Error in process <0.x.0> with exit value:` reports.** That is the VM logging the
  children the supervision tests deliberately crash — expected, not a failure. The exit code and
  Quill's summary line are what matter.
- The BEAM run is self-contained: Fable compiles the suite *and* the `Fable.Actor` sources it
  references into `build/tests-beam`, and generates the `rebar.config` for it.
- `test/native/beam_async_finalizer_probe.erl` is a hand-written probe of the compiler-bundled BEAM
  Async runtime, kept because the Actor CE owns its finalizers separately from plain `Async`.
  `docs/maintainers/lifetimes.md` runs it.

## Architecture

One F# library project: `src/Fable.Actor/`, packed as Fable source in the NuGet
package. Modules in compile order:

| Module | Responsibility |
| --- | --- |
| `Types.fs` | `ReplyChannel<'Reply>`, `TimerHandle`, `Next<'State>`, `ChildExited`, `ActorExit`, `StopResult`, `CallResult<'Reply>`, `Directive`, `Strategy` — no platform code |
| `Platform.fs` | BEAM wire protocol (tagged envelopes, selective receive, aliases, timers) and the managed-target Async context bridges |
| `Lifecycle.fs` | Managed-target lifetime primitives delegated to Fable's runtime, plus `Pending` settlement |
| `ExecutionNative.fs` | BEAM token propagation and finalization for actor expressions |
| `Actor.fs` | `Actor<'Msg>`, `ActorInbox<'Msg>`, `actor { }`, `ActorOp`, spawn/start, send/call, links, supervision |
| `LifetimesNative.fs` | BEAM cooperative lifecycle: ownership, monitored death, retained exit |
| `Lifetimes.fs` | `CooperativeActor<'Msg>` capability and its stop/release protocol |
| `Commands.fs` | `Command<'Msg>` — inert descriptions of finite owned work |
| `CommandRuntime.fs` | State-plus-command execution over the shared runtime |
| `ActorApi.fs` | The erased `Actor` factory/messaging surface every authoring form goes through |

### Core types

- **`Actor<'Msg>`** — typed message reference carrying its own send encoding; not a process handle
- **`ActorInbox<'Msg>`** — the receive side, handed only to an explicit body
- **`ActorOp<'T>`** — CPS chain on BEAM (`{ Run }`), `Async<'T>` on the managed targets
- **`Next<'State>`** — `Continue of 'State` | `Stop` | `StopAbnormal of exn`
- **`ReplyChannel<'Reply>`** — callback for request/response through `call`
- **`CallResult<'Reply>` / `StopResult`** — explicit reply, timeout, cancellation and termination outcomes
- **`ChildExited`** — linked-child failure delivered as an application message

### Platform split

There is no per-target runtime module to implement. One project carries both
runtimes behind `#if FABLE_COMPILER_BEAM`:

- **BEAM** — native Erlang processes and mailboxes. `Platform.fs`,
  `ExecutionNative.fs` and `LifetimesNative.fs` reach OTP through `Fable.Beam`
  typed bindings, and through bound-Erlang `emitErlExpr` where selective receive,
  aliases or monitors need variables F# cannot express.
- **.NET / JavaScript / Python** — Fable `MailboxProcessor` and `Async`, with the
  actor-level lifecycle rules added on top.

`apps/`, `build/` and every `fable_modules/` tree are generated; nothing there is
hand-written. Checked-in Erlang is limited to `examples/timeflies-beam/src/erl/`,
`examples/interop/erl/` and the probe above.

Never compile with Fable against a project whose sources other projects reference
without `--outDir`: the referenced project's JS lands next to its own `.fs` files
as `*.fs.js` sidecars. `just build-timeflies-js` is the historical example.

### Design principles

- **Actor is the only abstraction** — no Observable, Observer, or Rx types
- **`actor { }` CE is the composition mechanism** — maps to platform concurrency (BEAM process, asyncio task, JS promise)
- **Supervision via links** — `spawnLinked` + `trapExits` delivers EXIT signals as messages
- **Ordinary and cooperative shutdown differ on purpose** — ordinary BEAM `stop` force-kills and
  proves process death only; `Lifetimes` is the opt-in cooperative contract
- **Rx composition lives elsewhere** — AsyncRx uses `actor { }` instead of `MailboxProcessor`

## Examples

`examples/timeflies/src/Timeflies.fs` is the shared demo core (a distributor
fanning out to one linked actor per letter) that all three UI targets reference;
the suite executes it. `examples/interop` documents the BEAM wire protocol and is
compile-only, since the demo needs a hand-written Erlang peer.

## Dependencies

- .NET SDK 10+, `just`, Node.js, `uv`, and `mise` for the declared OTP/Rebar
- Fable 5.22 (local tool, see `.config/dotnet-tools.json`) — 5.11 emits Fable package
  sub-namespaces into nested `Sinks/src/` directories that rebar3 never compiles, which breaks
  Scriptorium.Parchment on BEAM
- Fable.Core 5.0.0 + Fable.Beam 5.2.0 (library) / Fable.Core 5.2.0 (test projects, required by Scriptorium)
- fable-library 5.22+ (Python target)
- Scriptorium.Quill 0.5.1 + Scriptorium.Nib 0.4.1 (test projects only)
- Fantomas 8.0.10, Erlang/OTP 27.3.4.9 and Rebar 3.27.0 via `mise.toml`
