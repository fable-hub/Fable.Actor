# Optional actor commands

The experimental `Fable.Actor.Commands` namespace supplies a third authoring
form alongside an explicit `Actor.spawn` loop and the state-only `Actor.start`
handler. All three use the same actor runtime. Existing APIs keep their names,
types, and behavior; an application need not reference command types.

## Quick start

```fsharp
open System.Threading
open Fable.Actor
open Fable.Actor.Types
open Fable.Actor.Commands

type LabelState = Idle | Loaded of CallResult<string>
type LabelMessage = Refresh | LabelLoaded of CallResult<string> | Close

let startLabel service =
    let update state message =
        match message with
        | Refresh ->
            let command =
                Command.ofOperation
                    (fun () -> Actor.callResult 2000 CancellationToken.None service ())
                    LabelLoaded
            Fable.Actor.Commands.Next.Continue(state, command)
        | LabelLoaded result ->
            Fable.Actor.Commands.Next.Continue(Loaded result, Command.none)
        | Close -> Fable.Actor.Commands.Next.Stop

    CommandActor.start Idle update
```

`CommandActor.send` accepts the application's message type. Its opaque handle
keeps the private mailbox envelope and child-exit notifications outside that
type. Use `CommandActor.startLinked` for a supervising `Actor<obj>`, and
`CommandActor.stop` or `stopAsync` for bounded shutdown observation.


## Description and execution

`Command<Message>` is an opaque, inert description of finite work. It contains
operation factories, not running tasks or actor handles.

| Operation | Contract |
| --- | --- |
| `Command.none` | Start no new work; existing work continues |
| `Command.ofOperation operation completed` | Run the factory in a linked worker and map its value to one owner message |
| `Command.batch commands` | Start the described operations independently; completion order is unspecified |
| `Command.map mapping command` | Map output messages inside their workers; constructing the mapped command remains inert |

The factory returns `ActorOp<Value>`. Domain errors in `Result` and transport
outcomes in `CallResult` are data: the completion function chooses their
application message. Commands neither catch defects as recoverable errors nor
retry requests. Factory, operation, and completion-mapping defects fail the
worker and then its owner through the existing linked failure path.

`CommandActor.start initial update` starts one owner mailbox. Each synchronous
update returns `Next.Continue(state, command)` or `Next.Stop`. The update
finishes successfully before command execution; a defective update starts
none of its returned work. Worker messages reenter the same serialized update
loop. Several commands may be active at once, including commands from separate
updates. The initial implementation supplies no admission or resource bound.

## Message and supervision boundaries

`CommandActor<Message>` is a typed handle for this optional host. Its private
runtime mailbox holds application envelopes and child-exit notifications, so
a notification cannot be mistaken for an application union case. This avoids
requiring an application to accept `obj` messages. The handle is not a raw
`Actor<Message>`; send through `CommandActor.send`. For response conversations,
include a typed reply channel or callback in the application's message union.

`CommandActor.startLinked parent initial update` attaches the owner to a
supervising `Actor<obj>` through `Actor.spawnLinked`. The parent owns the
restart policy. A restart produces a fresh owner and handle; the command layer
does not implement stable addressing or automatically retry failed work.

## Shutdown and cancellation

`CommandActor.stop` and `stopAsync` delegate to the existing actor operations.
A positive deadline is a precondition. `TimedOut` limits observation and does
not prove termination. Parent completion and external stop own linked workers
through the runtime's existing ownership mechanism.

| Host | Shutdown evidence |
| --- | --- |
| .NET, JavaScript, Python | Cooperative cancellation; observed owner exit includes owned-worker exit and actor-expression cleanup |
| Native BEAM | Native process death; kill skips finalizers and owner death is not a descendant-cleanup acknowledgement |

No command-specific lifecycle runtime or cooperative BEAM capability is added.
Arbitrary noncooperative host operations retain their runtime limitations.
Native ownership watchers stop workers after normal owner completion; callers
requiring a whole-tree acknowledgement still need stronger runtime support.

An operation may use an explicit cancellation token, as `Actor.callResult`
does. Its cancelled outcome remains a message while the owner is alive.
Cancelling a request does not recall a message delivered to an independent
service. Owner shutdown ends its workers rather than delivering a cancellation
message to an owner that is exiting.

Keyed cancellation, replacement, latest-result selection, backpressure,
subscriptions, and a single-active-command policy are outside this layer.
Correlate requests in the domain protocol where those policies are needed.
Direct actor loops and state-only handlers remain the escape hatches.

## Evidence

The shared `test/CommandTests.fs` catalog runs with the existing tests on .NET,
JavaScript, Python, and BEAM. It covers inert construction, mapping and batches,
success and domain errors, responsive concurrent work, request timeout,
cancellation and service termination, linked defects, defective updates, normal
owner completion, and external owner stop. Native worker death is observed
separately; native finalizers are deliberately not claimed.

`just test-command-package` packs the library and runs a fresh external consumer
on .NET and JavaScript. The consumer restores into an independent package cache
without a source-project reference; it tests mapped and batched completions,
accepted state, and asynchronous host shutdown.
Two negative builds also reject a command/update message mismatch and sending
the wrong message type through a command-owner handle.
