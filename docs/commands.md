# Actor-owned commands

Use the command-handler overload of `Actor.spawn` when each message decides a
new state and finite work. Explicit receive loops and state-only handlers use
that same factory and share the same `Actor<Message>` message-reference type.

## State and work

An update returns a tuple: **the new state, and work to start**. It does not wait
for the work to finish. Completion messages go through the same serialized update.

```fsharp
open Fable.Actor
open Fable.Actor.Types

type LabelState = Idle | Loading | Loaded of Result<string, string>
type LabelMessage = Refresh | LabelLoaded of Result<string, string> | Close

let startLabel (fetchLabel: unit -> Async<Result<string, string>>) =
    let update state message =
        match message with
        | Refresh -> Loading, Command.ofAsync fetchLabel () LabelLoaded
        | LabelLoaded result -> Loaded result, Command.none
        | Close -> state, Command.none

    Actor.spawn(
        Idle,
        update,
        shouldStop = (fun _ message -> match message with Close -> true | _ -> false))
```

Send with `Actor.send label Refresh`. The optional `shouldStop` predicate sees
accepted state and the incoming message **before update**. A matching message
finishes the owner normally without calling update or starting new work.
External shutdown uses `Actor.stop` or `Actor.asyncStop`.

## Command descriptions

`Command<Message>` is an opaque, inert description of finite work. Constructing,
mapping, and batching commands never runs their factories.

| Constructor | Contract |
| --- | --- |
| `Command.none` | Start no new work; existing work continues |
| `Command.ofAsync operation argument completed` | Run an Async workflow through the actor builder and map its value to one owner message |
| `Command.ofActor operation argument completed` | Run an actor operation and map its value to one owner message |
| `Command.ofMsg message` | Queue one message through the owned-work path after the current update |
| `Command.batch commands` | Start independent operations; completion order is unspecified |
| `Command.map mapping command` | Transform completion messages inside their workers; construction remains inert |

Factories and completion functions run inside linked workers. Domain errors in
`Result` and transport outcomes in `CallResult` remain data. Factory, operation,
and mapping defects fail the worker and then its owner through the linked
failure path. Commands do not automatically retry or turn defects into messages.

A successful update finishes before any returned work starts. An update that
fails starts none of its returned work. Multiple operations may be active at
once, including operations from separate updates. There is no admission bound,
keyed replacement, throttling, or stale-result rejection; those policies require
an explicit application protocol.

`ofMsg` produces a new input message. It is not an inline call to update and does
not guarantee ordering relative to other workers or independent senders.

## One typed actor reference

The host privately receives application envelopes and child-exit notifications.
Its returned `Actor<Message>` reference wraps application messages automatically.
Use the same `Actor.send`, `cast`, and request/reply APIs as for other actors.
An `Actor<Message>` reference has no `Receive`; loop bodies receive an
`ActorInbox<Message>` separately. Native `Actor.pid` interoperability bypasses
the send adapter and must respect the host's private protocol.

For reply conversations, include a typed `ReplyChannel<Value>` in the message
protocol. The standard tuple protocol, `Actor<Message * ReplyChannel<Value>>`,
works with command handlers as well as state handlers and explicit loops.

`Actor.spawnLinked(parent, initialState, update)` attaches a command owner to an
ordinary parent. The parent chooses the recovery policy. Restart creates a fresh
owner/reference and does not automatically retry work or preserve addressing.

## Cooperative ownership

Choose `Actor.spawnCooperative(initialState, update)` when shutdown must observe
registered worker cleanup across all targets. It returns a
`CooperativeActor<Message>` lifetime capability; share `Lifetimes.reference owner`
for messaging and retain the capability for `Lifetimes.stop` and release.

Every operation becomes a cooperative child registered before its factory runs.
Normal owner completion and external stop cancel active workers and observe
their body/finalizer exit recursively. Worker capabilities relinquish separate
observation automatically; cleanup stays owned by the command owner. Defects
remain retained terminal failures, even when stop races a child notification.

`Actor.spawnLinked(parentCapability, initialState, update)` creates a cooperative
command owner within an existing cooperative tree. Passing an ordinary message
reference selects ordinary ownership; lifetime policy is explicit at construction.

An ordinary command owner retains ordinary runtime semantics: .NET, JavaScript
and Python observe owned cleanup; native BEAM stop confirms process death but
skips finalizers and does not acknowledge descendant cleanup. A cooperative stop
never force-kills on timeout. Keep a timed-out capability to observe actual exit
later. See [Lifetimes](lifetimes.md) for release and retained-outcome rules.

A command completion message is an application outcome, not a whole-tree cleanup
acknowledgement. Opaque synchronous operations, independent services, timers and
other unregistered resources need their own settlement contract. Cancelling a
request does not retract a message already delivered to an independent service.
Recurring subscriptions are outside this finite-command API.

## Validation

The shared suite runs on .NET, JavaScript, Python and BEAM. It covers inert
construction, mapping/batches, request outcomes, linked defects, accepted-state
ordering, termination predicates, shared typed references, cooperative finalizers,
timeout/retry, retained failures and transitive ownership.

`just test-command-package` restores a packed artifact into an independent cache
and checks an external .NET/JavaScript consumer. Negative compiler checks reject
mismatched completion/input types, wrongly typed sends, and receiving through a
public message reference.
