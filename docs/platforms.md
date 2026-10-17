# Platform notes

Use `actor { }` for portable bodies and communicate through message handles.

| Target | Actor implementation | Ordinary `Actor.stop` |
| --- | --- | --- |
| .NET | `MailboxProcessor` and Async | Requests cancellation and observes body/owned-child exit |
| JavaScript | Fable `MailboxProcessor` and Async | Requests cancellation and observes body/owned-child exit |
| Python | Fable `MailboxProcessor` and Async | Requests cancellation and observes body/owned-child exit |
| BEAM | Native Erlang process and mailbox | Force-kills and observes native process death; skips finalizers |

For an ordinary BEAM actor, `Actor.stop` returns `Completed Cancelled` when it
observes process death. This does not prove that the body was cancelled or ran
its finalizers. It also does not preserve a `Normal` or `Failed` exit that
happened before the stop: the ordinary handle retains no terminal result.
Use [Lifetimes](lifetimes.md) for cooperative cancellation and retained exit
outcomes. Its stop operation can time out while noncooperative work remains
blocked.

## State and operations

BEAM processes have separate heaps. Captured values are copied, so mutable
variables or global references are not shared actor state. Keep state in the
actor loop and send messages to change it.

`ActorOp<Value>` is Async on managed targets and a synchronous continuation-based
operation on BEAM. `actor { }` provides the shared syntax. Sequential Async
operations can be awaited inside it, but arbitrary parallel Async work is not
automatically registered as an owned actor child.

Host code using `async { }` can call the explicit Async bridges, such as
`Actor.asyncCallResult`, `Actor.asyncStop` and `Lifetimes.asyncStop`. These bridges
do not change the target's shutdown contract. On BEAM, sequential Async executes
in the calling process.

`Actor.spawn(body, cancellationToken = token)` ties an ordinary actor to external cancellation.
On .NET, JavaScript and Python it requests cooperative cancellation; on BEAM it
requests native kill. For native integration, `Actor.pid` exposes a target-specific
handle rather than a portable application value.
On BEAM, `Actor.fromPid pid` wraps a local or remote `Pid<Message>` as a
send-only `Actor<Message>` reference. The process must use Fable.Actor's message
protocol. Rewrapping does not acquire lifetime ownership or retain its exit.

## Cleanup

[Lifetimes](lifetimes.md) supplies cooperative stop on every target with the same
builder. Cancellation is cooperative: a synchronous foreign call can continue
after a deadline. Adapters must acknowledge their actual resource cleanup before
the owner can safely reuse that resource.

Fable 5.21.0 fixes BEAM plain Async disposal and once-only finalization, including
scopes inside nested `async { }`. Plain Async follows .NET: cancellation remains
terminal even if its finalizer throws. The actor builder additionally reports
cleanup defects as failed exits. [Maintainer notes](maintainers/lifetimes.md)
provide executable runtime probes.

## Runtime versions

The repository pins Fable through [the tool manifest](../.config/dotnet-tools.json),
OTP/Rebar through [mise.toml](../mise.toml), and the Python runtime dependency
through [pyproject.toml](../pyproject.toml) and [uv.lock](../uv.lock). Use those
versions when reproducing repository behavior.
BEAM compilation requires Fable 5.21.0 or later for typed cancellation and the
named-token `Async.RunSynchronously` bridge.
The JavaScript and Python actor runtimes require Fable 5.22.0 or later so an idle
mailbox receive settles directly when its actor token is cancelled.
