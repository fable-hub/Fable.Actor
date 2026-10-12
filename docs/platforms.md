# Platform notes

Use `actor { }` for portable bodies and communicate through message handles.

| Target | Actor implementation | Ordinary `Actor.stop` |
| --- | --- | --- |
| .NET | `MailboxProcessor` and Async | Requests cancellation and observes body/owned-child exit |
| JavaScript | Fable `MailboxProcessor` and Async | Requests cancellation and observes body/owned-child exit |
| Python | Fable `MailboxProcessor` and Async | Requests cancellation and observes body/owned-child exit |
| BEAM | Native Erlang process and mailbox | Force-kills and observes native process death; skips finalizers |

## State and operations

BEAM processes have separate heaps. Captured values are copied, so mutable
variables or global references are not shared actor state. Keep state in the
actor loop and send messages to change it.

`ActorOp<Value>` is Async on managed targets and a synchronous continuation-based
operation on BEAM. `actor { }` provides the shared syntax. Sequential Async
operations can be awaited inside it, but arbitrary parallel Async work is not
automatically registered as an owned actor child.

Host code using `async { }` can call the explicit Async bridges, such as
`Actor.callResultAsync`, `Actor.stopAsync` and `Lifetimes.stopAsync`. These bridges
do not change the target's shutdown contract. On BEAM, sequential Async executes
in the calling process.

`Actor.spawnWithToken token body` ties an ordinary actor to external cancellation.
On .NET, JavaScript and Python it requests cooperative cancellation; on BEAM it
requests native kill. For native integration, `Actor.pid` exposes a target-specific
handle rather than a portable application value.

## Cleanup

[Lifetimes](lifetimes.md) supplies cooperative stop on every target with the same
builder. Cancellation is cooperative: a synchronous foreign call can continue
after a deadline. Adapters must acknowledge their actual resource cleanup before
the owner can safely reuse that resource.

Put guaranteed resource scopes in the actor expression. Published Fable 5.20.1
BEAM plain Async can repeat a throwing `finally` and fail to dispose a `use`
resource. This concerns scopes inside nested `async { }`, not the actor builder's
own `use`/`finally`. [Maintainer notes](maintainers/lifetimes.md) provide executable
probes and the remaining upstream work.

## Runtime versions

The repository pins Fable through [the tool manifest](../.config/dotnet-tools.json),
OTP/Rebar through [mise.toml](../mise.toml), and the Python runtime dependency
through [pyproject.toml](../pyproject.toml) and [uv.lock](../uv.lock). Use those
versions when reproducing repository behavior.
