# Cooperative actor lifetimes

The experimental `Cooperative` module provides lifetime authority separately from
an ordinary message reference. It supports observed cooperative cleanup without
changing the native actor force-kill contract.

## Public contract

- `spawn` returns a creator-held capability. `reference` returns the existing
  `Actor<Message>` message handle. Share message handles, retain lifetime authority.
- `spawnLinked parent` attaches ownership before the child can evaluate its body.
  A stopped but unpublished parent admits an already requested child with its
  token cancelled; a terminal parent rejects attachment without running the body.
- `stop deadline` requests cancellation and waits for actual body exit and all
  owned cooperative descendants. `stopAsync` supplies the advanced Async bridge.
  Deadlines must be positive. `TimedOut` means exit is still unobserved; it never
  kills work or asserts termination. A later stop can complete.
- Terminal `Normal`, `Cancelled` and `Failed error` are retained. A stop request
  does not overwrite an already normal or failed body. An original body defect
  takes precedence over descendant cleanup defects. Otherwise the first observed
  descendant defect becomes the terminal failure. An actor finalizer defect is a
  failed body exit, including when unwinding cancellation.
- `release` returns `Running` while work remains owned and `Released` at terminal
  publication. `releaseWhenExited` relinquishes future observation without
  detaching running work. Relinquished capabilities must not be reused.

BEAM allocates one control process per cooperative generation, with monitors for
its body and creating process. Creator death requests cancellation and releases
terminal state after cleanup. A long-lived creator must explicitly release
completed capabilities. Managed targets use the existing `ActorLifetime` and
reclaim unreachable state through garbage collection.

## Ordering and cancellation

The native controller creates a suspended body, then the parent acknowledges
ownership, then startup is authorized. Monitoring the creator before startup
also covers creator death during that handshake. If attachment is rejected, the
new body is started with cancellation requested and its private controller is
released on exit, without evaluating user code.

Actor CE delay, binding, return and loop boundaries checkpoint the owned token.
Receive checks it before selecting a message and wakes on a private cancellation
signal. Bounded, unbounded and typed request waits inherit the cooperative token;
request aliases, monitors and queued duplicate replies are retired on settlement.
Sequential Async adapters run inline with that token. No new Async scheduler is
introduced on BEAM.

A returned `IDisposable` bypasses the immediate value checkpoint so `use`/`use!`
can install `Using` first. Its delayed body checkpoints only after disposal is
installed. `TryFinally` captures the CPS body result before running compensation,
so finalization precedes continuation. Actor compensation runs once with ambient
actor checkpoints and adapter tokens shielded. Exceptions propagate as failures.
BEAM binary exceptions are normalized using the published runtime's `wrap_error`
so `ActorExit.Failed` supports the usual exception message, while exception maps
are retained unchanged.

Body monitor death acknowledges completion of actor finalizers. Each child
publishes only after its own body death and recursively owned cleanup, then
acknowledges its parent. Parent publication requires both body death and an empty
ownership set, regardless of acknowledgement arrival order. Publication clears
waiting observers before notifying them. Retained terminal state is separate
from live work and from any particular stop request.

Stop waiters use revocable aliases and monitor their calling process. Timeout
removes the observer, alias and monitor and flushes queued completion messages;
caller death removes an abandoned observer even if the caller cannot unwind.
No private process is created per stop request.

## Compatibility and limits

Native `Actor.spawn`, links, `kill` and `stop` retain their existing compatibility
contract. Native stop still confirms process death after force kill and cannot
promise finalizers or retained earlier outcomes. Using that escape hatch on a
cooperative actor's message reference also bypasses graceful cleanup.

Opaque synchronous host calls, independently owned services, raw processes and
arbitrary parallel work are outside cooperative ownership. They can outlive a
deadline. Adapters must acknowledge their own resource settlement before
returning. As with existing linked actors, a parent that handles child-failure notifications
uses an object mailbox and checks `Actor.tryAsChildExited` before application
messages. Typed message references can sit above that internal object mailbox.

Sequential Async adapters receive cancellation, but the stronger
resource/finalizer contract belongs to the **actor CE**.

Published BEAM plain Async has a separate defect: a throwing Async compensation
can run twice, including during cancellation. `fable_async_builder:try_finally/2`
catches an exception raised by its terminal compensation and runs compensation
again in its catch path. Actor CE finalizers use a different implementation and
pass once-only tests. Universal once-only cleanup for finalizers inside arbitrary
nested `async { ... }` remains blocked on a published BEAM runtime fix. No Fable
compiler or runtime is modified or substituted here.

The same published runtime's plain Async `Using` supplies an arity-zero
compensation to the arity-one finalizer protocol and never calls disposal.
`run_using/0` in the regression probe reports zero disposals and `badarity`.
This is a second native Async runtime defect, independent of Actor CE `Using`,
which runs disposal and passes late-acquisition regressions.

The executable regression is `test/native/beam_async_finalizer_probe.erl`:

```sh
just test-beam
mkdir -p build/runtime-probe
erlc -o build/runtime-probe test/native/beam_async_finalizer_probe.erl
erl -noshell -pa build/runtime-probe build/tests-beam/_build/default/lib/*/ebin \
  -eval 'beam_async_finalizer_probe:run(), halt().'
# Independently reproduce the plain Async Using defect:
erl -noshell -pa build/runtime-probe build/tests-beam/_build/default/lib/*/ebin \
  -eval 'beam_async_finalizer_probe:run_using(), halt().'
```

On the bundled Fable 5.20.1 runtime it reports **2 invocations** and fails its
one-invocation assertion. This is an intentional remaining upstream regression,
not a passing Actor check or a replacement runtime.

## Validation, 2026-10-12

All compilers are the released `dotnet fable` **5.20.1**. JavaScript and BEAM use
its bundled published runtime. Python uses published `fable-library==5.20.1` and
stable **CPython 3.15.0**, including the lockfile-selected cp315 wheel.
BEAM validation uses **OTP 29.1.1 / Rebar 3.27.0**. An earlier 75-test Actor suite
also passes on the host's OTP 25.

| Gate | Result |
| --- | --- |
| Actor .NET behavioral suite | 80 passed |
| Actor JavaScript behavioral suite | 78 passed |
| Actor Python 3.15 behavioral suite | 78 passed |
| Actor BEAM behavioral suite, OTP 29 | 76 passed |
| Library and test .NET builds | 0 warnings, 0 errors |
| Standalone BEAM library generation and compilation | Passed; existing generated/runtime warnings |
| Fantomas 8.0.8 check and `git diff --check` | Passed |
| Candidate NuGet pack | Passed; pre-existing missing-readme metadata warning |
| Independent packed actor consumer | 9 groups on all four targets; type-mismatch probes rejected |
| Packed actor consumer BEAM retention | 20 command generations and root release passed |
| Independent packed I/O consumer | File, directory and process catalogs passed on all four targets |
| Plain BEAM Async finalizer regression | Fails: compensation runs twice; upstream runtime blocker |
| Plain BEAM Async Using regression | Fails: zero disposals and badarity; upstream runtime blocker |

The Actor regressions cover idle receives, all request adapters, sequential
Sleep cancellation, 40 receive/cancel races, late acquisition and its cleanup
defect, once-only disposal, shielded delayed finalization, timeout/retry, retained
normal and original failed exits, original owner failure versus descendant
failure, transitive descendants, invalid deadlines, terminal attachment
rejection, repeated timeout observer retirement, dead observer callers, creator
death and deferred release. Existing native kill/link tests remain enabled.

Independent consumers are packed and restored into fresh package caches with no
project references. The candidate Actor package is the only unpublished
dependency replacement. Package builds and Fable compilation explicitly select
the cooperative capability. No local compiler, custom `--fableLib`, `PYTHONPATH`,
or copied BEAM runtime is used.

The packed actor catalog keeps its original case bodies and assertions. Its
noncooperative BEAM gate fixture explicitly executes an opaque wait outside the
ambient cooperative context and restores the context before the enclosing actor
continues. This preserves timeout, responsiveness, retry, cleanup and stale
completion assertions. Without this fixture correction, the gate becomes
cooperative because `Actor.callWithTimeout` now inherits actor cancellation.

The packed I/O catalog covers caller cancellation, command cancellation and
retired delivery, owner stop waiting for admitted I/O, directory-handle closure
and cleanup retries, and external-process exit, deadlines, byte limits and
inherited-pipe policy. No native I/O cases are skipped.
