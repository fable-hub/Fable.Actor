# Lifetime implementation and validation

For application usage, start with [Lifetimes](../lifetimes.md). This page records
the implementation boundary, regression evidence and runtime probes.

## Implementation boundary

`ExecutionNative.fs` provides token propagation, cancellation checkpoints,
receive wakeups, actor-expression finalization and the sequential Async bridge.
`LifetimesNative.fs` adds native recursive ownership and retained observation.
`Lifetimes.fs` exposes the opt-in API. Both policies share the actor builder and
ship in one package; including sources does not allocate a controller.
Managed actors retain their existing `ActorLifetime` implementation.

Ordinary native execution skips owned resource inspection, cleanup shielding and
per-Async-bind token creation. The shared builder still checks execution context;
the [ordinary execution comparison](ordinary-actor-costs.md) records its remaining
cost against rc.14. No controller allocation does not imply zero runtime overhead.

The native controller is F# using Fable.Beam 5.2.0 typed monitored spawning,
revocable aliases and immutable native maps. Small emits implement selective
receives matching a bound alias/monitor and native shape decoding.

Ownership acknowledgement precedes child startup. Rejected attachment cancels
and releases the suspended worker without evaluating its user body. Monitoring
the creator before startup covers creator death during the handshake.
Terminal publication requires observed body death and an empty child-ownership
set. Each child acknowledges only after its own recursive cleanup. Body failure
wins over descendant failures; otherwise the first descendant defect is retained.

Stop observers use aliases and caller monitors. Timeout retires the observer,
revokes the alias and flushes queued results; caller death removes abandoned
observers. No process is allocated per stop request. A controller remains until
release or creator death after owned cleanup.

The actor builder installs `Using` before the next resource-value checkpoint.
Finalization precedes the continuation and runs with ambient cancellation
shielded. Request operations resolve the ambient token when run, not constructed.
Owned cancellation bypasses ordinary error handlers; cleanup defects remain
failures. Legacy managed supervision keeps its handled-failure behavior.

## Validation summary

The module extraction was validated on 2026-10-12 with released Fable 5.20.1,
Fable.Beam 5.2.0, CPython 3.15.0 / fable-library 5.20.1 and pinned OTP 27.3.4.9 /
Rebar 3.27.0. No custom compiler or substituted runtime was used.

| Check | Result |
| --- | --- |
| Existing behavioral suites | .NET 84, JavaScript 82, Python 82, BEAM 79 passed |
| Independent packed consumer | Passed on all four targets with a fresh cache and no project references |
| Library and test .NET build | Zero warnings/errors |
| Formatting, whitespace and local documentation links | Passed |
| NuGet payload | One assembly, both modules' Fable sources and readme metadata |

The packed smoke covers ordinary requests, idle cancellation, once-only cleanup,
repeated terminal observation and release. Regression tests also cover timeout
retries, late acquisition, transitive children, earlier handled child cleanup
failures, creator death and abandoned observers. The extraction changes the
unreleased module name from `Cooperative` to `Lifetimes`; acceptance assertions
remain unchanged.

CI for the extraction commit also passed standalone BEAM and Timeflies builds.
Broader downstream actor and file/directory/process catalogs and live Timeflies
smoke belong to earlier candidates and were not rerun against the final extraction.
Historical details remain in Git history; they are not additional current validation.

## Async API naming

Explicit F# Async bridges use an `async` prefix: `Actor.asyncCall`,
`Actor.asyncCallWithTimeout`, `Actor.asyncCallResult`, `Actor.asyncStop` and
`Lifetimes.asyncStop`. Actor-expression operations keep their existing names.

The published Actor names `callAsync`, `callAsyncWithTimeout`, `callResultAsync`
and `stopAsync` remain deprecated forwarding aliases with compiler migration
messages. The new lifetime bridge is named `asyncStop` directly.
The renamed bridges pass the same four behavioral suites; .NET metadata checks
verify their Async return types and the aliases' migration warnings.

## Released BEAM Async fixes

Fable 5.21.0 includes [Fable #5057](https://github.com/fable-compiler/Fable/pull/5057),
which fixes the independent BEAM Async defects reproduced with 5.20.1:

| Former defect | Released behavior |
| --- | --- |
| Named-token `Async.RunSynchronously` lowers to unavailable `run_synchronously/3` | Compiler and runtime support the three-argument call |
| Cancellation loses its identity during F# catch normalization | Typed `OperationCanceledException` patterns survive catch and rethrow |
| Throwing plain Async finalizer can run twice | Finalization runs once; cancellation remains terminal even if cleanup throws |
| Plain Async `Using` supplies an incompatible finalizer callback | Resources are disposed once |

`ExecutionNative.fs` now uses standard `Async.RunSynchronously` and typed F# catches.
The explicit arity emit and both raw-catch adapters are removed. Actor-expression
finalizers retain their separate implementation because cleanup shielding and
observable cleanup failures are lifetime policy.

Release validation passes .NET 84, JavaScript 82, Python 82 and BEAM 81 tests.
At that validation point, the Python suite used locked fable-library 5.20.1;
the BEAM fixes came from the compiler-bundled runtime. The two new BEAM tests
cover compiled F# plain Async resource scopes and cancellation identity,
alongside the independent runtime probes below.

The ordinary actor benchmark compares rc.15 and this adapter removal with both
compiled by Fable 5.21.0, matching bundled runtime sources and pinned OTP. All five
workloads pass their result checks. Ordinary Async creates zero tokens per 1,000
binds in both revisions; reductions are effectively unchanged and no timing
regression is observed. These microbenchmarks do not establish application throughput.

Run the independent probes from the repository root after generating the suite:

```sh
just test-beam
mkdir -p build/runtime-probe
mise exec -- erlc -o build/runtime-probe test/native/beam_async_finalizer_probe.erl
mise exec -- erl -noshell -pa build/runtime-probe build/tests-beam/_build/default/lib/*/ebin \
  -eval 'beam_async_finalizer_probe:run(), halt().'
mise exec -- erl -noshell -pa build/runtime-probe build/tests-beam/_build/default/lib/*/ebin \
  -eval 'beam_async_finalizer_probe:run_using(), halt().'
```

Both probes pass with Fable 5.21.0: the cancelled computation retains its typed
cancellation exception after exactly one throwing finalizer, and `Using` disposes
once before returning 42. Their historical failures are recorded in Git history.
