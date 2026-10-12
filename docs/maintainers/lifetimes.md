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

The native controller is F# using Fable.Beam 5.2.0 typed monitored spawning,
revocable aliases and immutable native maps. Small emits implement selective
receives matching a bound alias/monitor, native shape decoding and temporary
runtime adapters.

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

## Independent Fable runtime defects

These published BEAM Async defects remain outside the lifetime controller:

| Defect | Current handling or evidence |
| --- | --- |
| Named-token `Async.RunSynchronously` lowers to unavailable `run_synchronously/3` | The actor bridge calls the supported two-argument runtime function |
| Cancellation loses its identity during F# catch normalization | Raw-catch adapters preserve cancellation before normalization |
| Throwing plain Async finalizer can run twice | `run/0` probe reports two invocations and fails its once-only assertion |
| Plain Async `Using` supplies an incompatible finalizer callback | `run_using/0` probe reports zero disposals and `badarity` |

Removable adapters in `ExecutionNative.fs` carry `TODO(upstream Fable)` comments
with their removal conditions. Actor-expression finalizers use a separate
implementation; the passing actor tests do not prove nested plain Async safe.

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

Both probes intentionally fail on the pinned runtime. They are upstream
reproductions, not passing library acceptance checks.
