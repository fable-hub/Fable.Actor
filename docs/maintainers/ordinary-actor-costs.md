# Ordinary actor execution comparison

Optional lifetime policy should preserve a lightweight ordinary actor path.
This comparison separates avoidable lifetime work from the cost of sharing the
actor builder. The [harness and runner](../../benchmarks/ordinary-beam/README.md)
are included in the repository for future execution changes.

## Results

Measured on 2026-10-13 against rc.14 (`08eb4c4`) and the previous lifetime candidate
(`686884c`). The updated native helper takes the ordinary path before inspecting
resources or cleanup state, delegates ordinary receive to the existing native
protocol, invokes ordinary compensation directly, and uses the default sequential
Async bridge. Typed native keys and F# inlining avoid helper-call overhead.
No builder, public signature or lifetime-policy guarantee changes.

Median nanoseconds per operation:

| Workload | rc.14 | Previous candidate | Updated candidate | Change vs rc.14 |
| --- | --- | --- | --- | --- |
| bind | 61.53 | 75.33 | 73.16 | +18.9% |
| async | 152.76 | 287.76 | 178.47 | +16.8% |
| receive | 305.64 | 343.91 | 317.64 | +3.9% |
| finally | 681.50 | 782.14 | 709.00 | +4.0% |
| call | 1402.80 | 1565.05 | 1405.10 | +0.2% |

Median worker reductions per operation:

| Workload | rc.14 | Previous candidate | Updated candidate |
| --- | --- | --- | --- |
| bind | 30.18 | 57.18 | 30.18 |
| async | 41.33 | 89.34 | 41.33 |
| receive | 32.77 | 83.75 | 32.76 |
| finally | 64.77 | 138.72 | 64.77 |
| call | 97.67 | 150.67 | 97.67 |

A separate trace of 1,000 ordinary Async binds records **0 / 1,000 / 0** calls to
`fable_cancellation:create/0` respectively. The update removes the extra per-bind
token allocation. No owned resource inspection, finalizer shielding or ownership
bookkeeping executes on the ordinary path. Its reduction counts match the release
baseline within sample variation; reductions alone do not establish equal latency.

## Remaining tradeoff

The shared builder still checks the executing process's context at continuation
boundaries. This lets operations constructed elsewhere or reused later inherit the
executing lifetime's cancellation. The cost remains visible in immediate-bind
microbenchmarks: about 12 ns per step for ActorOp binds, and 26 ns for Async binds.
Those medians are about 19% and 17% above rc.14. Receive/finalizer costs are about
4% above the baseline; request/reply is near parity in this comparison.

The allocation and avoidable-bookkeeping checks pass. Acceptance of the remaining
shared-builder latency remains a design judgement before merge, supported by these
measurements. Do not describe ordinary execution as zero-overhead or consider a
Fish compatibility run sufficient performance evidence. A requirement to eliminate
all lifetime-context checks needs a separately selected execution path, including
receive and request operations; another builder alone cannot establish that.

## Method and limits

All three variants use released Fable 5.20.1, matched bundled runtime sources,
OTP 27.3.4.9 / ERTS 15.2.7.6 and Rebar 3.27.0. rc.14 references Fable.Beam 5.1.0;
both candidates reference 5.2.0. Runtime versions and generated workload hashes
are checked before measurement. Six VMs per variant run one warm-up and five
measured rounds: 30 samples per workload/variant. Each VM uses one scheduler and
one async thread, pinned to one CPU; variant/workload order rotates and reverses.

Each sample uses a fresh ordinary worker; setup, spawning and reporting are outside
timing. Bind/Async/receive/finalizer samples contain 200,000 operations; calls contain
10,000 exchanges. Checksums and cleanup counts are verified. Reductions cover the
worker, whereas request latency also includes its responder. Token tracing is
separate from timed samples.

The [JSON summary](../../benchmarks/ordinary-beam/results.json) records medians,
ranges, VM/CPU metadata and source identification. The runner emits raw samples
and full runtime fingerprints when reproduced. These are microbenchmarks, not
application throughput, fairness or allocated-byte measurements. Managed builders
are unchanged by this update; these numbers do not measure managed startup or
ownership costs. Only matched OTP 27 comparisons are reported.

## Correctness checks

At the measured revision, the shared suites pass: .NET 84, JavaScript 82,
Python 82 and BEAM 79 tests.
They retain coverage for preconstructed requests, cancellation/error-handler
classification, late acquisition, once-only cleanup, finalizer shielding and
transitive ownership. The subsequent Fable 5.21.0 upgrade removes the BEAM Async
adapters; see [released BEAM Async fixes](lifetimes.md#released-beam-async-fixes).

The package also passes an independent ordinary-request and cooperative-shutdown
smoke with a fresh package cache on all four targets. That smoke is narrower than
the downstream actor/file/directory/process catalogs; those remain an adoption
gate and were not rerun for this change.
