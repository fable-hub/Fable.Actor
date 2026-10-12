# Ordinary actor execution costs

Use this benchmark when changing the shared actor builder or native execution
helpers. Optional lifetime policy must keep ordinary actors lightweight. Check
ordinary binds, sequential Async, receives, finalizers and request/reply calls
against a released baseline, alongside the behavioral suites.

The ordinary path must skip resource inspection, cleanup shielding and lifetime
bookkeeping. Ordinary Async binds must create no cancellation tokens. Keep the
execution-time context check that allows reused operations to inherit their
executing owner's cancellation. Measure its residual cost before accepting a
shared-execution change; absence of a controller alone does not establish cost.

## Run a comparison

Requires Linux CPU affinity support, Python 3, the repository's .NET/Fable tools,
and OTP 27.3.4.9 / Rebar 3.27.0 installed through mise.

Copy **this same harness** into `build/OrdinaryOverhead` in each checkout. This
keeps the workload identical even when a baseline predates these benchmark files.
Run these commands in each checkout:

```sh
mkdir -p build/OrdinaryOverhead
# Copy Bench.fs and OrdinaryOverhead.fsproj from this directory into that folder.
dotnet fable build/OrdinaryOverhead --exclude Fable.Core --lang beam --outDir build/ordinary-overhead-beam
cd build/ordinary-overhead-beam
mise exec erlang@27.3.4.9 rebar@3.27.0 -- rebar3 compile
```

Build each output with those explicit tool versions, including older checkouts
without `mise.toml`. From the candidate repository:

```sh
python3 benchmarks/ordinary-beam/compare.py \
  --variant baseline=/path/to/baseline/build/ordinary-overhead-beam \
  --variant candidate=/path/to/candidate/build/ordinary-overhead-beam \
  --output /tmp/actor-comparison
```

The runner checks matching OTP versions, generated workloads and bundled runtime
source hashes. It pins each VM to one allowed CPU, uses one scheduler, rotates
revision order, and records six VM runs with one discarded warm-up round and
five measured rounds each. It writes all raw measurements and a JSON summary.
A separate call-count trace measures token creation over 1,000 ordinary Async
binds. Optional extra `--variant` arguments include intermediate revisions.

Each sample runs in a fresh ordinary worker. Setup, spawning and reporting are
outside the timed region. Each result and finalizer count is checked. Bind,
Async, receive and finalizer samples use 200,000 operations; calls use 10,000.
Reductions cover the executing worker; call latency also includes the responder.

## Interpretation

Reductions measure scheduler work, not elapsed time or allocated bytes. Some
native operations add time without adding reductions. Inspect both metrics and
their spread; there is no noisy timing assertion in CI.

This is a microbenchmark, not application throughput, fairness or a managed-target
measurement. .NET/JavaScript/Python execution changes need separate evidence.
See [the recorded comparison](../../docs/maintainers/ordinary-actor-costs.md)
for the release baseline, results, remaining cost and validation scope.
