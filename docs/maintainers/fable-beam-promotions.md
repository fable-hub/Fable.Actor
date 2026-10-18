# BEAM bindings: keep, delete, or promote upstream

Audited 2026-10-18 against Fable.Beam 5.2.0 (NuGet, the version the library
references) and Fable.Beam `main` at `0c26bdc`. Re-check upstream before
writing a PR — it had already absorbed the alias and monitored-spawn work this
library needed once before.

The hand-written BEAM expressions in the library, shared test suite and interop
example were compared with the bindings Fable.Beam already ships. Three
outcomes guide the remaining work.

- **Bucket A** — Fable.Beam already binds it. Delete the local `Emit`, call the
  upstream binding. No upstream change.
- **Bucket B** — Fable.Beam cannot express it and the expression says nothing
  about actors. Promote upstream, then delete locally.
- **Bucket C** — the expression encodes this library's private wire contract.
  It stays, whatever Fable.Beam grows.

Bucket A and the `Alias` retyping are implemented here. The remaining upstream
work concerns missing bindings and selective receive support.

## Bucket A — already bound upstream, replaced here

| Local expression | Binding used |
| --- | --- |
| `Platform.fs` `beginCall` | `Erlang.aliasWithOptions [ExplicitUnalias]` + `Erlang.monitor` |
| `Platform.fs` `sendAliasReply` | `Erlang.sendAlias` after alias retyping |
| `Platform.fs` `endCall`, its `unalias` + `demonitor([flush])` half | `Erlang.unalias`, `Erlang.demonitorFlush` |
| `Lifetimes.fs` `element(1, $0)` | `Erlang.element 1` |
| `test/Helpers.fs` `sleepMs` (`timer:sleep`) | `Timer.sleep` |
| `test/Helpers.fs` `linkedPid`, `linkedToPid` (`lists:member`) | Fable.Beam `Lists` member binding (still needs B4 for `process_info`) |
| `test/Helpers.fs` priority flags | `Erlang.processFlag` |
| `test/CooperativeTests.fs` guard extraction, monitor and liveness check | `Erlang.element`, `Erlang.monitor`, `Erlang.isProcessAlive` |
| `examples/interop` `io:format` | `Io.format` |

`Platform.monotonicMilliseconds` remains until B3 resolves the return type of
Fable.Beam's `monotonicTimeMs` binding. Test-only `process_info` expressions
remain until B4.

`examples/interop` `rawSend` (`$0 ! $1`) is `Erlang.send` over an unboxed pid,
but the example exists to show the raw operator beside the envelope, so it reads
as Bucket C. Leave it.

Dead code deleted rather than promoted: `Platform.sendReply`,
`Platform.recvReply`, `Platform.recvReplyWithTimeout` had no callers — the
alias-based `callCore` path replaced them. `InternalMsg.Reply` itself stays; `receiveMsg` and
`ExecutionNative.WorkerMessage` still drop its tag.

## Bucket B — upstream candidates

Ranked by how much local code one upstream binding removes.

1. **Selective receive on a bound variable.** The root cause: `Fable.Core`
   `BeamInterop.Erlang.receive<'T>` matches message *shape*, never a value in an
   in-scope variable, which is why several receives are hand-emitted here.
   An alias is a delivery address, not a marker attached to a mailbox message.
   The current reply protocols echo the alias inside their tuples so a receive
   can match its bound value. An alias-only `receiveOnAlias` binding would be
   unsound for arbitrary `Alias<'Msg>` messages. Explore compiler support for
   bound-value receive patterns, or a typed interop facility that takes an
   explicit message shape. `Erlang.awaitDown (Ref<Pid<'P>>) (Pid<'P>) : unit`
   can be proposed independently for `Platform.waitProcessDeath`.

2. **Term predicates.** `is_map` + `is_map_key` (`Platform.isChildExited`,
   `CommandRuntime.exceptionReason`) and `is_reference`
   (`ExecutionNative.checkpointValue`). Fable.Beam binds `isList` only. Add
   `Erlang.isMap`, `Erlang.isMapKey`, `Erlang.isReference`.

3. **`monotonicTimeMs : int` needs a wider F# contract.** Erlang specifies
   `erlang:monotonic_time/1` as `integer()`; the BEAM integer itself is not
   limited to 64 bits. Actor deadlines use `int64` arithmetic. Consider
   `Erlang.monotonicTime (TimeUnit) : int64`, verify its generated BEAM behavior,
   then replace `monotonicMilliseconds` here.

4. **`erlang:process_info/2`.** `test/Helpers.fs` and `CooperativeTests.fs`
   bind `message_queue_len`, `monitors` and `links` by hand. Plain BIF, no actor
   vocabulary. Prefer typed `processLinks`, `processMonitors`,
   `processMessageQueueLen` over a raw
   `process_info` returning `Dynamic`. Each result must account for `undefined`
   when the PID is no longer alive; these helpers belong in `option` results.

5. **Monitor, kill, prove death.** `Platform.stopProcess` and
   `test/Helpers.observedDeath` are the same idiom written twice inside this
   repository — the strongest evidence it is not actor policy. Promote
   `Erlang.exitAndWait (Pid<'Msg>) (timeoutMs) : bool` and `Erlang.awaitDeath`
   for the observe-only variant.

6. **Cancellable delayed callback.** Fable.Beam's `Timer.applyAfter` binds only
   the module/function form, although OTP 27 also has `timer:apply_after/2` for
   a closure. Bind that overload with its timer-reference result and existing
   `Timer.cancel` contract. `Platform.timerSchedule` currently returns an
   immediately spawned PID and cancels it with a message; adopting the timer
   binding requires an explicit local cancellation change and regression test.

7. **Weak: the startup handshake inside `Platform.spawnOwnedProcess`.** The
   `spawn_monitor` + ready-ref + DOWN race is generic. The watcher process that
   `exit(Child, kill)`s on parent DOWN is ownership policy and stays. If this
   goes upstream, split the emit so only the handshake moves.

## Bucket C — stays in Fable.Actor

The `fable_actor_msg` / `fable_actor_reply` / `EXIT` envelope tags and the
`InternalMsg`, `WorkerMessage`, `ControlMessage` unions, `receiveMsg`'s
drop-stale-reply loop, `decodeBodyExit`'s `cooperative_body_exit`,
`CommandRuntime.exceptionReason`'s `CommandException__` shape,
`ExecutionNative.checkpointValue`'s resource lookup, the ownership watcher, and
the `fromContext` / `fromContextWithRun` Python and JS emits.

These are the library's private protocol, guarded by `invariant:` ADCs and
mirrored by `examples/interop/erl/interop_demo.erl`. Promoting them publishes a
protocol that has no upstream reason to stay stable.

## Typing fix completed with Bucket A

The call path previously typed the reply address as `Ref<obj>` (`beginCall`,
`sendAliasReply`, `recvCall`, `endCall`) while `LifetimesNative` already used
`Alias<ControlReply>`. Fable.Beam 5.2 supplies `Alias<'Msg>` for both paths.

`InternalMsg.Reply`'s first field is now `Alias<InternalMsg>`. The generated
Erlang still sends `{fable_actor_reply, Alias, Value}` directly to the alias;
`endCall` still revokes the alias before flushing queued duplicate replies.

## Resuming

- [x] Bucket A replacements + `Alias` retyping in this repo
- [x] `just format-check`, `just build`, `just test` and `just build-interop`
      completed locally; rerun before pushing
- [ ] Fable compiler/interop issue for B1; Fable.Beam PR for B2
- [ ] Fable.Beam issues for B3–B6, smaller and independent; B7 only if the emit
      splits cleanly
- [ ] Follow Fable.Beam's own rules in `BINDINGS-GUIDE.md` at
      `fable-compiler/Fable.Beam`: no bare `obj`, phantom tags on opaque
      handles, doc link to the Erlang page, ADCs in the same four-directive style
- [ ] Note in the upstream PR that `spawnMonitor`, `Alias`, `sendAlias` and
      `aliasWithOptions` were already promoted from this library
