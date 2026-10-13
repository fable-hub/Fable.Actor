# Messaging

Messages let other parts of an application change an actor's state without
sharing mutable variables. Sending is enough when the caller can move on;
requesting a reply is useful when it needs a result or confirmation before
continuing. Deadlines bound how long the caller waits for that reply.

After [Getting started](getting-started.md), this guide shows both styles with a
counter, then adds explicit timeout and cancellation outcomes.

## State and replies

`Actor.spawn(initialState, update)` runs a stateful handler. A simple handler
returns the new state directly. A `ReplyChannel<Value>` lets it reply to a request.

```fsharp
open Fable.Actor
open Fable.Actor.Types

type CounterMessage = Increment | GetCount

let counter: Actor<CounterMessage * ReplyChannel<int>> =
    Actor.spawn(0, (fun count (message, reply) ->
        match message with
        | Increment -> count + 1
        | GetCount ->
            reply.Reply count
            count))

let readCounter () = actor {
    Actor.cast counter Increment
    Actor.cast counter Increment
    let! count = Actor.callWithTimeout 2000 counter GetCount
    printfn "Count: %d" count
}
```

A handler can also return `Next<State>` when it decides termination explicitly:
`Continue nextState` keeps going, `Stop` finishes normally, and `StopAbnormal error`
fails. Pure state handlers can instead configure a `shouldStop` predicate on the
factory. [One actor API](actors.md) explains overload selection.

`Actor.cast` sends to a request-capable actor with a reply channel that ignores
replies. `Actor.call` waits for a reply without a deadline;
`Actor.callWithTimeout` bounds that wait and raises on timeout, cancellation or
target termination. Use `Actor.send` for ordinary message-only actors.

## Handle expected outcomes

Use `Actor.callResult` when you want explicit outcomes rather than exceptions:

```fsharp
open System.Threading

let tryReadCounter (token: CancellationToken) = actor {
    let! result = Actor.callResult 2000 token counter GetCount
    match result with
    | CallResult.Reply count -> printfn "Count: %d" count
    | CallResult.TimedOut -> printfn "No reply within the deadline"
    | CallResult.TargetTerminated exit -> printfn "Counter stopped: %A" exit
    | CallResult.Cancelled -> printfn "Request wait cancelled"
}
```

Deadlines must be positive milliseconds. A wait also observes its executing
actor's cancellation. Actor-expression cancellation can unwind the body before
a continuation runs; do not depend on a `Cancelled` match arm for cleanup.

Timeout and cancellation do not retract a delivered message or undo its work.
Calls ignore late replies and never retry automatically. Awaiting each reply
keeps the caller from submitting its next request before the current reply.

From an `async { }` host, use `asyncCall`, `asyncCallWithTimeout` or
`asyncCallResult`. Inside portable actor bodies, use `actor { }` and the ordinary
call operations; [Platform notes](platforms.md) explain the distinction.

## Timers

`Actor.schedule` schedules one callback. Keep its handle if you need to cancel it:

```fsharp
let scheduleIncrement () =
    Actor.schedule 1000 (fun () -> Actor.cast counter Increment)

let cancelIncrement timer = Actor.cancelTimer timer
```

Timer callbacks should send messages to the actor that owns the state. Timers
are not automatically registered as children by the lifetime API; manage their
handles explicitly.

Next: [Supervision](supervision.md).
