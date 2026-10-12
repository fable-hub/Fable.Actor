# Lifetimes

Closing a session or disposing a component often means stopping several workers
and releasing the resources they own. Requesting cancellation does not prove
that cleanup has finished, and a deadline can expire while work is still running.
Starting the next session too early can leave old work using the same resource.

`Lifetimes` gives an owner a way to request cooperative shutdown and observe its
actor-expression cleanup and registered children. It reports failures, retains
the terminal outcome for repeated observation, and leaves unfinished cleanup
observable after a timeout. Use it when that completion matters before moving on.
Ordinary `Actor.spawn` is sufficient when its existing shutdown behavior meets
your needs. For recovery after a worker crash, see [Supervision](supervision.md).

`Fable.Actor.Lifetimes` is an F# module in the **Fable.Actor** NuGet package.
Its actors use the shared `actor { }` computation expression.

## Start and message a worker

```fsharp
open Fable.Actor
open Fable.Actor.Types

let startWorker () : CooperativeActor<string> =
    Lifetimes.spawn (fun owner ->
        let rec loop () = actor {
            let! message = (Lifetimes.reference owner).Receive()
            printfn "%s" message
            return! loop ()
        }
        actor {
            try
                return! loop ()
            finally
                printfn "Worker cleanup finished"
        })

let sendWork owner text = Actor.send (Lifetimes.reference owner) text
```

`CooperativeActor<Message>` is a lifetime-control capability. Share
`Lifetimes.reference owner` for messaging and retain the capability for shutdown.
On BEAM, observation is scoped to the creating process; keep that process alive
while it owns the capability.

Use `use`/`use!` or `try`/`finally` in the actor expression to install cleanup.
Cancellation wakes receives, reaches request waits and sequential Async operations,
and stops the body at cooperative boundaries. Actor-expression finalizers run
once with ambient cancellation shielded. Ordinary error handlers do not intercept
owned cancellation; cleanup failures remain observable.

## Stop and inspect the result

```fsharp
let closeWorker owner = actor {
    let! result = Lifetimes.stop 2000 owner
    match result with
    | StopResult.Completed exit ->
        match exit with
        | ActorExit.Normal -> printfn "Worker finished normally"
        | ActorExit.Cancelled -> printfn "Worker cancelled and cleaned up"
        | ActorExit.Failed error -> printfn "Worker failed: %s" error.Message
        Lifetimes.release owner |> ignore
    | StopResult.TimedOut ->
        printfn "Cleanup is still unobserved; retain the capability to retry"
}
```

The deadline must be positive milliseconds. Stop requests cancellation and waits
for the body and all registered cooperative descendants to exit. `TimedOut`
means completion is unobserved; it never requests native kill. A later stop can
complete. Repeated stop returns the retained terminal result until release.

A body that has already finished normally or failed keeps that outcome. Body
failure takes precedence over descendant failures; otherwise the first observed
descendant failure becomes the owner's terminal failure. Handling a child's
failure notification does not erase that retained defect.

## Own children

Create owned children with `Lifetimes.spawnLinked`, using the lifetime capability
rather than its message reference:

```fsharp
let attachWorker (parent: CooperativeActor<string>) =
    let child: CooperativeActor<string> =
        Lifetimes.spawnLinked parent (fun child -> actor {
            try
                let! text = (Lifetimes.reference child).Receive()
                printfn "%s" text
            finally
                printfn "Child cleanup finished"
        })
    Lifetimes.releaseWhenExited child
    Lifetimes.reference child
```

Ownership is registered before the child starts. Parent completion cancels and
awaits owned children recursively, including when the parent finishes normally.
A parent whose terminal result is published rejects new children without running
their bodies. Use an object inbox and the [supervision](supervision.md) notification
pattern if the parent also handles child failures.

This example relinquishes separate child observation because the parent owns
shutdown. `releaseWhenExited` keeps the child owned until its actual exit.

## Release observation

On BEAM, a private controller retains the terminal outcome. After inspecting it,
`Lifetimes.release owner` returns `Released` at completion or `Running` while
cleanup remains owned. A `Running` result leaves observation available.
`releaseWhenExited` relinquishes future observation while keeping running work
owned until exit. Do not reuse a relinquished capability.

Creator death cancels native work and reclaims observation state after cleanup.
Managed runtimes reclaim unreachable metadata through garbage collection.
Ordinary actors allocate no cooperative controller.

## What completion covers

Completion covers the actor expression and registered cooperative children.
Opaque synchronous calls, independent services, raw processes and arbitrary
parallel work need their own resource-settlement acknowledgement. They can outlive
a stop deadline. A reply timeout does not undo the target's work.

On BEAM, calling `Actor.kill` or `Actor.stop` through the message reference skips
graceful cleanup. With Fable 5.21.0, plain nested `async { }` scopes dispose and
finalize once. Plain Async retains cancellation if cleanup throws; actor-expression
finalizers report cleanup failures. See [Platform notes](platforms.md) for the boundary.

For implementation and regression details, see
[Maintainer notes](maintainers/lifetimes.md).
