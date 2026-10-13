# Supervision

A worker can crash while the rest of an application keeps running. Its owner
needs to decide whether to replace it, stop using it, or propagate the failure.
Supervision makes that recovery policy explicit: a parent observes child failures
and chooses how to continue.

Use supervision for long-lived services or background workers whose owner can
start a fresh child after a failure. Restart creates a new actor; it does not
restore the failed child's state or replay its messages automatically. Choose a
policy that fits what the worker can safely restart.

For shutdown that waits for owned cleanup, see [Lifetimes](lifetimes.md).

## Linked children

`Actor.spawnLinked(parent, body)` starts a child linked to a parent. Children stop
when their parent exits, including normal completion. On BEAM, call
`Actor.trapExits ()` inside the parent to receive abnormal child exits as messages.
Managed targets already report child failures as messages.

A supervising actor uses an `obj` inbox so it can receive both application
messages and `ChildExited` notifications. Check `Actor.tryAsChildExited` before
interpreting application messages.

## Restart a child

`Actor.spawnSupervised` remembers the child's body and recovery policy.
The parent still receives and handles the failure notification; restart is not
automatic until it calls `Actor.handleChildExit`.

```fsharp
open Fable.Actor
open Fable.Actor.Types

let startSupervisor () : Actor<obj> =
    Actor.spawn (fun inbox ->
        Actor.trapExits ()

        let child =
            Actor.spawnSupervised inbox.Reference
                (OneForOne(fun _ -> Directive.Restart))
                (fun childInbox ->
                    let rec work () = actor {
                        let! text = childInbox.Receive()
                        printfn "%s" text
                        return! work ()
                    }
                    work ())

        let rec loop current = actor {
            let! message = inbox.Receive()
            match Actor.tryAsChildExited message with
            | Some exited ->
                match Actor.handleChildExit inbox.Reference current exited with
                | ChildExitResult.Restarted replacement ->
                    return! loop replacement
                | ChildExitResult.Stopped -> return ()
            | None ->
                match message with
                | :? string as text -> Actor.send current.Actor text
                | _ -> ()
                return! loop current
        }
        loop child)
```

The policy returns `Restart`, `Stop` or `Escalate`. Restart creates a new actor:
keep the returned replacement and share its new message handle as needed. Stop
ends supervision of that child. Escalate raises the failure in the parent.

For several children, route each `ChildExited` notification to the matching
current child before calling `handleChildExit`; it does not verify identity.

Native links can terminate work without running F# finalizers. To await owned
cleanup on every target, use the optional lifetime contract.

Next: [Lifetimes](lifetimes.md).
