namespace Fable.Actor

#if !FABLE_COMPILER_BEAM

open System
open System.Threading
open Fable.Actor.Types

module internal Lifetime =
#if FABLE_COMPILER_PYTHON
    let newGate = Platform.newGate
    // decision: owns a stable RLock because fable-library 5.19 util.lock creates a fresh lock for each invocation
    let synchronize gate action =
        Platform.enterGate gate

        try
            action ()
        finally
            Platform.leaveGate gate
#else
    let newGate () = obj ()
    let synchronize gate action = lock gate action
#endif

    let disposable f = {
        new IDisposable with
            member _.Dispose() = f ()
    }

    /// A single settlement with race-safe resource acquisition.
    ///
    /// invariant: settlement detaches every acquired resource before invoking the continuation
    /// decision: disposes resources outside the gate because token disposal can wait for another callback
    type Pending<'T>(continuation: 'T -> unit) =
        let gate = newGate ()
        let mutable completed = false
        let resources = ResizeArray<IDisposable>()
        let mutable callback = Some continuation

        member _.Add(resource: IDisposable) =
            let dispose =
                synchronize gate (fun () ->
                    if completed then
                        true
                    else
                        resources.Add resource
                        false)

            if dispose then
                resource.Dispose()

        member _.IsCompleted = synchronize gate (fun () -> completed)

        member _.Settle(value: 'T) =
            let work =
                synchronize gate (fun () ->
                    if completed then
                        None
                    else
                        completed <- true
                        let acquired = resources.ToArray()
                        resources.Clear()
                        let notify = callback
                        callback <- None
                        Some(acquired, notify))

            match work with
            | None -> ()
            | Some(acquired, notify) ->
                for resource in acquired do
                    resource.Dispose()

                notify |> Option.iter (fun f -> f value)

    let cancellation (token: CancellationToken) callback =
#if FABLE_COMPILER_PYTHON
        let id = Platform.addCancellationListener token callback

        let registration =
            disposable (fun () -> synchronize (Platform.cancellationGate token) (fun () -> Platform.removeCancellationListener token id))
#else
        let registration = token.Register(Action callback) :> IDisposable
#endif
        // Fable JS/Python Register does not invoke an already-cancelled token's callback.
        // invariant: registration followed by inspection covers cancellation before and during acquisition
        if token.IsCancellationRequested then
            callback ()

        registration

    let deadline (ms: int) callback =
#if FABLE_COMPILER_PYTHON
        let handle = Platform.startDeadline ms callback
        disposable (fun () -> Platform.cancelDeadline handle)
#else
        let cts = new CancellationTokenSource()

        Async.StartWithContinuations(
            async { do! Async.Sleep ms },
            (fun value ->
                ignore value
                callback ()),
            (fun _ -> ()),
            (fun _ -> ()),
            cts.Token
        )

        disposable (fun () ->
            cts.Cancel()
            cts.Dispose())
#endif

/// Internal state shared by handles for one emulated actor generation.
///
/// decision: separates workflow completion from stop requests so cancellation never masquerades as worker exit
/// invariant: terminal publication follows workflow exit and all owned child exits
/// invariant: cancellation and lifecycle cleanup run at most once per generation
/// tradeoff: cooperative user work can outlive a cleanup deadline and remains owned until it exits
[<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type ActorLifetime internal (cts: CancellationTokenSource) =
    let gate = Lifetime.newGate ()
    let mutable stopping = false
    let mutable cancelling = false
    let mutable completing = false
    let mutable bodyExit: ActorExit option = None
    let mutable result: ActorExit option = None
    let mutable failure: exn option = None
    let observers = System.Collections.Generic.Dictionary<int, ActorExit -> unit>()
    let children = System.Collections.Generic.Dictionary<int, ActorLifetime>()
    let resources = ResizeArray<IDisposable>()
    let mutable nextId = 0

    member _.IsStopping = Lifetime.synchronize gate (fun () -> stopping)
    member _.ObserverCount = Lifetime.synchronize gate (fun () -> observers.Count)
    member _.ChildCount = Lifetime.synchronize gate (fun () -> children.Count)

    member private _.RecordFailure(ex: exn) =
        Lifetime.synchronize gate (fun () -> failure <- Some ex)

    member private _.Publish() =
        let notification =
            Lifetime.synchronize gate (fun () ->
                match bodyExit, result with
                | Some exit, None when
                    not cancelling
                    && not completing
                    && children.Count = 0
                    ->
                    let outcome =
                        match failure with
                        | Some ex -> ActorExit.Failed ex
                        | None -> exit

                    result <- Some outcome
                    let callbacks = observers.Values |> Seq.toArray
                    observers.Clear()
                    Some(outcome, callbacks)
                | _ -> None)

        match notification with
        | None -> ()
        | Some(outcome, callbacks) ->
            cts.Dispose()

            for callback in callbacks do
                callback outcome

    member this.AddResource(resource: IDisposable) =
        let dispose =
            Lifetime.synchronize gate (fun () ->
                if bodyExit.IsSome then
                    true
                else
                    resources.Add resource
                    false)

        if dispose then
            resource.Dispose()

    member _.Observe(callback: ActorExit -> unit) =
        let id, terminal =
            Lifetime.synchronize gate (fun () ->
                nextId <- nextId + 1

                match result with
                | Some exit -> nextId, Some exit
                | None ->
                    observers.Add(nextId, callback)
                    nextId, None)

        terminal |> Option.iter callback
        Lifetime.disposable (fun () -> Lifetime.synchronize gate (fun () -> observers.Remove id |> ignore))

    member this.RequestStop() =
        let owned =
            Lifetime.synchronize gate (fun () ->
                if stopping then
                    None
                else
                    stopping <- true
                    cancelling <- true
                    Some(children.Values |> Seq.toArray))

        match owned with
        | None -> ()
        | Some owned ->
            for child in owned do
                child.RequestStop()

            try
                cts.Cancel()
            with ex ->
                this.RecordFailure ex

            Lifetime.synchronize gate (fun () -> cancelling <- false)
            this.Publish()

    member this.Complete(exit: ActorExit) =
        let acquired =
            Lifetime.synchronize gate (fun () ->
                if bodyExit.IsSome then
                    None
                else
                    bodyExit <- Some exit
                    completing <- true
                    let acquired = resources.ToArray()
                    resources.Clear()
                    Some acquired)

        match acquired with
        | None -> ()
        | Some acquired ->
            // decision: stops descendants even on normal parent completion to prevent orphan workers
            this.RequestStop()

            for resource in acquired do
                try
                    resource.Dispose()
                with ex ->
                    this.RecordFailure ex

            Lifetime.synchronize gate (fun () -> completing <- false)
            this.Publish()

    member this.Own(child: ActorLifetime) =
        let id, stop =
            Lifetime.synchronize gate (fun () ->
                nextId <- nextId + 1
                children.Add(nextId, child)
                nextId, stopping)

        child.Observe(fun exit ->
            match exit with
            | ActorExit.Failed ex when this.IsStopping -> this.RecordFailure ex
            | _ -> ()

            Lifetime.synchronize gate (fun () -> children.Remove id |> ignore)
            this.Publish())
        |> ignore

        if stop then
            child.RequestStop()

#endif
