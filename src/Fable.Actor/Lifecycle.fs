namespace Fable.Actor

#if !FABLE_COMPILER_BEAM

open System
open System.Threading
open Fable.Actor.Types

// TODO(upstream): retire the marked adapters only after the pinned compiler and runtime
// include the referenced fixes and the four-target suite passes without those adapters.
// Pending settlement and ActorLifetime ownership remain library responsibilities.
module internal Lifetime =
#if FABLE_COMPILER_PYTHON
    // TODO(upstream): https://github.com/fable-compiler/Fable/pull/5035
    // Replace this Python gate branch with obj()/lock and remove Platform's gate helpers
    // once the published Python runtime shares a stable lock per object.
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

    let withContext body : Async<'T> =
#if FABLE_COMPILER_PYTHON || FABLE_COMPILER_JAVASCRIPT
        // TODO(upstream): https://github.com/fable-compiler/Fable/pull/5037
        // and https://github.com/fable-compiler/Fable/pull/5038
        // Retire the Python adapter after unit continuations and terminal cancellation are fixed.
        // TODO(upstream): native JS TryFinally also checks cancellation before installing
        // compensation. Retire this adapter only when cancelled resource acquisition disposes.
        Platform.fromContext body
#else
        async {
            let! token = Async.CancellationToken
            return! Async.FromContinuations(fun continuations -> body (continuations, token))
        }
#endif

    let withContextRun body : Async<'T> =
#if FABLE_COMPILER_PYTHON || FABLE_COMPILER_JAVASCRIPT
        Platform.fromContextWithRun body
#else
        withContext (fun (continuations, token) ->
            body (continuations, token) (fun op (ok, error, cancelled) -> Async.StartWithContinuations(op, ok, error, cancelled, token)))
#endif

#if !FABLE_COMPILER
    let private cleanupFailureKey = obj ()

    // decision: carries cleanup failure on the cancellation signal because .NET TryCancelled preserves cancellation even when its callback throws
    let private recordCleanupFailure (cancelled: OperationCanceledException) failure =
        cancelled.Data[cleanupFailureKey] <- failure

    let cancelledExit (cancelled: OperationCanceledException) =
        match cancelled.Data[cleanupFailureKey] with
        | :? exn as failure -> ActorExit.Failed failure
        | _ -> ActorExit.Cancelled

    // invariant: cancellation cleanup is installed before any ambient cancellation checkpoint
    // decision: uses .NET's cancellation handler for cancellation and explicit continuations for other exits to preserve cleanup failures
    let finallyNative (body: Async<'T>) compensation =
        Async.TryCancelled(
            withContext (fun ((ok, error, cancelled), token) ->
                let finish next value =
                    let failure =
                        try
                            compensation ()
                            None
                        with ex ->
                            Some ex

                    match failure with
                    | Some ex -> error ex
                    | None -> next value

                Async.StartWithContinuations(body, finish ok, finish error, cancelled, token)),
            fun cancelled ->
                try
                    compensation ()
                with ex ->
                    recordCleanupFailure cancelled ex
        )
#endif

    let guard (expression: unit -> Async<'T>) : Async<'T> =
#if FABLE_COMPILER_PYTHON
        // TODO(upstream): https://github.com/fable-compiler/Fable/pull/5038
        // Collapse this Python guard to async.Delay after cancelled Delay/Bind no longer
        // run user work or receive a second terminal callback from a cancelled delay.
        withContextRun (fun ((ok, error, cancelled), token) run ->
            let gate = newGate ()
            let mutable finished = false

            let finish next value =
                let run =
                    synchronize gate (fun () ->
                        if finished then
                            false
                        else
                            finished <- true
                            true)

                if run then
                    next value

            if token.IsCancellationRequested then
                finish cancelled (OperationCanceledException())
            else
                let operation =
                    try
                        Choice1Of2(expression ())
                    with ex ->
                        Choice2Of2 ex

                match operation with
                | Choice2Of2 ex -> finish error ex
                | Choice1Of2 op -> run op (finish ok, finish error, finish cancelled))
#else
        async.Delay expression
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
        // TODO(upstream): https://github.com/fable-compiler/Fable/pull/5036
        // Use token.Register like the other targets once it returns a disposable handle
        // and safely handles disposal during cancellation; delete the listener-field adapters.
        let id = Platform.addCancellationListener token callback

        let registration =
            disposable (fun () -> synchronize (Platform.cancellationGate token) (fun () -> Platform.removeCancellationListener token id))
#else
        let registration = token.Register(Action callback) :> IDisposable
#endif
        // TODO(upstream): https://github.com/fable-compiler/Fable/pull/5036
        // Remove the post-registration inspection once published JS/Python Register invokes
        // already-cancelled callbacks immediately; keep the pending operation's settlement gate.
        // Fable JS/Python Register does not invoke an already-cancelled token's callback.
        // invariant: registration followed by inspection covers cancellation before and during acquisition
        if token.IsCancellationRequested then
            callback ()

        registration

    let deadline (ms: int) callback =
#if FABLE_COMPILER_PYTHON
        // TODO(upstream): https://github.com/fable-compiler/Fable/pull/5038
        // Reuse the CTS/Async.Sleep branch once cancellation retires the Python timer
        // and listener exactly once; remove Platform.startDeadline/cancelDeadline with it.
        let handle = Platform.startDeadline ms callback
        disposable (fun () -> Platform.cancelDeadline handle)
#else
        let cts = new CancellationTokenSource()

        Async.StartWithContinuations(
            async { do! Async.Sleep ms },
            // TODO(upstream): https://github.com/fable-compiler/Fable/pull/5037
            // This explicit value parameter avoids unit callbacks whose captured default
            // gets overwritten by None in Python; simplify only after generated-code validation.
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
type internal ActorLifetime(cts: CancellationTokenSource) =
    let gate = Lifetime.newGate ()
    let mutable stopping = false
    let mutable cancelling = false
    let mutable completing = false
    let mutable bodyExit: ActorExit option = None
    let mutable result: ActorExit option = None
    let mutable failure: exn option = None
    let observers = System.Collections.Generic.Dictionary<int, ActorExit -> unit>()

    let terminationObservers =
        System.Collections.Generic.Dictionary<int, ActorExit -> unit>()

    let mutable termination: ActorExit option = None
    let children = System.Collections.Generic.Dictionary<int, ActorLifetime>()
    let resources = ResizeArray<IDisposable>()
    let mutable nextId = 0

    member _.IsStopping = Lifetime.synchronize gate (fun () -> stopping)

    member _.ObserverCount =
        Lifetime.synchronize gate (fun () -> observers.Count + terminationObservers.Count)

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

    // invariant: delivery and admission closure are serialized so Post cannot race mailbox disposal
    member _.Post(deliver: unit -> unit) =
        Lifetime.synchronize gate (fun () ->
            // decision: checks the owned token because callers can cancel the public CTS without requesting lifecycle shutdown
            if not stopping && not cts.IsCancellationRequested then
                deliver ())

    /// Observe closure of request admission, independently of descendant cleanup.
    ///
    /// decision: settles calls on stop request because cooperative work can otherwise retain them beyond shutdown
    member _.ObserveTermination(callback: ActorExit -> unit) =
        let id, terminal =
            Lifetime.synchronize gate (fun () ->
                nextId <- nextId + 1

                match termination with
                | Some exit -> nextId, Some exit
                | None ->
                    terminationObservers.Add(nextId, callback)
                    nextId, None)

        terminal |> Option.iter callback
        Lifetime.disposable (fun () -> Lifetime.synchronize gate (fun () -> terminationObservers.Remove id |> ignore))

    member this.RequestStop() =
        let owned =
            Lifetime.synchronize gate (fun () ->
                if stopping then
                    None
                else
                    stopping <- true
                    cancelling <- true
                    let exit = defaultArg bodyExit ActorExit.Cancelled
                    termination <- Some exit
                    let callbacks = terminationObservers.Values |> Seq.toArray
                    terminationObservers.Clear()
                    Some(children.Values |> Seq.toArray, exit, callbacks))

        match owned with
        | None -> ()
        | Some(owned, exit, callbacks) ->
            for callback in callbacks do
                callback exit

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
