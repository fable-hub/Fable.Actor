// Platform-independent Actor abstraction.
//
// BEAM: actor { } is a CPS computation (no-op wrapper, BEAM processes block natively).
// Non-BEAM: actor { } delegates to async { }, Actor wraps MailboxProcessor.
//
// Actor<'Msg> provides MailboxProcessor-compatible API:
//   inbox.Receive() — get next message (inside body)
//   actor.Post(msg)  — send a message (from outside)
//
// decision: presents one actor API while selecting native BEAM processes or MailboxProcessor at compile time
// invariant: actors exchange state across process boundaries only through messages
// tradeoff: maintains two runtime implementations to preserve each target's native concurrency semantics
namespace Fable.Actor

open Fable.Actor.Types

// ============================================================================
// Actor type + Computation expression
// ============================================================================

#if FABLE_COMPILER_BEAM

open Fable.Beam
open Fable.Actor.Platform

// === BEAM: CPS-based, native processes ===

/// A synchronous continuation chain for an operation running inside one BEAM process.
///
/// decision: represents BEAM actor operations as CPS because receive blocks the lightweight process natively
/// invariant: a successfully completed operation invokes its continuation exactly once before Run returns
/// tradeoff: uses a target-specific computation type to avoid introducing an async scheduler on BEAM

type ActorOp<'T> = { Run: ('T -> unit) -> unit }

type Actor<'Msg> = {
    Pid: Pid<'Msg>
} with

    member _.Receive() : ActorOp<'Msg> = {
        Run = fun cont -> receiveMsg (fun raw -> cont (unbox<'Msg> raw))
    }

    member this.Post(msg: 'Msg) = sendMsg this.Pid msg

type ActorBuilder() =
    member _.Bind(op: ActorOp<'T>, f: 'T -> ActorOp<'U>) : ActorOp<'U> = {
        Run = fun cont -> op.Run(fun value -> (f value).Run cont)
    }

    member _.Return(value: 'T) : ActorOp<'T> = { Run = fun cont -> cont value }
    member _.ReturnFrom(op: ActorOp<'T>) : ActorOp<'T> = op
    member _.Zero() : ActorOp<unit> = { Run = fun cont -> cont () }
    member _.Delay(f: unit -> ActorOp<'T>) : ActorOp<'T> = { Run = fun cont -> (f ()).Run cont }

    member _.Combine(first: ActorOp<unit>, second: ActorOp<'T>) : ActorOp<'T> = {
        Run = fun cont -> first.Run(fun () -> second.Run cont)
    }

    member _.TryWith(body: ActorOp<'T>, handler: exn -> ActorOp<'T>) : ActorOp<'T> = {
        Run =
            fun cont ->
                try
                    body.Run cont
                with ex ->
                    (handler ex).Run cont
    }

    /// Run cleanup when a CPS body completes or raises.
    ///
    /// decision: captures the result before continuing so compensation precedes the rest of the actor body
    /// invariant: compensation runs exactly once on both normal and exceptional completion
    /// assumption: ActorOp continuations run synchronously — delayed continuation would expose an uninitialized result
    member _.TryFinally(body: ActorOp<'T>, compensation: unit -> unit) : ActorOp<'T> = {
        Run =
            fun cont ->
                let mutable result = Unchecked.defaultof<'T>

                (try
                    body.Run(fun value -> result <- value)
                 with _ ->
                     compensation ()
                     reraise ())

                compensation ()
                cont result
    }

    member this.Using(resource: 'a :> System.IDisposable, body: 'a -> ActorOp<'T>) : ActorOp<'T> =
        this.TryFinally(body resource, (fun () -> resource.Dispose()))

    member _.While(guard: unit -> bool, body: ActorOp<unit>) : ActorOp<unit> = {
        Run =
            fun cont ->
                let rec loop () =
                    if guard () then body.Run(fun () -> loop ()) else cont ()

                loop ()
    }

    member this.For(items: seq<'T>, body: 'T -> ActorOp<unit>) : ActorOp<unit> =
        this.Using(items.GetEnumerator(), fun enum -> this.While((fun () -> enum.MoveNext()), this.Delay(fun () -> body enum.Current)))

    /// Bridge an Async into the actor CE.
    ///
    /// decision: runs sequential Async inline so Async-returning APIs compose inside actor expressions
    /// assumption: Fable.Beam erases sequential Async to synchronous callbacks; Async.Parallel is the spawning exception
    /// tradeoff: supports shared Async APIs but does not add scheduler-based concurrency to a BEAM actor
    member _.Bind(op: Async<'T>, f: 'T -> ActorOp<'U>) : ActorOp<'U> = {
        Run = fun cont -> (f (Async.RunSynchronously op)).Run cont
    }

#else

// === Non-BEAM: MailboxProcessor-based ===

// decision: aliases actor operations to Async so existing Fable runtimes provide scheduling and cancellation
// invariant: ActorBuilder delegates control-flow semantics to the standard async builder on non-BEAM targets

type ActorOp<'T> = Async<'T>

type Actor<'Msg> internal (mb: MailboxProcessor<'Msg>, cts: System.Threading.CancellationTokenSource, lifetime: ActorLifetime) =
    member _.Mb = mb
    member _.Cts = cts
    member internal _.Lifetime = lifetime
    member _.Pid: obj = box mb

    member _.Receive() : Async<'Msg> =
        Async.FromContinuations(fun (ok, error, cancelled) ->
            Async.StartWithContinuations(
                mb.Receive(),
                (fun msg ->
                    if cts.IsCancellationRequested then
                        cancelled (System.OperationCanceledException())
                    else
                        ok msg),
                error,
                cancelled,
                cts.Token
            ))

    /// Queue a message while the actor accepts work.
    ///
    /// invariant: posting after cancellation or workflow exit is a no-op
    member _.Post(msg: 'Msg) =
        if not lifetime.IsStopping then
            mb.Post msg

type ActorBuilder() =
    member _.Bind(op: Async<'T>, f: 'T -> Async<'U>) : Async<'U> = async.Bind(op, f)
    member _.Return(value: 'T) : Async<'T> = async.Return(value)
    member _.ReturnFrom(op: Async<'T>) : Async<'T> = async.ReturnFrom(op)
    member _.Zero() : Async<unit> = async.Zero()
    member _.Delay(f: unit -> Async<'T>) : Async<'T> = async.Delay(f)

    member _.Combine(first: Async<unit>, second: Async<'T>) : Async<'T> =
        async.Combine(first, async.Delay(fun () -> second))

    member _.TryWith(body: Async<'T>, handler: exn -> Async<'T>) : Async<'T> = async.TryWith(body, handler)

    /// Run cleanup once and preserve its failure, including during cancellation.
    ///
    /// decision: routes cleanup exceptions explicitly because standard Async finalizers can lose failures during cancellation
    member _.TryFinally(body: Async<'T>, compensation: unit -> unit) : Async<'T> =
        async {
            let! token = Async.CancellationToken

            return!
                Async.FromContinuations(fun (ok, error, cancelled) ->
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

                    Async.StartWithContinuations(body, finish ok, finish error, finish cancelled, token))
        }

    member this.Using(resource: 'a :> System.IDisposable, body: 'a -> Async<'T>) : Async<'T> =
        this.TryFinally(
            body resource,
            fun () ->
                if not (isNull (box resource)) then
                    resource.Dispose()
        )

    member _.While(guard: unit -> bool, body: Async<unit>) : Async<unit> = async.While(guard, body)
    member _.For(items: seq<'T>, body: 'T -> Async<unit>) : Async<unit> = async.For(items, body)

#endif

/// A supervised child with the information required to restart it.
///
/// decision: retains the body and strategy in an immutable value so restart state is portable across targets
type SupervisedChild<'ParentMsg, 'Msg> = {
    Actor: Actor<'Msg>
    Body: Actor<'Msg> -> ActorOp<unit>
    Strategy: Strategy
}

/// Result of applying a supervision strategy to a child exit.
///
/// decision: returns the replacement explicitly because mutable record updates do not preserve it on every target
/// invariant: Restarted contains the child that accepts messages after the failed generation exits
/// tradeoff: callers replace their current supervised value after every restart
[<RequireQualifiedAccess>]
type ChildExitResult<'ParentMsg, 'Msg> =
    | Restarted of SupervisedChild<'ParentMsg, 'Msg>
    | Stopped

[<AutoOpen>]
module ActorCE =
    let actor = ActorBuilder()

// ============================================================================
// Core API
// ============================================================================

[<RequireQualifiedAccess>]
module Actor =

#if FABLE_COMPILER_BEAM

    /// Spawn an actor. Body receives inbox (self-reference) for Receive/Post.
    let spawn (body: Actor<'Msg> -> ActorOp<unit>) : Actor<'Msg> =
        let rawPid =
            Erlang.spawn (fun () ->
                let me: Actor<'Msg> = { Pid = Erlang.self () }
                (body me).Run(fun () -> ()))

        { Pid = rawPid }

    /// Spawn with external cancellation. BEAM cancellation requests native kill; it cannot unwind arbitrary ActorOp work.
    ///
    /// decision: releases the external registration in a watcher because kill cannot execute actor finalizers
    let spawnWithToken (token: System.Threading.CancellationToken) (body: Actor<'Msg> -> ActorOp<unit>) : Actor<'Msg> =
        let child = spawn body
        let registration = token.Register(fun () -> killProcess child.Pid)

        if token.IsCancellationRequested then
            killProcess child.Pid

        Erlang.spawn (fun () ->
            let monitor = Erlang.monitor child.Pid
            waitProcessDeath monitor child.Pid
            registration.Dispose())
        |> ignore

        child

    /// Spawn a linked child actor (parent gets EXIT signal on crash).
    ///
    /// decision: links to the supplied parent and monitors normal parent death so ownership does not depend on the caller
    let spawnLinked (parent: Actor<'ParentMsg>) (body: Actor<'Msg> -> ActorOp<unit>) : Actor<'Msg> =
        let rawPid =
            spawnOwnedProcess parent.Pid (fun () ->
                let me: Actor<'Msg> = { Pid = Erlang.self () }
                (body me).Run(fun () -> ()))

        { Pid = rawPid }

    /// Get own pid (only valid inside actor body).
    let self<'Msg> () : Actor<'Msg> = { Pid = Erlang.self () }

    /// Kill an actor and its linked children.
    let kill (actor: Actor<'Msg>) : unit = killProcess actor.Pid

    /// Kill and await native process death within a positive cleanup deadline.
    /// Native kill does not execute actor finalizers.
    let stop (cleanupDeadline: int) (target: Actor<'Msg>) : ActorOp<StopResult> =
        if cleanupDeadline <= 0 then
            invalidArg "cleanupDeadline" "Cleanup deadline must be positive"

        {
            Run =
                fun cont ->
                    cont (
                        if stopProcess target.Pid cleanupDeadline then
                            StopResult.Completed ActorExit.Cancelled
                        else
                            StopResult.TimedOut
                    )
        }

    let stopAsync cleanupDeadline target : Async<StopResult> =
        async {
            let mutable result = StopResult.TimedOut
            (stop cleanupDeadline target).Run(fun value -> result <- value)
            return result
        }

    /// Enable supervision — child EXIT signals become messages.
    ///
    /// assumption: called inside the supervising actor because trap_exit affects only the current BEAM process
    let trapExits () : unit = Platform.trapExits ()

    /// Format a crash reason as a string.
    let formatReason (reason: obj) : string = Platform.formatReason reason

    /// Send a message and await a reply (inside actor { }).
    ///
    /// decision: correlates every call with a fresh Erlang ref so concurrent replies cannot be confused
    /// invariant: waiting for this call consumes only the reply carrying its ref
    let call (actor: Actor<'Msg * ReplyChannel<'Reply>>) (msg: 'Msg) : ActorOp<'Reply> = {
        Run =
            fun cont ->
                let ref = Erlang.makeRef ()
                let callerPid = Erlang.self ()

                let rc: ReplyChannel<'Reply> = {
                    Reply = fun reply -> sendReply callerPid ref reply
                }

                sendMsg actor.Pid (msg, rc)
                cont (recvReply ref)
    }

    /// Send a message and await a reply as an Async (usable from async expressions).
    ///
    /// decision: captures the synchronous CPS result to bridge ActorOp back into shared Async-based APIs
    /// assumption: call invokes its continuation synchronously after the blocking BEAM receive completes
    let callAsync (actor: Actor<'Msg * ReplyChannel<'Reply>>) (msg: 'Msg) : Async<'Reply> =
        async {
            let mutable result = Unchecked.defaultof<'Reply>
            (call actor msg).Run(fun reply -> result <- reply)
            return result
        }

    /// Send a message and await a reply with a timeout in milliseconds.
    /// Raises TimeoutException if no reply is received within the timeout.
    ///
    /// invariant: the selective receive leaves unrelated mailbox messages untouched
    let callWithTimeout (timeout: int) (actor: Actor<'Msg * ReplyChannel<'Reply>>) (msg: 'Msg) : ActorOp<'Reply> = {
        Run =
            fun cont ->
                let ref = Erlang.makeRef ()
                let callerPid = Erlang.self ()

                let rc: ReplyChannel<'Reply> = {
                    Reply = fun reply -> sendReply callerPid ref reply
                }

                sendMsg actor.Pid (msg, rc)

                match recvReplyWithTimeout ref timeout with
                | Some reply -> cont reply
                | None -> raise (System.TimeoutException("Actor call timed out"))
    }

    /// Receive next message (free function).
    let receive<'Msg> () : ActorOp<'Msg> = {
        Run = fun cont -> receiveMsg (fun raw -> cont (unbox<'Msg> raw))
    }

#else

    // decision: runs the body explicitly to observe all three Async exit continuations on every emulated target
    let private spawnOwned token attach (body: Actor<'Msg> -> Async<unit>) =
        let cts = new System.Threading.CancellationTokenSource()
        let lifetime = ActorLifetime cts

        let mb =
            new MailboxProcessor<'Msg>((fun _ -> async { return () }), cancellationToken = cts.Token)

        let inbox = Actor(mb, cts, lifetime)
        // decision: wakes the portable mailbox on cancellation because JS/Python Receive does not register a wakeup
        // invariant: the synthetic wakeup never reaches user code because the workflow token is already cancelled
        lifetime.AddResource(Lifetime.cancellation cts.Token (fun () -> mb.Post Unchecked.defaultof<'Msg>))
#if !FABLE_COMPILER
        lifetime.AddResource(mb :> System.IDisposable)
#endif
        lifetime.AddResource(Lifetime.cancellation token lifetime.RequestStop)
        attach inbox

        Async.StartWithContinuations(
            async { do! body inbox },
            (fun () -> lifetime.Complete ActorExit.Normal),
            (fun ex -> lifetime.Complete(ActorExit.Failed ex)),
            (fun _ -> lifetime.Complete ActorExit.Cancelled),
            cts.Token
        )

        inbox

    /// Spawn cooperative work with an external lifetime token. The registration is released on exit.
    ///
    /// decision: passes the owned token to the workflow so cancellation reaches receives and cooperative Async work
    let spawnWithToken (cancellationToken: System.Threading.CancellationToken) (body: Actor<'Msg> -> Async<unit>) : Actor<'Msg> =
        spawnOwned cancellationToken ignore body

    /// Spawn an actor with an owned cooperative lifetime.
    let spawn (body: Actor<'Msg> -> Async<unit>) : Actor<'Msg> =
        spawnWithToken System.Threading.CancellationToken.None body

    /// Own a child until exit. Parent exit cancels the child; abnormal child exit notifies the parent.
    ///
    /// invariant: normal and cancelled child exits do not emit ChildExited on emulated targets
    /// decision: abnormal child exits notify rather than cancel the parent so existing emulated supervision remains compatible
    let spawnLinked (parent: Actor<'ParentMsg>) (body: Actor<'Msg> -> Async<unit>) : Actor<'Msg> =
        spawnOwned
            System.Threading.CancellationToken.None
            (fun child ->
                parent.Lifetime.Own child.Lifetime

                child.Lifetime.Observe(fun exit ->
                    match exit with
                    | ActorExit.Failed ex -> parent.Post(unbox { Pid = child.Pid; Reason = box ex })
                    | _ -> ())
                |> ignore)
            body

    /// Request cooperative cancellation. Use stop to observe actual exit.
    ///
    /// invariant: after kill returns, Post ignores new messages through this handle
    let kill (actor: Actor<'Msg>) : unit = actor.Lifetime.RequestStop()

    /// Request shutdown and observe workflow and owned descendant exit within a positive deadline.
    let stop (cleanupDeadline: int) (target: Actor<'Msg>) : ActorOp<StopResult> =
        if cleanupDeadline <= 0 then
            invalidArg "cleanupDeadline" "Cleanup deadline must be positive"

        async {
            let! token = Async.CancellationToken

            return!
                Async.FromContinuations(fun (ok, _, cancelled) ->
                    let pending =
                        Lifetime.Pending(fun result ->
                            match result with
                            | Choice1Of2 value -> ok value
                            | Choice2Of2 ex -> cancelled ex)

                    pending.Add(Lifetime.cancellation token (fun () -> pending.Settle(Choice2Of2(System.OperationCanceledException()))))
                    pending.Add(Lifetime.deadline cleanupDeadline (fun () -> pending.Settle(Choice1Of2 StopResult.TimedOut)))
                    pending.Add(target.Lifetime.Observe(fun exit -> pending.Settle(Choice1Of2(StopResult.Completed exit))))
                    target.Lifetime.RequestStop())
        }

    let stopAsync cleanupDeadline target : Async<StopResult> = stop cleanupDeadline target

    /// Enable supervision (stub on non-BEAM).
    ///
    /// decision: remains a no-op because spawnLinked already converts non-BEAM child crashes into messages
    let trapExits () : unit = ()

    /// Send a message and await a reply (inside actor { }).
    let call (target: Actor<'Msg * ReplyChannel<'Reply>>) (msg: 'Msg) : ActorOp<'Reply> =
        actor {
            let! reply =
                target.Mb.PostAndAsyncReply(fun rc -> (msg, { Reply = fun r -> rc.Reply(r) }))

            return reply
        }

    /// Send a message and await a reply as an Async (usable from async { } contexts).
    /// On non-BEAM targets ActorOp = Async, so this is a direct alias for call.
    let callAsync (target: Actor<'Msg * ReplyChannel<'Reply>>) (msg: 'Msg) : Async<'Reply> = call target msg

    /// Send a message and await a reply with a timeout in milliseconds.
    /// Raises TimeoutException if no reply is received within the timeout.
    ///
    /// decision: polls a ReplyChannel every 5 ms because the portable path cannot use MailboxProcessor timeout overloads
    /// tradeoff: timeout detection can lag by one polling interval to keep one implementation for .NET, Python, and JS
    let callWithTimeout (timeout: int) (target: Actor<'Msg * ReplyChannel<'Reply>>) (msg: 'Msg) : ActorOp<'Reply> =
        let mutable result: 'Reply option = None
        let rc: ReplyChannel<'Reply> = { Reply = fun r -> result <- Some r }
        target.Post((msg, rc))

        let step = 5

        let rec wait elapsed =
            actor {
                match result with
                | Some r -> return r
                | None ->
                    if elapsed >= timeout then
                        raise (System.TimeoutException("Actor call timed out"))

                    do! Async.Sleep step
                    return! wait (elapsed + step)
            }

        wait 0

    /// Receive next message (free function, for backwards compatibility).
    let receive<'Msg> (inbox: Actor<'Msg>) : Async<'Msg> = inbox.Receive()

#endif

    // ============================================================================
    // Supervision
    // ============================================================================

#if FABLE_COMPILER_BEAM

    /// Check if a message is a ChildExited notification.
    let tryAsChildExited (msg: obj) : ChildExited option =
        if isChildExited msg then
            Some(unbox<ChildExited> msg)
        else
            None

    /// Spawn a supervised child actor. Retains the body for restart.
    let spawnSupervised
        (parent: Actor<'ParentMsg>)
        (strategy: Strategy)
        (body: Actor<'Msg> -> ActorOp<unit>)
        : SupervisedChild<'ParentMsg, 'Msg> =
        let child = spawnLinked parent body

        {
            Actor = child
            Body = body
            Strategy = strategy
        }

    /// Handle a ChildExited event for a supervised child.
    /// Returns the replacement child if restarted, or Stopped.
    /// Raises ProcessExitException if Escalate.
    ///
    /// assumption: exited belongs to supervised — this function does not compare their process identifiers
    /// invariant: Restart returns the new actor handle instead of relying on target-specific record mutation
    let handleChildExit
        (parent: Actor<'ParentMsg>)
        (supervised: SupervisedChild<'ParentMsg, 'Msg>)
        (exited: ChildExited)
        : ChildExitResult<'ParentMsg, 'Msg> =
        let (OneForOne decider) = supervised.Strategy

        let ex =
            match exited.Reason with
            | :? exn as e -> e
            | r -> ProcessExitException(sprintf "%A" r)

        match decider ex with
        | Directive.Stop -> ChildExitResult.Stopped
        | Directive.Escalate -> raise ex
        | Directive.Restart ->
            let newChild = spawnLinked parent supervised.Body
            ChildExitResult.Restarted { supervised with Actor = newChild }

#else

    /// Check if a message is a ChildExited notification.
    let tryAsChildExited (msg: obj) : ChildExited option =
        match msg with
        | :? ChildExited as ce -> Some ce
        | _ -> None

    /// Spawn a supervised child actor. Retains the body for restart.
    let spawnSupervised
        (parent: Actor<'ParentMsg>)
        (strategy: Strategy)
        (body: Actor<'Msg> -> Async<unit>)
        : SupervisedChild<'ParentMsg, 'Msg> =
        let child = spawnLinked parent body

        {
            Actor = child
            Body = body
            Strategy = strategy
        }

    /// Handle a ChildExited event for a supervised child.
    /// Returns the replacement child if restarted, or Stopped.
    /// Raises ProcessExitException if Escalate.
    ///
    /// assumption: exited belongs to supervised — this function does not compare their process identifiers
    /// invariant: Restart returns the new actor handle instead of relying on target-specific record mutation
    let handleChildExit
        (parent: Actor<'ParentMsg>)
        (supervised: SupervisedChild<'ParentMsg, 'Msg>)
        (exited: ChildExited)
        : ChildExitResult<'ParentMsg, 'Msg> =
        let (OneForOne decider) = supervised.Strategy

        let ex =
            match exited.Reason with
            | :? exn as e -> e
            | r -> ProcessExitException(sprintf "%A" r)

        match decider ex with
        | Directive.Stop -> ChildExitResult.Stopped
        | Directive.Escalate -> raise ex
        | Directive.Restart ->
            let newChild = spawnLinked parent supervised.Body
            ChildExitResult.Restarted { supervised with Actor = newChild }

#endif

    // === Common API (both platforms) ===

    /// Send a message (fire and forget).
    let send (actor: Actor<'Msg>) (msg: 'Msg) : unit = actor.Post(msg)

    /// Fire-and-forget message to a call-capable actor (no-op reply channel).
    ///
    /// decision: supplies a no-op channel so one actor protocol can accept both casts and calls
    let cast (actor: Actor<'Msg * ReplyChannel<'Reply>>) (msg: 'Msg) : unit =
        actor.Post((msg, { Reply = fun _ -> () }))

    /// Start a stateful actor with a message handler.
    ///
    /// decision: threads state through a single receive loop so updates remain private and serialized
    /// invariant: the next message is not handled until the current handler returns its Next value
    let start (initialState: 'State) (handler: 'State -> 'Msg -> Next<'State>) : Actor<'Msg> =
        let body (inbox: Actor<'Msg>) =
            let rec loop state =
                actor {
                    let! msg = inbox.Receive()

                    match handler state msg with
                    | Continue newState -> return! loop newState
                    | Stop -> ()
                    | StopAbnormal ex -> raise ex
                }

            loop initialState

#if FABLE_COMPILER_BEAM
        let rawPid =
            Erlang.spawn (fun () ->
                let me: Actor<'Msg> = { Pid = Erlang.self () }
                (body me).Run(fun () -> ()))

        { Pid = rawPid }
#else
        spawn body
#endif

#if FABLE_COMPILER_BEAM

    /// Schedule a timer callback. Returns a typed handle for cancellation.
    let schedule (ms: int) (callback: unit -> unit) : TimerHandle = TimerHandle(timerSchedule ms callback)

    /// Cancel a scheduled timer.
    let cancelTimer (TimerHandle handle: TimerHandle) : unit = timerCancel handle

#else

    /// Schedule a timer callback. Returns a typed handle for cancellation.
    let schedule (ms: int) (callback: unit -> unit) : TimerHandle =
        let cts = new System.Threading.CancellationTokenSource()

        Async.StartImmediate(
            async {
                do! Async.Sleep ms
                callback ()
            },
            cts.Token
        )

        TimerHandle(box cts)

    /// Cancel a scheduled timer.
    let cancelTimer (TimerHandle handle: TimerHandle) : unit =
        (unbox<System.Threading.CancellationTokenSource> handle).Cancel()

#endif

    /// Extract the raw platform handle from an actor for native interoperability.
    ///
    /// decision: exposes an explicit escape hatch while keeping ordinary messaging behind the typed Actor wrapper
    /// tradeoff: the returned handle is target-specific and is not portable application state
#if FABLE_COMPILER_BEAM
    let pid (actor: Actor<'Msg>) : Pid<'Msg> = actor.Pid
#else
    let pid (actor: Actor<'Msg>) : obj = actor.Pid
#endif
