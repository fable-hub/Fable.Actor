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

type Actor<'Msg> = {
    Mb: MailboxProcessor<'Msg>
    Cts: System.Threading.CancellationTokenSource
} with

    member this.Pid: obj = box this.Mb

    member this.Receive() : Async<'Msg> = this.Mb.Receive()

    /// Queue a message unless this actor has already been killed.
    ///
    /// invariant: posting after cancellation is a no-op rather than an exception
    member this.Post(msg: 'Msg) =
        if not this.Cts.IsCancellationRequested then
            this.Mb.Post(msg)

type ActorBuilder() =
    member _.Bind(op: Async<'T>, f: 'T -> Async<'U>) : Async<'U> = async.Bind(op, f)
    member _.Return(value: 'T) : Async<'T> = async.Return(value)
    member _.ReturnFrom(op: Async<'T>) : Async<'T> = async.ReturnFrom(op)
    member _.Zero() : Async<unit> = async.Zero()
    member _.Delay(f: unit -> Async<'T>) : Async<'T> = async.Delay(f)

    member _.Combine(first: Async<unit>, second: Async<'T>) : Async<'T> =
        async.Combine(first, async.Delay(fun () -> second))

    member _.TryWith(body: Async<'T>, handler: exn -> Async<'T>) : Async<'T> = async.TryWith(body, handler)

    member _.TryFinally(body: Async<'T>, compensation: unit -> unit) : Async<'T> = async.TryFinally(body, compensation)

    member _.Using(resource: 'a :> System.IDisposable, body: 'a -> Async<'T>) : Async<'T> = async.Using(resource, body)

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

    /// Spawn a linked child actor (parent gets EXIT signal on crash).
    ///
    /// assumption: the caller is the supplied parent process — BEAM links the child to the calling process
    let spawnLinked (_parent: Actor<'ParentMsg>) (body: Actor<'Msg> -> ActorOp<unit>) : Actor<'Msg> =
        let rawPid =
            Erlang.spawnLink (fun () ->
                let me: Actor<'Msg> = { Pid = Erlang.self () }
                (body me).Run(fun () -> ()))

        { Pid = rawPid }

    /// Get own pid (only valid inside actor body).
    let self<'Msg> () : Actor<'Msg> = { Pid = Erlang.self () }

    /// Kill an actor and its linked children.
    let kill (actor: Actor<'Msg>) : unit = killProcess actor.Pid

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

    /// Spawn an actor with an optional external cancellation token.
    /// When the external token is cancelled, the actor's CTS is also cancelled.
    ///
    /// decision: links external cancellation to a private CTS instead of putting the token in the actor's Async context
    /// invariant: cancelling the external token prevents subsequent posts through the returned actor handle
    let spawnWithToken (cancellationToken: System.Threading.CancellationToken) (body: Actor<'Msg> -> Async<unit>) : Actor<'Msg> =
        let cts = new System.Threading.CancellationTokenSource()

        cancellationToken.Register(fun () -> cts.Cancel())
        |> ignore

        let mutable inbox: Actor<'Msg> option = None

        let mb =
            MailboxProcessor.Start(fun mb ->
                let actor = { Mb = mb; Cts = cts }
                inbox <- Some actor
                body actor)

        match inbox with
        | Some a -> a
        | None -> { Mb = mb; Cts = cts }

    /// Spawn an actor. Body receives inbox (self-reference) for Receive/Post.
    ///
    /// decision: owns a private CTS for explicit kill semantics without polluting operations in the actor body
    let spawn (body: Actor<'Msg> -> Async<unit>) : Actor<'Msg> =
        let cts = new System.Threading.CancellationTokenSource()
        let mutable inbox: Actor<'Msg> option = None

        let mb =
            MailboxProcessor.Start(fun mb ->
                let actor = { Mb = mb; Cts = cts }
                inbox <- Some actor
                body actor)

        match inbox with
        | Some a -> a
        | None -> { Mb = mb; Cts = cts }

    /// Spawn a linked child actor. On crash, delivers ChildExited to parent.
    ///
    /// decision: emulates BEAM links by converting an unhandled body exception into a message to the parent
    /// invariant: normal body completion does not emit ChildExited on non-BEAM targets
    let spawnLinked (parent: Actor<'ParentMsg>) (body: Actor<'Msg> -> Async<unit>) : Actor<'Msg> =
        let cts = new System.Threading.CancellationTokenSource()
        let mutable inbox: Actor<'Msg> option = None

        let mb =
            MailboxProcessor.Start(fun mb ->
                let actor = { Mb = mb; Cts = cts }
                inbox <- Some actor

                async {
                    try
                        do! body actor
                    with ex ->
                        parent.Post(unbox { Pid = box mb; Reason = box ex })
                })

        match inbox with
        | Some a -> a
        | None -> { Mb = mb; Cts = cts }

    /// Kill an actor by cancelling its lifecycle and disposing its mailbox.
    ///
    /// invariant: after kill returns, Post ignores new messages through this handle
    let kill (actor: Actor<'Msg>) : unit =
        actor.Cts.Cancel()
        (actor.Mb :> System.IDisposable).Dispose()

    /// Enable supervision (stub on non-BEAM).
    ///
    /// decision: remains a no-op because spawnLinked already converts non-BEAM child crashes into messages
    let trapExits () : unit = ()

    /// Send a message and await a reply (inside actor { }).
    let call (target: Actor<'Msg * ReplyChannel<'Reply>>) (msg: 'Msg) : ActorOp<'Reply> =
        actor {
            let! reply = target.Mb.PostAndAsyncReply(fun rc -> (msg, { Reply = fun r -> rc.Reply(r) }))

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
