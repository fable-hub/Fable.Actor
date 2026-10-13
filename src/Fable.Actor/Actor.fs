// Platform-independent Actor abstraction.
//
// BEAM: actor { } is a CPS computation (no-op wrapper, BEAM processes block natively).
// Non-BEAM: actor { } delegates to async { }, Actor wraps MailboxProcessor.
//
// Actor<'Msg> is the typed message reference; ActorInbox<'Msg> adds Receive inside a body:
//   inbox.Receive() — get next message (inside body)
//   actor.Post(msg)  — send a message (from outside)
//
// decision: presents one actor API while selecting native BEAM processes or MailboxProcessor at compile time
// invariant: application actor state crosses process boundaries only through messages
// decision: cooperative cancellation uses runtime-shared tokens outside the application message protocol
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

/// A typed message reference shared by every actor authoring form.
///
/// decision: message references carry their send encoding so private host envelopes stay outside application protocols
/// invariant: reference adaptation preserves the native process identity
/// tradeoff: one encoder call per send lets private host protocols share the same typed message-reference API
[<AllowNullLiteral>]
type Actor<'Msg> internal (pid: Pid<obj>, encode: 'Msg -> obj) =
    member _.Pid: Pid<'Msg> = unbox (box pid)
    member _.Post(message: 'Msg) = sendMsg pid (encode message)
    member internal _.MapInput(mapping: 'Input -> 'Msg) : Actor<'Input> = Actor(pid, mapping >> encode)

/// The message reference and receive operation supplied to a running actor body.
///
/// decision: the inbox composes a message reference so native handles remain process-portable and callers cannot receive private host messages
[<AllowNullLiteral>]
type ActorInbox<'Msg> internal (pid: Pid<obj>) =
    let reference = Actor<'Msg>(pid, box)
    member _.Reference = reference
    member _.Pid = reference.Pid
    member _.Post(message: 'Msg) = reference.Post message

    member _.Receive() : ActorOp<'Msg> = {
        Run = fun cont -> ExecutionNative.receive (fun raw -> cont (unbox<'Msg> raw))
    }

type ActorBuilder() =
    member _.Bind(op: ActorOp<'T>, f: 'T -> ActorOp<'U>) : ActorOp<'U> = {
        Run =
            fun cont ->
                op.Run(fun value ->
                    ExecutionNative.checkpointValue (box value)
                    (f value).Run cont)
    }

    member _.Return(value: 'T) : ActorOp<'T> = {
        Run =
            fun cont ->
                ExecutionNative.checkpointValue (box value)
                cont value
    }

    member _.ReturnFrom(op: ActorOp<'T>) : ActorOp<'T> = op
    member _.Zero() : ActorOp<unit> = { Run = fun cont -> cont () }

    member _.Delay(f: unit -> ActorOp<'T>) : ActorOp<'T> = {
        Run =
            fun cont ->
                ExecutionNative.checkpoint ()
                (f ()).Run cont
    }

    member _.Combine(first: ActorOp<unit>, second: ActorOp<'T>) : ActorOp<'T> = {
        Run = fun cont -> first.Run(fun () -> second.Run cont)
    }

    member _.TryWith(body: ActorOp<'T>, handler: exn -> ActorOp<'T>) : ActorOp<'T> = {
        Run = fun cont -> ExecutionNative.tryWith (fun () -> body.Run cont) (fun ex -> (handler ex).Run cont)
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
                     ExecutionNative.finalize compensation
                     reraise ())

                ExecutionNative.finalize compensation
                cont result
    }

    member this.Using(resource: 'a :> System.IDisposable, body: 'a -> ActorOp<'T>) : ActorOp<'T> =
        // invariant: install disposal before a cancelled delayed body can reject execution
        this.TryFinally(
            this.Delay(fun () -> body resource),
            fun () ->
                if not (isNull (box resource)) then
                    resource.Dispose()
        )

    member _.While(guard: unit -> bool, body: ActorOp<unit>) : ActorOp<unit> = {
        Run =
            fun cont ->
                let rec loop () =
                    ExecutionNative.checkpoint ()
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
        Run =
            fun cont ->
                let value = ExecutionNative.runAsync op
                ExecutionNative.checkpointValue (box value)
                (f value).Run cont
    }

#else

// === Non-BEAM: MailboxProcessor-based ===

// decision: aliases actor operations to Async so existing Fable runtimes provide scheduling and cancellation
// decision: guards actor binders and finalizers where portable Async cancellation differs from the owned lifecycle contract

type ActorOp<'T> = Async<'T>

/// A typed message reference shared by every actor authoring form.
///
/// decision: typed delivery avoids per-message .NET boxing while adapted references wrap private host envelopes
/// invariant: posting after cancellation or workflow exit is a no-op
/// invariant: adaptation preserves mailbox identity, lifetime and raw system-message delivery
/// tradeoff: typed delivery callbacks preserve a common reference contract without boxing ordinary value messages
[<AllowNullLiteral>]
type Actor<'Msg>
    internal
    (pid: obj, cts: System.Threading.CancellationTokenSource, lifetime: ActorLifetime, deliver: 'Msg -> unit, deliverSystem: obj -> unit) =
    member internal _.Cts = cts
    member internal _.Lifetime = lifetime
    member _.Pid = pid

    member _.Post(message: 'Msg) =
        lifetime.Post(fun () -> deliver message)

    member internal _.PostSystem(message: obj) =
        lifetime.Post(fun () -> deliverSystem message)

    member internal _.MapInput(mapping: 'Input -> 'Msg) : Actor<'Input> =
        Actor(pid, cts, lifetime, mapping >> deliver, deliverSystem)

/// The message reference and receive operation supplied to a running actor body.
///
/// decision: the inbox composes a message reference so native handles remain process-portable and callers cannot receive private host messages
[<AllowNullLiteral>]
type ActorInbox<'Msg> internal (mb: MailboxProcessor<'Msg>, cts: System.Threading.CancellationTokenSource, lifetime: ActorLifetime) =
    // decision: delivery callbacks capture only the mailbox so a shared reference does not retain its body inbox
    let reference =
        let deliveryMailbox = mb

        Actor<'Msg>(
            box mb,
            cts,
            lifetime,
            (fun message -> deliveryMailbox.Post message),
            (fun message -> deliveryMailbox.Post(unbox message))
        )

    member _.Reference = reference
    member _.Pid = reference.Pid
    member internal _.Lifetime = lifetime
    member _.Post(message: 'Msg) = reference.Post message
    // TODO(upstream): this check pairs with spawnOwned's synthetic cancellation wakeup.
    // Retire both only after published JS/Python Receive wakes and cancels correctly;
    // https://github.com/fable-compiler/Fable/pull/5038 documents this remaining mailbox gap.
    member _.Receive() : Async<'Msg> =
#if FABLE_COMPILER_PYTHON || FABLE_COMPILER_JAVASCRIPT
        // decision: reuses the workflow trampoline so recursive immediately completed receives remain stack safe
        Lifetime.withContextRun (fun ((ok, error, cancelled), _) run ->
            run
                (mb.Receive())
                ((fun msg ->
                    if cts.IsCancellationRequested then
                        cancelled (System.OperationCanceledException())
                    else
                        ok msg),
                 error,
                 cancelled))
#else
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
#endif


type ActorBuilder() =
    // decision: delays nonresource portable binders so cancellation rejects user code while acquired resources can first install Using
    member _.Bind(op: Async<'T>, f: 'T -> Async<'U>) : Async<'U> =
#if FABLE_COMPILER
        async.Bind(
            op,
            fun value ->
                // invariant: a resource handed back after cancellation reaches Using before its next checkpoint
                match box value with
                | :? System.IDisposable -> f value
                | _ -> async.Delay(fun () -> f value)
        )
#else
        // invariant: resource-producing binders enter their finalizer before the next cancellation check
        async.Bind(op, f)
#endif

    member _.Return(value: 'T) : Async<'T> = async.Return(value)
    member _.ReturnFrom(op: Async<'T>) : Async<'T> = async.ReturnFrom(op)
    member _.Zero() : Async<unit> = async.Zero()

    /// Delay the user expression until its actor continuation runs.
    ///
    /// invariant: an already cancelled actor continuation does not evaluate its delayed user expression
    member _.Delay(f: unit -> Async<'T>) : Async<'T> = async.Delay f

    member _.Combine(first: Async<unit>, second: Async<'T>) : Async<'T> =
        async.Combine(first, async.Delay(fun () -> second))

    member _.TryWith(body: Async<'T>, handler: exn -> Async<'T>) : Async<'T> = async.TryWith(body, handler)

    /// Run cleanup once and preserve its failure, including during cancellation.
    ///
    /// invariant: duplicate terminal callbacks from supplied Async work cannot repeat cleanup
    ///
    /// decision: routes cleanup exceptions explicitly because standard Async finalizers can lose failures during cancellation
    member _.TryFinally(body: Async<'T>, compensation: unit -> unit) : Async<'T> =
#if !FABLE_COMPILER
        Lifetime.finallyNative body compensation
#else
        Lifetime.withContextRun (fun ((ok, error, cancelled), token) run ->
            let gate = Lifetime.newGate ()
            let mutable finished = false

            let finish next value =
                // invariant: a duplicate terminal callback from a supplied Async cannot repeat actor cleanup
                let run =
                    Lifetime.synchronize gate (fun () ->
                        if finished then
                            false
                        else
                            finished <- true
                            true)

                if run then
                    let failure =
                        try
                            compensation ()
                            None
                        with ex ->
                            Some ex

                    match failure with
                    | Some ex -> error ex
                    | None -> next value

            if token.IsCancellationRequested then
                finish cancelled (System.OperationCanceledException())
            else
                run body (finish ok, finish error, finish cancelled))
#endif

    member this.Using(resource: 'a :> System.IDisposable, body: 'a -> Async<'T>) : Async<'T> =
        this.TryFinally(
            this.Delay(fun () -> body resource),
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
    Body: ActorInbox<'Msg> -> ActorOp<unit>
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
module internal ActorRuntime =

#if FABLE_COMPILER_BEAM

    /// Spawn an actor. Body receives inbox (self-reference) for Receive/Post.
    let spawn (body: ActorInbox<'Msg> -> ActorOp<unit>) : ActorInbox<'Msg> =
        let rawPid =
            Erlang.spawn (fun () ->
                let me = ActorInbox<'Msg>(Erlang.self<obj>())
                (body me).Run(fun () -> ()))

        ActorInbox<'Msg>(unbox (box rawPid))

    /// Spawn with external cancellation. BEAM cancellation requests native kill; it cannot unwind arbitrary ActorOp work.
    ///
    /// decision: releases the external registration in a watcher because kill cannot execute actor finalizers
    let spawnWithToken (token: System.Threading.CancellationToken) (body: ActorInbox<'Msg> -> ActorOp<unit>) : ActorInbox<'Msg> =
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
    let spawnLinked (parent: Actor<'ParentMsg>) (body: ActorInbox<'Msg> -> ActorOp<unit>) : ActorInbox<'Msg> =
        let rawPid =
            spawnOwnedProcess parent.Pid (fun () ->
                let me = ActorInbox<'Msg>(Erlang.self<obj>())
                (body me).Run(fun () -> ()))

        ActorInbox<'Msg>(unbox (box rawPid))

    /// Get own pid (only valid inside actor body).
    let self<'Msg> () : ActorInbox<'Msg> = ActorInbox(Erlang.self<obj>())

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

    /// Async bridge for shutdown observation; cleanupDeadline is a positive number of milliseconds.
    let asyncStop cleanupDeadline target : Async<StopResult> =
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

    /// Monitored call with a revocable reply destination.
    ///
    /// decision: uses a process alias so late replies are dropped before entering the caller mailbox
    /// invariant: every settlement removes its alias, monitor, and any already queued duplicate replies
    /// tradeoff: checks explicit caller cancellation in receive slices of at most 5 ms without a per-call helper process
    let private callCore
        timeout
        (token: System.Threading.CancellationToken)
        (ambient: System.Threading.CancellationToken)
        (target: Actor<'Msg * ReplyChannel<'Reply>>)
        msg
        : ActorOp<CallResult<'Reply>> = {
        Run =
            fun cont ->
                let alias, monitor = beginCall target.Pid

                let result =
                    try
                        let expires =
                            timeout
                            |> Option.map (fun ms -> monotonicMilliseconds () + int64 ms)

                        let rec wait () =
                            if
                                ambient.IsCancellationRequested
                                || token.IsCancellationRequested
                            then
                                CallResult.Cancelled
                            else
                                let remaining =
                                    expires
                                    |> Option.map (fun value -> value - monotonicMilliseconds ())

                                match remaining with
                                | Some ms when ms <= 0L -> CallResult.TimedOut
                                | _ ->
                                    let slice =
                                        remaining
                                        |> Option.map (fun ms -> min 5 (int ms))
                                        |> Option.defaultValue 5

                                    match recvCall alias monitor slice with
                                    | 0, value -> CallResult.Reply(unbox value)
                                    | 1, reason -> CallResult.TargetTerminated(exitReason reason)
                                    | _ -> wait ()

                        if
                            ambient.IsCancellationRequested
                            || token.IsCancellationRequested
                        then
                            CallResult.Cancelled
                        else
                            target.Post(
                                msg,
                                {
                                    Reply = fun value -> sendAliasReply alias value
                                }
                            )

                            wait ()
                    finally
                        endCall alias monitor

                cont result
    }

    let private replyOrRaise =
        function
        | CallResult.Reply value -> value
        | CallResult.TimedOut -> raise (System.TimeoutException("Actor call timed out"))
        | CallResult.TargetTerminated exit -> raise (TargetTerminatedException exit)
        | CallResult.Cancelled -> raise (System.OperationCanceledException())

    /// Await one request with a positive reply deadline and explicit caller lifetime.
    let callResult deadline token target msg : ActorOp<CallResult<'Reply>> =
        if deadline <= 0 then
            invalidArg "deadline" "Reply deadline must be positive"

        // invariant: reused operations inherit the executing actor's token rather than their constructing process's token
        {
            Run = fun cont -> (callCore (Some deadline) token (ExecutionNative.token ()) target msg).Run cont
        }

    let private asyncCallCore timeout token target msg : Async<CallResult<'Reply>> =
        async {
            let! ambient = Async.CancellationToken

            return!
                Async.FromContinuations(fun (ok, error, cancelled) ->
                    let mutable result = CallResult.TimedOut

                    let failure =
                        try
                            (callCore timeout token ambient target msg).Run(fun value -> result <- value)
                            None
                        with ex ->
                            Some ex

                    match failure with
                    | Some ex -> error ex
                    | None ->
                        match result with
                        | CallResult.Cancelled when ambient.IsCancellationRequested -> cancelled (System.OperationCanceledException())
                        | _ -> ok result)
        }

    /// Async bridge for a bounded request with explicit outcomes; deadline is positive milliseconds.
    let asyncCallResult deadline token target msg : Async<CallResult<'Reply>> =
        if deadline <= 0 then
            invalidArg "deadline" "Reply deadline must be positive"

        asyncCallCore (Some deadline) token target msg

    /// Await a reply without a reply deadline, preserving rc.11's default. Native target death settles the wait.
    let call target msg : ActorOp<'Reply> = {
        Run =
            fun cont ->
                (callCore None System.Threading.CancellationToken.None (ExecutionNative.token ()) target msg)
                    .Run(fun result ->
                        ExecutionNative.checkpoint ()
                        cont (replyOrRaise result))
    }

    /// Async bridge for awaiting a reply without a reply deadline.
    let asyncCall target msg : Async<'Reply> =
        async {
            let! result = asyncCallCore None System.Threading.CancellationToken.None target msg
            return replyOrRaise result
        }

    let callWithTimeout deadline target msg : ActorOp<'Reply> =
        if deadline <= 0 then
            invalidArg "deadline" "Reply deadline must be positive"

        {
            Run =
                fun cont ->
                    (callCore (Some deadline) System.Threading.CancellationToken.None (ExecutionNative.token ()) target msg)
                        .Run(fun result ->
                            ExecutionNative.checkpoint ()
                            cont (replyOrRaise result))
        }

    /// Async bridge for awaiting a reply within a positive deadline in milliseconds.
    let asyncCallWithTimeout deadline target msg : Async<'Reply> =
        if deadline <= 0 then
            invalidArg "deadline" "Reply deadline must be positive"

        async {
            let! result =
                asyncCallCore (Some deadline) System.Threading.CancellationToken.None target msg

            return replyOrRaise result
        }

    /// Receive next message (free function).
    let receive<'Msg> () : ActorOp<'Msg> = {
        Run = fun cont -> ExecutionNative.receive (fun raw -> cont (unbox<'Msg> raw))
    }

#else

    // decision: runs the body explicitly to observe all three Async exit continuations on every emulated target
    let private spawnOwned token attach (body: ActorInbox<'Msg> -> Async<unit>) =
        let cts = new System.Threading.CancellationTokenSource()
        let lifetime = ActorLifetime cts

        let mb =
            new MailboxProcessor<'Msg>((fun _ -> async { return () }), cancellationToken = cts.Token)

        let inbox = ActorInbox(mb, cts, lifetime)
        // TODO(upstream): mailbox wakeup is not fixed by https://github.com/fable-compiler/Fable/pull/5038.
        // Retire this synthetic post and Receive's cancellation check only when a published
        // JS/Python mailbox wakes idle Receive on cancellation without delivering user work.
        // decision: wakes the portable mailbox on cancellation because JS/Python Receive does not register a wakeup
        // invariant: the synthetic wakeup never reaches user code because the workflow token is already cancelled
        lifetime.AddResource(Lifetime.cancellation cts.Token (fun () -> mb.Post Unchecked.defaultof<'Msg>))
#if !FABLE_COMPILER
        lifetime.AddResource(mb :> System.IDisposable)
#endif
        lifetime.AddResource(Lifetime.cancellation token lifetime.RequestStop)

        try
            attach inbox
        with ex ->
            // invariant: rejected ownership releases startup resources without evaluating the child body
            lifetime.Complete(ActorExit.Failed ex)
            reraise ()

        let workflow = async.Delay(fun () -> body inbox)

        let start () =
            Async.StartWithContinuations(
                workflow,
                (fun () -> lifetime.Complete ActorExit.Normal),
                (fun ex -> lifetime.Complete(ActorExit.Failed ex)),
#if FABLE_COMPILER
                (fun _ -> lifetime.Complete ActorExit.Cancelled),
#else
                (fun ex -> lifetime.Complete(Lifetime.cancelledExit ex)),
#endif
                cts.Token
            )

#if FABLE_COMPILER
        start ()
#else
        // decision: queues startup independently so synchronous actor work cannot block the spawning thread
        System.Threading.ThreadPool.QueueUserWorkItem(fun _ -> start ())
        |> ignore
#endif

        inbox

    /// Spawn cooperative work with an external lifetime token. The registration is released on exit.
    ///
    /// decision: passes the owned token to the workflow so cancellation reaches receives and cooperative Async work
    let spawnWithToken (cancellationToken: System.Threading.CancellationToken) (body: ActorInbox<'Msg> -> Async<unit>) : ActorInbox<'Msg> =
        spawnOwned cancellationToken ignore body

    /// Spawn an actor with an owned cooperative lifetime.
    let spawn (body: ActorInbox<'Msg> -> Async<unit>) : ActorInbox<'Msg> =
        spawnWithToken System.Threading.CancellationToken.None body

    // invariant: normal and cancelled child exits do not emit ChildExited on emulated targets
    // decision: abnormal child exits notify rather than cancel the parent so existing emulated supervision remains compatible
    let private spawnLinkedCore retainChildFailure (parent: Actor<'ParentMsg>) (body: ActorInbox<'Msg> -> Async<unit>) : ActorInbox<'Msg> =
        spawnOwned
            System.Threading.CancellationToken.None
            (fun child ->
                parent.Lifetime.Own(child.Lifetime, retainChildFailure)

                child.Lifetime.Observe(fun exit ->
                    match exit with
                    | ActorExit.Failed ex ->
                        // invariant: shutdown rejects notifications before converting them to the parent's application message type
                        parent.PostSystem(box { Pid = child.Pid; Reason = box ex })
                    | _ -> ())
                |> ignore)
            body

    /// Own a child until exit. Parent exit cancels the child; abnormal child exit notifies the parent.
    let spawnLinked (parent: Actor<'ParentMsg>) (body: ActorInbox<'Msg> -> Async<unit>) : ActorInbox<'Msg> =
        spawnLinkedCore false parent body

    // decision: cooperative ownership retains earlier descendant defects without changing legacy supervision's handled-failure semantics
    let internal spawnCooperativeLinked parent body = spawnLinkedCore true parent body

    /// Request cooperative cancellation. Use stop to observe actual exit.
    ///
    /// invariant: after kill returns, Post ignores new messages through this handle
    let kill (actor: Actor<'Msg>) : unit = actor.Lifetime.RequestStop()

    /// Request shutdown and observe workflow and owned descendant exit within a positive deadline.
    let stop (cleanupDeadline: int) (target: Actor<'Msg>) : ActorOp<StopResult> =
        if cleanupDeadline <= 0 then
            invalidArg "cleanupDeadline" "Cleanup deadline must be positive"

        async.Delay(fun () ->
            Lifetime.withContext (fun ((ok, _, cancelled), token) ->
                let pending =
                    Lifetime.Pending(fun result ->
                        match result with
                        | Choice1Of2 value -> ok value
                        | Choice2Of2 ex -> cancelled ex)

                pending.Add(Lifetime.cancellation token (fun () -> pending.Settle(Choice2Of2(System.OperationCanceledException()))))
                pending.Add(Lifetime.deadline cleanupDeadline (fun () -> pending.Settle(Choice1Of2 StopResult.TimedOut)))
                pending.Add(target.Lifetime.Observe(fun exit -> pending.Settle(Choice1Of2(StopResult.Completed exit))))
                target.Lifetime.RequestStop()))

    /// Async bridge for shutdown observation; cleanupDeadline is a positive number of milliseconds.
    let asyncStop cleanupDeadline target : Async<StopResult> = stop cleanupDeadline target

    /// Enable supervision (stub on non-BEAM).
    ///
    /// decision: remains a no-op because spawnLinked already converts non-BEAM child crashes into messages
    let trapExits () : unit = ()

    /// One per-call settlement owns its reply callback, admission observer, timer, and caller registrations.
    ///
    /// invariant: each delivered request has one reply destination and settles at most once without retries
    /// invariant: a settled late-reply callback retains no caller continuation or reply value
    let private callCore
        timeout
        (token: System.Threading.CancellationToken)
        (target: Actor<'Msg * ReplyChannel<'Reply>>)
        msg
        : Async<CallResult<'Reply>> =
        Lifetime.withContext (fun ((ok, _, cancelled), ambient) ->
            let pending =
                Lifetime.Pending(fun result ->
                    match result with
                    | Choice1Of2 value -> ok value
                    | Choice2Of2 ex -> cancelled ex)

            let settle result = pending.Settle(Choice1Of2 result)
            pending.Add(Lifetime.cancellation ambient (fun () -> pending.Settle(Choice2Of2(System.OperationCanceledException()))))
            pending.Add(Lifetime.cancellation token (fun () -> settle CallResult.Cancelled))

            timeout
            |> Option.iter (fun ms -> pending.Add(Lifetime.deadline ms (fun () -> settle CallResult.TimedOut)))

            pending.Add(target.Lifetime.ObserveTermination(fun exit -> settle (CallResult.TargetTerminated exit)))

            if not pending.IsCompleted then
                target.Post(
                    msg,
                    {
                        Reply = fun value -> settle (CallResult.Reply value)
                    }
                ))

    /// Await one request with a positive reply deadline and explicit caller lifetime.
    /// Timeout or cancellation does not undo processing. Stop closes request admission and settles outstanding waits.
    let callResult deadline token target msg : ActorOp<CallResult<'Reply>> =
        if deadline <= 0 then
            invalidArg "deadline" "Reply deadline must be positive"

        callCore (Some deadline) token target msg

    /// Async bridge for a bounded request with explicit outcomes; deadline is positive milliseconds.
    let asyncCallResult deadline token target msg : Async<CallResult<'Reply>> = callResult deadline token target msg

    let private replyOrRaise =
        function
        | CallResult.Reply value -> value
        | CallResult.TimedOut -> raise (System.TimeoutException("Actor call timed out"))
        | CallResult.TargetTerminated exit -> raise (TargetTerminatedException exit)
        | CallResult.Cancelled -> raise (System.OperationCanceledException())

    /// Await a reply without a reply deadline, preserving rc.11's default. Target shutdown and caller cancellation settle the wait.
    let call target msg : ActorOp<'Reply> =
        async.Delay(fun () ->
            async {
                let! result = callCore None System.Threading.CancellationToken.None target msg
                return replyOrRaise result
            })

    /// Async bridge for awaiting a reply without a reply deadline.
    let asyncCall target msg : Async<'Reply> = call target msg

    /// Await a reply within a positive deadline; raises on timeout, target termination, or cancellation.
    let callWithTimeout deadline target msg : ActorOp<'Reply> =
        if deadline <= 0 then
            invalidArg "deadline" "Reply deadline must be positive"

        async.Delay(fun () ->
            async {
                let! result =
                    callCore (Some deadline) System.Threading.CancellationToken.None target msg

                return replyOrRaise result
            })

    /// Async bridge for awaiting a reply within a positive deadline in milliseconds.
    let asyncCallWithTimeout deadline target msg : Async<'Reply> = callWithTimeout deadline target msg

    /// Receive next message (free function, for backwards compatibility).
    let receive<'Msg> (inbox: ActorInbox<'Msg>) : Async<'Msg> = inbox.Receive()

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
        (body: ActorInbox<'Msg> -> ActorOp<unit>)
        : SupervisedChild<'ParentMsg, 'Msg> =
        let child = spawnLinked parent body

        {
            Actor = child.Reference
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

            ChildExitResult.Restarted {
                supervised with
                    Actor = newChild.Reference
            }

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
        (body: ActorInbox<'Msg> -> Async<unit>)
        : SupervisedChild<'ParentMsg, 'Msg> =
        let child = spawnLinked parent body

        {
            Actor = child.Reference
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

            ChildExitResult.Restarted {
                supervised with
                    Actor = newChild.Reference
            }

#endif

    // === Common API (both platforms) ===

    /// Send a message (fire and forget).
    let send (actor: Actor<'Msg>) (msg: 'Msg) : unit = actor.Post(msg)

    /// Fire-and-forget message to a call-capable actor (no-op reply channel).
    ///
    /// decision: supplies a no-op channel so one actor protocol can accept both casts and calls
    let cast (actor: Actor<'Msg * ReplyChannel<'Reply>>) (msg: 'Msg) : unit =
        actor.Post((msg, { Reply = fun _ -> () }))

    // decision: one state loop serves ordinary and cooperative factories without changing handler termination semantics
    // invariant: the next message is not handled until the current handler returns its Next value
    let stateBody (initialState: 'State) (handler: 'State -> 'Msg -> Next<'State>) (receive: unit -> ActorOp<'Msg>) =
        let rec loop state =
            actor {
                let! msg = receive ()

                match handler state msg with
                | Continue newState -> return! loop newState
                | Stop -> return ()
                | StopAbnormal ex -> return raise ex
            }

        loop initialState

    let start initialState handler =
        spawn (fun inbox -> stateBody initialState handler inbox.Receive)

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
