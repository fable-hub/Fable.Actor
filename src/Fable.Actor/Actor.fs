// Platform-independent Actor abstraction.
//
// BEAM: actor { } is a CPS computation (no-op wrapper, BEAM processes block natively).
// Non-BEAM: actor { } delegates to async { }, Actor wraps MailboxProcessor.
//
// Actor<'Msg> provides MailboxProcessor-compatible API:
//   inbox.Receive() — get next message (inside body)
//   actor.Post(msg)  — send a message (from outside)
namespace Fable.Actor

open Fable.Actor.Types

// ============================================================================
// Actor type + Computation expression
// ============================================================================

#if FABLE_COMPILER_BEAM

open Fable.Beam
open Fable.Actor.Platform

// === BEAM: CPS-based, native processes ===

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

#else

// === Non-BEAM: MailboxProcessor-based ===

type ActorOp<'T> = Async<'T>

type Actor<'Msg> = {
    Mb: MailboxProcessor<'Msg>
    Cts: System.Threading.CancellationTokenSource
} with

    member this.Pid: obj = box this.Mb

    member this.Receive() : Async<'Msg> = this.Mb.Receive()

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

#endif

/// A supervised child — wraps an actor ref with restart capability.
type SupervisedChild<'ParentMsg, 'Msg> = {
    mutable Actor: Actor<'Msg>
    Body: Actor<'Msg> -> ActorOp<unit>
    Strategy: Strategy
}

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
    let trapExits () : unit = Platform.trapExits ()

    /// Format a crash reason as a string.
    let formatReason (reason: obj) : string = Platform.formatReason reason

    /// Send a message and await a reply (inside actor { }).
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

    /// Send a message and await a reply with a timeout in milliseconds.
    /// Raises TimeoutException if no reply is received within the timeout.
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

    /// Kill an actor — cancels its token and disposes the MailboxProcessor.
    let kill (actor: Actor<'Msg>) : unit =
        actor.Cts.Cancel()
        (actor.Mb :> System.IDisposable).Dispose()

    /// Enable supervision (stub on non-BEAM).
    let trapExits () : unit = ()

    /// Send a message and await a reply (inside actor { }).
    let call (target: Actor<'Msg * ReplyChannel<'Reply>>) (msg: 'Msg) : ActorOp<'Reply> =
        actor {
            let! reply = target.Mb.PostAndAsyncReply(fun rc -> (msg, { Reply = fun r -> rc.Reply(r) }))

            return reply
        }

    /// Send a message and await a reply with a timeout in milliseconds.
    /// Raises TimeoutException if no reply is received within the timeout.
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
    /// Returns true if the child was restarted, false if stopped/not matching.
    /// Raises ProcessExitException if Escalate.
    let handleChildExit (parent: Actor<'ParentMsg>) (supervised: SupervisedChild<'ParentMsg, 'Msg>) (exited: ChildExited) : bool =
        let (OneForOne decider) = supervised.Strategy

        let ex =
            match exited.Reason with
            | :? exn as e -> e
            | r -> ProcessExitException(sprintf "%A" r)

        match decider ex with
        | Directive.Stop -> false
        | Directive.Escalate -> raise ex
        | Directive.Restart ->
            let newChild = spawnLinked parent supervised.Body
            supervised.Actor <- newChild
            true

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
    /// Returns true if the child was restarted, false if stopped/not matching.
    /// Raises ProcessExitException if Escalate.
    let handleChildExit (parent: Actor<'ParentMsg>) (supervised: SupervisedChild<'ParentMsg, 'Msg>) (exited: ChildExited) : bool =
        let (OneForOne decider) = supervised.Strategy

        let ex =
            match exited.Reason with
            | :? exn as e -> e
            | r -> ProcessExitException(sprintf "%A" r)

        match decider ex with
        | Directive.Stop -> false
        | Directive.Escalate -> raise ex
        | Directive.Restart ->
            let newChild = spawnLinked parent supervised.Body
            supervised.Actor <- newChild
            true

#endif

    // === Common API (both platforms) ===

    /// Send a message (fire and forget).
    let send (actor: Actor<'Msg>) (msg: 'Msg) : unit = actor.Post(msg)

    /// Fire-and-forget message to a call-capable actor (no-op reply channel).
    let cast (actor: Actor<'Msg * ReplyChannel<'Reply>>) (msg: 'Msg) : unit =
        actor.Post((msg, { Reply = fun _ -> () }))

    /// Start a stateful actor with a message handler.
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

    /// Extract the raw platform handle from an actor.
#if FABLE_COMPILER_BEAM
    let pid (actor: Actor<'Msg>) : Pid<'Msg> = actor.Pid
#else
    let pid (actor: Actor<'Msg>) : obj = actor.Pid
#endif
