namespace Fable.Actor

open Fable.Actor.Types

/// Creator-held cooperative lifetime control. Share its ordinary Actor reference for messaging.
/// Native observation state lasts until release or the creating process dies.
type CooperativeActor<'Message> = internal CooperativeActor of Actor<'Message> * obj

/// Inbox for a cooperative actor body, with its lifetime-control capability.
///
/// decision: the body retains its owner capability for cooperative child attachment while shared references expose only messaging
[<AllowNullLiteral>]
type CooperativeInbox<'Message> internal (inbox: ActorInbox<'Message>, owner: CooperativeActor<'Message>) =
    member _.Owner = owner
    member _.Reference: Actor<'Message> = inbox.Reference
    member _.Receive() = inbox.Receive()
    member _.Post(message: 'Message) = inbox.Post message

/// Whether terminal observation state could be released.
[<RequireQualifiedAccess>]
type LifetimeRelease =
    /// Terminal observation state was released. The lifetime capability must not be used again.
    | Released
    /// Work or descendant cleanup is still running; observation state remains available.
    | Running

/// Opt-in cooperative actors with observed descendant cleanup and retained exits.
/// Native kill remains available through the ordinary Actor reference as an advanced escape hatch.
/// The once-only shielded finalizer contract belongs to the actor CE, not to nested plain Async finalizers.
///
/// decision: keeps strong lifetime policy opt-in through cooperative factories on Actor
/// decision: supplies lifetime context at spawn so ordinary and cooperative actors share the same actor expression builder
/// decision: separates lifetime authority from message references to keep native kill compatibility explicit
/// tradeoff: a long-lived BEAM creator must release completed capabilities to reclaim retained terminal outcomes
[<RequireQualifiedAccess>]
module internal LifetimesRuntime =
    /// Obtain an ordinary message reference without exposing cooperative lifetime control.
    let reference (CooperativeActor(target, _)) = target

#if FABLE_COMPILER_BEAM
    open Fable.Core
    open Fable.Core.BeamInterop

    let private workerPid (handle: obj) : Fable.Beam.Pid<'Message> =
        Fable.Beam.Erlang.element 1 handle |> unbox

    let private create parent body =
        let handle =
            LifetimesNative.spawn parent (fun handle ->
                let inbox = ActorInbox<'Message>(unbox (box (workerPid handle)))

                body (CooperativeInbox(inbox, CooperativeActor(inbox.Reference, handle)))
                |> fun work -> work.Run ignore)

        CooperativeActor((ActorInbox<'Message>(unbox (box (workerPid handle)))).Reference, handle)

    /// Spawn a cooperative owner. Cancellation wakes receives and flows through sequential Async adapters.
    /// Arbitrary synchronous work may outlive a stop deadline. Exit history is scoped to the creator process.
    let spawn (body: CooperativeInbox<'Message> -> ActorOp<unit>) : CooperativeActor<'Message> = create (box None) body

    /// Attach before starting the child; parent completion cancels and observes every cooperative descendant.
    /// A parent whose terminal result is already published rejects attachment.
    let spawnLinked (CooperativeActor(_, parent)) (body: CooperativeInbox<'Message> -> ActorOp<unit>) : CooperativeActor<'Message> =
        create parent body

    /// Request cooperative cancellation and observe actual body/descendant exit. Deadline never requests native kill.
    /// Repeated stop returns the retained exit while this capability remains valid.
    let stop deadline (CooperativeActor(_, handle)) : ActorOp<StopResult> =
        if deadline <= 0 then
            invalidArg "deadline" "Cleanup deadline must be positive"

        {
            Run =
                fun cont ->
                    match LifetimesNative.stop handle deadline with
                    | Some result -> cont (StopResult.Completed(unbox<ActorExit> result))
                    | None -> cont StopResult.TimedOut
        }

    /// Observe cooperative shutdown through Async with a positive deadline in milliseconds.
    /// Sequential execution stays in the calling BEAM process.
    let asyncStop deadline target : Async<StopResult> =
        async {
            let mutable result = StopResult.TimedOut
            (stop deadline target).Run(fun exit -> result <- exit)
            return result
        }

    /// Relinquish future observation. Work remains owned; native metadata is released only after actual exit.
    /// The capability must not be used again after this call. Message references retain their usual behavior.
    let releaseWhenExited (CooperativeActor(_, handle)) =
        LifetimesNative.releaseWhenExited handle

    /// Release terminal native observation state. Running work is left owned and observation remains available.
    let release (CooperativeActor(_, handle)) =
        if LifetimesNative.release handle then
            LifetimeRelease.Released
        else
            LifetimeRelease.Running
#else
    /// Spawn an owner using the target's existing cooperative actor runtime.
    let spawn (body: CooperativeInbox<'Message> -> ActorOp<unit>) : CooperativeActor<'Message> =
        let target =
            ActorRuntime.spawn (fun inbox -> body (CooperativeInbox(inbox, CooperativeActor(inbox.Reference, null))))

        CooperativeActor(target.Reference, null)

    /// Attach a cooperative child to the parent's existing owned lifetime.
    let spawnLinked (CooperativeActor(parent, _)) (body: CooperativeInbox<'Message> -> ActorOp<unit>) : CooperativeActor<'Message> =
        let target =
            ActorRuntime.spawnCooperativeLinked parent (fun inbox ->
                body (CooperativeInbox(inbox, CooperativeActor(inbox.Reference, null))))

        CooperativeActor(target.Reference, null)

    /// Observe body and descendant cleanup using the target's existing cooperative runtime.
    let stop deadline (CooperativeActor(target, _)) = ActorRuntime.stop deadline target

    /// Observe cooperative shutdown through Async with a positive deadline in milliseconds.
    let asyncStop deadline (CooperativeActor(target, _)) = ActorRuntime.asyncStop deadline target

    /// Relinquish future observation; managed runtimes reclaim metadata when references disappear.
    let releaseWhenExited (_: CooperativeActor<'Message>) = ()

    /// Release terminal observation state; managed runtimes reclaim the capability when no references remain.
    let release (CooperativeActor(target, _)) =
        if target.Lifetime.IsCompleted then
            LifetimeRelease.Released
        else
            LifetimeRelease.Running
#endif

/// Cooperative shutdown and terminal observation for capabilities created through Actor.
[<RequireQualifiedAccess>]
module Lifetimes =
    /// Share the typed message reference while retaining lifetime authority.
    let reference owner = LifetimesRuntime.reference owner
    /// Request cancellation and observe body and registered descendant cleanup.
    let stop deadline owner = LifetimesRuntime.stop deadline owner

    /// Observe shutdown from an Async host.
    let asyncStop deadline owner =
        LifetimesRuntime.asyncStop deadline owner

    /// Relinquish observation while leaving work owned until actual exit.
    let releaseWhenExited owner =
        LifetimesRuntime.releaseWhenExited owner

    /// Release completed observation; Running leaves the capability available.
    let release owner = LifetimesRuntime.release owner
