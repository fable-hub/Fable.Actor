namespace Fable.Actor

open Fable.Actor.Types

/// Creator-held cooperative lifetime control. Share its ordinary Actor reference for messaging.
/// Native observation state lasts until release or the creating process dies.
type CooperativeActor<'Message> = internal CooperativeActor of Actor<'Message> * obj

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
/// decision: separates lifetime authority from message references to keep native kill compatibility explicit
/// tradeoff: a long-lived BEAM creator must release completed capabilities to reclaim retained terminal outcomes
[<RequireQualifiedAccess>]
module Cooperative =
    /// Obtain an ordinary message reference without exposing cooperative lifetime control.
    let reference (CooperativeActor(target, _)) = target

#if FABLE_COMPILER_BEAM
    open Fable.Core
    open Fable.Core.BeamInterop

    [<Emit("element(1, $0)")>]
    let private workerPid (handle: obj) : Fable.Beam.Pid<'Message> = nativeOnly

    let private create parent body =
        let handle =
            CooperativeNative.spawn parent (fun handle ->
                body (CooperativeActor({ Pid = workerPid handle }, handle))
                |> fun work -> work.Run ignore)

        CooperativeActor({ Pid = workerPid handle }, handle)

    /// Spawn a cooperative owner. Cancellation wakes receives and flows through sequential Async adapters.
    /// Arbitrary synchronous work may outlive a stop deadline. Exit history is scoped to the creator process.
    let spawn (body: CooperativeActor<'Message> -> ActorOp<unit>) : CooperativeActor<'Message> = create (box None) body

    /// Attach before starting the child; parent completion cancels and observes every cooperative descendant.
    /// A parent whose terminal result is already published rejects attachment.
    let spawnLinked (CooperativeActor(_, parent)) (body: CooperativeActor<'Message> -> ActorOp<unit>) : CooperativeActor<'Message> =
        create parent body

    /// Request cooperative cancellation and observe actual body/descendant exit. Deadline never requests native kill.
    /// Repeated stop returns the retained exit while this capability remains valid.
    let stop deadline (CooperativeActor(_, handle)) : ActorOp<StopResult> =
        if deadline <= 0 then
            invalidArg "deadline" "Cleanup deadline must be positive"

        {
            Run =
                fun cont ->
                    match CooperativeNative.stop handle deadline with
                    | Some result -> cont (StopResult.Completed(unbox<ActorExit> result))
                    | None -> cont StopResult.TimedOut
        }

    /// Async bridge for advanced hosts; sequential execution stays in the calling BEAM process.
    let stopAsync deadline target : Async<StopResult> =
        async {
            let mutable result = StopResult.TimedOut
            (stop deadline target).Run(fun exit -> result <- exit)
            return result
        }

    /// Relinquish future observation. Work remains owned; native metadata is released only after actual exit.
    /// The capability must not be used again after this call. Message references retain their usual behavior.
    let releaseWhenExited (CooperativeActor(_, handle)) =
        CooperativeNative.releaseWhenExited handle

    /// Release terminal native observation state. Running work is left owned and observation remains available.
    let release (CooperativeActor(_, handle)) =
        if CooperativeNative.release handle then
            LifetimeRelease.Released
        else
            LifetimeRelease.Running
#else
    /// Spawn an owner using the target's existing cooperative actor runtime.
    let spawn (body: CooperativeActor<'Message> -> ActorOp<unit>) : CooperativeActor<'Message> =
        let target = Actor.spawn (fun inbox -> body (CooperativeActor(inbox, null)))
        CooperativeActor(target, null)

    /// Attach a cooperative child to the parent's existing owned lifetime.
    let spawnLinked (CooperativeActor(parent, _)) (body: CooperativeActor<'Message> -> ActorOp<unit>) : CooperativeActor<'Message> =
        let target =
            Actor.spawnCooperativeLinked parent (fun inbox -> body (CooperativeActor(inbox, null)))

        CooperativeActor(target, null)

    /// Observe body and descendant cleanup using the target's existing cooperative runtime.
    let stop deadline (CooperativeActor(target, _)) = Actor.stop deadline target

    /// Async bridge for advanced hosts.
    let stopAsync deadline (CooperativeActor(target, _)) = Actor.stopAsync deadline target

    /// Relinquish future observation; managed runtimes reclaim metadata when references disappear.
    let releaseWhenExited (_: CooperativeActor<'Message>) = ()

    /// Release terminal observation state; managed runtimes reclaim the capability when no references remain.
    let release (CooperativeActor(target, _)) =
        if target.Lifetime.IsCompleted then
            LifetimeRelease.Released
        else
            LifetimeRelease.Running
#endif
