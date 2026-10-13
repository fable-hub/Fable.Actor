namespace Fable.Actor

open System.Threading
open Fable.Core
open Fable.Actor.Types

/// Construction and messaging for all actor authoring forms.
/// Share Actor<Message> references; retain CooperativeActor<Message> capabilities for stronger shutdown observation.
///
/// decision: erased static factories put all authoring forms under Actor without introducing a runtime factory object
/// decision: overloads select execution from the handler type while each factory returns the same typed message-reference contract
/// decision: absent termination predicates stay absent so the default path evaluates only the update
/// invariant: ordinary factories keep native kill semantics; cooperative factories register every cooperative descendant
[<Erase>]
type Actor =
    /// Spawn an explicit receive loop. Only its body receives an inbox.
    static member spawn(body: ActorInbox<'Message> -> ActorOp<unit>, ?cancellationToken: CancellationToken) : Actor<'Message> =
        match cancellationToken with
        | None -> (ActorRuntime.spawn body).Reference
        | Some token -> (ActorRuntime.spawnWithToken token body).Reference

    /// Spawn a serialized state handler with explicit normal or abnormal termination.
    static member spawn(initial: 'State, update: 'State -> 'Message -> Next<'State>) : Actor<'Message> =
        (ActorRuntime.start initial update).Reference

    /// Spawn a pure state handler. The optional predicate terminates before the matching update runs.
    static member spawn(initial: 'State, update: 'State -> 'Message -> 'State, ?shouldStop: 'State -> 'Message -> bool) : Actor<'Message> =
        (ActorRuntime.start initial (fun state message ->
            match shouldStop with
            | Some terminate when terminate state message -> Stop
            | _ -> Continue(update state message)))
            .Reference

    /// Spawn a serialized update returning its new state and inert finite commands.
    /// Workers run concurrently; the optional predicate terminates before the matching update runs.
    static member spawn
        (initial: 'State, update: 'State -> 'Message -> 'State * Command<'Message>, ?shouldStop: 'State -> 'Message -> bool)
        : Actor<'Message> =
        CommandRuntime.spawn initial update shouldStop

    /// Spawn an ordinary linked receive loop; its failures follow the parent's supervision protocol.
    static member spawnLinked(parent: Actor<'Parent>, body: ActorInbox<'Message> -> ActorOp<unit>) : Actor<'Message> =
        (ActorRuntime.spawnLinked parent body).Reference

    /// Spawn an ordinary linked state handler.
    static member spawnLinked(parent: Actor<'Parent>, initial: 'State, update: 'State -> 'Message -> Next<'State>) : Actor<'Message> =
        (ActorRuntime.spawnLinked parent (fun inbox -> ActorRuntime.stateBody initial update inbox.Receive)).Reference

    /// Spawn an ordinary linked pure state handler; termination is checked before update.
    static member spawnLinked
        (parent: Actor<'Parent>, initial: 'State, update: 'State -> 'Message -> 'State, ?shouldStop: 'State -> 'Message -> bool)
        : Actor<'Message> =
        let handler state message =
            match shouldStop with
            | Some terminate when terminate state message -> Stop
            | _ -> Continue(update state message)

        (ActorRuntime.spawnLinked parent (fun inbox -> ActorRuntime.stateBody initial handler inbox.Receive)).Reference

    /// Spawn an ordinary linked command handler.
    static member spawnLinked
        (
            parent: Actor<'Parent>,
            initial: 'State,
            update: 'State -> 'Message -> 'State * Command<'Message>,
            ?shouldStop: 'State -> 'Message -> bool
        ) : Actor<'Message> =
        CommandRuntime.spawnLinked parent initial update shouldStop

    /// Spawn a cooperative receive loop. Its inbox exposes Owner for child attachment.
    static member spawnCooperative(body: CooperativeInbox<'Message> -> ActorOp<unit>) : CooperativeActor<'Message> =
        LifetimesRuntime.spawn body

    /// Spawn a cooperative state handler with explicit normal or abnormal termination.
    static member spawnCooperative(initial: 'State, update: 'State -> 'Message -> Next<'State>) : CooperativeActor<'Message> =
        LifetimesRuntime.spawn (fun inbox -> ActorRuntime.stateBody initial update inbox.Receive)

    /// Spawn a cooperative pure state handler; termination is checked before update.
    static member spawnCooperative
        (initial: 'State, update: 'State -> 'Message -> 'State, ?shouldStop: 'State -> 'Message -> bool)
        : CooperativeActor<'Message> =
        let handler state message =
            match shouldStop with
            | Some terminate when terminate state message -> Stop
            | _ -> Continue(update state message)

        LifetimesRuntime.spawn (fun inbox -> ActorRuntime.stateBody initial handler inbox.Receive)

    /// Spawn a cooperative command owner whose workers join its observed cleanup tree.
    static member spawnCooperative
        (initial: 'State, update: 'State -> 'Message -> 'State * Command<'Message>, ?shouldStop: 'State -> 'Message -> bool)
        : CooperativeActor<'Message> =
        CommandRuntime.spawnCooperative initial update shouldStop

    /// Attach a cooperative receive loop before its body starts; parent completion observes its cleanup.
    static member spawnLinked
        (parent: CooperativeActor<'Parent>, body: CooperativeInbox<'Message> -> ActorOp<unit>)
        : CooperativeActor<'Message> =
        LifetimesRuntime.spawnLinked parent body

    /// Attach a cooperative state handler to the parent's cleanup tree.
    static member spawnLinked
        (parent: CooperativeActor<'Parent>, initial: 'State, update: 'State -> 'Message -> Next<'State>)
        : CooperativeActor<'Message> =
        LifetimesRuntime.spawnLinked parent (fun inbox -> ActorRuntime.stateBody initial update inbox.Receive)

    /// Attach a cooperative pure state handler to the parent's cleanup tree.
    static member spawnLinked
        (parent: CooperativeActor<'Parent>, initial: 'State, update: 'State -> 'Message -> 'State, ?shouldStop: 'State -> 'Message -> bool)
        : CooperativeActor<'Message> =
        let handler state message =
            match shouldStop with
            | Some terminate when terminate state message -> Stop
            | _ -> Continue(update state message)

        LifetimesRuntime.spawnLinked parent (fun inbox -> ActorRuntime.stateBody initial handler inbox.Receive)

    /// Attach a cooperative command owner and all its finite workers to the parent's cleanup tree.
    static member spawnLinked
        (
            parent: CooperativeActor<'Parent>,
            initial: 'State,
            update: 'State -> 'Message -> 'State * Command<'Message>,
            ?shouldStop: 'State -> 'Message -> bool
        ) : CooperativeActor<'Message> =
        CommandRuntime.spawnCooperativeLinked parent initial update shouldStop

    /// Queue a typed application message without acknowledging processing.
    static member send (target: Actor<'Message>) (message: 'Message) = ActorRuntime.send target message
    /// Send to a request protocol with a no-op reply channel.
    static member cast (target: Actor<'Message * ReplyChannel<'Reply>>) message = ActorRuntime.cast target message
    /// Await a reply with an unbounded deadline; cancellation and target death settle the wait.
    static member call target message = ActorRuntime.call target message
    /// Await a reply from an Async host.
    static member asyncCall target message = ActorRuntime.asyncCall target message

    /// Await a reply within a positive deadline, raising on expiry.
    static member callWithTimeout deadline target message =
        ActorRuntime.callWithTimeout deadline target message

    /// Await a bounded reply from an Async host.
    static member asyncCallWithTimeout deadline target message =
        ActorRuntime.asyncCallWithTimeout deadline target message

    /// Await a bounded request with explicit reply, timeout, cancellation and termination outcomes.
    static member callResult deadline token target message =
        ActorRuntime.callResult deadline token target message

    /// Await explicit request outcomes from an Async host.
    static member asyncCallResult deadline token target message =
        ActorRuntime.asyncCallResult deadline token target message

    /// Request ordinary shutdown; native BEAM kill skips finalizers.
    static member kill target = ActorRuntime.kill target
    /// Observe ordinary shutdown; use Lifetimes.stop for a cooperative capability.
    static member stop deadline target = ActorRuntime.stop deadline target
    /// Observe ordinary shutdown from an Async host.
    static member asyncStop deadline target = ActorRuntime.asyncStop deadline target
    /// Enable native exit trapping inside a supervising body; emulated bodies already receive child failures.
    static member trapExits() = ActorRuntime.trapExits ()
    /// Recognize a runtime child-failure notification in an object inbox.
    static member tryAsChildExited message = ActorRuntime.tryAsChildExited message

    /// Spawn a child with a restart policy; retain its replacement after a restart.
    static member spawnSupervised parent strategy body =
        ActorRuntime.spawnSupervised parent strategy body

    /// Apply the supervised child's recovery policy.
    static member handleChildExit parent child exited =
        ActorRuntime.handleChildExit parent child exited

    /// Schedule a callback after the given delay.
    static member schedule milliseconds callback =
        ActorRuntime.schedule milliseconds callback

    /// Cancel a scheduled callback.
    static member cancelTimer timer = ActorRuntime.cancelTimer timer
    /// Obtain the raw target-specific identity for native interoperability.
    static member pid target = ActorRuntime.pid target
    /// Receive through a body inbox.
    static member receive(inbox: ActorInbox<'Message>) = inbox.Receive()
#if FABLE_COMPILER_BEAM
    /// Receive the current BEAM process's next actor-protocol message for native integration.
    /// This does not receive from a shared Actor reference or another process's mailbox.
    static member receive<'Message>() = ActorRuntime.receive<'Message>()

    /// Obtain a typed reference to the current process on BEAM.
    static member self<'Message>() =
        (ActorRuntime.self<'Message>()).Reference

    /// Format a native exit reason for diagnostics.
    static member formatReason reason = ActorRuntime.formatReason reason
#endif
