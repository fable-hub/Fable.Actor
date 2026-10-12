/// Cooperative execution support shared by ordinary actor expressions and optional lifetime policies.
///
/// decision: keep token propagation and finalization independent of descendant ownership and terminal history
/// invariant: a cancellation request does not assert body completion or foreign-resource settlement
/// assumption: BEAM compilation uses Fable 5.21.0 or later for typed cancellation and named-token Async.RunSynchronously
module internal Fable.Actor.ExecutionNative

#if FABLE_COMPILER_BEAM
open System.Threading
open Fable.Core
open Fable.Core.BeamInterop
open Fable.Beam
open Fable.Actor.Types

// invariant: ordinary actor envelopes retain their existing native tags
// decision: the private wake is shared by every cooperative executor without selecting an ownership policy
type WorkerMessage =
    | [<CompiledName("cooperative_cancel")>] Cancel
    | [<CompiledName("fable_actor_msg")>] Message of obj
    | [<CompiledName("fable_actor_reply")>] Reply of Ref<obj> * obj
    | [<CompiledName("EXIT")>] Exit of Pid<obj> * obj

// decision: typed native tags avoid converting constant strings to atoms at each execution boundary
type ExecutionKey =
    | [<CompiledName("$fable_actor_cooperative")>] Context
    | [<CompiledName("$fable_actor_cooperative_cleanup")>] Cleanup

let private normal = Atom.ofString "normal"

// decision: inline context dispatch avoids helper calls on ordinary execution while retaining the shared builder
// tradeoff: shared expressions look up execution context at runtime so preconstructed operations inherit their executing owner's cancellation
let inline private context () =
    Erlang.get<ExecutionKey, obj * CancellationToken> Context

let inCleanup () =
    Erlang.get<ExecutionKey, bool> Cleanup = Some true

// decision: ordinary execution checks for lifetime context before inspecting cleanup state
let inline token () : CancellationToken =
    match context () with
    | None -> CancellationToken.None
    | Some(_, _) when inCleanup () -> CancellationToken.None
    | Some(_, token) -> token

let inline checkpoint () =
    match context () with
    | Some(_, token) when not (inCleanup ()) -> token.ThrowIfCancellationRequested()
    | _ -> ()

let shield compensation =
    let previous = Erlang.put Cleanup true

    try
        compensation ()
    finally
        match previous with
        | Some value -> Erlang.put Cleanup value |> ignore
        | None -> Erlang.erase<ExecutionKey, bool> Cleanup |> ignore

// invariant: finalizers run without ambient cancellation checkpoints, and their defects remain observable
// decision: ordinary finalizers invoke compensation directly because they have no owned token to shield
let inline finalize compensation =
    match context () with
    | None -> compensation ()
    | Some _ -> shield compensation

// invariant: resource-producing continuations install Using before the next cancellation checkpoint
// decision: ordinary values skip native resource-shape inspection because no lifetime cancellation checkpoint can reject them
let inline checkpointValue (value: obj) =
    match context () with
    | None -> ()
    | Some(_, token) ->
        let resource: bool =
            emitErlExpr
                value
                "(fun() -> ResourceValue__ = case is_reference($0) of true -> get($0); false -> $0 end, is_map(ResourceValue__) andalso is_map_key(dispose, ResourceValue__) end)()"

        if not resource && not (inCleanup ()) then
            token.ThrowIfCancellationRequested()

let inline runAsync (operation: Async<'Value>) : 'Value =
    match context () with
    | None -> Async.RunSynchronously operation
    | Some(_, cancellationToken) ->
        let activeToken =
            if inCleanup () then
                CancellationToken.None
            else
                cancellationToken

        Async.RunSynchronously(operation, cancellationToken = activeToken)

// invariant: owned cancellation bypasses ordinary error handlers; cleanup defects still reach those handlers
let tryWith (body: unit -> unit) (handler: exn -> unit) =
    try
        body ()
    with
    | :? System.OperationCanceledException when (token ()).IsCancellationRequested -> reraise ()
    | error -> handler error

// decision: classify only the runtime cancellation signal as cancellation, even if a defect races a stop request
let private bodyResult (token: CancellationToken) (body: unit -> unit) : ActorExit =
    try
        body ()
        ActorExit.Normal
    with
    | :? System.OperationCanceledException when token.IsCancellationRequested -> ActorExit.Cancelled
    | error -> ActorExit.Failed error

// invariant: cancellation wakes an idle receive without becoming an application message
let rec receiveOwned cont =
    checkpoint ()

    match Erlang.receive<WorkerMessage>() with
    | Message value ->
        checkpoint ()
        cont value
    | Cancel
    | Reply _ -> receiveOwned cont
    | Exit(_, reason) when Erlang.exactEquals reason (box normal) -> receiveOwned cont
    | Exit(pid, reason) -> cont (box ({ Pid = box pid; Reason = reason }: ChildExited))

// decision: ordinary receives use the existing native message protocol and avoid owned cancellation checks
let inline receive cont =
    match context () with
    | None -> Fable.Actor.Platform.receiveMsg cont
    | Some _ -> receiveOwned cont

// invariant: the caller's execution context is restored after body finalization and before completion delivery
let run scope (token: CancellationToken) body =
    let previous = Erlang.put Context (scope, token)

    try
        bodyResult token (fun () ->
            checkpoint ()
            body ())
    finally
        match previous with
        | Some value -> Erlang.put Context value |> ignore
        | None ->
            Erlang.erase<ExecutionKey, obj * CancellationToken> Context
            |> ignore
#endif
