/// Cooperative execution support shared by ordinary actor expressions and optional lifetime policies.
///
/// decision: keep token propagation and finalization independent of descendant ownership and terminal history
/// invariant: a cancellation request does not assert body completion or foreign-resource settlement
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

let private contextKey = Atom.ofString "$fable_actor_cooperative"
let private cleanupKey = Atom.ofString "$fable_actor_cooperative_cleanup"
let private normal = Atom.ofString "normal"

let private context () =
    Erlang.get<Atom, obj * CancellationToken> contextKey

let private inCleanup () =
    Erlang.get<Atom, bool> cleanupKey = Some true

let token () : CancellationToken =
    if inCleanup () then
        CancellationToken.None
    else
        context ()
        |> Option.map snd
        |> Option.defaultWith (fun () -> CancellationToken.None)

let checkpoint () =
    if not (inCleanup ()) then
        context ()
        |> Option.iter (fun (_, token) -> token.ThrowIfCancellationRequested())

// invariant: finalizers run without ambient cancellation checkpoints, and their defects remain observable
let finalize compensation =
    let previous = Erlang.put cleanupKey true

    try
        compensation ()
    finally
        match previous with
        | Some value -> Erlang.put cleanupKey value |> ignore
        | None -> Erlang.erase<Atom, bool> cleanupKey |> ignore

// invariant: resource-producing continuations install Using before the next cancellation checkpoint
let checkpointValue (value: obj) =
    let resource: bool =
        emitErlExpr
            value
            "(fun() -> Value = case is_reference($0) of true -> get($0); false -> $0 end, is_map(Value) andalso is_map_key(dispose, Value) end)()"

    if not resource then
        checkpoint ()

let runAsync (operation: Async<'Value>) : 'Value =
    // TODO(upstream Fable): replace this emit with Async.RunSynchronously(operation, cancellationToken = token ()) once BEAM optional-argument lowering uses run_synchronously/2 instead of nonexistent /3.
    // decision: pass the ambient token explicitly because the published runtime exposes run_synchronously/2, not /3
    emitErlExpr (operation, token ()) "fable_async:run_synchronously($0, $1)"

// decision: preserve the raw cancellation signal before F# catch normalization can erase its identity
// invariant: owned cancellation bypasses ordinary error handlers; cleanup defects still reach those handlers
// TODO(upstream Fable): replace this raw-catch shim with typed F# cancellation filtering in ActorBuilder.TryWith once BEAM normalization preserves OperationCanceledException identity.
let tryWith (body: unit -> unit) (handler: exn -> unit) =
    emitErlExpr
        (body, handler, token ())
        """
    try $0(ok) catch
        error:operation_cancelled:Stack ->
            case fable_cancellation:is_cancellation_requested($2) of
                true -> erlang:raise(error, operation_cancelled, Stack);
                false -> $1(fable_async:wrap_error(operation_cancelled))
            end;
        _:Failure -> $1(fable_async:wrap_error(Failure))
    end
    """

// decision: classify only the runtime cancellation signal as cancellation, even if a defect races a stop request
// TODO(upstream Fable): replace this raw-catch adapter with F# try/with once runtime cancellation has a preserved OperationCanceledException identity.
let private bodyResult (token: CancellationToken) (body: unit -> unit) : ActorExit =
    emitErlExpr
        (token, body)
        """
    try $1(ok), normal catch
        error:operation_cancelled ->
            case fable_cancellation:is_cancellation_requested($0) of
                true -> cancelled; false -> {failed, fable_async:wrap_error(operation_cancelled)}
            end;
        _:Failure -> {failed, fable_async:wrap_error(Failure)}
    end
    """

// invariant: cancellation wakes an idle receive without becoming an application message
let rec receive cont =
    checkpoint ()

    match Erlang.receive<WorkerMessage>() with
    | Message value ->
        checkpoint ()
        cont value
    | Cancel
    | Reply _ -> receive cont
    | Exit(_, reason) when Erlang.exactEquals reason (box normal) -> receive cont
    | Exit(pid, reason) -> cont (box ({ Pid = box pid; Reason = reason }: ChildExited))

// invariant: the caller's execution context is restored after body finalization and before completion delivery
let run scope (token: CancellationToken) body =
    let previous = Erlang.put contextKey (scope, token)

    try
        bodyResult token (fun () ->
            checkpoint ()
            body ())
    finally
        match previous with
        | Some value -> Erlang.put contextKey value |> ignore
        | None ->
            Erlang.erase<Atom, obj * CancellationToken> contextKey
            |> ignore
#endif
