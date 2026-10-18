/// Interop example: F# actor "Dag" talks to Erlang process "Joe"
///
/// Shows the wire format for cross-language actor communication:
///   - F# DU cases compile to Erlang tagged tuples: HelloFrom → {hello_from, ...}
///   - Actor.send wraps in {fable_actor_msg, Msg} envelope
///   - ReplyChannel compiles to #{reply => fun(V) -> ... end}
///   - Raw Erlang sends bypass the envelope
///
/// decision: demonstrates the wire protocol explicitly so native Erlang can interoperate without an F# adapter
/// invariant: envelope and reply tags match Fable.Actor.Platform.InternalMsg
/// assumption: Fable preserves the documented DU tuple and record map representations
module InteropExample

open Fable.Core
open Fable.Actor.Types
open Fable.Actor
open Fable.Beam

// --- Erlang FFI helpers ---

/// Send a raw message to an Erlang pid (no {fable_actor_msg, ...} envelope)
[<Emit("$0 ! $1")>]
let private rawSend (pid: obj) (msg: obj) : unit = nativeOnly

// --- Messages Dag understands ---
// These compile to Erlang as:
//   HelloFrom "Joe"         → {hello_from, <<"Joe">>}
//   AskName replyChannel    → {ask_name, #{reply => fun(V) -> ... end}}

type DagMsg =
    | HelloFrom of string
    | AskName

// --- Dag: the F# actor ---

/// Start Dag and keep Joe's raw pid as private actor state.
///
/// decision: replies to HelloFrom with a raw Erlang message to demonstrate bypassing the actor envelope
/// invariant: AskName replies through ReplyChannel while HelloFrom replies directly to joePid
let startDag (joePid: obj) : Actor<DagMsg * ReplyChannel<string>> =
    Actor.spawn (
        joePid,
        (fun joePid (msg, rc) ->
            match msg with
            | HelloFrom name ->
                Io.format "  [Dag/F#]     Received hello from ~s~n" [ name ]
                // Reply directly to Joe using raw Erlang send (no envelope)
                rawSend joePid "Hei Joe! Dag her. Hyggelig å møte deg!"
                Continue joePid

            | AskName ->
                Io.format "  [Dag/F#]     Someone asked my name~n" []
                rc.Reply "Dag Brattli"
                Continue joePid)
    )

/// Return the native PID so the Erlang peer can use the documented actor envelope.
let startDagPid joePid = Actor.pid (startDag joePid)

#if FABLE_COMPILER_BEAM
/// Exercise a wrapped native Erlang actor through the public request protocol.
/// invariant: the native peer replies through the supplied channel rather than a raw mailbox send
let startNativeJoeCall (nativePid: Pid<DagMsg * ReplyChannel<string>>) (reportPid: obj) =
    Actor.spawn (fun _ ->
        actor {
            let nativeJoe = Actor.fromPid nativePid
            let! name = Actor.callWithTimeout 5000 nativeJoe AskName
            rawSend reportPid name
        })
    |> ignore
#endif
