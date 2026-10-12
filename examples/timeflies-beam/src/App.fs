/// Cowboy HTTP server setup for the timeflies demo.
module FableActorTimefliesApp

open Fable.Beam
open Fable.Beam.Application
open Fable.Beam.Io
open Fable.Beam.Cowboy.Cowboy
open Fable.Beam.Cowboy.CowboyRouter

let start () =
    match ensureAllStarted (Erlang.binaryToAtom "cowboy") with
    | Error error -> failwithf "Cannot start Cowboy: %s" error
    | Ok _ -> ()

    // decision: serve HTTP through the F# Cowboy binding and retain the native WebSocket actor-interoperability sample
    let dispatch =
        compile [
            hostRule wildcard [
                route "/" (Erlang.binaryToAtom "timeflies_http") []
                route "/ws" (Erlang.binaryToAtom "fable_actor_timeflies_ws") []
            ]
        ]

    match startClear (Erlang.binaryToAtom "timeflies_listener") (tcpPort 3000) (protocolOpts dispatch) with
    | Error error -> failwithf "Cannot start Timeflies listener: %s" error
    | Ok _ -> format "Timeflies demo running at http://localhost:3000~n" []
