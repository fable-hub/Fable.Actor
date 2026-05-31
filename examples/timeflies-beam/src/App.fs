/// Cowboy HTTP server setup for the timeflies demo.
module FableActorTimefliesApp

open Fable.Beam
open Fable.Beam.Application
open Fable.Beam.Io
open Fable.Beam.Cowboy.Cowboy
open Fable.Beam.Cowboy.CowboyRouter

let start () =
    ensureAllStarted (Erlang.binaryToAtom "cowboy")
    |> ignore

    let dispatch =
        compile [
            hostRule wildcard [
                route "/" (Erlang.binaryToAtom "fable_actor_timeflies_http") []
                route "/ws" (Erlang.binaryToAtom "fable_actor_timeflies_ws") []
            ]
        ]

    startClear (Erlang.binaryToAtom "timeflies_listener") (tcpPort 3000) (protocolOpts dispatch)
    |> ignore

    io.format ("Timeflies demo running at http://localhost:3000~n", [])
