/// Cowboy HTTP server setup for the timeflies demo.
module FableActorTimefliesApp

open Fable.Core
open Fable.Beam
open Fable.Beam.Application
open Fable.Beam.Io
open Fable.Beam.Cowboy.Cowboy
open Fable.Beam.Cowboy.CowboyRouter

/// Cowboy route: {Path, Handler, Opts}
[<Emit("{$0, $1, []}")>]
let private route (path: string) (handler: obj) : obj = nativeOnly

/// Cowboy host rule: {'_', Routes}
[<Emit("{'_', $0}")>]
let private hostRule (routes: obj list) : obj = nativeOnly

/// Protocol options: #{env => #{dispatch => Dispatch}}
[<Emit("#{env => #{dispatch => $0}}")>]
let private protoOpts (dispatch: obj) : obj = nativeOnly

/// Transport options: [{port, Port}]
[<Emit("[{port, $0}]")>]
let private transportOpts (port: int) : obj = nativeOnly

let start () =
    application.ensure_all_started (Erlang.binaryToAtom "cowboy") |> ignore

    let dispatch =
        compile [
            hostRule [
                route "/" (Erlang.binaryToAtom "fable_actor_timeflies_http")
                route "/ws" (Erlang.binaryToAtom "fable_actor_timeflies_ws")
            ]
        ]

    startClear (Erlang.binaryToAtom "timeflies_listener") (transportOpts 3000) (protoOpts dispatch)
    |> ignore

    io.format ("Timeflies demo running at http://localhost:3000~n", [])
