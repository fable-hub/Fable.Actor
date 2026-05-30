module Timeflies

open Fable.Actor.Types
open Fable.Actor

let text = "TIME FLIES LIKE AN ARROW"

type MousePos = { X: int; Y: int }

type LetterMsg =
    | MouseMove of MousePos
    | Delayed of MousePos

/// Sets up the timeflies demo using actors.
///
/// Each letter is an actor with its own delay timer.
/// A distributor actor fans out mouse events to all letter actors.
/// Returns the distributor actor — send MousePos to it.
/// Kill the distributor to clean up (linked children die automatically).
let setupPipeline (sendFn: string -> unit) : Actor<MousePos> =
    Actor.spawn (fun inbox ->
        // Each letter is a linked child actor with a delay
        let letters =
            text
            |> Seq.toList
            |> List.mapi (fun index char ->
                Actor.spawnLinked inbox (fun letterInbox ->
                    let rec loop () =
                        actor {
                            let! msg = letterInbox.Receive()

                            match msg with
                            | MouseMove pos ->
                                Actor.schedule (80 * index) (fun () -> Actor.send letterInbox (Delayed pos))
                                |> ignore

                                return! loop ()
                            | Delayed pos ->
                                let json =
                                    sprintf
                                        "{\"index\":%d,\"char\":\"%s\",\"x\":%d,\"y\":%d}"
                                        index
                                        (string char)
                                        (pos.X + index * 14 + 15)
                                        pos.Y

                                sendFn json
                                return! loop ()
                        }

                    loop ()))

        // Distributor loop: receive MousePos, fan out to all letters
        let rec loop () =
            actor {
                let! pos = inbox.Receive()
                letters |> List.iter (fun letter -> Actor.send letter (MouseMove pos))
                return! loop ()
            }

        loop ())
