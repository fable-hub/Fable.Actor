module Fable.Actor.Tests.TimefliesExampleTests

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test
open Fable.Actor
open Fable.Actor.Types
open Fable.Actor.Tests.CommandObservations

// The Timeflies demo is the library's flagship example: a distributor actor fans one mouse
// event out to one linked actor per letter, and each letter re-sends its own position after a
// delay proportional to its index. The suite drives it through the only sink the demo exposes —
// the JSON callback every target's UI passes in — and observes emissions through the journal.
//
// decision: exercises the example through its sendFn sink because every position already travels
//           there, so the demo stays a teaching artifact with no test-only seam
// invariant: the JSON payload is the contract between the demo's actors and each target's UI
// tradeoff: full fan-out waits on the last letter's per-index delay, so this group costs seconds
//           the rest of the suite avoids

let private deadline = 2000
let private letterCount = Timeflies.text.Length

/// The last letter schedules 80ms per earlier letter; the deadline bounds that plus scheduling.
let private fanOutDeadline = 8000

/// The demo's payload format and geometry, restated so a change to either side fails loudly.
let private payload (index: int) (x: int) (y: int) =
    sprintf "{\"index\":%d,\"char\":\"%s\",\"x\":%d,\"y\":%d}" index (string Timeflies.text.[index]) (x + index * 14 + 15) y

let private fanOutReachesEveryLetter () =
    actor {
        let emissions = journal<string>()
        let distributor = Timeflies.setupPipeline (record emissions)

        Actor.send distributor { Timeflies.X = 100; Y = 200 }
        let! values = Actor.callWithTimeout fanOutDeadline emissions (Take letterCount)

        let expected = [ for index in 0 .. letterCount - 1 -> payload index 100 200 ]

        // Every letter emits exactly once, with its own offset applied.
        assertThat (List.sort values) (isEqualTo (List.sort expected))

        // The first letter schedules no delay and the last schedules the longest one, so arrival
        // order is fixed at both ends even when the middle letters interleave.
        assertThat (List.head values) (isEqualTo (payload 0 100 200))
        assertThat (List.item (letterCount - 1) values) (isEqualTo (payload (letterCount - 1) 100 200))

        let! _ = Actor.stop deadline distributor
        return ()
    }

let private killStopsLinkedLetters () =
    actor {
        let emissions = journal<string>()
        let distributor = Timeflies.setupPipeline (record emissions)

        Actor.send distributor { Timeflies.X = 10; Y = 20 }
        let! first = Actor.callWithTimeout fanOutDeadline emissions (Take 1)

        Actor.kill distributor
        Actor.send distributor { Timeflies.X = 30; Y = 40 }
        do! sleep 300

        let! after = Actor.callWithTimeout deadline emissions (Take 1)

        // Letters still waiting on their own timer must not emit for the second event.
        assertThat (List.length after) (isEqualTo (List.length first))
        return ()
    }

let tests =
    testList (
        "Timeflies example",
        [
            testAsync (
                "one mouse event reaches every letter with its delay and offset",
                timeout 12000,
                fun _ -> toAsync (fanOutReachesEveryLetter ())
            )
            testAsync ("killing the distributor stops its linked letters", timeout 8000, fun _ -> toAsync (killStopsLinkedLetters ()))
        ]
    )
