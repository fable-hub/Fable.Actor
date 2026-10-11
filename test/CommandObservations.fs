module Fable.Actor.Tests.CommandObservations

open Fable.Actor
open Fable.Actor.Types

let private observationDeadline = 2000

type JournalMessage<'Value> =
    | Record of 'Value
    | Take of int

// decision: observed-count replies are deterministic across native processes and emulated actors
// An actor owns observations so native and emulated tests use the same barriers.
let journal<'Value> () : Actor<JournalMessage<'Value> * ReplyChannel<'Value list>> =
    Actor.start ([], []) (fun (values, waiters) (message, reply) ->
        match message with
        | Record value ->
            let next = values @ [ value ]

            let ready, pending =
                waiters
                |> List.partition (fun (expected, _) -> expected <= next.Length)

            for _, waiter in ready do
                waiter.Reply next

            Continue(next, pending)
        | Take expected when expected > values.Length -> Continue(values, (expected, reply) :: waiters)
        | Take _ ->
            reply.Reply values
            Continue(values, waiters))

let record<'Value> (journal: Actor<JournalMessage<'Value> * ReplyChannel<'Value list>>) (value: 'Value) = Actor.cast journal (Record value)

let take<'Value> expected (journal: Actor<JournalMessage<'Value> * ReplyChannel<'Value list>>) =
    Actor.callWithTimeout observationDeadline journal (Take expected)
