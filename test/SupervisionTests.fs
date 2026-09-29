module Fable.Actor.Tests.SupervisionTests

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Actor
open Fable.Actor.Types

// ============================================================================
// spawnLinked
// ============================================================================

let private linkTests =
    testList (
        "spawnLinked",
        [
            testAsync (
                "a child crash does not take down a parent that traps exits",
                fun _ ->
                    toAsync (
                        actor {
                            let supervisor =
                                Actor.spawn (fun inbox ->
                                    Actor.trapExits ()

                                    let child: Actor<string> =
                                        Actor.spawnLinked inbox (fun childInbox ->
                                            let rec loop () =
                                                actor {
                                                    let! _msg = childInbox.Receive()
                                                    failwith "crash!"
                                                    return! loop ()
                                                }

                                            loop ())

                                    // Send a message to make the child crash
                                    Actor.send child "boom"

                                    // Receive the EXIT signal
                                    let rec loop (crashCount: int) =
                                        actor {
                                            let! _msg = inbox.Receive()
                                            return! loop (crashCount + 1)
                                        }

                                    loop 0)

                            do! sleep 100
                            // Getting here without the supervisor crashing means supervision works
                            assertThat true isTrue
                        }
                    )
            )
        ]
    )

// ============================================================================
// spawnSupervised
// ============================================================================

let private supervisedTests =
    testList (
        "spawnSupervised",
        [
            testAsync (
                "OneForOne Restart returns a replacement child that accepts messages",
                fun _ ->
                    toAsync (
                        actor {
                            let replacementReceived = reporter false

                            let _parent: Actor<obj> =
                                Actor.spawn (fun inbox ->
                                    Actor.trapExits ()

                                    let child =
                                        Actor.spawnSupervised inbox (OneForOne(fun _ex -> Directive.Restart)) (fun childInbox ->
                                            let rec loop () =
                                                actor {
                                                    let! msg = childInbox.Receive()

                                                    if msg = "crash" then
                                                        failwith "intentional crash"

                                                    if msg = "probe" then
                                                        Actor.cast replacementReceived (Some true)

                                                    return! loop ()
                                                }

                                            loop ())

                                    Actor.send child.Actor "crash"

                                    let rec loop current =
                                        actor {
                                            let! msg = inbox.Receive()

                                            match Actor.tryAsChildExited msg with
                                            | Some exited ->
                                                match Actor.handleChildExit inbox current exited with
                                                | ChildExitResult.Restarted replacement ->
                                                    Actor.send replacement.Actor "probe"
                                                    return! loop replacement
                                                | ChildExitResult.Stopped -> return ()
                                            | None -> return! loop current
                                        }

                                    loop child)

                            do! sleep 200
                            let! received = Actor.call replacementReceived None
                            assertThat received isTrue
                        }
                    )
            )

            testAsync (
                "OneForOne Stop leaves a crashed child stopped",
                fun _ ->
                    toAsync (
                        actor {
                            let flag = reporter false

                            let _parent: Actor<obj> =
                                Actor.spawn (fun inbox ->
                                    Actor.trapExits ()

                                    let child =
                                        Actor.spawnSupervised inbox (OneForOne(fun _ex -> Directive.Stop)) (fun childInbox ->
                                            let rec loop () =
                                                actor {
                                                    let! _msg = childInbox.Receive()
                                                    failwith "crash!"
                                                    return! loop ()
                                                }

                                            loop ())

                                    Actor.send child.Actor "boom"

                                    let rec loop () =
                                        actor {
                                            let! msg = inbox.Receive()

                                            match Actor.tryAsChildExited msg with
                                            | Some exited ->
                                                match Actor.handleChildExit inbox child exited with
                                                | ChildExitResult.Stopped -> Actor.cast flag (Some true)
                                                | ChildExitResult.Restarted _ -> ()
                                            | None -> ()

                                            return! loop ()
                                        }

                                    loop ())

                            do! sleep 200
                            let! stopped = Actor.call flag None
                            assertThat stopped isTrue
                        }
                    )
            )
        ]
    )

// ============================================================================
// StopAbnormal
// ============================================================================

let private stopAbnormalTests =
    testList (
        "StopAbnormal",
        [
            testAsync (
                "a raised ProcessExitException reaches the parent as ChildExited",
                fun _ ->
                    toAsync (
                        actor {
                            let flag = reporter false

                            let _parent: Actor<obj> =
                                Actor.spawn (fun inbox ->
                                    Actor.trapExits ()

                                    let stoppingChild =
                                        Actor.spawnSupervised inbox (OneForOne(fun _ex -> Directive.Stop)) (fun childInbox ->
                                            let rec loop () =
                                                actor {
                                                    let! msg = childInbox.Receive()

                                                    if msg = "stop-abnormal" then
                                                        raise (ProcessExitException "intentional abnormal stop")

                                                    return! loop ()
                                                }

                                            loop ())

                                    Actor.send stoppingChild.Actor "stop-abnormal"

                                    let rec loop () =
                                        actor {
                                            let! msg = inbox.Receive()

                                            match Actor.tryAsChildExited msg with
                                            | Some exited ->
                                                Actor.handleChildExit inbox stoppingChild exited
                                                |> ignore

                                                Actor.cast flag (Some true)
                                            | None -> ()

                                            return! loop ()
                                        }

                                    loop ())

                            do! sleep 200
                            let! gotExit = Actor.call flag None
                            assertThat gotExit isTrue
                        }
                    )
            )

            testAsync (
                "an abnormal stop from a stateful loop propagates as ChildExited",
                fun _ ->
                    toAsync (
                        actor {
                            let flag = reporter false

                            let _parent: Actor<obj> =
                                Actor.spawn (fun inbox ->
                                    Actor.trapExits ()

                                    let child =
                                        Actor.spawnSupervised inbox (OneForOne(fun _ex -> Directive.Stop)) (fun childInbox ->
                                            let rec loop (state: int) =
                                                actor {
                                                    let! msg = childInbox.Receive()

                                                    match msg with
                                                    | "fail" -> raise (ProcessExitException "bad state")
                                                    | _ -> return! loop (state + 1)
                                                }

                                            loop 0)

                                    Actor.send child.Actor "fail"

                                    let rec loop () =
                                        actor {
                                            let! msg = inbox.Receive()

                                            match Actor.tryAsChildExited msg with
                                            | Some exited ->
                                                Actor.handleChildExit inbox child exited |> ignore
                                                Actor.cast flag (Some true)
                                            | None -> ()

                                            return! loop ()
                                        }

                                    loop ())

                            do! sleep 200
                            let! gotExit = Actor.call flag None
                            assertThat gotExit isTrue
                        }
                    )
            )

            testAsync (
                "a handler returning StopAbnormal triggers supervision",
                fun _ ->
                    toAsync (
                        actor {
                            let flag = reporter false

                            let _parent: Actor<obj> =
                                Actor.spawn (fun inbox ->
                                    Actor.trapExits ()

                                    let child =
                                        Actor.spawnSupervised inbox (OneForOne(fun _ex -> Directive.Restart)) (fun childInbox ->
                                            let handler state msg =
                                                match msg with
                                                | "crash" -> StopAbnormal(ProcessExitException "handler decided to crash")
                                                | _ -> Continue(state + 1)

                                            let rec loop state =
                                                actor {
                                                    let! msg = childInbox.Receive()

                                                    match handler state msg with
                                                    | Continue newState -> return! loop newState
                                                    | Stop -> ()
                                                    | StopAbnormal ex -> raise ex
                                                }

                                            loop 0)

                                    Actor.send child.Actor "crash"

                                    let rec loop () =
                                        actor {
                                            let! msg = inbox.Receive()

                                            match Actor.tryAsChildExited msg with
                                            | Some exited ->
                                                Actor.handleChildExit inbox child exited |> ignore
                                                Actor.cast flag (Some true)
                                            | None -> ()

                                            return! loop ()
                                        }

                                    loop ())

                            do! sleep 200
                            let! gotExit = Actor.call flag None
                            assertThat gotExit isTrue
                        }
                    )
            )
        ]
    )

let tests =
    testList ("Supervision", [ linkTests; supervisedTests; stopAbnormalTests ])
