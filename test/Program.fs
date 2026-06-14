module Fable.Actor.TestRunner

open Fable.Actor
open Fable.Actor.ActorTest

let runTest name (f: unit -> ActorOp<unit>) =
    actor {
        try
            do! f ()
            printfn "  PASS: %s" name
            return true
        with ex ->
            printfn "  FAIL: %s - %s" name (ex.Message)
            return false
    }

let mainAsync =
    actor {
        printfn "Fable.Actor Tests"
        printfn "================="

        // basic spawn
        let! r1 = runTest "actor_empty" actor_empty_test
        let! r2 = runTest "actor_single_receive" actor_single_receive_test
        let! r3 = runTest "actor_multiple_receive" actor_multiple_receive_test
        let! r4 = runTest "actor_post" actor_post_test

        // stateful (start)
        let! r5 = runTest "actor_start_basic" actor_start_basic_test
        let! r6 = runTest "actor_start_stop" actor_start_stop_test

        // call/reply
        let! r7 = runTest "actor_call_reply" actor_call_reply_test
        let! r8 = runTest "actor_multiple_calls" actor_multiple_calls_test

        // spawn + CE
        let! r9 = runTest "actor_spawn_send" actor_spawn_send_test
        let! r10 = runTest "actor_spawn_receive_forward" actor_spawn_receive_forward_test
        let! r11 = runTest "actor_async_work" actor_async_work_test

        // timers
        let! r12 = runTest "actor_schedule" actor_schedule_test

        // supervision
        let! r13 = runTest "actor_linked_crash" actor_linked_crash_test
        let! r14 = runTest "actor_supervised_restart" actor_supervised_restart_test
        let! r15 = runTest "actor_supervised_stop" actor_supervised_stop_test

        // StopAbnormal
        let! r16 = runTest "actor_stop_abnormal" actor_stop_abnormal_test
        let! r17 = runTest "actor_start_stop_abnormal" actor_start_stop_abnormal_test
        let! r18 = runTest "actor_start_handler_stop_abnormal" actor_start_handler_stop_abnormal_test

        // callWithTimeout
        let! r19 = runTest "actor_call_with_timeout_success" actor_call_with_timeout_success_test
        let! r20 = runTest "actor_call_with_timeout_expires" actor_call_with_timeout_expires_test

        // kill
        let! r21 = runTest "actor_kill" actor_kill_test

        // CE control flow (for/while/use/try-finally) + async bridge
        let! r22 = runTest "actor_for_loop" actor_for_loop_test
        let! r23 = runTest "actor_while_loop" actor_while_loop_test
        let! r24 = runTest "actor_use_dispose" actor_use_dispose_test
        let! r25 = runTest "actor_try_finally_normal" actor_try_finally_normal_test
        let! r26 = runTest "actor_try_finally_exn" actor_try_finally_exn_test
        let! r27 = runTest "actor_async_bind" actor_async_bind_test

        let results = [
            r1
            r2
            r3
            r4
            r5
            r6
            r7
            r8
            r9
            r10
            r11
            r12
            r13
            r14
            r15
            r16
            r17
            r18
            r19
            r20
            r21
            r22
            r23
            r24
            r25
            r26
            r27
        ]

        let passed = results |> List.filter id |> List.length
        let total = results.Length
        printfn ""
        printfn "%d/%d tests passed" passed total

        return if passed = total then 0 else 1
    }

#if FABLE_COMPILER_BEAM

// BEAM: run the CPS computation
[<EntryPoint>]
let main _argv =
    let mutable exitCode = 1
    mainAsync.Run(fun code -> exitCode <- code)
    exitCode

#else

// .NET / Python: run via Async.RunSynchronously
[<EntryPoint>]
let main _argv = Async.RunSynchronously mainAsync

#endif
