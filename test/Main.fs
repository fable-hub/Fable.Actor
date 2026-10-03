module Fable.Actor.Tests.Main

open type Scriptorium.Quill.Runner

// One runner project for every target. Fable.Actor is a single library project with
// `#if FABLE_COMPILER_BEAM` inside, so — unlike Fable.Giraffe, which has a src project per target —
// there is nothing here that needs to vary by target. Quill knows how to end the process on each
// platform:
//
//   .NET / Python  Async.RunSynchronously, so the value returned here is the process exit code
//   BEAM           the run is synchronous; Quill calls halt/1, so `erl` exits non-zero on failure
//   JS / Node      nothing can block, so runTests returns 0 immediately and chains process.exit
//                  onto the resolved promise — the value returned here is ignored
[<EntryPoint>]
let main _ =
    runTests [
        ActorTests.tests
        SupervisionTests.tests
        BuilderTests.tests
        LifecycleTests.tests
        CallLivenessTests.tests
    ]
