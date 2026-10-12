# Documentation

Read the guides in order, or jump to the feature you need.

| Step | Guide | What you learn |
| --- | --- | --- |
| 1 | [Getting started](getting-started.md) | Install the library and write a message loop |
| 2 | [Messaging](messaging.md) | Keep state, request replies and handle deadlines |
| 3 | [Supervision](supervision.md) | Own linked children and choose recovery policies |
| 4 | [Lifetimes](lifetimes.md) | Opt into cooperative shutdown and retained outcomes |

The library is experimental. These guides describe this branch; the `Lifetimes`
module is new in PR #77 and is not part of the published 5.0.0-rc.14 package.

For target differences, see [Platform notes](platforms.md). To build the repo,
run tests or try Timeflies, see [Development](development.md).
[Maintainer notes](maintainers/lifetimes.md) cover controller internals, runtime
probes and validation evidence.
