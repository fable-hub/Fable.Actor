# Documentation

Read the guides in order, or jump to the feature you need.
[One actor API](actors.md) explains the factory overloads, message references,
inboxes and migration from the earlier surface.

| Step | Guide | What you learn |
| --- | --- | --- |
| 1 | [Getting started](getting-started.md) | Install the library and write a message loop |
| 2 | [Messaging](messaging.md) | Keep state, request replies and handle deadlines |
| 3 | [Supervision](supervision.md) | Own linked children and choose recovery policies |
| 4 | [Lifetimes](lifetimes.md) | Opt into cooperative shutdown and retained outcomes |

For state handlers that return finite work descriptions, see
[Optional actor commands](commands.md).

The library is experimental.

For target differences, see [Platform notes](platforms.md). To build the repo,
run tests or try Timeflies, see [Development](development.md).
[Maintainer notes](maintainers/lifetimes.md) cover controller internals, runtime
probes and validation evidence.
