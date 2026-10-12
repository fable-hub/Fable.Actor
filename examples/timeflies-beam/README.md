# Timeflies - Fable.Actor Demo

A classic Reactive Extensions demo reimplemented with actors using Fable.Actor (F#/Fable.Beam) and Cowboy WebSockets.

The server setup and HTTP handler use `Fable.Beam.Cowboy`. The native Erlang
WebSocket handler demonstrates exchanging messages with the F# actor pipeline.

## What it does

The letters of "TIME FLIES LIKE AN ARROW" follow your mouse cursor, with each successive letter
delayed by an increasing amount (80ms per letter). This creates a trailing snake-like effect where the first letter
follows immediately, while later letters lag behind.

## Architecture

```text
Browser                              BEAM Server (Cowboy + Fable.Actor)
┌─────────────────┐                  ┌────────────────────────────────────┐
│  mousemove      │ ──WebSocket──>   │  Distributor actor                 │
│  events (x,y)   │                  │         │                          │
│                 │                  │         ▼                          │
│                 │                  │  fan out MouseMove to all          │
│                 │                  │  letter actors (one per char)      │
│                 │                  │         │                          │
│                 │                  │  Each letter actor:                │
│                 │                  │    schedule(80ms * index)          │
│                 │                  │    then send JSON back             │
│                 │                  │         │                          │
│  render letters │ <──WebSocket──   │         ▼                          │
│  at positions   │                  │  sendFn -> WebSocket frame         │
└─────────────────┘                  └────────────────────────────────────┘
```

## Key features demonstrated

- **`spawn` / `spawnLinked`** - Distributor spawns linked letter actors; killing the distributor cleans up all children
- **`actor { }`** - CPS computation expression for message receive loops
- **`receive`** - Each actor blocks until a message arrives
- **`schedule`** - Per-letter delay timer (80ms * index) before sending position update
- **`send`** - Fan-out from distributor to letter actors

## Prerequisites

- .NET SDK 10+
- Fable BEAM compiler
- Erlang/OTP 27+
- rebar3

## Running

```sh
# From the repository root
just run-timeflies
```

Then open <http://localhost:3000> in your browser and move your mouse around.

## Original

This is a port of the classic Rx Timeflies example. See the original F# version in [FSharp.Control.AsyncRx](https://github.com/dbrattli/AsyncRx).
