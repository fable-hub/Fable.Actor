# Development

Work from the repository root with .NET SDK 10+, [just](https://just.systems),
Node.js and [uv](https://docs.astral.sh/uv/). BEAM commands use
[mise](https://mise.jdx.dev/) to select the OTP and Rebar versions in `mise.toml`.

## Set up and build

```sh
dotnet tool restore
uv sync
mise trust
mise install
just check       # Type-check library and tests
just build       # Generate Erlang and compile the BEAM library
just format      # Format F# source
```

## Test

The same behavioral suite is compiled to all targets. Tests use
[Scriptorium](https://github.com/fable-hub/Scriptorium).

```sh
just test-native
just test-js
just test-python
just test-beam
just test        # All four targets
```

For local compiler work, `just dev=true test-beam` uses the sibling `../Fable`
checkout instead of the pinned released compiler. The same switch applies to
other Fable compilation recipes.

The [ordinary BEAM benchmark](../benchmarks/ordinary-beam/README.md) compares
execution costs against a release baseline, including cancellation-token creation.
Run it when changing the shared builder or its native execution helpers.

## Run Timeflies

Timeflies gives each letter an actor and fans out mouse positions to produce a
trailing animation.

| Target | Command | UI |
| --- | --- | --- |
| BEAM | `just run-timeflies` | Cowboy WebSocket server |
| Python | `just run-timeflies-python` | tkinter |
| JavaScript | `just run-timeflies-js` | React/Feliz and Vite |

## Package

`just pack` creates the single Fable.Actor NuGet package using the version in
[CHANGELOG.md](../CHANGELOG.md). It includes the ordinary Actor and optional
Lifetimes modules, their assembly and the Fable source payload.

Controller design and runtime regressions are documented in
[Maintainer notes](maintainers/lifetimes.md).
