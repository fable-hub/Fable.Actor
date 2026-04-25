# Fable.Actor development tasks

# Development mode: use local Fable repo instead of dotnet tool
# Usage: just dev=true test-beam
dev := "false"
fable_repo := justfile_directory() / "../fable"
fable := if dev == "true" { "dotnet run --project " + fable_repo / "beam-improvements-17/src/Fable.Cli" + " --" } else { "dotnet fable" }
fable_python := if dev == "true" { "dotnet run --project " + fable_repo / "main/src/Fable.Cli" + " --" } else { "dotnet fable" }

src_path := "src/Fable.Actor"
build_path := "build"
test_path := "test"

# List available recipes
default:
    @just --list

# Clean build artifacts
clean:
    rm -rf apps _build {{build_path}}

# --- Build ---

# Build F# to Erlang via Fable.Beam, then compile with rebar3
build: clean
    {{fable}} src/Fable.Actor --exclude Fable.Core --lang beam --outDir apps/fable_actor --noCache
    rebar3 compile

# Build F# projects only (type check)
check:
    dotnet build src/Fable.Actor

# Format source files
format:
    dotnet fantomas src

# Setup tooling
restore:
    dotnet tool restore

# Build and check
all: check build

# --- Packaging ---

# Create NuGet package with version from CHANGELOG.md
pack:
    #!/usr/bin/env bash
    set -euo pipefail
    VERSION=$(grep -m1 '^## ' CHANGELOG.md | sed 's/^## \([^ ]*\).*/\1/')
    dotnet pack {{src_path}} -c Release -p:PackageVersion=$VERSION -p:InformationalVersion=$VERSION

# Create NuGet package with specific version (used in CI)
pack-version version:
    dotnet pack {{src_path}} -c Release -p:PackageVersion={{version}} -p:InformationalVersion={{version}}

# Release: pack and push to NuGet
release: pack
    dotnet nuget push 'src/**/Release/*.nupkg' -s https://api.nuget.org/v3/index.json -k $NUGET_KEY

# Run EasyBuild.ShipIt for release management
shipit *args:
    dotnet shipit --pre-release rc {{args}}

# --- Tests ---

# Run all tests (.NET + Python + BEAM)
test: test-native test-python test-beam

# Run .NET tests only
test-native:
    dotnet build {{test_path}}
    @echo "Running .NET tests..."
    dotnet run --project {{test_path}}

# Run Python tests: compile F# → Python via Fable, then run
test-python:
    rm -rf {{build_path}}/tests
    {{fable_python}} {{test_path}} --lang python --outDir {{build_path}}/tests --exclude Fable.Core --noCache
    @echo "Running Python tests..."
    cd {{build_path}}/tests && uv run --project ../.. python program.py

# Run BEAM tests: compile F# → Erlang via Fable, then run
test-beam: build
    {{fable}} {{test_path}} --exclude Fable.Core --lang beam --outDir apps/test --noCache
    cd {{justfile_directory()}} && rebar3 compile
    @echo "Running BEAM tests..."
    cd {{justfile_directory()}} && erl \
        -pa _build/default/lib/*/ebin \
        -noshell \
        -eval "program:main([])" \
        -s init stop

# --- Timeflies example ---

timeflies_path := "examples/timeflies-beam"
timeflies_src := timeflies_path / "src"
timeflies_app := timeflies_path / "apps/timeflies"

# Build timeflies example: F# → Erlang, compile with rebar3
build-timeflies: build
    {{fable}} {{timeflies_src}} --exclude Fable.Core --lang beam --outDir {{timeflies_app}} --noCache
    cp {{timeflies_src}}/erl/*.erl {{timeflies_app}}/src/
    cd {{timeflies_path}} && rebar3 compile

# Run timeflies demo server on http://localhost:3000
run-timeflies: build-timeflies
    cd {{timeflies_path}} && erl \
        -pa _build/default/lib/*/ebin \
        -noshell \
        -eval "fable_actor_timeflies_app:start()" \
        -eval "receive stop -> ok end"

# --- Timeflies Python example ---

timeflies_py_path := "examples/timeflies-python"
timeflies_py_src := timeflies_py_path / "src"
timeflies_py_out := timeflies_py_path / "output"

# Build timeflies-python: F# → Python via Fable
build-timeflies-python:
    rm -rf {{timeflies_py_out}}
    {{fable_python}} {{timeflies_py_src}} --lang python --outDir {{timeflies_py_out}} --exclude Fable.Core --noCache
    touch {{timeflies_py_out}}/src/__init__.py
    touch {{timeflies_py_out}}/src/Fable_Actor/__init__.py

# Run timeflies-python demo
run-timeflies-python: build-timeflies-python
    cd {{timeflies_py_out}} && uv run --project ../pyproject.toml python program.py

# --- Timeflies JS example ---

timeflies_js_path := "examples/timeflies-js"
timeflies_js_src := timeflies_js_path / "src"

# Build timeflies-js: F# → JavaScript via Fable
build-timeflies-js:
    cd {{timeflies_js_path}} && npm install
    cd {{timeflies_js_path}} && dotnet fable src --noCache

# Run timeflies-js demo on http://localhost:3000
run-timeflies-js: build-timeflies-js
    cd {{timeflies_js_path}} && npx vite
