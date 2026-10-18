# Fable.Actor development tasks

# Development mode: compile with a local Fable checkout instead of the pinned dotnet tool.
# Whatever branch that checkout has out is what gets used — every backend lives in one repo.
# Usage: just dev=true test-beam
# decision: switches every backend through one fable variable so dev mode cannot silently mix compiler versions
# assumption: the sibling ../Fable checkout contains src/Fable.Cli when dev=true
dev := "false"
fable_repo := justfile_directory() / "../Fable"
fable := if dev == "true" { "dotnet run --project " + fable_repo / "src/Fable.Cli" + " --" } else { "dotnet fable" }

# decision: select the declared OTP and Rebar for both compilation and execution, including recipes entered from isolated worktrees
beam := "mise exec --"

src_path := "src/Fable.Actor"
build_path := "build"
test_path := "test"

# Every hand-written F# directory, including the examples and the benchmark harness.
# decision: formats examples and benchmarks because a recipe that no CI job compiles also never gets formatted
# assumption: Fantomas skips generated fable_modules, node_modules, bin and obj through .gitignore
format_paths := "src " + test_path + " benchmarks/ordinary-beam examples/timeflies/src examples/timeflies-js/src examples/timeflies-python/src examples/timeflies-beam/src examples/interop/src"

# List available recipes
default:
    @just --list

# invariant: no generated *.fs.js survives in the library sources, so a sidecar can never shadow a module
# Clean build artifacts and any generated sidecar left in the library sources
clean:
    rm -rf apps _build {{build_path}}
    rm -rf {{timeflies_path}}/apps {{timeflies_path}}/_build {{timeflies_py_out}} {{timeflies_js_out}}
    rm -f {{src_path}}/*.fs.js

# --- Build ---

# Build F# to Erlang via Fable.Beam, then compile with rebar3
build: clean
    {{fable}} src/Fable.Actor --exclude Fable.Core --lang beam --outDir apps/fable_actor --noCache
    {{beam}} rebar3 compile

# Build F# projects only (type check)
check:
    dotnet build src/Fable.Actor
    dotnet build {{test_path}}

# Format source files
format:
    dotnet fantomas {{format_paths}}

# Verify formatting without rewriting files
format-check:
    dotnet fantomas --check {{format_paths}}

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

# One suite in test/, compiled to each target from the same project. Assertions come from
# Scriptorium.Nib, the runner from Scriptorium.Quill.
# decision: compiles one test project to all targets because the library itself has one conditional project
# invariant: every test recipe executes the same F# suite through its target runtime
# tradeoff: target-specific setup stays in recipes and Helpers.fs to prevent divergent suites

# Run the behavioral suite on every target (.NET + Python + JS + BEAM)
test: test-native test-python test-js test-beam

# .NET target: a real behavioral run — the non-BEAM Actor is MailboxProcessor-based
test-native:
    dotnet run --project {{test_path}}

# invariant: this consumer has no source-project reference or access to internal command constructors
# invariant: the packed source executes on every supported target, not just the source-project suite
# Restore the command surface from a packed artifact in a fresh external consumer.
test-command-package:
    #!/usr/bin/env bash
    set -euo pipefail
    command_package_workspace=$(mktemp -d)
    trap 'rm -rf "$command_package_workspace"' EXIT
    dotnet pack {{src_path}} -c Release -p:PackageVersion=0.0.0-command-test -o "$command_package_workspace/feed"
    mkdir "$command_package_workspace/consumer"
    cp {{test_path}}/CommandPackageConsumer/Consumer.fsproj {{test_path}}/CommandPackageConsumer/Main.fs {{test_path}}/CommandPackageConsumer/Misuse.fs "$command_package_workspace/consumer/"
    cat > "$command_package_workspace/consumer/NuGet.Config" <<EOF
    <configuration><packageSources><clear /><add key="command-test" value="$command_package_workspace/feed" /><add key="nuget" value="https://api.nuget.org/v3/index.json" /></packageSources></configuration>
    EOF
    export NUGET_PACKAGES="$command_package_workspace/cache"
    dotnet run --project "$command_package_workspace/consumer"
    for command_check in COMMAND_MESSAGE_MISMATCH COMMAND_SEND_MISMATCH ACTOR_REFERENCE_RECEIVE; do
        command_check_log="$command_package_workspace/$command_check.log"
        if dotnet build "$command_package_workspace/consumer" -p:DefineConstants="$command_check" > "$command_check_log" 2>&1; then
            cat "$command_check_log"
            echo "Expected command message-type rejection: $command_check" >&2
            exit 1
        fi
        grep -Eq 'error FS(0001|0041|0039)' "$command_check_log" || { cat "$command_check_log"; exit 1; }
    done
    echo 'Packed actor compiler checks rejected message-type mismatches and receiving through a message reference.'
    {{fable}} "$command_package_workspace/consumer" --exclude Fable.Core --lang javascript --outDir "$command_package_workspace/output"
    echo '{"type":"module"}' > "$command_package_workspace/output/package.json"
    node "$command_package_workspace/output/Main.js"
    {{fable}} "$command_package_workspace/consumer" --exclude Fable.Core --lang python --outDir "$command_package_workspace/python"
    uv run python "$command_package_workspace/python/main.py"
    {{fable}} "$command_package_workspace/consumer" --exclude Fable.Core --lang beam --outDir "$command_package_workspace/beam"
    (cd "$command_package_workspace/beam" && {{beam}} rebar3 compile && {{beam}} erl -noshell -pa _build/default/lib/*/ebin -eval 'main:main([])')

# Python target: compile the suite to Python and run the explicit runner
test-python:
    rm -rf {{build_path}}/tests-py
    {{fable}} {{test_path}} --exclude Fable.Core --lang python --outDir {{build_path}}/tests-py
    uv run python {{build_path}}/tests-py/main.py

# JS target: compile the suite to JS and run it under Node
test-js:
    rm -rf {{build_path}}/tests-js
    {{fable}} {{test_path}} --exclude Fable.Core --lang javascript --outDir {{build_path}}/tests-js
    echo '{"type":"module"}' > {{build_path}}/tests-js/package.json
    node {{build_path}}/tests-js/Main.js

# BEAM target: compile the suite to Erlang, build with rebar3, run on the BEAM VM.
# Fable pulls the Fable.Actor sources into the same outDir, so this app is self-contained
# and the generated rebar.config needs no edits. Quill calls halt/1 with the exit code.
test-beam:
    rm -rf {{build_path}}/tests-beam
    {{fable}} {{test_path}} --exclude Fable.Core --lang beam --outDir {{build_path}}/tests-beam
    cd {{build_path}}/tests-beam && {{beam}} rebar3 compile
    cd {{build_path}}/tests-beam && {{beam}} erl -noshell -pa _build/default/lib/*/ebin -eval 'main:main([])'

# --- Timeflies example ---

timeflies_path := "examples/timeflies-beam"
timeflies_src := timeflies_path / "src"
timeflies_app := timeflies_path / "apps/timeflies"

# Build timeflies example: F# → Erlang, compile with rebar3
build-timeflies: build
    {{fable}} {{timeflies_src}} --exclude Fable.Core --lang beam --outDir {{timeflies_app}} --noCache
    cp {{timeflies_src}}/erl/*.erl {{timeflies_app}}/src/
    cd {{timeflies_path}} && {{beam}} rebar3 compile

# Run timeflies demo server on http://localhost:3000
run-timeflies: build-timeflies
    cd {{timeflies_path}} && {{beam}} erl \
        -pa _build/default/lib/*/ebin \
        -noshell \
        -eval "timeflies_app:start()" \
        -eval "receive stop -> ok end"

# --- Timeflies Python example ---

timeflies_py_path := "examples/timeflies-python"
timeflies_py_src := timeflies_py_path / "src"
timeflies_py_out := timeflies_py_path / "output"

# Build timeflies-python: F# → Python via Fable
build-timeflies-python:
    rm -rf {{timeflies_py_out}}
    {{fable}} {{timeflies_py_src}} --lang python --outDir {{timeflies_py_out}} --exclude Fable.Core --noCache
    touch {{timeflies_py_out}}/src/__init__.py
    touch {{timeflies_py_out}}/src/Fable_Actor/__init__.py

# Run timeflies-python demo
run-timeflies-python: build-timeflies-python
    cd {{timeflies_py_out}} && uv run --project ../pyproject.toml python program.py

# --- Timeflies JS example ---

timeflies_js_path := "examples/timeflies-js"
timeflies_js_src := timeflies_js_path / "src"
timeflies_js_out := timeflies_js_path / "output"

# decision: emits to output/ so the referenced Fable.Actor sources keep their own directory free of generated JS
# invariant: index.html loads ./output/App.js, matching this recipe's outDir
# Build timeflies-js: F# → JavaScript via Fable
build-timeflies-js:
    cd {{timeflies_js_path}} && npm install
    cd {{timeflies_js_path}} && {{fable}} src --noCache --outDir output

# Run timeflies-js demo on http://localhost:3000
run-timeflies-js: build-timeflies-js
    cd {{timeflies_js_path}} && npx vite

# --- Interop example ---

# Compile the native Erlang interoperability example to BEAM
build-interop:
    rm -rf {{build_path}}/interop
    {{fable}} examples/interop/src --exclude Fable.Core --lang beam --outDir {{build_path}}/interop --noCache
    cd {{build_path}}/interop && {{beam}} rebar3 compile

# invariant: Erlang-to-F# messages and replies must match the compiled actor wire protocol
# Run the native Erlang peer against the compiled F# example
test-interop: build-interop
    cp examples/interop/erl/interop_demo.erl {{build_path}}/interop/src/
    cd {{build_path}}/interop && {{beam}} rebar3 compile
    cd {{build_path}}/interop && {{beam}} erl -noshell -pa _build/default/lib/*/ebin -eval 'try interop_demo:run() of ok -> halt(0) catch Class:Reason:Stack -> io:format("Interop failed: ~p:~p~n~p~n", [Class, Reason, Stack]), halt(1) end.'
