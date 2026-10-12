"""Compare precompiled ordinary actor workloads using one verified OTP runtime."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess


def run(directory, expression, paths=(), cpu=None):
    command = ["mise", "exec", "erlang@27.3.4.9", "rebar@3.27.0", "--",
               "erl", "+S", "1:1", "+A", "1", "-noshell"]
    if paths:
        command += ["-pa", *map(str, paths)]
    command += ["-eval", expression]
    if cpu is not None:
        command = ["taskset", "-c", str(cpu), *command]
    return subprocess.run(command, cwd=directory, text=True, capture_output=True,
                          check=True, timeout=60).stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", action="append", required=True,
                        metavar="NAME=COMPILED_DIRECTORY")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    variants = dict(value.split("=", 1) for value in args.variant)
    if len(variants) < 2 or len(variants) != len(args.variant):
        parser.error("provide at least two uniquely named variants")
    variants = {name: Path(directory).resolve() for name, directory in variants.items()}
    cpu = min(os.sched_getaffinity(0))
    paths, versions, hashes, tokens = {}, {}, {}, {}
    for name, directory in variants.items():
        paths[name] = sorted((directory / "_build/default/lib").glob("*/ebin"))
        if not paths[name]:
            parser.error(f"no compiled application found in {directory}")
        versions[name] = run(directory, 'io:format("~s", [erlang:system_info(system_version)]), halt().')
        if "Erlang/OTP 27 " not in versions[name]:
            raise RuntimeError(f"unexpected runtime: {versions[name]}")
        # invariant: every variant executes the same generated benchmark and bundled runtime sources
        sources = [directory / "src/ordinary_overhead_bench.erl"]
        sources += sorted(p for p in (directory / "_build/default/lib").glob("**/src/fable_*.erl")
                          if not p.name.startswith("fable_beam_"))
        hashes[name] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
        if not {"fable_async.erl", "fable_cancellation.erl"}.issubset(hashes[name]):
            raise RuntimeError(f"bundled runtime sources missing in {directory}")
        tokens[name] = int(run(directory,
            'code:ensure_loaded(fable_cancellation), '
            'erlang:trace_pattern({fable_cancellation,create,0},true,[call_count]), '
            'ordinary_overhead_bench:sample(<<"async">>,1000), '
            '{call_count,N}=erlang:trace_info({fable_cancellation,create,0},call_count), '
            'io:format("~p",[N]), halt().', paths[name], cpu))
    if len(set(versions.values())) != 1 or any(h != next(iter(hashes.values())) for h in hashes.values()):
        raise RuntimeError("runtime versions or benchmark/runtime sources differ")
    rows = []
    # decision: rotate and reverse revision order and exclude warm-up rounds to limit order-dependent timing bias
    for repetition in range(6):
        order = list(variants)
        offset = repetition % len(order)
        order = order[offset:] + order[:offset]
        if repetition % 2:
            order.reverse()
        for name in order:
            output = run(variants[name], 'main:main([<<"200000">>, <<"5">>]), halt().', paths[name], cpu)
            for line in output.splitlines():
                row = json.loads(line)
                if row["checksum"] != row["iterations"]:
                    raise RuntimeError(f"invalid result: {row}")
                rows.append(dict(row, variant=name, repetition=repetition))
            print(f"{name}: repetition {repetition + 1}/6", flush=True)
    summary = {}
    for case in ["bind", "async", "receive", "finally", "call"]:
        summary[case] = {}
        for name in variants:
            samples = [r for r in rows if r["variant"] == name and r["case"] == case and r["round"] > 0]
            timings = [r["microseconds"] * 1000 / r["iterations"] for r in samples]
            summary[case][name] = {
                "samples": len(samples),
                "median_ns_per_operation": statistics.median(timings),
                "min_ns_per_operation": min(timings),
                "max_ns_per_operation": max(timings),
                "median_reductions_per_operation": statistics.median(r["reductions"] / r["iterations"] for r in samples),
            }
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "raw.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    (args.output / "summary.json").write_text(json.dumps({
        "cpu": cpu, "runtime_versions": versions, "source_sha256": hashes,
        "async_token_creations_per_1000_binds": tokens, "cases": summary,
    }, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
