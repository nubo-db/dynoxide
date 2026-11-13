#!/usr/bin/env python3
"""Check that every iai-callgrind benchmark in a PR run had a baseline to compare with.

The PR job benchmarks the base commit with `--save-baseline=base` and then the
PR's merge commit with `--baseline=base`. iai-callgrind compares where it finds
a baseline and silently prints N/A where it does not, so a run whose baseline
went missing (stale or cleared target directory, a renamed output path, a base
run that never happened) looks exactly like a clean one. This script turns that
into a failure.

Both runs pass `--save-summary=json`, which writes a summary.json beside each
benchmark's output under target/iai. Two subcommands:

    record  after the base run: list the benchmarks the base produced, then
            delete their summary.json files, so a benchmark the PR removes
            cannot leave a base summary behind for `check` to read.
    check   after the head run: every benchmark must show an old instruction
            count, unless the base never had it. A benchmark only the head has
            is new, reported, and allowed: a PR that adds or renames a
            benchmark must be able to pass.

Exit codes, which the workflow relies on:

    0  every benchmark compared, or is new in the head
    1  could not check (no summaries at all, unreadable file)
    4  a benchmark the base produced has no baseline in the head run

Usage:
    python3 scripts/check_iai_baselines.py record \\
        --iai-dir target/iai --output iai_base_benchmarks.txt
    python3 scripts/check_iai_baselines.py check \\
        --iai-dir target/iai --base-list iai_base_benchmarks.txt
"""

import argparse
import json
import os
import sys

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_MISSING_BASELINE = 4

SUMMARY_FILE = "summary.json"


def find_summaries(iai_dir):
    """Return the paths of every summary.json under iai_dir, sorted."""
    found = []
    for root, _dirs, files in os.walk(iai_dir):
        if SUMMARY_FILE in files:
            found.append(os.path.join(root, SUMMARY_FILE))
    return sorted(found)


def benchmark_name(summary):
    """The benchmark's name as iai-callgrind prints it, e.g. `iai_core::g::f id`."""
    name = summary["module_path"]
    if summary.get("id"):
        name += " " + summary["id"]
    return name


def old_instructions(summary):
    """The baseline's instruction count, or None when there was nothing to compare with.

    The total for each event kind is serialised as {"Left": new} when only the
    new run has a value, {"Both": [new, old]} when both do, and {"Right": old}
    when only the old one does.
    """
    callgrind = summary.get("callgrind_summary") or {}
    total = (callgrind.get("callgrind_run") or {}).get("total") or {}
    ir = (total.get("summary") or {}).get("Ir")
    if not ir:
        return None
    metrics = ir.get("metrics") or {}
    if "Both" in metrics:
        return metrics["Both"][1]
    if "Right" in metrics:
        return metrics["Right"]
    return None


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def record(iai_dir, output):
    paths = find_summaries(iai_dir)
    if not paths:
        print(f"error: the base run left no {SUMMARY_FILE} under {iai_dir}", file=sys.stderr)
        return EXIT_ERROR
    names = sorted({benchmark_name(load(p)) for p in paths})
    with open(output, "w", encoding="utf-8") as f:
        f.write("\n".join(names) + "\n")
    for p in paths:
        os.remove(p)
    print(f"Base run produced {len(names)} benchmarks")
    return EXIT_OK


def check(iai_dir, base_list):
    try:
        with open(base_list, encoding="utf-8") as f:
            base = {line.strip() for line in f if line.strip()}
    except OSError as e:
        print(f"error: cannot read the base benchmark list: {e}", file=sys.stderr)
        return EXIT_ERROR
    if not base:
        print("error: the base run produced no benchmarks", file=sys.stderr)
        return EXIT_ERROR

    paths = find_summaries(iai_dir)
    if not paths:
        print(f"error: the head run left no {SUMMARY_FILE} under {iai_dir}", file=sys.stderr)
        return EXIT_ERROR

    compared, new, missing = [], [], []
    for p in paths:
        summary = load(p)
        name = benchmark_name(summary)
        old = old_instructions(summary)
        if old is not None:
            compared.append((name, old))
        elif name in base:
            missing.append(name)
        else:
            new.append(name)

    for name, old in compared:
        print(f"compared  {name} (base: {old} instructions)")
    for name in new:
        print(f"new       {name} (not in the base, nothing to compare with)")
    for name in missing:
        print(f"MISSING   {name} (in the base, but no baseline was found)")
    print(
        f"{len(paths)} benchmarks checked: {len(compared)} compared, "
        f"{len(new)} new, {len(missing)} without a baseline"
    )
    return EXIT_MISSING_BASELINE if missing else EXIT_OK


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    rec = sub.add_parser("record", help="list the base run's benchmarks")
    rec.add_argument("--iai-dir", required=True)
    rec.add_argument("--output", required=True)

    chk = sub.add_parser("check", help="check the head run against the base list")
    chk.add_argument("--iai-dir", required=True)
    chk.add_argument("--base-list", required=True)

    args = parser.parse_args(argv)
    try:
        if args.command == "record":
            return record(args.iai_dir, args.output)
        return check(args.iai_dir, args.base_list)
    except (OSError, ValueError, KeyError) as e:
        print(f"error: {e}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
