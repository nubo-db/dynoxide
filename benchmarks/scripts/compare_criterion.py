#!/usr/bin/env python3
"""Compare criterion results against recent runs stored on the benchmark-data branch.

The baseline is the per-benchmark median of the last N stored runs, not the
single newest one. Wall-clock numbers on GitHub's hosted runners move with
whatever host a job lands on, so any one run can sit well outside the usual
spread. Taking the newest run as the reference lets a single anomalous run
become the baseline for every comparison until the next refresh, which both
floods PRs with false regressions and hides real ones behind the offset.

Reads either criterion's bencher-format output or an already-parsed
criterion_baseline.json and writes a markdown comparison table. Both workflows
branch on the exit code, so it is a contract:

    0  clean, nothing over the threshold
    1  could not compare
    2  usage error (argparse's own)
    3  threshold exceeded, advisory
    4  compared, but benchmarks in the baseline produced no result

Usage:
    python3 benchmarks/scripts/compare_criterion.py \
        --current-output benchmarks/criterion_output.txt \
        --output comparison.md

    python3 benchmarks/scripts/compare_criterion.py \
        --current-json benchmarks/results/criterion_baseline.json \
        --window 5 --output comparison.md
"""

import argparse
import json
import re
import statistics
import subprocess
import sys


# criterion --output-format bencher writes a result as "test NAME ... " and
# "bench: N,NNN ns/iter (+/- N)", in two separate writes with the measurement
# in between. On a clean run they land on one line:
#
#     test get_item ... bench: 15,543 ns/iter (+/- 755)
#
# Criterion prints its own errors to stdout rather than stderr, so anything it
# has to report during the run splits that line in two:
#
#     test get_item ... Criterion.rs ERROR: error: Failed to access file "..."
#     bench: 15,543 ns/iter (+/- 755)
#
# Matching the header and the measurement separately reads both shapes. A
# header with no measurement after it is a benchmark that produced no result,
# and is left out so the caller reports it as missing.
HEADER_RE = re.compile(r"^test\s+(.+?)\s+\.\.\.\s*(.*)$")
BENCH_RE = re.compile(r"^\s*bench:\s+([\d,]+)\s+ns/iter")

EXIT_OK = 0
EXIT_ERROR = 1
# Not 2: argparse exits 2 on a usage error, and a caller branching on the
# regression signal must not read a mistyped flag as a regression.
EXIT_REGRESSED = 3
# A run that lost benchmarks compared the ones it still had, so the report is
# worth keeping. It is separate from 1 for that reason: the caller has a table
# to show, where an exit 1 leaves it nothing.
EXIT_INCOMPLETE = 4


def parse_bencher(text):
    """Parse criterion bencher-format output into {benchmark_name: ns}."""
    results = {}
    pending = None
    for line in text.splitlines():
        header = HEADER_RE.match(line)
        if header:
            name, remainder = header.group(1), header.group(2)
            measurement = BENCH_RE.match(remainder)
            if measurement:
                results[name] = _nanoseconds(measurement)
                pending = None
            else:
                # Whatever criterion printed here pushed the measurement onto
                # the next line. Hold the name until it arrives; if it never
                # does, the next header discards it.
                pending = name
            continue

        measurement = BENCH_RE.match(line)
        if measurement and pending is not None:
            results[pending] = _nanoseconds(measurement)
            pending = None
    return results


def _nanoseconds(measurement):
    """Read the ns/iter figure out of a matched measurement."""
    return int(measurement.group(1).replace(",", ""))


def git(*args):
    """Run a git command, returning stdout or None if it failed."""
    proc = subprocess.run(
        ("git",) + args, capture_output=True, text=True
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def load_history(ref, window):
    """Load up to `window` most recent stored runs from `ref`.

    Returns a list of (run_dir, {benchmark: ns}), oldest first, or None when the
    ref could not be read at all. None and [] are deliberately different: a ref
    we cannot read means no comparison happened, which must not be reported as a
    comparison that found nothing wrong.

    Run directories are named runs/YYYY-MM-DDTHHMMSSZ-<sha>, so sorting them
    lexically is chronological. Runs stored before that naming landed are
    date-only; they sort before same-day timestamped runs, which is the right
    order since they are older.

    Runs without a criterion_baseline.json, or with an empty or unparseable one,
    are skipped rather than counted against the window. An empty file otherwise
    consumes a slot and shrinks the sample the median rests on.
    """
    listing = git("ls-tree", "--name-only", ref, "runs/")
    if listing is None:
        return None

    run_dirs = sorted(d for d in listing.splitlines() if d)
    history = []
    for run_dir in reversed(run_dirs):
        if len(history) >= window:
            break
        raw = git("show", "{}:{}/criterion_baseline.json".format(ref, run_dir))
        if raw is None:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not parsed:
            continue
        history.append((run_dir, parsed))
    history.reverse()
    return history


def build_baseline(history):
    """Reduce history to {benchmark: (median, low, high, sample_count)}."""
    samples = {}
    for _, run in history:
        for name, value in run.items():
            samples.setdefault(name, []).append(value)
    return {
        name: (statistics.median(vals), min(vals), max(vals), len(vals))
        for name, vals in samples.items()
    }


def sanitize(text):
    """Escape markdown table metacharacters in a benchmark name.

    The backslash is escaped first (it leads the character class), otherwise a
    name containing a literal \\| would emerge as \\\\| and the pipe would still
    read as a real cell delimiter.
    """
    return re.sub(r"([\\|`\[\]<>])", r"\\\1", text)


def build_report(current, baseline, history, threshold):
    """Render the markdown comparison.

    Returns (markdown, regressions, missing), where `missing` names the
    baselined benchmarks this run produced no result for.
    """
    regressions = []
    missing = []
    rows = []

    for name in sorted(set(current) | set(baseline)):
        now = current.get(name)
        stats = baseline.get(name)

        if now is not None and stats:
            median, low, high, count = stats
            ratio = now / median if median else None
            if ratio is None:
                change = "—"
            else:
                pct = (ratio - 1) * 100
                change = "{}{:.1f}%".format("+" if pct >= 0 else "", pct)
                # A single sample is not a median. Flagging against it is the
                # same one-run comparison this script exists to replace, so a
                # benchmark new to the window reports its change without the
                # warning until a second run backs it up.
                if ratio > threshold and count > 1:
                    regressions.append(name)
                    change += " :warning:"
            baseline_cell = "{:,}".format(round(median))
            if count < len(history):
                baseline_cell += " (n={})".format(count)
            rows.append((
                name,
                baseline_cell,
                "{:,} - {:,}".format(low, high),
                "{:,}".format(now),
                change,
            ))
        elif now is not None:
            rows.append((name, "—", "—", "{:,}".format(now), "new"))
        else:
            median, low, high, _ = stats
            missing.append(name)
            rows.append((
                name,
                "{:,}".format(round(median)),
                "{:,} - {:,}".format(low, high),
                "—",
                "removed",
            ))

    window = len(history)
    lines = ["## Criterion Benchmark Results", ""]

    if window:
        lines.append(
            "Baseline is the per-benchmark median of the last {} stored {}, "
            "so one unusually fast or slow runner cannot skew the comparison. "
            "The range column is the spread across those runs.".format(
                window, "run" if window == 1 else "runs"
            )
        )
        lines.append("")

    lines.append("| Benchmark | Baseline (ns/iter) | Range | Current | Change |")
    lines.append("|-----------|-------------------:|------:|--------:|--------|")
    for name, median, spread, now, change in rows:
        lines.append("| {} | {} | {} | {} | {} |".format(
            sanitize(name), median, spread, now, sanitize(change)
        ))
    lines.append("")

    if not window:
        lines.append(
            "> No stored runs to compare against. Baselines are recorded by "
            "the benchmark refresh workflow."
        )
    else:
        if regressions:
            lines.append(
                "> :warning: {} benchmark(s) came in more than {:.0%} above the "
                "{}-run median. Check whether the current value falls outside "
                "the range column before treating it as real; iai-callgrind "
                "gives a deterministic instruction-count answer.".format(
                    len(regressions), threshold - 1, window
                )
            )
        # Not an elif. A run can both lose benchmarks and regress on the ones
        # it kept, and the missing list is the more important of the two to
        # read, so it must not be hidden behind the regression warning.
        if missing:
            lines.append(
                "> :warning: {} benchmark(s) in the baseline produced no result "
                "in this run: {}. A partial bench run reports the rest as "
                "clean, so treat this as a failed comparison rather than a "
                "pass.".format(
                    len(missing), ", ".join(sanitize(n) for n in missing)
                )
            )
        if not regressions and not missing:
            lines.append(
                "> All benchmarks within {:.0%} of the {}-run median.".format(
                    threshold - 1, window
                )
            )

    if window:
        lines.append("")
        lines.append("<details><summary>Runs in the baseline</summary>")
        lines.append("")
        for run_dir, _ in history:
            lines.append("- `{}`".format(sanitize(run_dir)))
        lines.append("")
        lines.append("</details>")

    return "\n".join(lines) + "\n", regressions, missing


def main():
    parser = argparse.ArgumentParser(
        description="Compare criterion results against recent stored runs")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--current-output",
                        help="criterion bencher-format output to compare")
    source.add_argument("--current-json",
                        help="already-parsed criterion results JSON")
    parser.add_argument("--ref", default="benchmark-data",
                        help="git ref holding stored runs (default: benchmark-data)")
    parser.add_argument("--window", type=int, default=5,
                        help="how many recent runs the median spans (default: 5)")
    parser.add_argument("--threshold", type=float, default=1.50,
                        help="flag above this multiple of the median (default: 1.50)")
    parser.add_argument("--output",
                        help="write markdown here instead of stdout")
    args = parser.parse_args()

    if args.window < 1:
        parser.error("--window must be at least 1")

    try:
        if args.current_output:
            with open(args.current_output) as handle:
                current = parse_bencher(handle.read())
        else:
            with open(args.current_json) as handle:
                current = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        print("could not read current results: {}".format(exc), file=sys.stderr)
        return EXIT_ERROR

    if not current:
        print("no criterion results parsed, nothing to compare", file=sys.stderr)
        return EXIT_ERROR

    history = load_history(args.ref, args.window)
    if history is None:
        print("could not read run history from '{}'; no comparison was made"
              .format(args.ref), file=sys.stderr)
        return EXIT_ERROR
    if not history:
        print("no stored runs found on '{}'".format(args.ref), file=sys.stderr)

    markdown, regressions, missing = build_report(
        current, build_baseline(history), history, args.threshold
    )

    if args.output:
        with open(args.output, "w") as handle:
            handle.write(markdown)
    else:
        sys.stdout.write(markdown)

    if regressions:
        print("exceeded threshold: {}".format(", ".join(regressions)),
              file=sys.stderr)

    # Reported before the regression exit, and takes precedence over it. A run
    # missing benchmarks cannot say the rest are clean, so it must not leave
    # the advisory exit 3 -- or worse, exit 0 -- as the whole signal.
    if missing:
        print("no result for: {}".format(", ".join(missing)), file=sys.stderr)
        return EXIT_INCOMPLETE
    if regressions:
        return EXIT_REGRESSED
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
