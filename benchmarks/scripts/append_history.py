#!/usr/bin/env python3
"""Consolidate benchmark result files into a single run_summary.json.

Reads JSON/CSV results from a given directory and produces a summary with
timestamp, commit SHA, and key metrics. Output format is designed to be
directly consumable by a future S3-hosted dashboard.

Usage:
    python3 append_history.py --results-dir benchmarks/results --output benchmarks/results/run_summary.json
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone


def git_info():
    """Get current git commit SHA and branch."""
    sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()
    short_sha = subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"], text=True
    ).strip()
    branch = subprocess.check_output(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True
    ).strip()
    return {"sha": sha, "short_sha": short_sha, "branch": branch}


def load_json(path):
    """Load a JSON file, returning None if it doesn't exist or is invalid."""
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def extract_ci_ratios(results_dir):
    """Extract CI pipeline speedup ratios."""
    data = load_json(os.path.join(results_dir, "ci_ratios.json"))
    if not data:
        return None
    return data


def extract_ci_pipeline(results_dir):
    """Extract CI pipeline benchmark details."""
    data = load_json(os.path.join(results_dir, "ci_pipeline.json"))
    if not data:
        return None
    return data


def extract_workload(results_dir, filename):
    """Extract key metrics from a workload driver result file."""
    data = load_json(os.path.join(results_dir, filename))
    if not data:
        return None
    summary = {}
    if isinstance(data, list):
        for step in data:
            name = step.get("step", step.get("name", "unknown"))
            summary[name] = {
                k: v
                for k, v in step.items()
                if k in ("ops_per_sec", "total_ms", "p50_ms", "p95_ms", "p99_ms")
            }
    elif isinstance(data, dict):
        summary = data
    return summary


def extract_startup(results_dir):
    """Extract startup benchmark results."""
    return load_json(os.path.join(results_dir, "startup.json"))


def extract_system_info(results_dir):
    """Extract system info."""
    return load_json(os.path.join(results_dir, "system_info.json"))


def extract_criterion_baseline(results_dir):
    """Extract criterion baseline results."""
    return load_json(os.path.join(results_dir, "criterion_baseline.json"))


def main():
    parser = argparse.ArgumentParser(description="Consolidate benchmark results")
    parser.add_argument(
        "--results-dir",
        required=True,
        help="Directory containing benchmark result files",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to write the consolidated run_summary.json",
    )
    args = parser.parse_args()

    results_dir = args.results_dir
    if not os.path.isdir(results_dir):
        print(f"Error: {results_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    git = git_info()

    summary = {
        "schema_version": 1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "git": git,
        "system_info": extract_system_info(results_dir),
        "results": {
            "ci_ratios": extract_ci_ratios(results_dir),
            "ci_pipeline": extract_ci_pipeline(results_dir),
            "dynoxide_http": extract_workload(results_dir, "dynoxide_http.json"),
            "dynamodb_local": extract_workload(results_dir, "dynamodb_local.json"),
            "startup": extract_startup(results_dir),
            "criterion_baseline": extract_criterion_baseline(results_dir),
        },
    }

    # Remove None entries
    summary["results"] = {
        k: v for k, v in summary["results"].items() if v is not None
    }

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"Wrote run summary to {args.output}")
    print(f"  Commit: {git['short_sha']} ({git['branch']})")
    print(f"  Result sections: {', '.join(summary['results'].keys())}")


if __name__ == "__main__":
    main()
