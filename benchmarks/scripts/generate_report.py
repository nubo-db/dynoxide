#!/usr/bin/env python3
"""Generate benchmark visualisation charts from JSON/CSV results.

Reads results from `benchmarks/results/` and produces:
- Bar chart: throughput (ops/sec) across targets, per operation
- Latency percentile chart (p50/p95/p99) per operation, per target
- Stacked bar: CI pipeline time breakdown (startup + execution + teardown)
- Memory over time: line graph from memory profiler CSV
- Binary size comparison: horizontal bar chart

Usage:
    python3 generate_report.py                           # local mode, PNG output
    python3 generate_report.py --ci-mode                 # CI mode, also outputs benchmark_action.json
    python3 generate_report.py --results-dir path/to/results --output-dir path/to/charts
"""

import argparse
import csv
import json
import os
import sys

try:
    import matplotlib

    matplotlib.use("Agg")  # Non-interactive backend (no display needed)
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
except ImportError:
    print(
        "matplotlib is required: pip install matplotlib",
        file=sys.stderr,
    )
    sys.exit(1)


# ---- Colour palette ----
COLORS = {
    "dynoxide_embedded": "#2563eb",  # Blue
    "dynoxide_http": "#7c3aed",  # Purple
    "dynamodb_local": "#dc2626",  # Red
    "localstack": "#ea580c",  # Orange
}

LABELS = {
    "dynoxide_embedded": "Dynoxide (embedded)",
    "dynoxide_http": "Dynoxide (HTTP)",
    "dynamodb_local": "DynamoDB Local",
    "localstack": "LocalStack",
}


def load_json(path):
    """Load a JSON file, returning None if missing or invalid."""
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def load_csv(path):
    """Load a CSV file, returning list of dicts or None."""
    try:
        with open(path) as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return None


def save_figure(fig, output_dir, name, formats=("png", "svg")):
    """Save a figure in multiple formats."""
    for fmt in formats:
        path = os.path.join(output_dir, f"{name}.{fmt}")
        fig.savefig(path, dpi=150, bbox_inches="tight")
        print(f"  Saved: {path}")
    plt.close(fig)


# ---- Chart generators ----


def chart_throughput(results_dir, output_dir):
    """Bar chart: throughput (ops/sec) across targets, per operation."""
    dynoxide_http = load_json(os.path.join(results_dir, "dynoxide_http.json"))
    dynamodb_local = load_json(os.path.join(results_dir, "dynamodb_local.json"))

    if not dynoxide_http and not dynamodb_local:
        print("  Skipping throughput chart (no workload data)")
        return

    # Collect operations that have ops_per_sec
    operations = []
    data = {"dynoxide_http": {}, "dynamodb_local": {}}

    for source, label in [(dynoxide_http, "dynoxide_http"), (dynamodb_local, "dynamodb_local")]:
        if not source:
            continue
        items = source if isinstance(source, list) else source.get("steps", [])
        for step in items:
            name = step.get("step", step.get("name", ""))
            ops = step.get("ops_per_sec")
            if ops and name:
                data[label][name] = ops
                if name not in operations:
                    operations.append(name)

    if not operations:
        print("  Skipping throughput chart (no ops_per_sec data)")
        return

    fig, ax = plt.subplots(figsize=(12, 6))
    x = range(len(operations))
    width = 0.35

    for i, (target, values) in enumerate(data.items()):
        bars = [values.get(op, 0) for op in operations]
        offset = (i - 0.5) * width
        ax.bar([xi + offset for xi in x], bars, width, label=LABELS.get(target, target),
               color=COLORS.get(target, "#999"))

    ax.set_xlabel("Operation")
    ax.set_ylabel("Throughput (ops/sec)")
    ax.set_title("Throughput Comparison: Dynoxide HTTP vs DynamoDB Local")
    ax.set_xticks(x)
    ax.set_xticklabels(operations, rotation=45, ha="right")
    ax.legend()
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    fig.tight_layout()
    save_figure(fig, output_dir, "throughput_comparison")


def chart_latency_percentiles(results_dir, output_dir):
    """Latency percentile chart (p50/p95/p99) per operation, per target."""
    dynoxide_http = load_json(os.path.join(results_dir, "dynoxide_http.json"))
    dynamodb_local = load_json(os.path.join(results_dir, "dynamodb_local.json"))

    if not dynoxide_http and not dynamodb_local:
        print("  Skipping latency chart (no workload data)")
        return

    # Find operations with percentile data
    operations = []
    data = {"dynoxide_http": {}, "dynamodb_local": {}}

    for source, label in [(dynoxide_http, "dynoxide_http"), (dynamodb_local, "dynamodb_local")]:
        if not source:
            continue
        items = source if isinstance(source, list) else source.get("steps", [])
        for step in items:
            name = step.get("step", step.get("name", ""))
            if step.get("p50_ms") is not None and name:
                data[label][name] = {
                    "p50": step["p50_ms"],
                    "p95": step.get("p95_ms", 0),
                    "p99": step.get("p99_ms", 0),
                }
                if name not in operations:
                    operations.append(name)

    if not operations:
        print("  Skipping latency chart (no percentile data)")
        return

    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=True)

    for ax, (target, values) in zip(axes, data.items()):
        ops = [op for op in operations if op in values]
        if not ops:
            continue
        x = range(len(ops))
        width = 0.25
        for j, (pct, color) in enumerate(
            [("p50", "#22c55e"), ("p95", "#f59e0b"), ("p99", "#ef4444")]
        ):
            bars = [values[op].get(pct, 0) for op in ops]
            ax.bar([xi + j * width for xi in x], bars, width, label=pct)

        ax.set_title(LABELS.get(target, target))
        ax.set_xticks([xi + width for xi in x])
        ax.set_xticklabels(ops, rotation=45, ha="right")
        ax.legend()
        ax.set_ylabel("Latency (ms)")

    fig.suptitle("Latency Percentiles by Operation")
    fig.tight_layout()
    save_figure(fig, output_dir, "latency_percentiles")


def chart_ci_pipeline(results_dir, output_dir):
    """Stacked bar: CI pipeline time breakdown (startup + execution + teardown)."""
    ci_data = load_json(os.path.join(results_dir, "ci_pipeline.json"))
    if not ci_data:
        print("  Skipping CI pipeline chart (no ci_pipeline.json)")
        return

    summary = ci_data.get("summary", [])
    if not summary:
        print("  Skipping CI pipeline chart (no summary data)")
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    modes = []
    startup_times = []
    exec_times = []
    teardown_times = []

    for row in summary:
        label = f"{row.get('mode', '?')} ({row.get('execution', '?')})"
        modes.append(label)
        startup_times.append(row.get("startup_ms", 0) / 1000)
        exec_times.append(row.get("execution_ms", row.get("total_ms", 0)) / 1000)
        teardown_times.append(row.get("teardown_ms", 0) / 1000)

    x = range(len(modes))
    ax.bar(x, startup_times, label="Startup", color="#3b82f6")
    ax.bar(x, exec_times, bottom=startup_times, label="Execution", color="#22c55e")
    bottoms = [s + e for s, e in zip(startup_times, exec_times)]
    ax.bar(x, teardown_times, bottom=bottoms, label="Teardown", color="#f59e0b")

    ax.set_ylabel("Time (seconds)")
    ax.set_title("CI Pipeline: 50-Test Suite Time Breakdown")
    ax.set_xticks(x)
    ax.set_xticklabels(modes, rotation=30, ha="right")
    ax.legend()
    fig.tight_layout()
    save_figure(fig, output_dir, "ci_pipeline_breakdown")


def chart_memory(results_dir, output_dir):
    """Line graph: memory over workload steps from memory profiler CSV."""
    csv_path = os.path.join(results_dir, "memory_profile.csv")
    rows = load_csv(csv_path)
    if not rows:
        print("  Skipping memory chart (no memory_profile.csv)")
        return

    fig, ax = plt.subplots(figsize=(10, 5))

    steps = []
    rss_mb = []
    for row in rows:
        step = row.get("step", "")
        rss = row.get("rss_bytes")
        if rss is not None:
            steps.append(step)
            rss_mb.append(int(rss) / (1024 * 1024))

    if not steps:
        print("  Skipping memory chart (no rss_bytes data)")
        return

    ax.plot(range(len(steps)), rss_mb, marker="o", color="#2563eb", linewidth=2)
    ax.set_xticks(range(len(steps)))
    ax.set_xticklabels(steps, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("RSS (MB)")
    ax.set_title("Memory Usage Over Workload Steps")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    save_figure(fig, output_dir, "memory_profile")


def chart_binary_size(results_dir, output_dir):
    """Horizontal bar chart: binary and Docker image size comparison."""
    # Try to load from size comparison output or system_info
    sizes = {}

    system_info = load_json(os.path.join(results_dir, "system_info.json"))
    if system_info:
        if "dynoxide_binary_bytes" in system_info:
            sizes["Dynoxide (binary)"] = system_info["dynoxide_binary_bytes"]
        if "dynamodb_local_docker_bytes" in system_info:
            sizes["DynamoDB Local (Docker)"] = system_info["dynamodb_local_docker_bytes"]
        if "localstack_docker_bytes" in system_info:
            sizes["LocalStack (Docker)"] = system_info["localstack_docker_bytes"]

    if not sizes:
        print("  Skipping size chart (no size data)")
        return

    fig, ax = plt.subplots(figsize=(10, 4))
    labels = list(sizes.keys())
    values_mb = [v / (1024 * 1024) for v in sizes.values()]
    colors = ["#2563eb", "#dc2626", "#ea580c"][: len(labels)]

    ax.barh(labels, values_mb, color=colors)
    ax.set_xlabel("Size (MB)")
    ax.set_title("Install Size Comparison")

    for i, (v, mb) in enumerate(zip(sizes.values(), values_mb)):
        if mb > 100:
            ax.text(mb + 10, i, f"{mb:.0f} MB", va="center")
        else:
            ax.text(mb + 1, i, f"{mb:.1f} MB", va="center")

    fig.tight_layout()
    save_figure(fig, output_dir, "size_comparison")


def generate_benchmark_action_json(results_dir, output_dir):
    """Generate benchmark_action.json for historical tracking (CI mode)."""
    entries = []

    ci_data = load_json(os.path.join(results_dir, "ci_pipeline.json"))
    if ci_data:
        for row in ci_data.get("summary", []):
            speedup = row.get("speedup_vs_ddb_local")
            if speedup is not None:
                entries.append(
                    {
                        "name": f"CI {row['mode']} {row['execution']} speedup",
                        "unit": "x faster",
                        "value": round(speedup, 1),
                    }
                )

    startup = load_json(os.path.join(results_dir, "startup.json"))
    if startup:
        for target, data in startup.items():
            if isinstance(data, dict) and "cold_start_ms" in data:
                entries.append(
                    {
                        "name": f"Startup {target} cold",
                        "unit": "ms",
                        "value": data["cold_start_ms"],
                        "range": str(data.get("stddev_ms", "")),
                    }
                )

    if entries:
        path = os.path.join(output_dir, "benchmark_action.json")
        with open(path, "w") as f:
            json.dump(entries, f, indent=2)
        print(f"  Wrote benchmark_action.json ({len(entries)} entries)")


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark charts")
    parser.add_argument(
        "--results-dir",
        default="results",
        help="Directory containing benchmark result files (default: results)",
    )
    parser.add_argument(
        "--output-dir",
        default="results/charts",
        help="Directory to write charts (default: results/charts)",
    )
    parser.add_argument(
        "--ci-mode",
        action="store_true",
        help="CI mode: also generate benchmark_action.json for historical tracking",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    print(f"Reading results from: {args.results_dir}")
    print(f"Writing charts to: {args.output_dir}")

    # List available result files
    if os.path.isdir(args.results_dir):
        files = os.listdir(args.results_dir)
        print(f"  Found: {', '.join(sorted(f for f in files if not f.startswith('.')))}")
    else:
        print(f"  Warning: {args.results_dir} does not exist")
        sys.exit(1)

    print("\nGenerating charts:")
    chart_throughput(args.results_dir, args.output_dir)
    chart_latency_percentiles(args.results_dir, args.output_dir)
    chart_ci_pipeline(args.results_dir, args.output_dir)
    chart_memory(args.results_dir, args.output_dir)
    chart_binary_size(args.results_dir, args.output_dir)

    if args.ci_mode:
        print("\nCI mode:")
        generate_benchmark_action_json(args.results_dir, args.output_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
