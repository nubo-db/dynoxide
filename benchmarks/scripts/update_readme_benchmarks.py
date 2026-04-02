#!/usr/bin/env python3
"""Update README benchmark numbers from CI benchmark data.

Reads run_summary.json and memory_profile.csv produced by the
benchmark-comparative workflow, then replaces values between
<!-- bench:KEY -->...<!-- /bench --> markers in both READMEs.

Markers prefixed with local_ or prose_ are skipped (manual values).

Usage:
    python3 benchmarks/scripts/update_readme_benchmarks.py \
        --data-dir results/ \
        --readme README.md \
        --benchmarks-readme benchmarks/README.md

    # Dry run:
    python3 benchmarks/scripts/update_readme_benchmarks.py \
        --data-dir results/ --dry-run
"""

import argparse
import csv
import json
import math
import os
import re
import sys


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def fmt_startup_ms(ms):
    """Format a startup time in milliseconds (CI style, lossy for sub-ms)."""
    if ms < 1:
        return "<1ms"
    return "~{:,.0f}ms".format(ms)


def fmt_startup_ms_local(ms):
    """Format a startup time for local benchmarks (precise, stable machine)."""
    if ms < 1:
        return "~{:.1f}ms".format(ms)
    if ms < 100:
        return "~{:.0f}ms".format(ms)
    return "~{:,.0f}ms".format(ms)


def fmt_latency_ms(ms):
    """Format a latency value (p50 etc.)."""
    if ms < 0.1:
        # Sub-100µs: show as µs
        us = ms * 1000
        return "{:.0f}µs".format(us)
    if ms < 1:
        return "{:.2f}ms".format(ms)
    if ms < 10:
        return "{:.1f}ms".format(ms)
    return "{:.1f}ms".format(ms)


def fmt_latency_ms_ci_table(ms):
    """Format latency for the main README CI table (less precision)."""
    if ms < 0.1:
        us = ms * 1000
        return "{:.0f}µs".format(us)
    if ms < 1:
        return "{:.1f}ms".format(ms)
    return "{:.1f}ms".format(ms)


def fmt_stddev_ms(ms):
    """Format a stddev value."""
    return "±{:,.1f}ms".format(ms)


def fmt_ratio(ratio, bold=True, prefix=""):
    """Format a speedup ratio like **6.9x** or ~1x."""
    if ratio < 1.3:
        return "~1x"
    if ratio >= 100:
        val = "**~{:,.0f}x{}**".format(ratio, prefix) if bold else "~{:,.0f}x{}".format(ratio, prefix)
    elif ratio >= 10:
        val = "**{:.0f}x{}**".format(ratio, prefix) if bold else "{:.0f}x{}".format(ratio, prefix)
    else:
        val = "**{:.1f}x{}**".format(ratio, prefix) if bold else "{:.1f}x{}".format(ratio, prefix)
    return val


def fmt_ratio_slower(ratio):
    """Format a 'slower' ratio like 1.5x slower."""
    return "{:.1f}x slower".format(ratio)


def fmt_ratio_faster(ratio, bold=True):
    """Format a 'faster' ratio like **~7,200x faster**."""
    if ratio >= 100:
        s = "~{:,.0f}x faster".format(ratio)
    elif ratio >= 10:
        s = "~{:.0f}x faster".format(ratio)
    else:
        s = "{:.1f}x faster".format(ratio)
    return "**{}**".format(s) if bold else s


def fmt_workload_total(ms):
    """Format a total workload time."""
    if ms >= 1000:
        return "{:.1f}s".format(ms / 1000)
    return "{:,.0f}ms".format(ms)


def fmt_suite_ms(ms):
    """Format a CI suite wall clock time."""
    return "{:,.0f}ms".format(ms)


def fmt_memory_mb(bytes_val):
    """Format memory in MB."""
    mb = bytes_val / (1024 * 1024)
    if mb < 10:
        return "~{:.1f} MB".format(mb)
    return "~{:.0f} MB".format(mb)


def fmt_memory_mb_exact(bytes_val):
    """Format memory in MB with one decimal."""
    mb = bytes_val / (1024 * 1024)
    return "{:.1f} MB".format(mb)


def fmt_disk(bytes_val):
    """Format disk size."""
    mb = bytes_val / (1024 * 1024)
    if mb < 1:
        kb = bytes_val / 1024
        return "{:.0f} KB".format(kb)
    return "{:.1f} MB".format(mb)


def fmt_size_mb(bytes_val):
    """Format binary/image size in MB."""
    mb = bytes_val / (1024 * 1024)
    return "{:.0f} MB".format(round(mb))


def fmt_size_gb(bytes_val):
    """Format image size in GB."""
    gb = bytes_val / (1024 * 1024 * 1024)
    return "{:.1f} GB".format(gb)


def fmt_criterion_ns(ns):
    """Format criterion benchmark results (ns/iter) as human-readable latency."""
    us = ns / 1000
    if us < 1000:
        return "{:.0f}µs".format(us) if us >= 1 else "<1µs"
    ms = us / 1000
    if ms < 10:
        return "{:.1f}ms".format(ms)
    return "{:.0f}ms".format(ms)


def fmt_prose_startup_range(mean_ms, stddev_ms):
    """Format a prose startup range like '3\u20134 seconds'."""
    low_s = max(0, (mean_ms - stddev_ms)) / 1000
    high_s = (mean_ms + stddev_ms) / 1000
    low = int(round(low_s))
    high = int(round(high_s))
    if low == high:
        return "~{:.0f} seconds".format(low)
    return "{}\u2013{} seconds".format(low, high)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_run_summary(data_dir):
    """Load and validate run_summary.json."""
    path = os.path.join(data_dir, "run_summary.json")
    if not os.path.exists(path):
        print("ERROR: {} not found".format(path), file=sys.stderr)
        sys.exit(1)
    with open(path) as f:
        data = json.load(f)
    if data.get("schema_version") != 1:
        print("ERROR: unsupported schema_version: {}".format(
            data.get("schema_version")), file=sys.stderr)
        sys.exit(1)
    return data


def load_memory_profile(data_dir):
    """Load memory_profile.csv, returning list of dicts. Returns [] if missing."""
    path = os.path.join(data_dir, "memory_profile.csv")
    if not os.path.exists(path):
        print("WARNING: {} not found, memory markers will be skipped".format(path),
              file=sys.stderr)
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------
# Value mapping builder
# ---------------------------------------------------------------------------

def find_startup(results, target, mode="cold"):
    """Find a startup result by target and mode."""
    for r in results.get("startup", {}).get("results", []):
        if r.get("target") == target and r.get("mode") == mode:
            return r
    return None


def find_step(steps, step_name):
    """Find a workload step by name."""
    for s in steps:
        if s.get("step") == step_name:
            return s
    return None


def find_pipeline_summary(results, mode, execution):
    """Find a CI pipeline summary entry."""
    for row in results.get("ci_pipeline", {}).get("summary", []):
        if row.get("mode") == mode and row.get("execution") == execution:
            return row
    return None


def find_memory_row(memory_rows, mode, step):
    """Find a memory profile row by mode and step."""
    for r in memory_rows:
        if r.get("mode") == mode and r.get("step") == step:
            return r
    return None


def build_value_mapping(run_summary, memory_rows):
    """Build a dict mapping marker keys to formatted string values."""
    results = run_summary.get("results", {})
    git = run_summary.get("git", {})
    system_info = run_summary.get("system_info", {})
    values = {}

    # --- Commit links ---
    short_sha = git.get("short_sha")
    sha = git.get("sha", short_sha)
    if short_sha:
        values["ci_commit_link_root"] = "[`{}`](../../commit/{})".format(short_sha, sha)
        values["ci_commit_link_benchmarks"] = "[`{}`](../../../commit/{})".format(short_sha, sha)

    # --- Startup (main README simplified + benchmarks README detailed) ---
    emb = find_startup(results, "dynoxide_embedded")
    http = find_startup(results, "dynoxide_http")
    ddb = find_startup(results, "dynamodb_local")
    ls = find_startup(results, "localstack")

    if emb and ddb:
        # Main README (simplified)
        values["ci_startup_embedded"] = "**{}**".format(fmt_startup_ms(emb["mean_ms"]))
        values["ci_startup_ddb_local"] = fmt_startup_ms(ddb["mean_ms"])
        # Benchmarks README (detailed)
        values["ci_startup_embedded_mean"] = "{:.1f}ms".format(emb["mean_ms"])
        values["ci_startup_embedded_stddev"] = fmt_stddev_ms(emb["stddev_ms"])
        ratio = ddb["mean_ms"] / emb["mean_ms"]
        values["ci_startup_embedded_ratio"] = fmt_ratio_faster(ratio)

    if http and ddb:
        values["ci_startup_http"] = "**{}**".format(fmt_startup_ms(http["mean_ms"]))
        values["ci_startup_http_mean"] = "{:.1f}ms".format(http["mean_ms"])
        values["ci_startup_http_stddev"] = fmt_stddev_ms(http["stddev_ms"])
        ratio = ddb["mean_ms"] / http["mean_ms"]
        values["ci_startup_http_ratio"] = fmt_ratio_faster(ratio)

    if ddb:
        values["ci_startup_ddb_local_mean"] = "{:,.0f}ms".format(ddb["mean_ms"])
        values["ci_startup_ddb_local_stddev"] = "\u00b1{:,.0f}ms".format(ddb["stddev_ms"])

    if ls:
        values["ci_startup_localstack"] = fmt_startup_ms(ls["mean_ms"])
        values["ci_startup_localstack_mean"] = "{:,.0f}ms".format(ls["mean_ms"])
        values["ci_startup_localstack_stddev"] = "\u00b1{:,.0f}ms".format(ls["stddev_ms"])
        if ddb:
            ratio = ls["mean_ms"] / ddb["mean_ms"]
            values["ci_startup_localstack_ratio"] = fmt_ratio_slower(ratio)

    # --- Per-operation comparison ---
    # Maps marker key suffix -> JSON step name
    STEP_MAP = {
        "CreateTable": "CreateTable",
        "GetItem": "GetItem",
        "PutItem": "PutItem",
        "Query": "Query_Base",
        "QueryGSI": "Query_GSI",
        "QueryPaginated": "Query_Paginated",
        "Scan": "Scan",
        "UpdateItem": "UpdateItem",
        "TransactWriteItems": "TransactWriteItems",
        "BatchGetItem": "BatchGetItem",
        "BatchWriteItem": "BatchWriteItem",
        "DeleteItem": "DeleteItem",
    }

    http_steps = results.get("dynoxide_http", {}).get("steps", [])
    ddb_steps = results.get("dynamodb_local", {}).get("steps", [])

    for marker_name, json_name in STEP_MAP.items():
        h = find_step(http_steps, json_name)
        d = find_step(ddb_steps, json_name)
        if h:
            values["ci_op_{}_http".format(marker_name)] = fmt_latency_ms(h["p50_ms"])
        if d:
            values["ci_op_{}_ddb".format(marker_name)] = fmt_latency_ms(d["p50_ms"])
        if h and d and d["p50_ms"] > 0:
            ratio = d["p50_ms"] / h["p50_ms"]
            values["ci_op_{}_ratio".format(marker_name)] = fmt_ratio(ratio)

    # --- GetItem for main README (embedded uses criterion) ---
    criterion = results.get("criterion_baseline", {})
    get_item_ns = criterion.get("get_item")
    if get_item_ns:
        values["ci_getitem_embedded"] = fmt_criterion_ns(get_item_ns)

    h = find_step(http_steps, "GetItem")
    d = find_step(ddb_steps, "GetItem")
    if h:
        values["ci_getitem_http"] = fmt_latency_ms_ci_table(h["p50_ms"])
    if d:
        values["ci_getitem_ddb_local"] = fmt_latency_ms_ci_table(d["p50_ms"])

    # --- Workload totals ---
    http_total = results.get("dynoxide_http", {}).get("total_ms")
    ddb_total = results.get("dynamodb_local", {}).get("total_ms")
    if http_total:
        values["ci_workload_http"] = "**{}**".format(fmt_workload_total(http_total))
    if ddb_total:
        values["ci_workload_ddb_local"] = fmt_workload_total(ddb_total)
        values["ci_workload_ddb"] = "**{}**".format(fmt_workload_total(ddb_total))
    if http_total and ddb_total and http_total > 0:
        ratio = ddb_total / http_total
        values["ci_workload_ratio"] = "**{:.1f}x**".format(ratio)

    # --- CI pipeline suite ---
    PIPELINE_MODES = [
        ("dynoxide_embedded", "sequential", "ci_suite_embedded_seq"),
        ("dynoxide_embedded", "parallel_4", "ci_suite_embedded_par"),
        ("dynoxide_http", "sequential", "ci_suite_http_seq"),
        ("dynoxide_http", "parallel_4", "ci_suite_http_par"),
        ("dynamodb_local", "sequential", "ci_suite_ddb_seq"),
        ("dynamodb_local", "parallel_4", "ci_suite_ddb_par"),
    ]

    for mode, execution, key in PIPELINE_MODES:
        row = find_pipeline_summary(results, mode, execution)
        if row:
            values[key] = fmt_suite_ms(row["wall_clock_ms"])
            speedup = row.get("speedup_vs_ddb_local")
            if speedup and speedup > 1:
                values["{}_ratio".format(key)] = fmt_ratio(speedup)

    # Also populate the _prose variants (inline text references)
    ddb_seq = find_pipeline_summary(results, "dynamodb_local", "sequential")
    ddb_par = find_pipeline_summary(results, "dynamodb_local", "parallel_4")
    if ddb_seq:
        values["ci_suite_ddb_seq_prose"] = fmt_suite_ms(ddb_seq["wall_clock_ms"])
        values["ci_suite_ddb_local_seq"] = fmt_suite_ms(ddb_seq["wall_clock_ms"])
    if ddb_par:
        values["ci_suite_ddb_par_prose"] = fmt_suite_ms(ddb_par["wall_clock_ms"])

    # --- Criterion micro-benchmarks ---
    CRITERION_MAP = {
        "ci_criterion_get_item": "get_item",
        "ci_criterion_put_item_small": "put_item/put_item/small",
        "ci_criterion_put_item_medium": "put_item/put_item/medium",
        "ci_criterion_put_item_large": "put_item/put_item/large",
        "ci_criterion_query_base": "query_base_table",
        "ci_criterion_query_gsi": "query_gsi",
        "ci_criterion_scan": "scan_with_filter",
        "ci_criterion_update_item": "update_item",
        "ci_criterion_delete_item": "delete_item",
        "ci_criterion_batch_write": "batch_write_item_25",
        "ci_criterion_batch_get": "batch_get_item_100",
        "ci_criterion_transact_write": "transact_write_items_4",
    }

    for key, bench_path in CRITERION_MAP.items():
        ns = criterion.get(bench_path)
        if ns:
            values[key] = fmt_criterion_ns(ns)

    # --- Memory & Disk ---
    mem_idle = find_memory_row(memory_rows, "memory", "db_created")
    mem_loaded = find_memory_row(memory_rows, "memory", "loaded_10k")
    file_idle = find_memory_row(memory_rows, "file", "db_created")
    file_loaded = find_memory_row(memory_rows, "file", "loaded_10k")
    file_empty_table = find_memory_row(memory_rows, "file", "table_created")
    ddb_idle = find_memory_row(memory_rows, "docker_idle", "dynamodb_local")
    ls_idle = find_memory_row(memory_rows, "docker_idle", "localstack")

    if mem_idle:
        values["ci_mem_memory_idle"] = fmt_memory_mb_exact(int(mem_idle["rss_bytes"]))
        values["ci_memory_embedded_idle"] = fmt_memory_mb(int(mem_idle["rss_bytes"]))
    if mem_loaded:
        values["ci_mem_memory_loaded"] = fmt_memory_mb_exact(int(mem_loaded["rss_bytes"]))
    if file_idle:
        values["ci_mem_file_idle"] = fmt_memory_mb_exact(int(file_idle["rss_bytes"]))
        # Note: ci_memory_http_idle is not populated here — the memory profiler
        # measures file-backed SQLite RSS (includes mmap overhead), not HTTP server
        # idle RSS. The HTTP server idle memory is measured separately.
    if file_loaded:
        values["ci_mem_file_loaded"] = fmt_memory_mb_exact(int(file_loaded["rss_bytes"]))
    if file_loaded and "disk_bytes" in file_loaded:
        values["ci_disk_file_loaded"] = fmt_disk(int(file_loaded["disk_bytes"]))
    if file_empty_table and "disk_bytes" in file_empty_table:
        values["ci_disk_file_empty"] = fmt_disk(int(file_empty_table["disk_bytes"]))
    if ddb_idle:
        values["ci_mem_ddb_local_idle"] = fmt_memory_mb_exact(int(ddb_idle["rss_bytes"]))
        values["ci_memory_ddb_local_idle"] = fmt_memory_mb(int(ddb_idle["rss_bytes"]))
    if ls_idle:
        values["ci_mem_localstack_idle"] = fmt_memory_mb_exact(int(ls_idle["rss_bytes"]))
        values["ci_memory_localstack_idle"] = fmt_memory_mb(int(ls_idle["rss_bytes"]))

    # --- Binary / Docker image sizes ---
    binary_bytes = system_info.get("dynoxide_binary_bytes")
    if binary_bytes:
        values["ci_binary_size"] = fmt_size_mb(binary_bytes)
        values["ci_binary_size_http"] = fmt_size_mb(binary_bytes)

    ddb_image_bytes = system_info.get("dynamodb_local_image_bytes")
    if ddb_image_bytes:
        values["ci_image_ddb_local"] = fmt_size_mb(ddb_image_bytes)

    ls_image_bytes = system_info.get("localstack_image_bytes")
    if ls_image_bytes:
        if ls_image_bytes >= 1024 * 1024 * 1024:
            values["ci_image_localstack"] = fmt_size_gb(ls_image_bytes)
        else:
            values["ci_image_localstack"] = fmt_size_mb(ls_image_bytes)

    # --- Prose values (derived from CI data) ---
    if ddb:
        values["ddb_local_cold_start"] = fmt_prose_startup_range(
            ddb["mean_ms"], ddb["stddev_ms"])
    if ddb_idle:
        values["ddb_local_idle_memory"] = fmt_memory_mb(int(ddb_idle["rss_bytes"]))
    if ddb_image_bytes:
        values["ddb_local_image_size"] = fmt_memory_mb(ddb_image_bytes)
    if mem_idle:
        values["dynoxide_idle_memory"] = fmt_memory_mb(int(mem_idle["rss_bytes"]))
    if binary_bytes:
        values["dynoxide_binary_size"] = fmt_memory_mb(binary_bytes)

    return values


def build_local_value_mapping(run_summary, memory_rows):
    """Build a dict mapping local_ marker keys to formatted string values.

    Uses the same run_summary.json format as CI, but populates local_ keys
    for the Apple Silicon / local development sections of both READMEs.
    """
    results = run_summary.get("results", {})
    values = {}

    # --- Startup ---
    emb = find_startup(results, "dynoxide_embedded")
    http = find_startup(results, "dynoxide_http")
    ddb = find_startup(results, "dynamodb_local")
    ls = find_startup(results, "localstack")

    if emb:
        values["local_startup_embedded"] = "**{}**".format(fmt_startup_ms_local(emb["mean_ms"]))
    if http:
        values["local_startup_http"] = "**{}**".format(fmt_startup_ms_local(http["mean_ms"]))
    if ddb:
        values["local_startup_ddb_local"] = fmt_startup_ms_local(ddb["mean_ms"])
    if ls:
        values["local_startup_localstack"] = fmt_startup_ms_local(ls["mean_ms"])

    # Startup ratios (benchmarks README)
    if emb and ddb and emb["mean_ms"] > 0:
        values["local_startup_embedded_ratio"] = fmt_ratio_faster(
            ddb["mean_ms"] / emb["mean_ms"])
    if http and ddb and http["mean_ms"] > 0:
        values["local_startup_http_ratio"] = fmt_ratio_faster(
            ddb["mean_ms"] / http["mean_ms"])
    if ls and ddb and ddb["mean_ms"] > 0:
        values["local_startup_localstack_ratio"] = fmt_ratio_slower(
            ls["mean_ms"] / ddb["mean_ms"])

    # --- Per-operation comparison ---
    STEP_MAP = {
        "CreateTable": "CreateTable",
        "GetItem": "GetItem",
        "PutItem": "PutItem",
        "Query": "Query_Base",
        "QueryGSI": "Query_GSI",
        "QueryPaginated": "Query_Paginated",
        "Scan": "Scan",
        "UpdateItem": "UpdateItem",
        "TransactWriteItems": "TransactWriteItems",
        "BatchGetItem": "BatchGetItem",
        "BatchWriteItem": "BatchWriteItem",
        "DeleteItem": "DeleteItem",
    }

    http_steps = results.get("dynoxide_http", {}).get("steps", [])
    ddb_steps = results.get("dynamodb_local", {}).get("steps", [])

    for marker_name, json_name in STEP_MAP.items():
        h = find_step(http_steps, json_name)
        d = find_step(ddb_steps, json_name)
        if h:
            values["local_op_{}_http".format(marker_name)] = fmt_latency_ms(h["p50_ms"])
        if d:
            values["local_op_{}_ddb".format(marker_name)] = fmt_latency_ms(d["p50_ms"])
        if h and d and d["p50_ms"] > 0:
            ratio = d["p50_ms"] / h["p50_ms"]
            values["local_op_{}_ratio".format(marker_name)] = fmt_ratio(ratio)

    # --- GetItem for main README (embedded uses criterion) ---
    criterion = results.get("criterion_baseline", {})
    get_item_ns = criterion.get("get_item")
    if get_item_ns:
        values["local_getitem_embedded"] = fmt_criterion_ns(get_item_ns)

    h = find_step(http_steps, "GetItem")
    d = find_step(ddb_steps, "GetItem")
    if h:
        values["local_getitem_http"] = fmt_latency_ms_ci_table(h["p50_ms"])
    if d:
        values["local_getitem_ddb_local"] = fmt_latency_ms_ci_table(d["p50_ms"])

    # --- PutItem throughput for main README ---
    put_h = find_step(http_steps, "PutItem")
    put_d = find_step(ddb_steps, "PutItem")
    put_item_ns = criterion.get("put_item/put_item/medium")
    if put_item_ns:
        ops = 1_000_000_000 / put_item_ns
        values["local_putitem_embedded"] = "~{:,.0f} ops/s".format(ops)
    if put_h and put_h.get("ops_per_sec"):
        values["local_putitem_http"] = "~{:,.0f} ops/s".format(put_h["ops_per_sec"])
    if put_d and put_d.get("ops_per_sec"):
        values["local_putitem_ddb_local"] = "~{:,.0f} ops/s".format(put_d["ops_per_sec"])

    # --- Workload totals ---
    http_total = results.get("dynoxide_http", {}).get("total_ms")
    ddb_total = results.get("dynamodb_local", {}).get("total_ms")
    if http_total:
        values["local_workload_http"] = "**{}**".format(fmt_workload_total(http_total))
    if ddb_total:
        values["local_workload_ddb"] = "**{}**".format(fmt_workload_total(ddb_total))
    if http_total and ddb_total and http_total > 0:
        ratio = ddb_total / http_total
        values["local_workload_ratio"] = "**{:.1f}x**".format(ratio)

    # --- CI pipeline suite ---
    PIPELINE_MODES = [
        ("dynoxide_embedded", "sequential", "local_ci_embedded_seq"),
        ("dynoxide_embedded", "parallel_4", "local_ci_embedded_par"),
        ("dynoxide_http", "sequential", "local_ci_http_seq"),
        ("dynoxide_http", "parallel_4", "local_ci_http_par"),
        ("dynamodb_local", "sequential", "local_ci_ddb_seq"),
        ("dynamodb_local", "parallel_4", "local_ci_ddb_par"),
    ]

    for mode, execution, key in PIPELINE_MODES:
        row = find_pipeline_summary(results, mode, execution)
        if row:
            values[key] = fmt_startup_ms_local(row["wall_clock_ms"])
            speedup = row.get("speedup_vs_ddb_local")
            if speedup and speedup > 1:
                values["{}_ratio".format(key)] = fmt_ratio(speedup)

    # Also populate the main README's simplified suite keys
    MAIN_README_SUITE = [
        ("dynoxide_embedded", "sequential", "local_ci_suite_embedded_seq"),
        ("dynoxide_embedded", "parallel_4", "local_ci_suite_embedded_par"),
        ("dynoxide_http", "sequential", "local_ci_suite_http_seq"),
        ("dynoxide_http", "parallel_4", "local_ci_suite_http_par"),
        ("dynamodb_local", "sequential", "local_ci_suite_ddb_local_seq"),
        ("dynamodb_local", "parallel_4", "local_ci_suite_ddb_local_par"),
    ]

    for mode, execution, key in MAIN_README_SUITE:
        row = find_pipeline_summary(results, mode, execution)
        if row:
            values[key] = fmt_startup_ms_local(row["wall_clock_ms"])

    # --- Criterion micro-benchmarks ---
    CRITERION_MAP = {
        "local_criterion_get_item": "get_item",
        "local_criterion_put_item_small": "put_item/put_item/small",
        "local_criterion_put_item_medium": "put_item/put_item/medium",
        "local_criterion_put_item_large": "put_item/put_item/large",
        "local_criterion_query_base": "query_base_table",
        "local_criterion_query_gsi": "query_gsi",
        "local_criterion_scan": "scan_with_filter",
        "local_criterion_update_item": "update_item",
        "local_criterion_delete_item": "delete_item",
        "local_criterion_batch_write": "batch_write_item_25",
        "local_criterion_batch_get": "batch_get_item_100",
        "local_criterion_transact_write": "transact_write_items_4",
    }

    for key, bench_path in CRITERION_MAP.items():
        ns = criterion.get(bench_path)
        if ns:
            values[key] = fmt_criterion_ns(ns)

    return values


# ---------------------------------------------------------------------------
# Sanity checking
# ---------------------------------------------------------------------------

def parse_numeric(text):
    """Try to extract a numeric value from formatted text. Returns None if unparseable."""
    # Strip bold markers and whitespace
    cleaned = text.strip().strip("*").strip()
    # Handle special cases
    if cleaned.startswith("<") or cleaned.startswith("~"):
        cleaned = cleaned.lstrip("<~")

    # Try to parse with units
    for suffix, multiplier in [
        ("µs", 0.001), ("ms", 1), ("s", 1000),
        ("GB", 1024 * 1024 * 1024), ("MB", 1024 * 1024), ("KB", 1024),
        ("x faster", 1), ("x slower", 1), ("x", 1),
        ("ops/s", 1),
    ]:
        if cleaned.endswith(suffix):
            num_str = cleaned[: -len(suffix)].strip().rstrip(" ").replace(",", "")
            try:
                return float(num_str) * multiplier
            except ValueError:
                continue

    # Plain number
    try:
        return float(cleaned.replace(",", ""))
    except ValueError:
        return None


def classify_key(key):
    """Classify a key for sanity check thresholds.

    Returns: 'ratio', 'latency', 'memory', or 'skip'.
    """
    if key.startswith("ci_commit"):
        return "skip"
    if "_ratio" in key or key.endswith("_ratio"):
        return "ratio"
    if any(x in key for x in ["mem_", "memory_", "disk_", "image_", "binary_size"]):
        return "memory"
    if any(x in key for x in ["startup", "op_", "workload", "suite_", "criterion_",
                                "getitem", "putitem"]):
        return "latency"
    # prose keys
    if "idle_memory" in key or "image_size" in key or "binary_size" in key:
        return "memory"
    if "cold_start" in key:
        return "latency"
    return "latency"


THRESHOLDS = {
    "ratio": 0.5,     # 50% for ratios/multipliers
    "latency": 2.0,   # 200% for absolute wall-clock values
    "memory": 1.0,    # 100% for memory/disk values
}


def sanity_check(key, old_text, new_text):
    """Check if the change is within acceptable bounds.

    Returns (ok, reason) tuple.
    """
    # First-time population: empty or newly added
    stripped = old_text.strip()
    if not stripped:
        return True, "first-time population"

    category = classify_key(key)
    if category == "skip":
        return True, "non-numeric"

    old_val = parse_numeric(old_text)
    new_val = parse_numeric(new_text)

    if old_val is None or new_val is None:
        return True, "unparseable (skipping check)"

    if old_val == 0:
        return True, "old value is zero"

    pct_change = abs(new_val - old_val) / abs(old_val)
    threshold = THRESHOLDS.get(category, 2.0)

    if pct_change > threshold:
        return False, "{}: {:.0%} change exceeds {:.0%} threshold (old={}, new={})".format(
            category, pct_change, threshold, old_text.strip(), new_text.strip())

    return True, None


# ---------------------------------------------------------------------------
# Marker replacement
# ---------------------------------------------------------------------------

# Matches <!-- bench:KEY -->VALUE<!-- /bench --> and <!-- prose:KEY -->VALUE<!-- /bench -->
MARKER_RE = re.compile(
    r"(<!-- (?:bench|prose):(\w+) -->)(.*?)(<!-- /bench -->)"
)


def replace_markers(content, values, filename, mode="ci", force=False):
    """Replace marker values in content. Returns (new_content, stats).

    mode: 'ci' skips local_ markers, 'local' skips non-local_ markers.
    force: bypass sanity checks (for environment transitions).
    """
    updated = 0
    skipped = 0
    missing = 0
    warnings = []

    def replacer(match):
        nonlocal updated, skipped, missing
        open_tag = match.group(1)
        key = match.group(2)
        old_value = match.group(3)
        close_tag = match.group(4)

        # In CI mode, skip local_ markers; in local mode, skip non-local_ markers
        if mode == "ci" and key.startswith("local_"):
            return match.group(0)
        if mode == "local" and not key.startswith("local_"):
            return match.group(0)

        # Look up new value
        new_value = values.get(key)
        if new_value is None:
            missing += 1
            warnings.append("{}: {} \u2014 no data available".format(filename, key))
            return match.group(0)

        # Null/zero guard
        if new_value in ("null", "0", "", None):
            missing += 1
            warnings.append("{}: {} \u2014 null/zero value skipped".format(filename, key))
            return match.group(0)

        # Sanity check
        ok, reason = sanity_check(key, old_value, new_value)
        if not ok:
            if force:
                warnings.append("FORCED {}: {} \u2014 {}".format(filename, key, reason))
            else:
                skipped += 1
                warnings.append("SANITY SKIP {}: {} \u2014 {}".format(filename, key, reason))
                return match.group(0)

        if old_value != new_value:
            updated += 1

        return "{}{}{}".format(open_tag, new_value, close_tag)

    new_content = MARKER_RE.sub(replacer, content)
    return new_content, {"updated": updated, "skipped": skipped,
                         "missing": missing, "warnings": warnings}


def validate_markers(content, values, filename, mode="ci"):
    """Warn about markers with no corresponding data mapping."""
    warnings = []
    for match in MARKER_RE.finditer(content):
        key = match.group(2)
        if mode == "ci" and key.startswith("local_"):
            continue
        if mode == "local" and not key.startswith("local_"):
            continue
        if key not in values:
            warnings.append("{}: marker '{}' has no data mapping".format(filename, key))
    return warnings


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Update README benchmark numbers from CI or local data")
    parser.add_argument("--data-dir", required=True,
                        help="Directory containing run_summary.json and memory_profile.csv")
    parser.add_argument("--readme", default="README.md",
                        help="Path to main README.md")
    parser.add_argument("--benchmarks-readme", default="benchmarks/README.md",
                        help="Path to benchmarks/README.md")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would change without writing files")
    parser.add_argument("--local", action="store_true",
                        help="Update local_ markers instead of ci_ markers")
    parser.add_argument("--force", action="store_true",
                        help="Bypass sanity checks (for environment transitions)")
    args = parser.parse_args()

    mode = "local" if args.local else "ci"

    run_summary = load_run_summary(args.data_dir)
    memory_rows = load_memory_profile(args.data_dir)

    if mode == "local":
        values = build_local_value_mapping(run_summary, memory_rows)
    else:
        values = build_value_mapping(run_summary, memory_rows)

    total_updated = 0
    total_skipped = 0
    total_missing = 0
    all_warnings = []

    for readme_path in [args.readme, args.benchmarks_readme]:
        if not os.path.exists(readme_path):
            print("WARNING: {} not found, skipping".format(readme_path), file=sys.stderr)
            continue

        with open(readme_path, encoding="utf-8") as f:
            content = f.read()

        # Validate markers first
        validation_warnings = validate_markers(content, values, readme_path, mode=mode)
        all_warnings.extend(validation_warnings)

        new_content, stats = replace_markers(content, values, readme_path, mode=mode,
                                              force=args.force)
        total_updated += stats["updated"]
        total_skipped += stats["skipped"]
        total_missing += stats["missing"]
        all_warnings.extend(stats["warnings"])

        if not args.dry_run and new_content != content:
            with open(readme_path, "w", encoding="utf-8", newline="") as f:
                f.write(new_content)
            print("Wrote {}".format(readme_path))
        elif args.dry_run and new_content != content:
            print("Would update {}".format(readme_path))

    # Print summary
    for w in all_warnings:
        print("WARNING: {}".format(w), file=sys.stderr)

    print("Updated {} values, skipped {}, missing {}".format(
        total_updated, total_skipped, total_missing))

    if total_skipped > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
