#!/usr/bin/env python3
"""Tests for check_iai_baselines: every benchmark in a PR run must have had a baseline.

Run with: python3 -m unittest discover -s benchmarks/scripts -p 'test_*.py'
"""

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from check_iai_baselines import (
    EXIT_ERROR,
    EXIT_MISSING_BASELINE,
    EXIT_OK,
    main,
    old_instructions,
)


def summary(function, bench_id, new, old=None):
    """A summary.json trimmed to the fields the script reads, in the shape
    iai-callgrind 0.14.2 writes with --save-summary=json."""
    metrics = {"Both": [new, old]} if old is not None else {"Left": new}
    return {
        "version": "3",
        "module_path": f"iai_core::core_operations::{function}",
        "id": bench_id,
        "callgrind_summary": {
            "callgrind_run": {
                "total": {
                    "summary": {"Ir": {"metrics": metrics, "diffs": None}},
                    "regressions": [],
                }
            }
        },
    }


class Run:
    """A target/iai directory holding one summary.json per benchmark."""

    def __init__(self, root):
        self.iai = os.path.join(root, "iai")
        self.base_list = os.path.join(root, "base.txt")

    def write(self, *summaries):
        for s in summaries:
            name = s["module_path"].rsplit("::", 1)[1] + "." + s["id"]
            d = os.path.join(self.iai, "dynoxide-benchmarks", "iai_core", "core_operations", name)
            os.makedirs(d, exist_ok=True)
            with open(os.path.join(d, "summary.json"), "w") as f:
                json.dump(s, f)

    def run(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            status = main(list(args))
        return status, out.getvalue() + err.getvalue()

    def record(self):
        return self.run("record", "--iai-dir", self.iai, "--output", self.base_list)

    def check(self):
        return self.run("check", "--iai-dir", self.iai, "--base-list", self.base_list)


class CheckIaiBaselinesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.r = Run(self.tmp.name)

    def test_every_benchmark_compared_passes(self):
        self.r.write(summary("bench_put_item", "medium", 100), summary("bench_scan", "with_filter", 200))
        self.assertEqual(self.r.record()[0], EXIT_OK)
        self.r.write(
            summary("bench_put_item", "medium", 101, 100),
            summary("bench_scan", "with_filter", 200, 200),
        )
        status, out = self.r.check()
        self.assertEqual(status, EXIT_OK, out)
        self.assertIn("2 benchmarks checked: 2 compared, 0 new, 0 without a baseline", out)

    def test_benchmark_in_base_without_baseline_fails(self):
        # The failure this script exists for: the base ran it, yet the head
        # run found nothing to compare with and printed N/A.
        self.r.write(summary("bench_put_item", "medium", 100), summary("bench_scan", "with_filter", 200))
        self.r.record()
        self.r.write(
            summary("bench_put_item", "medium", 101, 100),
            summary("bench_scan", "with_filter", 200),
        )
        status, out = self.r.check()
        self.assertEqual(status, EXIT_MISSING_BASELINE, out)
        self.assertIn("MISSING   iai_core::core_operations::bench_scan with_filter", out)

    def test_benchmark_only_in_head_is_new_and_passes(self):
        self.r.write(summary("bench_put_item", "medium", 100))
        self.r.record()
        self.r.write(
            summary("bench_put_item", "medium", 100, 100),
            summary("bench_get_item", "by_key", 50),
        )
        status, out = self.r.check()
        self.assertEqual(status, EXIT_OK, out)
        self.assertIn("new       iai_core::core_operations::bench_get_item by_key", out)

    def test_benchmark_removed_in_head_does_not_linger(self):
        # record deletes the base summaries, so one the head no longer has is
        # not read back as a head benchmark without a baseline.
        self.r.write(summary("bench_put_item", "medium", 100), summary("bench_scan", "with_filter", 200))
        self.r.record()
        self.r.write(summary("bench_put_item", "medium", 100, 100))
        status, out = self.r.check()
        self.assertEqual(status, EXIT_OK, out)
        self.assertIn("1 benchmarks checked", out)

    def test_base_run_with_no_summaries_fails_record(self):
        os.makedirs(self.r.iai)
        status, out = self.r.record()
        self.assertEqual(status, EXIT_ERROR)
        self.assertIn("base run left no summary.json", out)

    def test_missing_base_list_fails_check(self):
        self.r.write(summary("bench_put_item", "medium", 100, 100))
        status, out = self.r.check()
        self.assertEqual(status, EXIT_ERROR)
        self.assertIn("cannot read the base benchmark list", out)

    def test_empty_base_list_fails_check(self):
        open(self.r.base_list, "w").close()
        self.r.write(summary("bench_put_item", "medium", 100, 100))
        self.assertEqual(self.r.check()[0], EXIT_ERROR)

    def test_head_run_with_no_summaries_fails_check(self):
        self.r.write(summary("bench_put_item", "medium", 100))
        self.r.record()
        status, out = self.r.check()
        self.assertEqual(status, EXIT_ERROR)
        self.assertIn("head run left no summary.json", out)

    def test_old_value_shapes(self):
        self.assertEqual(old_instructions(summary("f", "a", 5, 7)), 7)
        self.assertIsNone(old_instructions(summary("f", "a", 5)))
        self.assertIsNone(old_instructions({"callgrind_summary": None}))


if __name__ == "__main__":
    unittest.main()
