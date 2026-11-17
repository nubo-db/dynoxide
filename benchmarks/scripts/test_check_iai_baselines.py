#!/usr/bin/env python3
"""Tests for check_iai_baselines: every benchmark in a PR run must have had a baseline.

Run with: python3 -m unittest discover -s benchmarks/scripts -p 'test_*.py'
"""

import contextlib
import io
import json
import os
import subprocess
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


SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "check_iai_baselines.py")


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
            self.write_raw(name, json.dumps(s))

    def write_raw(self, name, text):
        d = os.path.join(self.iai, "dynoxide-benchmarks", "iai_core", "core_operations", name)
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "summary.json"), "w") as f:
            f.write(text)

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
        self.assertIn(
            "2 benchmarks checked: 2 compared, 0 new, 0 removed, 0 without a baseline", out
        )

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
        self.assertIn("1 benchmarks checked: 1 compared, 0 new, 1 removed", out)

    def test_benchmark_removed_in_head_is_reported(self):
        self.r.write(summary("bench_put_item", "medium", 100), summary("bench_scan", "with_filter", 200))
        self.r.record()
        self.r.write(
            summary("bench_put_item", "medium", 100, 100),
            summary("bench_get_item", "by_key", 50),
        )
        status, out = self.r.check()
        self.assertEqual(status, EXIT_OK, out)
        self.assertIn("removed   iai_core::core_operations::bench_scan with_filter", out)
        self.assertIn("1 new, 1 removed, 0 without a baseline", out)

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

    def test_right_shape_counts_as_compared(self):
        # Only the old run has a value.
        right = summary("bench_put_item", "medium", 0)
        right["callgrind_summary"]["callgrind_run"]["total"]["summary"]["Ir"]["metrics"] = {
            "Right": 90
        }
        self.assertEqual(old_instructions(right), 90)
        self.r.write(summary("bench_put_item", "medium", 100))
        self.r.record()
        self.r.write(right)
        status, out = self.r.check()
        self.assertEqual(status, EXIT_OK, out)
        self.assertIn("(base: 90 instructions)", out)

    def assert_malformed_head_fails_cleanly(self, text, reason):
        self.r.write(summary("bench_put_item", "medium", 100))
        self.r.record()
        self.r.write_raw("bench_put_item.medium", text)
        status, out = self.r.check()
        self.assertEqual(status, EXIT_ERROR, out)
        self.assertTrue(out.startswith("error: "), out)
        self.assertEqual(len(out.strip().splitlines()), 1, out)
        self.assertIn(reason, out)

    def test_json_list_fails_cleanly(self):
        self.assert_malformed_head_fails_cleanly("[1, 2]", "not a benchmark summary")

    def test_both_with_one_element_fails_cleanly(self):
        bad = summary("bench_put_item", "medium", 100, 100)
        bad["callgrind_summary"]["callgrind_run"]["total"]["summary"]["Ir"]["metrics"] = {
            "Both": [100]
        }
        self.assert_malformed_head_fails_cleanly(json.dumps(bad), "should hold [new, old]")

    def test_invalid_json_fails_cleanly(self):
        self.assert_malformed_head_fails_cleanly("{not json", "Expecting property name")

    def test_malformed_base_summary_fails_record_cleanly(self):
        self.r.write_raw("bench_put_item.medium", "[1, 2]")
        status, out = self.r.record()
        self.assertEqual(status, EXIT_ERROR, out)
        self.assertEqual(len(out.strip().splitlines()), 1, out)
        self.assertIn("not a benchmark summary", out)


class ExitStatusTest(unittest.TestCase):
    """The workflow reads the process exit status, not main()'s return value."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.r = Run(self.tmp.name)

    def run_script(self, *args):
        return subprocess.run(
            [sys.executable, SCRIPT, *args], capture_output=True, text=True
        ).returncode

    def test_process_exits_4_for_a_missing_baseline_and_0_when_clean(self):
        self.r.write(summary("bench_put_item", "medium", 100))
        self.assertEqual(
            self.run_script("record", "--iai-dir", self.r.iai, "--output", self.r.base_list),
            EXIT_OK,
        )
        check = ("check", "--iai-dir", self.r.iai, "--base-list", self.r.base_list)

        self.r.write(summary("bench_put_item", "medium", 100))
        self.assertEqual(self.run_script(*check), EXIT_MISSING_BASELINE)

        self.r.write(summary("bench_put_item", "medium", 100, 100))
        self.assertEqual(self.run_script(*check), EXIT_OK)


if __name__ == "__main__":
    unittest.main()
