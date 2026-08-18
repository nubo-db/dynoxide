#!/usr/bin/env python3
"""Tests for the criterion bencher-output parser.

Run with: python3 -m unittest discover -s benchmarks/scripts -p 'test_*.py'
"""

import json
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import compare_criterion
from compare_criterion import build_baseline, build_report, parse_bencher


# What criterion writes to stdout when nothing goes wrong. The workflows pipe
# through `tee`, which captures stdout only, so criterion's own progress notes
# ("Gnuplot not found", "Warning: Unable to complete 100 samples...") never
# reach the file being parsed -- it writes those with eprintln!. The blank line
# is the separator it emits between benchmark groups.
CLEAN = """test put_item/put_item/small ... bench:       26475 ns/iter (+/- 1730)
test get_item ... bench:       14739 ns/iter (+/- 273)

test query_base_table ... bench:     1,112,845 ns/iter (+/- 12302)
"""

# What it writes when a stale target/criterion leaves a base directory behind
# without the sample.json inside it. That error goes to stdout, unlike the
# progress notes above, so it lands between the header and the measurement.
INTERLEAVED = """test put_item/put_item/small ... Criterion.rs ERROR: error: \
Failed to access file \
"/tmp/benchmarks/target/criterion/put_item/put_item/small/base/sample.json": \
No such file or directory (os error 2)
bench:       27261 ns/iter (+/- 2501)
test get_item ... Criterion.rs ERROR: error: Failed to access file \
"/tmp/benchmarks/target/criterion/get_item/base/sample.json": No such file or \
directory (os error 2)
bench:       15543 ns/iter (+/- 755)
"""


class ParseBencherTest(unittest.TestCase):
    def test_reads_clean_output(self):
        self.assertEqual(
            parse_bencher(CLEAN),
            {
                "put_item/put_item/small": 26475,
                "get_item": 14739,
                "query_base_table": 1112845,
            },
        )

    def test_reads_output_split_by_a_criterion_error(self):
        self.assertEqual(
            parse_bencher(INTERLEAVED),
            {"put_item/put_item/small": 27261, "get_item": 15543},
        )

    def test_ignores_noise_that_is_not_a_result(self):
        self.assertEqual(parse_bencher("\nCriterion.rs ERROR: error: boom\n"), {})

    def test_leaves_out_a_benchmark_that_produced_no_measurement(self):
        # The caller reports a baselined benchmark with no current result as a
        # failed comparison, so a name with nothing behind it must not appear.
        self.assertEqual(parse_bencher("test get_item ... \n"), {})

    def test_does_not_credit_a_measurement_to_the_wrong_benchmark(self):
        # get_item died before reporting. Its name must not pick up the figure
        # belonging to the benchmark that ran next.
        text = "test get_item ... \ntest delete_item ... bench: 53258 ns/iter (+/- 5623)\n"
        self.assertEqual(parse_bencher(text), {"delete_item": 53258})

    def test_reads_a_measurement_that_follows_two_lines_of_noise(self):
        text = (
            "test get_item ... Criterion.rs ERROR: first\n"
            "Criterion.rs ERROR: second\n"
            "bench:       15543 ns/iter (+/- 755)\n"
        )
        self.assertEqual(parse_bencher(text), {"get_item": 15543})


if __name__ == "__main__":
    unittest.main()


class BuildReportTest(unittest.TestCase):
    """The report's own text calls a partial run a failed comparison, so the
    caller has to be able to see that state rather than infer it."""

    HISTORY = [
        ("runs/2026-01-01-aaaaaaa", {"get_item": 100, "put_item": 200}),
        ("runs/2026-01-02-bbbbbbb", {"get_item": 100, "put_item": 200}),
    ]

    def report(self, current, threshold=1.50):
        baseline = build_baseline(self.HISTORY)
        return build_report(current, baseline, self.HISTORY, threshold)

    def test_names_a_baselined_benchmark_that_produced_no_result(self):
        _, regressions, missing = self.report({"get_item": 100})
        self.assertEqual(missing, ["put_item"])
        self.assertEqual(regressions, [])

    def test_reports_nothing_missing_when_every_benchmark_ran(self):
        _, regressions, missing = self.report({"get_item": 100, "put_item": 200})
        self.assertEqual(missing, [])
        self.assertEqual(regressions, [])

    def test_a_benchmark_new_to_the_window_is_not_missing(self):
        current = {"get_item": 100, "put_item": 200, "scan": 300}
        _, _, missing = self.report(current)
        self.assertEqual(missing, [])

    def test_warns_about_a_regression_and_a_missing_benchmark_together(self):
        # The two warnings used to share an if/elif chain, so a run that both
        # regressed and lost a benchmark only ever mentioned the regression.
        markdown, regressions, missing = self.report({"get_item": 1000})
        self.assertEqual(regressions, ["get_item"])
        self.assertEqual(missing, ["put_item"])
        self.assertIn("above the", markdown)
        self.assertIn("produced no result", markdown)

    def test_says_so_when_everything_is_within_threshold(self):
        markdown, _, _ = self.report({"get_item": 100, "put_item": 200})
        self.assertIn("All benchmarks within", markdown)


class ExitCodeTest(unittest.TestCase):
    """Both workflows branch on these, so they are a contract."""

    STORED = json.dumps({"get_item": 100, "put_item": 200})

    def run_main(self, current, tmpdir):
        """Run main() against a synthetic history.

        Two stored runs, not one: a benchmark with a single sample behind it
        reports its change without flagging it, so a one-run history could
        never reach the regression exit.
        """
        current_path = os.path.join(tmpdir, "current.json")
        with open(current_path, "w") as handle:
            json.dump(current, handle)

        def fake_git(*args):
            if args[0] == "ls-tree":
                return "runs/2026-01-01-aaaaaaa\nruns/2026-01-02-bbbbbbb\n"
            return self.STORED

        argv = [
            "compare_criterion.py",
            "--current-json", current_path,
            "--output", os.path.join(tmpdir, "out.md"),
        ]
        with mock.patch.object(compare_criterion, "git", fake_git), \
                mock.patch.object(sys, "argv", argv):
            return compare_criterion.main()

    def test_clean_run_exits_ok(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            status = self.run_main({"get_item": 100, "put_item": 200}, tmpdir)
        self.assertEqual(status, compare_criterion.EXIT_OK)

    def test_a_run_over_the_threshold_exits_regressed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            status = self.run_main({"get_item": 1000, "put_item": 200}, tmpdir)
        self.assertEqual(status, compare_criterion.EXIT_REGRESSED)

    def test_a_run_missing_a_benchmark_exits_incomplete(self):
        # Exit 0 here is how a partial run used to reach a green check while
        # the comparison it posted said not to trust it.
        with tempfile.TemporaryDirectory() as tmpdir:
            status = self.run_main({"get_item": 100}, tmpdir)
        self.assertEqual(status, compare_criterion.EXIT_INCOMPLETE)

    def test_missing_takes_precedence_over_a_regression(self):
        # Exit 3 is advisory and leaves the step green, which a run that lost
        # benchmarks has not earned.
        with tempfile.TemporaryDirectory() as tmpdir:
            status = self.run_main({"get_item": 1000}, tmpdir)
        self.assertEqual(status, compare_criterion.EXIT_INCOMPLETE)

    def test_a_run_that_parsed_nothing_exits_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            status = self.run_main({}, tmpdir)
        self.assertEqual(status, compare_criterion.EXIT_ERROR)
