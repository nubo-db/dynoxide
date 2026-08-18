#!/usr/bin/env python3
"""Tests for the criterion bencher-output parser.

Run with: python3 -m unittest discover -s benchmarks/scripts -p 'test_*.py'
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from compare_criterion import parse_bencher


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
