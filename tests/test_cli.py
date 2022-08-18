import logging
import unittest
import preql
import arrow
import subprocess
import sys

from data_diff import diff_tables, connect_to_table

from .common import TEST_MYSQL_CONN_STRING


def run_datadiff_cli(*args):
    try:
        stdout = subprocess.check_output([sys.executable, "-m", "data_diff", '--no-tracking'] + list(args), stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        logging.error(e.stderr)
        raise
    return stdout.splitlines()


class TestCLI(unittest.TestCase):
    def setUp(self) -> None:
        self.preql = preql.Preql(TEST_MYSQL_CONN_STRING)
        self.preql(
            r"""
            table test_cli {
                datetime: datetime
                comment: string
            }
            commit()

            func add(date, comment) {
                new test_cli(date, comment)
            }
        """
        )
        self.now = now = arrow.get(self.preql.now())
        self.preql.add(now, "now")
        self.preql.add(now, self.now.shift(seconds=-10))
        self.preql.add(now, self.now.shift(seconds=-7))
        self.preql.add(now, self.now.shift(seconds=-6))

        self.preql(
            r"""
            const table test_cli_2 = test_cli
            commit()
        """
        )

        self.preql.add(self.now.shift(seconds=-3), "3 seconds ago")
        self.preql.commit()

    def tearDown(self) -> None:
        self.preql.run_statement("drop table if exists test_cli")
        self.preql.run_statement("drop table if exists test_cli_2")
        self.preql.commit()
        self.preql.close()

        return super().tearDown()

    def test_basic(self):
        diff = run_datadiff_cli(TEST_MYSQL_CONN_STRING, "test_cli", TEST_MYSQL_CONN_STRING, "test_cli_2")
        assert len(diff) == 1

    def test_options(self):
        diff = run_datadiff_cli(
            TEST_MYSQL_CONN_STRING,
            "test_cli",
            TEST_MYSQL_CONN_STRING,
            "test_cli_2",
            "--bisection-factor",
            "16",
            "--bisection-threshold",
            "10000",
            "--limit",
            "5",
            "-t",
            "datetime",
            "--max-age",
            "1h",
        )
        assert len(diff) == 1
