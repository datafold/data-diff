import logging
import unittest
import arrow
import subprocess
import sys
from datetime import datetime, timedelta

from data_diff.databases import MySQL
from data_diff.sqeleton.queries import table, commit

from .common import TEST_MYSQL_CONN_STRING, get_conn


def _commit(conn):
    conn.query(commit)


def run_datadiff_cli(*args):
    try:
        stdout = subprocess.check_output(
            [sys.executable, "-m", "data_diff", "--no-tracking"] + list(args), stderr=subprocess.PIPE
        )
    except subprocess.CalledProcessError as e:
        logging.error(e.stderr)
        raise
    return stdout.splitlines()


class TestCLI(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_conn(MySQL)

        table_src_name = "test_cli"
        table_dst_name = "test_cli_2"

        self.table_src = table(table_src_name)
        self.table_dst = table(table_dst_name)
        self.conn.query(self.table_src.drop(True))
        self.conn.query(self.table_dst.drop(True))

        src_table = table(table_src_name, schema={"id": int, "datetime": datetime, "text_comment": str})
        self.conn.query(src_table.create())
        self.conn.query("SET @@session.time_zone='+00:00'")
        now = self.conn.query("select now()", datetime)

        rows = [
            (now, "now"),
            (now - timedelta(seconds=10), "a"),
            (now - timedelta(seconds=7), "b"),
            (now - timedelta(seconds=6), "c"),
        ]

        self.conn.query(src_table.insert_rows((i, ts, s) for i, (ts, s) in enumerate(rows)))
        _commit(self.conn)

        self.conn.query(self.table_dst.create(self.table_src))
        _commit(self.conn)

        self.conn.query(src_table.insert_row(len(rows), now - timedelta(seconds=3), "3 seconds ago"))
        _commit(self.conn)

    def tearDown(self) -> None:
        self.conn.query(self.table_src.drop(True))
        self.conn.query(self.table_dst.drop(True))
        _commit(self.conn)

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
