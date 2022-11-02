import logging
import unittest
import arrow
import subprocess
import sys
from datetime import datetime

from data_diff import diff_tables, connect_to_table
from data_diff.databases import MySQL
from data_diff.queries import table

from .common import TEST_MYSQL_CONN_STRING, get_conn


def _commit(conn):
    conn.query("COMMIT", None)


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
        self.conn.query("drop table if exists test_cli")
        self.conn.query("drop table if exists test_cli_2")
        table_src_name = "test_cli"
        table_dst_name = "test_cli_2"

        src_table = table(table_src_name, schema={"id": int, "datetime": datetime, "text_comment": str})
        self.conn.query(src_table.create())

        self.conn.query("SET @@session.time_zone='+00:00'")
        db_time = self.conn.query("select now()", datetime)
        self.now = now = arrow.get(db_time)

        rows = [
            (now, "now"),
            (self.now.shift(seconds=-10), "a"),
            (self.now.shift(seconds=-7), "b"),
            (self.now.shift(seconds=-6), "c"),
        ]

        self.conn.query(src_table.insert_rows((i, ts.datetime, s) for i, (ts, s) in enumerate(rows)))
        _commit(self.conn)

        self.conn.query(f"CREATE TABLE {table_dst_name} AS SELECT * FROM {table_src_name}")
        _commit(self.conn)

        self.conn.query(src_table.insert_row(len(rows), self.now.shift(seconds=-3).datetime, "3 seconds ago"))
        _commit(self.conn)

    def tearDown(self) -> None:
        self.conn.query("drop table if exists test_cli")
        self.conn.query("drop table if exists test_cli_2")
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
