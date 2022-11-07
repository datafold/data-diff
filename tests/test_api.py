import unittest
import arrow
from datetime import datetime

from data_diff import diff_tables, connect_to_table
from data_diff.databases import MySQL
from data_diff.sqeleton.queries.api import table

from .common import TEST_MYSQL_CONN_STRING, get_conn


def _commit(conn):
    conn.query("COMMIT", None)


class TestApi(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_conn(MySQL)
        table_src_name = "test_api"
        table_dst_name = "test_api_2"
        self.conn.query(f"drop table if exists {table_src_name}")
        self.conn.query(f"drop table if exists {table_dst_name}")

        src_table = table(table_src_name, schema={"id": int, "datetime": datetime, "text_comment": str})
        self.conn.query(src_table.create())
        self.now = now = arrow.get()

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
        self.conn.query("drop table if exists test_api")
        self.conn.query("drop table if exists test_api_2")
        _commit(self.conn)

        return super().tearDown()

    def test_api(self):
        t1 = connect_to_table(TEST_MYSQL_CONN_STRING, "test_api")
        t2 = connect_to_table(TEST_MYSQL_CONN_STRING, ("test_api_2",))
        diff = list(diff_tables(t1, t2))
        assert len(diff) == 1

        t1.database.close()
        t2.database.close()

        # test where
        diff_id = diff[0][1][0]
        where = f"id != {diff_id}"

        t1 = connect_to_table(TEST_MYSQL_CONN_STRING, "test_api", where=where)
        t2 = connect_to_table(TEST_MYSQL_CONN_STRING, "test_api_2", where=where)
        diff = list(diff_tables(t1, t2))
        assert len(diff) == 0

        t1.database.close()
        t2.database.close()
