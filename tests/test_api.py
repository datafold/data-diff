import unittest
import io
import unittest.mock
import arrow
from datetime import datetime

from data_diff import diff_tables, connect_to_table
from data_diff.databases import MySQL
from data_diff.sqeleton.queries import table, commit

from .common import TEST_MYSQL_CONN_STRING, get_conn, random_table_suffix


def _commit(conn):
    conn.query(commit)


class TestApi(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_conn(MySQL)
        suffix = random_table_suffix()
        self.table_src_name = f"test_api{suffix}"
        self.table_dst_name = f"test_api_2{suffix}"

        self.table_src = table(self.table_src_name)
        self.table_dst = table(self.table_dst_name)

        self.conn.query(self.table_src.drop(True))
        self.conn.query(self.table_dst.drop(True))

        src_table = table(self.table_src_name, schema={"id": int, "datetime": datetime, "text_comment": str})
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

        self.conn.query(self.table_dst.create(self.table_src))
        _commit(self.conn)

        self.conn.query(src_table.insert_row(len(rows), self.now.shift(seconds=-3).datetime, "3 seconds ago"))
        _commit(self.conn)

    def tearDown(self) -> None:
        self.conn.query(self.table_src.drop(True))
        self.conn.query(self.table_dst.drop(True))
        _commit(self.conn)

        return super().tearDown()

    def test_api(self):
        t1 = connect_to_table(TEST_MYSQL_CONN_STRING, self.table_src_name)
        t2 = connect_to_table(TEST_MYSQL_CONN_STRING, (self.table_dst_name,))
        diff = list(diff_tables(t1, t2))
        assert len(diff) == 1

        t1.database.close()
        t2.database.close()

        # test where
        diff_id = diff[0][1][0]
        where = f"id != {diff_id}"

        t1 = connect_to_table(TEST_MYSQL_CONN_STRING, self.table_src_name, where=where)
        t2 = connect_to_table(TEST_MYSQL_CONN_STRING, self.table_dst_name, where=where)
        diff = list(diff_tables(t1, t2))
        assert len(diff) == 0

        t1.database.close()
        t2.database.close()

    def test_api_get_stats_string(self):
        expected_string = "5 rows in table A\n4 rows in table B\n1 rows exclusive to table A (not present in B)\n0 rows exclusive to table B (not present in A)\n0 rows updated\n4 rows unchanged\n20.00% difference score\n\nExtra-Info:\n  rows_downloaded = 5\n"
        t1 = connect_to_table(TEST_MYSQL_CONN_STRING, self.table_src_name)
        t2 = connect_to_table(TEST_MYSQL_CONN_STRING, self.table_dst_name)
        diff = diff_tables(t1, t2)
        diff_list = list(diff)
        output = diff.get_stats_string()

        self.assertEqual(expected_string, output)
        self.assertIsNotNone(diff)
        assert len(diff_list) == 1

        t1.database.close()
        t2.database.close()
    
    def test_api_get_stats_json(self):
        expected_dict = {'rows_A': 5, 'rows_B': 4, 'exclusive_A': 1, 'exclusive_B': 0, 'updated': 0, 'unchanged': 4, 'total': 1, 'stats': {'rows_downloaded': 5}}
        t1 = connect_to_table(TEST_MYSQL_CONN_STRING, self.table_src_name)
        t2 = connect_to_table(TEST_MYSQL_CONN_STRING, self.table_dst_name)
        diff = diff_tables(t1, t2)
        diff_list = list(diff)
        output = diff.get_stats_json()

        self.assertEqual(expected_dict, output)
        self.assertIsNotNone(diff)
        assert len(diff_list) == 1

        t1.database.close()
        t2.database.close()

    def test_api_print_error(self):
        t1 = connect_to_table(TEST_MYSQL_CONN_STRING, self.table_src_name)
        t2 = connect_to_table(TEST_MYSQL_CONN_STRING, (self.table_dst_name,))
        diff = diff_tables(t1, t2)

        with self.assertRaises(RuntimeError):
            diff.get_stats_string()

        t1.database.close()
        t2.database.close()
