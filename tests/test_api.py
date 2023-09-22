from datetime import datetime, timedelta

from data_diff import diff_tables, connect_to_table, Algorithm
from data_diff.databases.mysql import MySQL
from data_diff.queries.api import table, commit

from tests.common import TEST_MYSQL_CONN_STRING, get_conn, random_table_suffix, DiffTestCase


class TestApi(DiffTestCase):
    src_schema = {"id": int, "datetime": datetime, "text_comment": str}
    db_cls = MySQL

    def setUp(self) -> None:
        super().setUp()

        self.conn = self.connection

        self.now = now = datetime.now()

        rows = [
            (now, "now"),
            (self.now - timedelta(seconds=10), "a"),
            (self.now - timedelta(seconds=7), "b"),
            (self.now - timedelta(seconds=6), "c"),
        ]

        self.conn.query(
            [
                self.src_table.insert_rows((i, ts, s) for i, (ts, s) in enumerate(rows)),
                self.dst_table.create(self.src_table),
                self.src_table.insert_row(len(rows), self.now - timedelta(seconds=3), "3 seconds ago"),
                commit,
            ]
        )

    def test_api(self):
        # test basic
        t1 = connect_to_table(TEST_MYSQL_CONN_STRING, self.table_src_name)
        t2 = connect_to_table(TEST_MYSQL_CONN_STRING, (self.table_dst_name,))
        diff = list(diff_tables(t1, t2, algorithm=Algorithm.JOINDIFF))
        assert len(diff) == 1

        # test algorithm
        # (also tests shared connection on connect_to_table)
        for algo in (Algorithm.HASHDIFF, Algorithm.JOINDIFF):
            diff = list(diff_tables(t1, t2, algorithm=algo))
            assert len(diff) == 1

        # test where
        diff_id = diff[0][1][0]
        where = f"id != {diff_id} OR id = 90000000"

        t1 = connect_to_table(TEST_MYSQL_CONN_STRING, self.table_src_name, where=where)
        t2 = connect_to_table(TEST_MYSQL_CONN_STRING, self.table_dst_name, where=where)
        diff = list(diff_tables(t1, t2))
        assert len(diff) == 0

    def test_api_get_stats_dict(self):
        # XXX Likely to change in the future
        expected_dict = {
            "rows_A": 5,
            "rows_B": 4,
            "exclusive_A": 1,
            "exclusive_B": 0,
            "updated": 0,
            "unchanged": 4,
            "total": 1,
            "values": {},
        }
        t1 = connect_to_table(TEST_MYSQL_CONN_STRING, self.table_src_name)
        t2 = connect_to_table(TEST_MYSQL_CONN_STRING, self.table_dst_name)
        diff = diff_tables(t1, t2)

        output = diff.get_stats_dict()
        output.pop("stats")
        self.assertEqual(expected_dict, output)
        self.assertIsNotNone(diff)
        assert len(list(diff)) == 1
