import unittest

from data_diff.queries.api import table, commit
from data_diff import TableSegment, HashDiffer
from data_diff import databases as db
from tests.common import get_conn, random_table_suffix, connect


class TestClickzetta(unittest.TestCase):
    def setUp(self) -> None:
        # clickzetta uri pattern is like this:
        # clickzetta://user:password@instance.host:port/workspace?virtualcluster=vc&schema=schema
        self.connection = get_conn(db.Clickzetta)

        table_suffix = random_table_suffix()

        self.table_src_name = f"src{table_suffix}"
        self.table_dst_name = f"dst{table_suffix}"

        self.table_src = table(self.table_src_name)
        self.table_dst = table(self.table_dst_name)

    def test_compare(self):
        queries = [
            self.table_src.drop(True),
            self.table_dst.drop(True),
            f"CREATE TABLE {self.table_src_name} (id BIGINT, comment VARCHAR)",
            self.table_src.insert_rows([[i, str(i + 1)] for i in range(100)], columns=["id", "comment"]),
            self.table_dst.create(self.table_src),
            self.table_src.insert_row(200, "This one is different", columns=["id", "comment"]),
        ]

        for query in queries:
            self.connection.query(query)

        a = TableSegment(self.connection, self.table_src.path, ("id",), "comment")
        b = TableSegment(self.connection, self.table_dst.path, ("id",), "comment")

        differ = HashDiffer()
        diff = list(differ.diff_tables(a, b))
        id = diff[0][1][0]
        self.assertEqual(diff, [("-", (id, "This one is different"))])

        # Compare with MySql
        mysql_conn = get_conn(db.MySQL)

        rows = self.connection.query(self.table_src.select(), list)

        queries = [
            f"CREATE TABLE {self.table_dst_name} (id BIGINT, comment VARCHAR(128))",
            commit,
            self.table_dst.insert_rows(rows, columns=["id", "comment"]),
            commit,
        ]

        for q in queries:
            mysql_conn.query(q)

        c = TableSegment(mysql_conn, (self.table_dst_name,), ("id",), "comment")
        diff = list(differ.diff_tables(a, c))
        assert not diff, diff
        diff = list(differ.diff_tables(c, a))
        assert not diff, diff

        self.connection.query(self.table_src.drop(True))
        self.connection.query(self.table_dst.drop(True))
        mysql_conn.query(self.table_dst.drop(True))
