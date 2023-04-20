import unittest

from data_diff.sqeleton.queries import table, commit

from data_diff import TableSegment, HashDiffer
from data_diff import databases as db
from .common import get_conn, random_table_suffix


class TestUUID(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = get_conn(db.PostgreSQL)

        table_suffix = random_table_suffix()

        self.table_src_name = f"src{table_suffix}"
        self.table_dst_name = f"dst{table_suffix}"

        self.table_src = table(self.table_src_name)
        self.table_dst = table(self.table_dst_name)

    def test_uuid(self):
        self.connection.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";', None)

        queries = [
            self.table_src.drop(True),
            self.table_dst.drop(True),
            f"CREATE TABLE {self.table_src_name} (id uuid DEFAULT uuid_generate_v4 (), comment VARCHAR, PRIMARY KEY (id))",
            commit,
            self.table_src.insert_rows([[i] for i in range(100)], columns=["comment"]),
            commit,
            self.table_dst.create(self.table_src),
            commit,
            self.table_src.insert_row("This one is different", columns=["comment"]),
            commit,
        ]

        for query in queries:
            self.connection.query(query)

        a = TableSegment(self.connection, self.table_src.path, ("id",), "comment")
        b = TableSegment(self.connection, self.table_dst.path, ("id",), "comment")

        differ = HashDiffer()
        diff = list(differ.diff_tables(a, b))
        uuid = diff[0][1][0]
        self.assertEqual(diff, [("-", (uuid, "This one is different"))])

        # Compare with MySql
        mysql_conn = get_conn(db.MySQL)

        rows = self.connection.query(self.table_src.select(), list)

        queries = [
            f"CREATE TABLE {self.table_dst_name} (id VARCHAR(128), comment VARCHAR(128))",
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
