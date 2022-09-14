import unittest

from data_diff import TableSegment, HashDiffer, connect
from .common import TEST_POSTGRESQL_CONN_STRING, random_table_suffix


class TestWithConnection(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = connect(TEST_POSTGRESQL_CONN_STRING)

        self.connection.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";', None)

        table_suffix = random_table_suffix()

        self.table_src = f"src{table_suffix}"
        self.table_dst = f"dst{table_suffix}"

    def test_uuid(self):
        queries = [
            f"DROP TABLE IF EXISTS {self.table_src}",
            f"DROP TABLE IF EXISTS {self.table_dst}",
            f"CREATE TABLE {self.table_src} (id uuid DEFAULT uuid_generate_v4 (), comment VARCHAR, PRIMARY KEY (id))",
            "COMMIT",
        ]
        for i in range(100):
            queries.append(f"INSERT INTO {self.table_src}(comment) VALUES ('{i}')")

        queries += [
            "COMMIT",
            f"CREATE TABLE {self.table_dst} AS SELECT * FROM {self.table_src}",
            "COMMIT",
        ]

        queries.append(f"INSERT INTO {self.table_src}(comment) VALUES ('This one is different')")

        for query in queries:
            self.connection.query(query, None)

        a = TableSegment(self.connection, (self.table_src,), "id", "comment")
        b = TableSegment(self.connection, (self.table_dst,), "id", "comment")

        differ = HashDiffer()
        diff = list(differ.diff_tables(a, b))
        uuid = diff[0][1][0]
        self.assertEqual(diff, [("-", (uuid, "This one is different"))])
