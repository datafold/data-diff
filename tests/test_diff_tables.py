import datetime
import unittest

import preql

from xdiff.database import connect_to_uri
from xdiff.diff_tables import TableDiffer, TableSegment

from .common import TEST_MYSQL_CONN_STRING, str_to_checksum


class TestDiffTables(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Avoid leaking connections that require waiting for the GC, which can
        # cause deadlocks for table-level modifications.
        cls.preql = preql.Preql(TEST_MYSQL_CONN_STRING)
        cls.connection = connect_to_uri(TEST_MYSQL_CONN_STRING)

    def setUp(self):
        self.table_name = "RatingsTest"
        self.table = TableSegment(TestDiffTables.connection,
                                  (self.table_name, ),
                                  'id',
                                  ('timestamp', ))

        self.table2 = TableSegment(TestDiffTables.connection,
                                   ("RatingsTest2", ),
                                   'id',
                                   ('timestamp', ))
        self.connection.query("DROP TABLE IF EXISTS RatingsTest", None)
        self.connection.query("DROP TABLE IF EXISTS RatingsTest2", None)
        self.preql.load("./tests/setup.pql")
        self.preql.commit()

        self.differ = TableDiffer(3, 4)

    def test_properties_on_empty_table(self):
        self.assertEqual(0, self.table.count)
        self.assertEqual(["id", "timestamp"], self.table._relevant_columns)
        self.assertEqual(0, self.table.checksum)

    def test_get_values(self):
        time = "2022-01-01 00:00:00"
        res = self.preql(f"""
            new RatingsTest(1, 1, 9, '{time}')
        """)
        self.preql.commit()

        self.assertEqual(1, self.table.count)
        concatted = str(res['id']) + time
        self.assertEqual(str_to_checksum(concatted), self.table.checksum)

    def test_checkpoints(self):
        time = "2022-01-01 00:00:00"
        self.preql(f"""
            new RatingsTest(userId: 1, movieId: 1, rating: 9, timestamp: '{time}')
            new RatingsTest(userId: 1, movieId: 1, rating: 9, timestamp: '{time}')
            new RatingsTest(userId: 1, movieId: 1, rating: 9, timestamp: '{time}')
            new RatingsTest(userId: 1, movieId: 1, rating: 9, timestamp: '{time}')
        """)
        self.preql.commit()
        self.assertEqual([2, 4], self.table.choose_checkpoints(2))

    def test_diff_small_tables(self):
        time = "2022-01-01 00:00:00"
        self.preql(f"""
            new RatingsTest(userId: 1, movieId: 1, rating: 9, timestamp: '{time}')
            new RatingsTest(userId: 2, movieId: 2, rating: 9, timestamp: '{time}')

            new RatingsTest2(userId: 1, movieId: 1, rating: 9, timestamp: '{time}')
        """)
        self.preql.commit()
        diff = list(self.differ.diff_tables(self.table, self.table2))
        expected = [('+', (2, datetime.datetime(2022, 1, 1, 0, 0)))]
        self.assertEqual(expected, diff)

    def test_diff_table_above_bisection_threshold(self):
        time = "2022-01-01 00:00:00"
        self.preql(f"""
            new RatingsTest(userId: 1, movieId: 1, rating: 9, timestamp: '{time}')
            new RatingsTest(userId: 2, movieId: 2, rating: 9, timestamp: '{time}')
            new RatingsTest(userId: 3, movieId: 3, rating: 9, timestamp: '{time}')
            new RatingsTest(userId: 4, movieId: 4, rating: 9, timestamp: '{time}')
            new RatingsTest(userId: 5, movieId: 5, rating: 9, timestamp: '{time}')

            new RatingsTest2(userId: 1, movieId: 1, rating: 9, timestamp: '{time}')
            new RatingsTest2(userId: 2, movieId: 2, rating: 9, timestamp: '{time}')
            new RatingsTest2(userId: 3, movieId: 3, rating: 9, timestamp: '{time}')
            new RatingsTest2(userId: 4, movieId: 4, rating: 9, timestamp: '{time}')
        """)
        self.preql.commit()
        diff = list(self.differ.diff_tables(self.table, self.table2))
        expected = [('+', (5, datetime.datetime(2022, 1, 1, 0, 0)))]
        self.assertEqual(expected, diff)

    def test_return_empty_array_when_same(self):
        time = "2022-01-01 00:00:00"
        self.preql(f"""
            new RatingsTest(userId: 1, movieId: 1, rating: 9, timestamp: '{time}')
            new RatingsTest2(userId: 1, movieId: 1, rating: 9, timestamp: '{time}')
        """)
        self.preql.commit()
        diff = list(self.differ.diff_tables(self.table, self.table2))
        self.assertEqual([], diff)
