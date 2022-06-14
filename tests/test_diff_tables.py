import datetime
import unittest

import preql
import arrow  # comes with preql

from data_diff.database import connect_to_uri
from data_diff.diff_tables import TableDiffer, TableSegment, split_space

from .common import TEST_MYSQL_CONN_STRING, str_to_checksum


class TestUtils(unittest.TestCase):
    def test_split_space(self):
        for i in range(0, 10):
            for j in range(1, 16328, 17):
                for n in range(1, 32):
                    r = split_space(i, j + i + n, n)
                    assert len(r) == n, f"split_space({i}, {j+n}, {n}) = {(r)}"


class TestWithConnection(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Avoid leaking connections that require waiting for the GC, which can
        # cause deadlocks for table-level modifications.
        cls.preql = preql.Preql(TEST_MYSQL_CONN_STRING)
        cls.connection = connect_to_uri(TEST_MYSQL_CONN_STRING)

    @classmethod
    def tearDownClass(cls):
        cls.preql.close()
        cls.connection.close()


class TestDates(TestWithConnection):
    def setUp(self):
        self.connection.query("DROP TABLE IF EXISTS a", None)
        self.connection.query("DROP TABLE IF EXISTS b", None)
        self.preql(
            r"""
            table a {
                datetime: datetime
                comment: string
            }
            commit()

            func add(date, comment) {
                new a(date, comment)
            }
        """
        )
        self.now = now = arrow.get(self.preql.now())
        self.preql.add(now.shift(days=-50), "50 days ago")
        self.preql.add(now.shift(hours=-3), "3 hours ago")
        self.preql.add(now.shift(minutes=-10), "10 mins ago")
        self.preql.add(now.shift(seconds=-1), "1 second ago")
        self.preql.add(now, "now")

        self.preql(
            r"""
            const table b = a
            commit()
        """
        )

        self.preql.add(self.now.shift(seconds=-3), "2 seconds ago")
        self.preql.commit()

    def test_init(self):
        a = TableSegment(self.connection, ("a",), "id", "datetime", max_update=self.now.datetime)
        self.assertRaises(ValueError, TableSegment, self.connection, ("a",), "id", max_update=self.now.datetime)

    def test_basic(self):
        differ = TableDiffer(10, 100)
        a = TableSegment(self.connection, ("a",), "id", "datetime")
        b = TableSegment(self.connection, ("b",), "id", "datetime")
        assert a.count() == 6
        assert b.count() == 5

        assert not list(differ.diff_tables(a, a))
        self.assertEqual(len(list(differ.diff_tables(a, b))), 1)

    def test_offset(self):
        differ = TableDiffer(2, 10)
        sec1 = self.now.shift(seconds=-1).datetime
        a = TableSegment(self.connection, ("a",), "id", "datetime", max_update=sec1)
        b = TableSegment(self.connection, ("b",), "id", "datetime", max_update=sec1)
        assert a.count() == 4
        assert b.count() == 3

        assert not list(differ.diff_tables(a, a))
        self.assertEqual(len(list(differ.diff_tables(a, b))), 1)

        a = TableSegment(self.connection, ("a",), "id", "datetime", min_update=sec1)
        b = TableSegment(self.connection, ("b",), "id", "datetime", min_update=sec1)
        assert a.count() == 2
        assert b.count() == 2
        assert not list(differ.diff_tables(a, b))

        day1 = self.now.shift(days=-1).datetime

        a = TableSegment(self.connection, ("a",), "id", "datetime", min_update=day1, max_update=sec1)
        b = TableSegment(self.connection, ("b",), "id", "datetime", min_update=day1, max_update=sec1)
        assert a.count() == 3
        assert b.count() == 2
        assert not list(differ.diff_tables(a, a))
        self.assertEqual(len(list(differ.diff_tables(a, b))), 1)


class TestDiffTables(TestWithConnection):
    def setUp(self):
        self.connection.query("DROP TABLE IF EXISTS ratings_test", None)
        self.connection.query("DROP TABLE IF EXISTS ratings_test2", None)
        self.preql.load("./tests/setup.pql")
        self.preql.commit()

        self.table = TableSegment(self.connection, ("ratings_test",), "id", "timestamp")

        self.table2 = TableSegment(self.connection, ("ratings_test2",), "id", "timestamp")

        self.differ = TableDiffer(3, 4)

    def test_properties_on_empty_table(self):
        self.assertEqual(0, self.table.count())
        self.assertEqual(None, self.table.count_and_checksum()[1])

    def test_get_values(self):
        time = "2022-01-01 00:00:00"
        res = self.preql(
            f"""
            new ratings_test(1, 1, 9, '{time}')
        """
        )
        self.preql.commit()

        self.assertEqual(1, self.table.count())
        concatted = str(res["id"]) + time
        self.assertEqual(str_to_checksum(concatted), self.table.count_and_checksum()[1])

    def test_diff_small_tables(self):
        time = "2022-01-01 00:00:00"
        self.preql(
            f"""
            new ratings_test(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
            new ratings_test(userid: 2, movieid: 2, rating: 9, timestamp: '{time}')

            new ratings_test2(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
        """
        )
        self.preql.commit()
        diff = list(self.differ.diff_tables(self.table, self.table2))
        expected = [("-", (2, datetime.datetime(2022, 1, 1, 0, 0)))]
        self.assertEqual(expected, diff)

    def test_diff_table_above_bisection_threshold(self):
        time = "2022-01-01 00:00:00"
        self.preql(
            f"""
            new ratings_test(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
            new ratings_test(userid: 2, movieid: 2, rating: 9, timestamp: '{time}')
            new ratings_test(userid: 3, movieid: 3, rating: 9, timestamp: '{time}')
            new ratings_test(userid: 4, movieid: 4, rating: 9, timestamp: '{time}')
            new ratings_test(userid: 5, movieid: 5, rating: 9, timestamp: '{time}')

            new ratings_test2(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
            new ratings_test2(userid: 2, movieid: 2, rating: 9, timestamp: '{time}')
            new ratings_test2(userid: 3, movieid: 3, rating: 9, timestamp: '{time}')
            new ratings_test2(userid: 4, movieid: 4, rating: 9, timestamp: '{time}')
        """
        )
        self.preql.commit()
        diff = list(self.differ.diff_tables(self.table, self.table2))
        expected = [("-", (5, datetime.datetime(2022, 1, 1, 0, 0)))]
        self.assertEqual(expected, diff)

    def test_return_empty_array_when_same(self):
        time = "2022-01-01 00:00:00"
        self.preql(
            f"""
            new ratings_test(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
            new ratings_test2(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
        """
        )
        self.preql.commit()
        diff = list(self.differ.diff_tables(self.table, self.table2))
        self.assertEqual([], diff)

    def test_diff_sorted_by_key(self):
        time = "2022-01-01 00:00:00"
        time2 = "2021-01-01 00:00:00"
        self.preql(
            f"""
            new ratings_test(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
            new ratings_test(userid: 2, movieid: 2, rating: 9, timestamp: '{time2}')
            new ratings_test(userid: 3, movieid: 3, rating: 9, timestamp: '{time}')
            new ratings_test(userid: 4, movieid: 4, rating: 9, timestamp: '{time2}')
            new ratings_test(userid: 5, movieid: 5, rating: 9, timestamp: '{time}')

            new ratings_test2(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
            new ratings_test2(userid: 2, movieid: 2, rating: 9, timestamp: '{time}')
            new ratings_test2(userid: 3, movieid: 3, rating: 9, timestamp: '{time}')
            new ratings_test2(userid: 4, movieid: 4, rating: 9, timestamp: '{time}')
            new ratings_test2(userid: 5, movieid: 5, rating: 9, timestamp: '{time}')
        """
        )
        self.preql.commit()
        differ = TableDiffer()
        diff = list(differ.diff_tables(self.table, self.table2))
        expected = [
            ("-", (2, datetime.datetime(2021, 1, 1, 0, 0))),
            ("+", (2, datetime.datetime(2022, 1, 1, 0, 0))),
            ("-", (4, datetime.datetime(2021, 1, 1, 0, 0))),
            ("+", (4, datetime.datetime(2022, 1, 1, 0, 0))),
        ]
        self.assertEqual(expected, diff)


class TestTableSegment(TestWithConnection):
    def setUp(self) -> None:
        self.table = TableSegment(self.connection, ("ratings_test",), "id", "timestamp")
        self.table2 = TableSegment(self.connection, ("ratings_test2",), "id", "timestamp")
        return super().setUp()

    def test_table_segment(self):
        early = datetime.datetime(2021, 1, 1, 0, 0)
        late = datetime.datetime(2022, 1, 1, 0, 0)
        self.assertRaises(ValueError, self.table.replace, min_update=late, max_update=early)

        self.assertRaises(ValueError, self.table.replace, min_key=10, max_key=0)
