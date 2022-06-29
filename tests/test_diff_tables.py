import datetime
import unittest
import uuid

import preql
import arrow  # comes with preql

from data_diff.databases import connect_to_uri
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

    # Fallback for test runners that doesn't support setUpClass/tearDownClass
    def setUp(self) -> None:
        if not hasattr(self, "connection"):
            self.setUpClass.__func__(self)
            self.private_connection = True

        return super().setUp()

    def tearDown(self) -> None:
        if hasattr(self, "private_connection"):
            self.tearDownClass.__func__(self)

        return super().tearDown()


class TestDates(TestWithConnection):
    def setUp(self):
        super().setUp()
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
        super().setUp()
        self.connection.query("DROP TABLE IF EXISTS ratings_test", None)
        self.connection.query("DROP TABLE IF EXISTS ratings_test2", None)
        self.preql.load("./tests/setup.pql")
        self.preql.commit()

        self.table = TableSegment(self.connection, ("ratings_test",), "id", "timestamp")

        self.table2 = TableSegment(self.connection, ("ratings_test2",), "id", "timestamp")

        self.differ = TableDiffer(3, 4)

    def test_properties_on_empty_table(self):
        table = self.table.with_schema()
        self.assertEqual(0, table.count())
        self.assertEqual(None, table.count_and_checksum()[1])

    def test_get_values(self):
        time = "2022-01-01 00:00:00.000000"
        res = self.preql(
            f"""
            new ratings_test(1, 1, 9, '{time}')
        """
        )
        self.preql.commit()

        table = self.table.with_schema()

        self.assertEqual(1, table.count())
        concatted = str(res["id"]) + time
        self.assertEqual(str_to_checksum(concatted), table.count_and_checksum()[1])

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
        expected = [("-", ("2", time + ".000000"))]
        self.assertEqual(expected, diff)
        self.assertEqual(2, self.differ.stats["table1_count"])
        self.assertEqual(1, self.differ.stats["table2_count"])

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
        expected = [("-", ("5", time + ".000000"))]
        self.assertEqual(expected, diff)
        self.assertEqual(5, self.differ.stats["table1_count"])
        self.assertEqual(4, self.differ.stats["table2_count"])

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
            ("-", ("2", time2 + ".000000")),
            ("+", ("2", time + ".000000")),
            ("-", ("4", time2 + ".000000")),
            ("+", ("4", time + ".000000")),
        ]
        self.assertEqual(expected, diff)


class TestStringKeys(TestWithConnection):
    def setUp(self):
        super().setUp()

        queries = [
            "DROP TABLE IF EXISTS a",
            "DROP TABLE IF EXISTS b",
            "CREATE TABLE a(id varchar(100), comment varchar(1000))",
            "COMMIT",
        ]
        for i in range(100):
            queries.append(f"INSERT INTO a VALUES ('{uuid.uuid1(i)}', '{i}')")

        queries += [
            "COMMIT",
            "CREATE TABLE b AS SELECT * FROM a",
            "COMMIT",
        ]

        self.new_uuid = uuid.uuid1(32132131)
        queries.append(f"INSERT INTO a VALUES ('{self.new_uuid}', 'This one is different')")

        # TODO test unexpected values?

        for query in queries:
            self.connection.query(query, None)

        self.a = TableSegment(self.connection, ("a",), "id", "comment")
        self.b = TableSegment(self.connection, ("b",), "id", "comment")

    def test_string_keys(self):
        differ = TableDiffer()
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.new_uuid), "This one is different"))])

        self.connection.query(f"INSERT INTO a VALUES ('unexpected', '<-- this bad value should not break us')", None)

        self.assertRaises(ValueError, differ.diff_tables, self.a, self.b)


class TestTableSegment(TestWithConnection):
    def setUp(self) -> None:
        super().setUp()
        self.table = TableSegment(self.connection, ("ratings_test",), "id", "timestamp")
        self.table2 = TableSegment(self.connection, ("ratings_test2",), "id", "timestamp")

    def test_table_segment(self):
        early = datetime.datetime(2021, 1, 1, 0, 0)
        late = datetime.datetime(2022, 1, 1, 0, 0)
        self.assertRaises(ValueError, self.table.replace, min_update=late, max_update=early)

        self.assertRaises(ValueError, self.table.replace, min_key=10, max_key=0)
