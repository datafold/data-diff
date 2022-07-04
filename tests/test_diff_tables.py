import datetime
import unittest
import uuid

import preql
import arrow  # comes with preql

from data_diff.databases import connect_to_uri
from data_diff.diff_tables import TableDiffer, TableSegment, split_space

from .common import TEST_MYSQL_CONN_STRING, str_to_checksum, random_table_suffix


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

        table_suffix = random_table_suffix()

        self.table_src = f"src{table_suffix}"
        self.table_dst = f"dst{table_suffix}"

        return super().setUp()

    def tearDown(self) -> None:
        if hasattr(self, "private_connection"):
            self.tearDownClass.__func__(self)

        return super().tearDown()


class TestDates(TestWithConnection):
    def setUp(self):
        super().setUp()
        self.connection.query(f"DROP TABLE IF EXISTS {self.table_src}", None)
        self.connection.query(f"DROP TABLE IF EXISTS {self.table_dst}", None)
        self.preql(
            f"""
            table {self.table_src} {{
                datetime: datetime
                comment: string
            }}
            commit()

            func add(date, comment) {{
                new {self.table_src}(date, comment)
            }}
        """
        )
        self.now = now = arrow.get(self.preql.now())
        self.preql.add(now.shift(days=-50), "50 days ago")
        self.preql.add(now.shift(hours=-3), "3 hours ago")
        self.preql.add(now.shift(minutes=-10), "10 mins ago")
        self.preql.add(now.shift(seconds=-1), "1 second ago")
        self.preql.add(now, "now")

        self.preql(
            f"""
            const table {self.table_dst} = {self.table_src}
            commit()
        """
        )

        self.preql.add(self.now.shift(seconds=-3), "2 seconds ago")
        self.preql.commit()

    def test_init(self):
        a = TableSegment(self.connection, (self.table_src,), "id", "datetime", max_update=self.now.datetime)
        self.assertRaises(
            ValueError, TableSegment, self.connection, (self.table_src,), "id", max_update=self.now.datetime
        )

    def test_basic(self):
        differ = TableDiffer(10, 100)
        a = TableSegment(self.connection, (self.table_src,), "id", "datetime")
        b = TableSegment(self.connection, (self.table_dst,), "id", "datetime")
        assert a.count() == 6
        assert b.count() == 5

        assert not list(differ.diff_tables(a, a))
        self.assertEqual(len(list(differ.diff_tables(a, b))), 1)

    def test_offset(self):
        differ = TableDiffer(2, 10)
        sec1 = self.now.shift(seconds=-1).datetime
        a = TableSegment(self.connection, (self.table_src,), "id", "datetime", max_update=sec1)
        b = TableSegment(self.connection, (self.table_dst,), "id", "datetime", max_update=sec1)
        assert a.count() == 4
        assert b.count() == 3

        assert not list(differ.diff_tables(a, a))
        self.assertEqual(len(list(differ.diff_tables(a, b))), 1)

        a = TableSegment(self.connection, (self.table_src,), "id", "datetime", min_update=sec1)
        b = TableSegment(self.connection, (self.table_dst,), "id", "datetime", min_update=sec1)
        assert a.count() == 2
        assert b.count() == 2
        assert not list(differ.diff_tables(a, b))

        day1 = self.now.shift(days=-1).datetime

        a = TableSegment(self.connection, (self.table_src,), "id", "datetime", min_update=day1, max_update=sec1)
        b = TableSegment(self.connection, (self.table_dst,), "id", "datetime", min_update=day1, max_update=sec1)
        assert a.count() == 3
        assert b.count() == 2
        assert not list(differ.diff_tables(a, a))
        self.assertEqual(len(list(differ.diff_tables(a, b))), 1)


class TestDiffTables(TestWithConnection):
    def setUp(self):
        super().setUp()
        self.connection.query(f"DROP TABLE IF EXISTS {self.table_src}", None)
        self.connection.query(f"DROP TABLE IF EXISTS {self.table_dst}", None)
        self.preql(
            f"""
            func run_sql(code) {{
                force_eval( SQL( nulltype, code ))
            }}

            table {self.table_src} {{
                userid: int
                movieid: int
                rating: float
                timestamp: timestamp
            }}

            table {self.table_dst} {{
                userid: int
                movieid: int
                rating: float
                timestamp: timestamp
            }}
            commit()
        """
        )
        self.preql.commit()

        self.table = TableSegment(self.connection, (self.table_src,), "id", "timestamp")
        self.table2 = TableSegment(self.connection, (self.table_dst,), "id", "timestamp")

        self.differ = TableDiffer(3, 4)

    def test_properties_on_empty_table(self):
        table = self.table.with_schema()
        self.assertEqual(0, table.count())
        self.assertEqual(None, table.count_and_checksum()[1])

    def test_get_values(self):
        time = "2022-01-01 00:00:00.000000"
        res = self.preql(
            f"""
            new {self.table_src}(1, 1, 9, '{time}')
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
            new {self.table_src}(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
            new {self.table_src}(userid: 2, movieid: 2, rating: 9, timestamp: '{time}')

            new {self.table_dst}(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
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
            new {self.table_src}(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
            new {self.table_src}(userid: 2, movieid: 2, rating: 9, timestamp: '{time}')
            new {self.table_src}(userid: 3, movieid: 3, rating: 9, timestamp: '{time}')
            new {self.table_src}(userid: 4, movieid: 4, rating: 9, timestamp: '{time}')
            new {self.table_src}(userid: 5, movieid: 5, rating: 9, timestamp: '{time}')

            new {self.table_dst}(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
            new {self.table_dst}(userid: 2, movieid: 2, rating: 9, timestamp: '{time}')
            new {self.table_dst}(userid: 3, movieid: 3, rating: 9, timestamp: '{time}')
            new {self.table_dst}(userid: 4, movieid: 4, rating: 9, timestamp: '{time}')
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
            new {self.table_src}(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
            new {self.table_dst}(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
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
            new {self.table_src}(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
            new {self.table_src}(userid: 2, movieid: 2, rating: 9, timestamp: '{time2}')
            new {self.table_src}(userid: 3, movieid: 3, rating: 9, timestamp: '{time}')
            new {self.table_src}(userid: 4, movieid: 4, rating: 9, timestamp: '{time2}')
            new {self.table_src}(userid: 5, movieid: 5, rating: 9, timestamp: '{time}')

            new {self.table_dst}(userid: 1, movieid: 1, rating: 9, timestamp: '{time}')
            new {self.table_dst}(userid: 2, movieid: 2, rating: 9, timestamp: '{time}')
            new {self.table_dst}(userid: 3, movieid: 3, rating: 9, timestamp: '{time}')
            new {self.table_dst}(userid: 4, movieid: 4, rating: 9, timestamp: '{time}')
            new {self.table_dst}(userid: 5, movieid: 5, rating: 9, timestamp: '{time}')
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
            f"DROP TABLE IF EXISTS {self.table_src}",
            f"DROP TABLE IF EXISTS {self.table_dst}",
            f"CREATE TABLE {self.table_src}(id varchar(100), comment varchar(1000))",
            "COMMIT",
        ]
        for i in range(100):
            queries.append(f"INSERT INTO {self.table_src} VALUES ('{uuid.uuid1(i)}', '{i}')")

        queries += [
            "COMMIT",
            f"CREATE TABLE {self.table_dst} AS SELECT * FROM {self.table_src}",
            "COMMIT",
        ]

        self.new_uuid = uuid.uuid1(32132131)
        queries.append(f"INSERT INTO {self.table_src} VALUES ('{self.new_uuid}', 'This one is different')")

        # TODO test unexpected values?

        for query in queries:
            self.connection.query(query, None)

        self.a = TableSegment(self.connection, (self.table_src,), "id", "comment")
        self.b = TableSegment(self.connection, (self.table_dst,), "id", "comment")

    def test_string_keys(self):
        differ = TableDiffer()
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.new_uuid), "This one is different"))])

        self.connection.query(
            f"INSERT INTO {self.table_src} VALUES ('unexpected', '<-- this bad value should not break us')", None
        )

        self.assertRaises(ValueError, differ.diff_tables, self.a, self.b)


class TestTableSegment(TestWithConnection):
    def setUp(self) -> None:
        super().setUp()
        self.table = TableSegment(self.connection, (self.table_src,), "id", "timestamp")
        self.table2 = TableSegment(self.connection, (self.table_dst,), "id", "timestamp")

    def test_table_segment(self):
        early = datetime.datetime(2021, 1, 1, 0, 0)
        late = datetime.datetime(2022, 1, 1, 0, 0)
        self.assertRaises(ValueError, self.table.replace, min_update=late, max_update=early)

        self.assertRaises(ValueError, self.table.replace, min_key=10, max_key=0)


class TestTableUUID(TestWithConnection):
    def setUp(self):
        super().setUp()

        queries = [
            f"DROP TABLE IF EXISTS {self.table_src}",
            f"DROP TABLE IF EXISTS {self.table_dst}",
            f"CREATE TABLE {self.table_src}(id varchar(100), comment varchar(1000))",
        ]
        for i in range(10):
            uuid_value = uuid.uuid1(i)
            queries.append(f"INSERT INTO {self.table_src} VALUES ('{uuid_value}', '{uuid_value}')")

        self.null_uuid = uuid.uuid1(32132131)
        queries += [
            f"CREATE TABLE {self.table_dst} AS SELECT * FROM {self.table_src}",

            f"INSERT INTO {self.table_src} VALUES ('{self.null_uuid}', NULL)",

            "COMMIT"
        ]

        for query in queries:
            self.connection.query(query, None)

        self.a = TableSegment(self.connection, (self.table_src,), "id", "comment")
        self.b = TableSegment(self.connection, (self.table_dst,), "id", "comment")

    def test_uuid_column_with_nulls(self):
        differ = TableDiffer()
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.null_uuid), None))])


class TestTableNullRowChecksum(TestWithConnection):
    def setUp(self):
        super().setUp()

        self.null_uuid = uuid.uuid1(1)
        queries = [
            f"DROP TABLE IF EXISTS {self.table_src}",
            f"DROP TABLE IF EXISTS {self.table_dst}",
            f"CREATE TABLE {self.table_src}(id varchar(100), comment varchar(1000))",

            f"INSERT INTO {self.table_src} VALUES ('{uuid.uuid1(1)}', '1')",

            f"CREATE TABLE {self.table_dst} AS SELECT * FROM {self.table_src}",

            # Add a row where a column has NULL value
            f"INSERT INTO {self.table_src} VALUES ('{self.null_uuid}', NULL)",

            "COMMIT"
        ]

        for query in queries:
            self.connection.query(query, None)

        self.a = TableSegment(self.connection, (self.table_src,), "id", "comment")
        self.b = TableSegment(self.connection, (self.table_dst,), "id", "comment")

    def test_uuid_columns_with_nulls(self):
        """
        Here we test a case when in one segment one or more columns has only null values. For example,
        Table A:
        | id   |   value   |
        |------|-----------|
        | pk_1 | 'value_1' |
        | pk_2 |    NULL   |

        Table B:
        | id   |   value   |
        |------|-----------|
        | pk_1 | 'value_1' |

        We can choose some bisection factor and bisection threshold (2 and 3 for our example, respectively)
        that one segment will look like ('pk_2', NULL). Some databases, when we do a cast these values to string and
        try to concatenate, some databases return NULL when concatenating (for example, MySQL). As the result, all next
        operations like substring, sum etc return nulls that leads incorrect diff results: ('pk_2', null) should be in
        diff results, but it's not. This test helps to detect such cases.
        """

        differ = TableDiffer(bisection_factor=2, bisection_threshold=3)
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.null_uuid), None))])


class TestConcatMultipleColumnWithNulls(TestWithConnection):
    def setUp(self):
        super().setUp()

        queries = [
            f"DROP TABLE IF EXISTS {self.table_src}",
            f"DROP TABLE IF EXISTS {self.table_dst}",
            f"CREATE TABLE {self.table_src}(id varchar(100), c1 varchar(100), c2 varchar(100))",
            f"CREATE TABLE {self.table_dst}(id varchar(100), c1 varchar(100), c2 varchar(100))",
        ]

        self.diffs = []
        for i in range(0, 8):
            pk = uuid.uuid1(i)
            table_src_c1_val = str(i)
            table_dst_c1_val = str(i) + "-different"

            queries.append(f"INSERT INTO {self.table_src} VALUES ('{pk}', '{table_src_c1_val}', NULL)")
            queries.append(f"INSERT INTO {self.table_dst} VALUES ('{pk}', '{table_dst_c1_val}', NULL)")

            self.diffs.append(("-", (str(pk), table_src_c1_val, None)))
            self.diffs.append(("+", (str(pk), table_dst_c1_val, None)))

        queries.append("COMMIT")

        for query in queries:
            self.connection.query(query, None)

        self.a = TableSegment(self.connection, (self.table_src,), "id", extra_columns=("c1", "c2"))
        self.b = TableSegment(self.connection, (self.table_dst,), "id", extra_columns=("c1", "c2"))

    def test_tables_are_different(self):
        """
        Here we test a case when in one segment one or more columns has only null values. For example,
        Table A:
        | id   | c1 |  c2  |
        |------|----|------|
        | pk_1 | 1  | NULL |
        | pk_2 | 2  | NULL |
                ...
        | pk_n | n | NULL |

        Table B:
        | id   |   c1   |  c2  |
        |------|--------|------|
        | pk_1 | 1-diff | NULL |
        | pk_2 | 2-diff | NULL |
                  ...
        | pk_n | n-diff | NULL |

        To calculate a checksum, we need to concatenate string values by rows. If both tables have columns with NULL
        value, it may lead that concat(pk_i, i, NULL) == concat(pk_i, i-diff, NULL). This test handle such cases.
        """

        differ = TableDiffer(bisection_factor=2, bisection_threshold=4)
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, self.diffs)


class TestTableTableEmpty(TestWithConnection):
    def setUp(self):
        super().setUp()

        self.null_uuid = uuid.uuid1(1)
        queries = [
            f"DROP TABLE IF EXISTS {self.table_src}",
            f"DROP TABLE IF EXISTS {self.table_dst}",
            f"CREATE TABLE {self.table_src}(id varchar(100), comment varchar(1000))",
            f"CREATE TABLE {self.table_dst}(id varchar(100), comment varchar(1000))",
        ]

        self.diffs = [(uuid.uuid1(i), i) for i in range(100)]
        for pk, value in self.diffs:
            queries.append(f"INSERT INTO {self.table_src} VALUES ('{pk}', '{value}')")

        queries.append("COMMIT")

        for query in queries:
            self.connection.query(query, None)

        self.a = TableSegment(self.connection, (self.table_src,), "id", "comment")
        self.b = TableSegment(self.connection, (self.table_dst,), "id", "comment")

    def test_right_table_empty(self):
        differ = TableDiffer()
        self.assertRaises(ValueError, differ.diff_tables, self.a, self.b)

    def test_left_table_empty(self):
        queries = [
            f"INSERT INTO {self.table_dst} SELECT id, comment FROM {self.table_src}",
            f"TRUNCATE {self.table_src}",
            "COMMIT"
        ]
        for query in queries:
            self.connection.query(query, None)

        differ = TableDiffer()
        self.assertRaises(ValueError, differ.diff_tables, self.a, self.b)
