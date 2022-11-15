from datetime import datetime
from typing import Callable
import uuid
import unittest

import arrow  # comes with preql

from data_diff.queries import table, this, commit

from data_diff.hashdiff_tables import HashDiffer
from data_diff.table_segment import TableSegment, split_space
from data_diff import databases as db
from data_diff.utils import ArithAlphanumeric, numberToAlphanum

from .common import str_to_checksum, test_each_database_in_list, TestPerDatabase


TEST_DATABASES = {
    db.MySQL,
    db.PostgreSQL,
    db.Oracle,
    db.Redshift,
    db.Snowflake,
    db.BigQuery,
    db.Presto,
    db.Trino,
    db.Vertica,
}

test_each_database: Callable = test_each_database_in_list(TEST_DATABASES)


def _table_segment(database, table_path, key_columns, *args, **kw):
    if isinstance(key_columns, str):
        key_columns = (key_columns,)
    return TableSegment(database, table_path, key_columns, *args, **kw)


class TestUtils(unittest.TestCase):
    def test_split_space(self):
        for i in range(0, 10):
            for j in range(1, 16328, 17):
                for n in range(1, 32):
                    r = split_space(i, j + i + n, n)
                    assert len(r) == n, f"split_space({i}, {j+n}, {n}) = {(r)}"


@test_each_database
class TestDates(TestPerDatabase):
    def setUp(self):
        super().setUp()

        src_table = table(self.table_src_path, schema={"id": int, "datetime": datetime, "text_comment": str})
        self.connection.query(src_table.create())
        self.now = now = arrow.get()

        rows = [
            (now.shift(days=-50), "50 days ago"),
            (now.shift(hours=-3), "3 hours ago"),
            (now.shift(minutes=-10), "10 mins ago"),
            (now.shift(seconds=-1), "1 second ago"),
            (now, "now"),
        ]

        self.connection.query(
            [
                src_table.insert_rows((i, ts.datetime, s) for i, (ts, s) in enumerate(rows)),
                table(self.table_dst_path).create(src_table),
                commit,
                src_table.insert_row(len(rows), self.now.shift(seconds=-3).datetime, "3 seconds ago"),
                commit,
            ]
        )

    def test_init(self):
        a = _table_segment(
            self.connection, self.table_src_path, "id", "datetime", max_update=self.now.datetime, case_sensitive=False
        )
        self.assertRaises(
            ValueError, _table_segment, self.connection, self.table_src_path, "id", max_update=self.now.datetime
        )

    def test_basic(self):
        differ = HashDiffer(bisection_factor=10, bisection_threshold=100)
        a = _table_segment(self.connection, self.table_src_path, "id", "datetime", case_sensitive=False)
        b = _table_segment(self.connection, self.table_dst_path, "id", "datetime", case_sensitive=False)
        assert a.count() == 6
        assert b.count() == 5

        assert not list(differ.diff_tables(a, a))
        self.assertEqual(len(list(differ.diff_tables(a, b))), 1)

    def test_offset(self):
        differ = HashDiffer(bisection_factor=2, bisection_threshold=10)
        sec1 = self.now.shift(seconds=-2).datetime
        a = _table_segment(
            self.connection, self.table_src_path, "id", "datetime", max_update=sec1, case_sensitive=False
        )
        b = _table_segment(
            self.connection, self.table_dst_path, "id", "datetime", max_update=sec1, case_sensitive=False
        )
        assert a.count() == 4, a.count()
        assert b.count() == 3

        assert not list(differ.diff_tables(a, a))
        self.assertEqual(len(list(differ.diff_tables(a, b))), 1)

        a = _table_segment(
            self.connection, self.table_src_path, "id", "datetime", min_update=sec1, case_sensitive=False
        )
        b = _table_segment(
            self.connection, self.table_dst_path, "id", "datetime", min_update=sec1, case_sensitive=False
        )
        assert a.count() == 2
        assert b.count() == 2
        assert not list(differ.diff_tables(a, b))

        day1 = self.now.shift(days=-1).datetime

        a = _table_segment(
            self.connection,
            self.table_src_path,
            "id",
            "datetime",
            min_update=day1,
            max_update=sec1,
            case_sensitive=False,
        )
        b = _table_segment(
            self.connection,
            self.table_dst_path,
            "id",
            "datetime",
            min_update=day1,
            max_update=sec1,
            case_sensitive=False,
        )
        assert a.count() == 3
        assert b.count() == 2
        assert not list(differ.diff_tables(a, a))
        self.assertEqual(len(list(differ.diff_tables(a, b))), 1)


@test_each_database
class TestDiffTables(TestPerDatabase):
    def setUp(self):
        super().setUp()

        self.src_table = table(
            self.table_src_path,
            schema={"id": int, "userid": int, "movieid": int, "rating": float, "timestamp": datetime},
        )
        self.dst_table = table(
            self.table_dst_path,
            schema={"id": int, "userid": int, "movieid": int, "rating": float, "timestamp": datetime},
        )

        self.connection.query([self.src_table.create(), self.dst_table.create(), commit])

        self.table = _table_segment(self.connection, self.table_src_path, "id", "timestamp", case_sensitive=False)
        self.table2 = _table_segment(self.connection, self.table_dst_path, "id", "timestamp", case_sensitive=False)

        self.differ = HashDiffer(bisection_factor=3, bisection_threshold=4)

    def test_properties_on_empty_table(self):
        table = self.table.with_schema()
        self.assertEqual(0, table.count())
        self.assertEqual(None, table.count_and_checksum()[1])

    def test_get_values(self):
        time = "2022-01-01 00:00:00.000000"
        time_obj = datetime.fromisoformat(time)

        cols = "id userid movieid rating timestamp".split()
        id_ = self.connection.query(
            [self.src_table.insert_row(1, 1, 1, 9, time_obj, columns=cols), commit, self.src_table.select(this.id)], int
        )

        table = self.table.with_schema()

        self.assertEqual(1, table.count())
        concatted = str(id_) + "|" + time
        self.assertEqual(str_to_checksum(concatted), table.count_and_checksum()[1])

    def test_diff_small_tables(self):
        time = "2022-01-01 00:00:00"
        time_obj = datetime.fromisoformat(time)

        cols = "id userid movieid rating timestamp".split()
        self.connection.query(
            [
                self.src_table.insert_rows([[1, 1, 1, 9, time_obj], [2, 2, 2, 9, time_obj]], columns=cols),
                self.dst_table.insert_rows([[1, 1, 1, 9, time_obj]], columns=cols),
                commit,
            ]
        )

        diff = list(self.differ.diff_tables(self.table, self.table2))
        expected = [("-", ("2", time + ".000000"))]
        self.assertEqual(expected, diff)
        self.assertEqual(2, self.differ.stats["table1_count"])
        self.assertEqual(1, self.differ.stats["table2_count"])

    def test_non_threaded(self):
        differ = HashDiffer(bisection_factor=3, bisection_threshold=4, threaded=False)

        time = "2022-01-01 00:00:00"
        time_obj = datetime.fromisoformat(time)
        cols = "id userid movieid rating timestamp".split()
        self.connection.query(
            [
                self.src_table.insert_row(1, 1, 1, 9, time_obj, columns=cols),
                self.dst_table.insert_row(1, 1, 1, 9, time_obj, columns=cols),
                commit,
            ]
        )

        diff = list(differ.diff_tables(self.table, self.table2))
        self.assertEqual(diff, [])

    def test_diff_table_above_bisection_threshold(self):
        time = "2022-01-01 00:00:00"
        time_obj = datetime.fromisoformat(time)

        cols = "id userid movieid rating timestamp".split()

        self.connection.query(
            [
                self.src_table.insert_rows(
                    [
                        [1, 1, 1, 9, time_obj],
                        [2, 2, 2, 9, time_obj],
                        [3, 3, 3, 9, time_obj],
                        [4, 4, 4, 9, time_obj],
                        [5, 5, 5, 9, time_obj],
                    ],
                    columns=cols,
                ),
                self.dst_table.insert_rows(
                    [[1, 1, 1, 9, time_obj], [2, 2, 2, 9, time_obj], [3, 3, 3, 9, time_obj], [4, 4, 4, 9, time_obj],],
                    columns=cols,
                ),
                commit,
            ]
        )

        diff = list(self.differ.diff_tables(self.table, self.table2))
        expected = [("-", ("5", time + ".000000"))]
        self.assertEqual(expected, diff)
        self.assertEqual(5, self.differ.stats["table1_count"])
        self.assertEqual(4, self.differ.stats["table2_count"])

    def test_return_empty_array_when_same(self):
        time = "2022-01-01 00:00:00"
        time_obj = datetime.fromisoformat(time)

        cols = "id userid movieid rating timestamp".split()

        self.connection.query(
            [
                self.src_table.insert_row(1, 1, 1, 9, time_obj, columns=cols),
                self.dst_table.insert_row(1, 1, 1, 9, time_obj, columns=cols),
                commit,
            ]
        )

        diff = list(self.differ.diff_tables(self.table, self.table2))
        self.assertEqual([], diff)

    def test_diff_sorted_by_key(self):
        time = "2022-01-01 00:00:00"
        time2 = "2021-01-01 00:00:00"

        time_obj = datetime.fromisoformat(time)
        time_obj2 = datetime.fromisoformat(time2)

        cols = "id userid movieid rating timestamp".split()

        self.connection.query(
            [
                self.src_table.insert_rows(
                    [
                        [1, 1, 1, 9, time_obj],
                        [2, 2, 2, 9, time_obj2],
                        [3, 3, 3, 9, time_obj],
                        [4, 4, 4, 9, time_obj2],
                        [5, 5, 5, 9, time_obj],
                    ],
                    columns=cols,
                ),
                self.dst_table.insert_rows(
                    [
                        [1, 1, 1, 9, time_obj],
                        [2, 2, 2, 9, time_obj],
                        [3, 3, 3, 9, time_obj],
                        [4, 4, 4, 9, time_obj],
                        [5, 5, 5, 9, time_obj],
                    ],
                    columns=cols,
                ),
                commit,
            ]
        )

        differ = HashDiffer()
        diff = list(differ.diff_tables(self.table, self.table2))
        expected = [
            ("-", ("2", time2 + ".000000")),
            ("+", ("2", time + ".000000")),
            ("-", ("4", time2 + ".000000")),
            ("+", ("4", time + ".000000")),
        ]
        self.assertEqual(expected, diff)


@test_each_database
class TestDiffTables2(TestPerDatabase):
    def test_diff_column_names(self):

        self.src_table = table(self.table_src_path, schema={"id": int, "rating": float, "timestamp": datetime})
        self.dst_table = table(self.table_dst_path, schema={"id2": int, "rating2": float, "timestamp2": datetime})

        self.connection.query([self.src_table.create(), self.dst_table.create(), commit])

        time = "2022-01-01 00:00:00"
        time2 = "2021-01-01 00:00:00"

        time_obj = datetime.fromisoformat(time)
        time_obj2 = datetime.fromisoformat(time2)

        self.connection.query(
            [
                self.src_table.insert_rows(
                    [[1, 9, time_obj], [2, 9, time_obj2], [3, 9, time_obj], [4, 9, time_obj2], [5, 9, time_obj],],
                    columns=["id", "rating", "timestamp"],
                ),
                self.dst_table.insert_rows(
                    [[1, 9, time_obj], [2, 9, time_obj2], [3, 9, time_obj], [4, 9, time_obj2], [5, 9, time_obj],],
                    columns=["id2", "rating2", "timestamp2"],
                ),
            ]
        )

        table1 = _table_segment(self.connection, self.table_src_path, "id", "timestamp", case_sensitive=False)
        table2 = _table_segment(self.connection, self.table_dst_path, "id2", "timestamp2", case_sensitive=False)

        differ = HashDiffer()
        diff = list(differ.diff_tables(table1, table2))
        assert diff == []


@test_each_database
class TestUUIDs(TestPerDatabase):
    def setUp(self):
        super().setUp()

        self.src_table = src_table = table(self.table_src_path, schema={"id": str, "text_comment": str})

        self.new_uuid = uuid.uuid1(32132131)

        self.connection.query(
            [
                src_table.create(),
                src_table.insert_rows((uuid.uuid1(i), str(i)) for i in range(100)),
                table(self.table_dst_path).create(src_table),
                src_table.insert_row(self.new_uuid, "This one is different"),
                commit,
            ]
        )

        self.a = _table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = _table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

    def test_string_keys(self):
        differ = HashDiffer()
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.new_uuid), "This one is different"))])

        self.connection.query(self.src_table.insert_row("unexpected", "<-- this bad value should not break us"))

        self.assertRaises(ValueError, list, differ.diff_tables(self.a, self.b))

    def test_where_sampling(self):
        a = self.a.replace(where="1=1")

        differ = HashDiffer()
        diff = list(differ.diff_tables(a, self.b))
        self.assertEqual(diff, [("-", (str(self.new_uuid), "This one is different"))])

        a_empty = self.a.replace(where="1=0")
        self.assertRaises(ValueError, list, differ.diff_tables(a_empty, self.b))


@test_each_database_in_list(TEST_DATABASES - {db.MySQL})
class TestAlphanumericKeys(TestPerDatabase):
    def setUp(self):
        super().setUp()

        self.src_table = src_table = table(self.table_src_path, schema={"id": str, "text_comment": str})
        self.new_alphanum = "aBcDeFgHiz"

        values = []
        for i in range(0, 10000, 1000):
            a = ArithAlphanumeric(numberToAlphanum(i), max_len=10)
            if not a and isinstance(self.connection, db.Oracle):
                # Skip empty string, because Oracle treats it as NULL ..
                continue

            values.append((str(a), str(i)))

        queries = [
            src_table.create(),
            src_table.insert_rows(values),
            table(self.table_dst_path).create(src_table),
            src_table.insert_row(self.new_alphanum, "This one is different"),
            commit,
        ]

        for query in queries:
            self.connection.query(query, None)

        self.a = _table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = _table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

    def test_alphanum_keys(self):

        differ = HashDiffer(bisection_factor=2, bisection_threshold=3)
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.new_alphanum), "This one is different"))])

        self.connection.query([self.src_table.insert_row("@@@", "<-- this bad value should not break us"), commit])

        self.a = _table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = _table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

        self.assertRaises(NotImplementedError, list, differ.diff_tables(self.a, self.b))


@test_each_database_in_list(TEST_DATABASES - {db.MySQL})
class TestVaryingAlphanumericKeys(TestPerDatabase):
    def setUp(self):
        super().setUp()

        self.src_table = src_table = table(self.table_src_path, schema={"id": str, "text_comment": str})

        values = []
        for i in range(0, 10000, 1000):
            a = ArithAlphanumeric(numberToAlphanum(i * i))
            if not a and isinstance(self.connection, db.Oracle):
                # Skip empty string, because Oracle treats it as NULL ..
                continue

            values.append((str(a), str(i)))

        self.new_alphanum = "aBcDeFgHiJ"

        queries = [
            src_table.create(),
            src_table.insert_rows(values),
            table(self.table_dst_path).create(src_table),
            src_table.insert_row(self.new_alphanum, "This one is different"),
            commit,
        ]

        self.connection.query(queries)

        self.a = _table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = _table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

    def test_varying_alphanum_keys(self):
        # Test the class itself
        values = ["---", "0123", "Z9", "ZZ", "_", "a", "a-", "a123", "a_"]
        alphanums = [ArithAlphanumeric(v) for v in values]
        alphanums.sort()
        self.assertEqual(values, [str(i) for i in alphanums])

        for a in alphanums:
            assert a - a == 0

        differ = HashDiffer()
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.new_alphanum), "This one is different"))])

        self.connection.query(
            self.src_table.insert_row("@@@", "<-- this bad value should not break us"), commit,
        )

        self.a = _table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = _table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

        self.assertRaises(NotImplementedError, list, differ.diff_tables(self.a, self.b))


@test_each_database
class TestTableSegment(TestPerDatabase):
    def setUp(self) -> None:
        super().setUp()
        self.table = _table_segment(self.connection, self.table_src_path, "id", "timestamp", case_sensitive=False)
        self.table2 = _table_segment(self.connection, self.table_dst_path, "id", "timestamp", case_sensitive=False)

    def test_table_segment(self):
        early = datetime(2021, 1, 1, 0, 0)
        late = datetime(2022, 1, 1, 0, 0)
        self.assertRaises(ValueError, self.table.replace, min_update=late, max_update=early)

        self.assertRaises(ValueError, self.table.replace, min_key=10, max_key=0)

    def test_case_awareness(self):
        src_table = table(self.table_src_path, schema={"id": int, "userid": int, "timestamp": datetime})

        cols = "id userid timestamp".split()
        time = "2022-01-01 00:00:00.000000"
        time_obj = datetime.fromisoformat(time)

        self.connection.query(
            [src_table.create(), src_table.insert_rows([[1, 9, time_obj], [2, 2, time_obj]], columns=cols), commit]
        )

        res = tuple(self.table.replace(key_columns=("Id",), case_sensitive=False).with_schema().query_key_range())
        assert res == ("1", "2")

        self.assertRaises(
            KeyError, self.table.replace(key_columns=("Id",), case_sensitive=True).with_schema().query_key_range
        )


@test_each_database
class TestTableUUID(TestPerDatabase):
    def setUp(self):
        super().setUp()

        src_table = table(self.table_src_path, schema={"id": str, "text_comment": str})

        values = []
        for i in range(10):
            uuid_value = uuid.uuid1(i)
            values.append((uuid_value, uuid_value))

        self.null_uuid = uuid.uuid1(32132131)

        self.connection.query(
            [
                src_table.create(),
                src_table.insert_rows(values),
                table(self.table_dst_path).create(src_table),
                src_table.insert_row(self.null_uuid, None),
                commit,
            ]
        )

        self.a = _table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = _table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

    def test_uuid_column_with_nulls(self):
        differ = HashDiffer()
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.null_uuid), None))])


@test_each_database
class TestTableNullRowChecksum(TestPerDatabase):
    def setUp(self):
        super().setUp()

        src_table = table(self.table_src_path, schema={"id": str, "text_comment": str})

        self.null_uuid = uuid.uuid1(1)
        self.connection.query(
            [
                src_table.create(),
                src_table.insert_row(uuid.uuid1(1), "1"),
                table(self.table_dst_path).create(src_table),
                src_table.insert_row(self.null_uuid, None),  # Add a row where a column has NULL value
                commit,
            ]
        )

        self.a = _table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = _table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

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

        differ = HashDiffer(bisection_factor=2, bisection_threshold=3)
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.null_uuid), None))])


@test_each_database
class TestConcatMultipleColumnWithNulls(TestPerDatabase):
    def setUp(self):
        super().setUp()

        src_table = table(self.table_src_path, schema={"id": str, "c1": str, "c2": str})
        dst_table = table(self.table_dst_path, schema={"id": str, "c1": str, "c2": str})

        src_values = []
        dst_values = []

        self.diffs = []
        for i in range(0, 8):
            pk = uuid.uuid1(i)
            src_row = (str(pk), str(i), None)
            dst_row = (str(pk), str(i) + "-different", None)

            src_values.append(src_row)
            dst_values.append(dst_row)

            self.diffs.append(("-", src_row))
            self.diffs.append(("+", dst_row))

        self.connection.query(
            [
                src_table.create(),
                dst_table.create(),
                src_table.insert_rows(src_values),
                dst_table.insert_rows(dst_values),
                commit,
            ]
        )

        self.a = _table_segment(
            self.connection, self.table_src_path, "id", extra_columns=("c1", "c2"), case_sensitive=False
        )
        self.b = _table_segment(
            self.connection, self.table_dst_path, "id", extra_columns=("c1", "c2"), case_sensitive=False
        )

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

        differ = HashDiffer(bisection_factor=2, bisection_threshold=4)
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, self.diffs)


@test_each_database
class TestTableTableEmpty(TestPerDatabase):
    def setUp(self):
        super().setUp()

        self.src_table = table(self.table_src_path, schema={"id": str, "text_comment": str})
        self.dst_table = table(self.table_dst_path, schema={"id": str, "text_comment": str})

        self.null_uuid = uuid.uuid1(1)

        self.diffs = [(uuid.uuid1(i), str(i)) for i in range(100)]

        self.a = _table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = _table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

    def test_right_table_empty(self):
        self.connection.query(
            [self.src_table.create(), self.dst_table.create(), self.src_table.insert_rows(self.diffs), commit]
        )

        differ = HashDiffer()
        self.assertRaises(ValueError, list, differ.diff_tables(self.a, self.b))

    def test_left_table_empty(self):
        self.connection.query(
            [self.src_table.create(), self.dst_table.create(), self.dst_table.insert_rows(self.diffs), commit]
        )

        differ = HashDiffer()
        self.assertRaises(ValueError, list, differ.diff_tables(self.a, self.b))
