from datetime import datetime, timedelta
from typing import Callable
import uuid
import unittest

import attrs

from data_diff.queries.api import table, this, commit, code
from data_diff.utils import ArithAlphanumeric, numberToAlphanum

from data_diff.hashdiff_tables import HashDiffer
from data_diff.joindiff_tables import JoinDiffer
from data_diff.table_segment import TableSegment, split_space, Vector
from data_diff import databases as db

from tests.common import str_to_checksum, test_each_database_in_list, DiffTestCase, table_segment


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


class TestUtils(unittest.TestCase):
    def test_split_space(self):
        for i in range(0, 10):
            for j in range(1, 16328, 17):
                for n in range(1, 32):
                    r = split_space(i, j + i + n, n)
                    assert len(r) == n, f"split_space({i}, {j+n}, {n}) = {(r)}"


@test_each_database
class TestDates(DiffTestCase):
    src_schema = {"id": int, "datetime": datetime, "text_comment": str}

    def setUp(self):
        super().setUp()

        src_table = self.src_table
        self.now = now = datetime.now()

        rows = [
            (now - timedelta(days=50), "50 days ago"),
            (now - timedelta(hours=3), "3 hours ago"),
            (now - timedelta(minutes=10), "10 mins ago"),
            (now - timedelta(seconds=1), "1 second ago"),
            (now, "now"),
        ]

        self.connection.query(
            [
                src_table.insert_rows((i, ts, s) for i, (ts, s) in enumerate(rows)),
                table(self.table_dst_path).create(src_table),
                commit,
                src_table.insert_row(len(rows), self.now - timedelta(seconds=5), "5 seconds ago"),
                commit,
            ]
        )

    def test_init(self):
        a = table_segment(
            self.connection, self.table_src_path, "id", "datetime", max_update=self.now, case_sensitive=False
        )
        self.assertRaises(ValueError, table_segment, self.connection, self.table_src_path, "id", max_update=self.now)

    def test_basic(self):
        differ = HashDiffer(bisection_factor=10, bisection_threshold=100)
        a = table_segment(self.connection, self.table_src_path, "id", "datetime", case_sensitive=False)
        b = table_segment(self.connection, self.table_dst_path, "id", "datetime", case_sensitive=False)
        assert a.count() == 6
        assert b.count() == 5

        assert not list(differ.diff_tables(a, a))
        self.assertEqual(len(list(differ.diff_tables(a, b))), 1)

    def test_offset(self):
        differ = HashDiffer(bisection_factor=2, bisection_threshold=10)
        sec1 = self.now - timedelta(seconds=3)
        a = table_segment(self.connection, self.table_src_path, "id", "datetime", max_update=sec1, case_sensitive=False)
        b = table_segment(self.connection, self.table_dst_path, "id", "datetime", max_update=sec1, case_sensitive=False)
        assert a.count() == 4, a.count()
        assert b.count() == 3

        assert not list(differ.diff_tables(a, a))
        self.assertEqual(len(list(differ.diff_tables(a, b))), 1)

        a = table_segment(self.connection, self.table_src_path, "id", "datetime", min_update=sec1, case_sensitive=False)
        b = table_segment(self.connection, self.table_dst_path, "id", "datetime", min_update=sec1, case_sensitive=False)
        assert a.count() == 2
        assert b.count() == 2
        assert not list(differ.diff_tables(a, b))

        day1 = self.now - timedelta(days=1)

        a = table_segment(
            self.connection,
            self.table_src_path,
            "id",
            "datetime",
            min_update=day1,
            max_update=sec1,
            case_sensitive=False,
        )
        b = table_segment(
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
class TestDiffTables(DiffTestCase):
    src_schema = {"id": int, "userid": int, "movieid": int, "rating": float, "timestamp": datetime}
    dst_schema = {"id": int, "userid": int, "movieid": int, "rating": float, "timestamp": datetime}

    def setUp(self):
        super().setUp()

        self.table = table_segment(self.connection, self.table_src_path, "id", "timestamp", case_sensitive=False)
        self.table2 = table_segment(self.connection, self.table_dst_path, "id", "timestamp", case_sensitive=False)

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

        diff_res = self.differ.diff_tables(self.table, self.table2)
        info = diff_res.info_tree.info
        diff = list(diff_res)

        expected = [("-", ("2", time + ".000000"))]
        self.assertEqual(expected, diff)
        self.assertEqual(2, info.rowcounts[1])
        self.assertEqual(1, info.rowcounts[2])

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
                    [
                        [1, 1, 1, 9, time_obj],
                        [2, 2, 2, 9, time_obj],
                        [3, 3, 3, 9, time_obj],
                        [4, 4, 4, 9, time_obj],
                    ],
                    columns=cols,
                ),
                commit,
            ]
        )

        diff_res = self.differ.diff_tables(self.table, self.table2)
        info = diff_res.info_tree.info
        diff = list(diff_res)

        expected = [("-", ("5", time + ".000000"))]
        self.assertEqual(expected, diff)
        self.assertEqual(5, info.rowcounts[1])
        self.assertEqual(4, info.rowcounts[2])

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

        differ = HashDiffer(bisection_factor=2)
        diff = set(differ.diff_tables(self.table, self.table2))
        expected = {
            ("-", ("2", time2 + ".000000")),
            ("+", ("2", time + ".000000")),
            ("-", ("4", time2 + ".000000")),
            ("+", ("4", time + ".000000")),
        }
        self.assertEqual(expected, diff)


@test_each_database
class TestDiffTables2(DiffTestCase):
    src_schema = {"id": int, "rating": float, "timestamp": datetime}
    dst_schema = {"id2": int, "rating2": float, "timestamp2": datetime}

    def test_diff_column_names(self):
        time = "2022-01-01 00:00:00"
        time2 = "2021-01-01 00:00:00"

        time_obj = datetime.fromisoformat(time)
        time_obj2 = datetime.fromisoformat(time2)

        self.connection.query(
            [
                self.src_table.insert_rows(
                    [
                        [1, 9, time_obj],
                        [2, 9, time_obj2],
                        [3, 9, time_obj],
                        [4, 9, time_obj2],
                        [5, 9, time_obj],
                    ],
                    columns=["id", "rating", "timestamp"],
                ),
                self.dst_table.insert_rows(
                    [
                        [1, 9, time_obj],
                        [2, 9, time_obj2],
                        [3, 9, time_obj],
                        [4, 9, time_obj2],
                        [5, 9, time_obj],
                    ],
                    columns=["id2", "rating2", "timestamp2"],
                ),
            ]
        )

        table1 = table_segment(self.connection, self.table_src_path, "id", "timestamp", case_sensitive=False)
        table2 = table_segment(self.connection, self.table_dst_path, "id2", "timestamp2", case_sensitive=False)

        differ = HashDiffer(bisection_factor=2)
        diff = list(differ.diff_tables(table1, table2))
        assert diff == []


@test_each_database
class TestUUIDs(DiffTestCase):
    src_schema = {"id": str, "text_comment": str}

    def setUp(self):
        super().setUp()

        src_table = self.src_table

        self.new_uuid = uuid.uuid1(32132131)

        self.connection.query(
            [
                src_table.insert_rows((uuid.uuid1(i), str(i)) for i in range(100)),
                table(self.table_dst_path).create(src_table),
                src_table.insert_row(self.new_uuid, "This one is different"),
                commit,
            ]
        )

        self.a = table_segment(
            self.connection, self.table_src_path, "id", extra_columns=("text_comment",), case_sensitive=False
        ).with_schema()
        self.b = table_segment(
            self.connection, self.table_dst_path, "id", extra_columns=("text_comment",), case_sensitive=False
        ).with_schema()

    def test_string_keys(self):
        differ = HashDiffer(bisection_factor=2)
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.new_uuid), "This one is different"))])

        # At this point the tables should already have a schema, to ensure the column is detected as UUID and not alpanum
        self.connection.query(self.src_table.insert_row("unexpected", "<-- this bad value should not break us"))
        self.assertRaises(ValueError, list, differ.diff_tables(self.a, self.b))

    def test_where_sampling(self):
        a = attrs.evolve(self.a, where="1=1")

        differ = HashDiffer(bisection_factor=2)
        diff = list(differ.diff_tables(a, self.b))
        self.assertEqual(diff, [("-", (str(self.new_uuid), "This one is different"))])

        a_empty = attrs.evolve(self.a, where="1=0")
        self.assertRaises(ValueError, list, differ.diff_tables(a_empty, self.b))


@test_each_database_in_list(TEST_DATABASES - {db.MySQL})
class TestAlphanumericKeys(DiffTestCase):
    src_schema = {"id": str, "text_comment": str}

    def setUp(self):
        super().setUp()

        src_table = self.src_table
        self.new_alphanum = "aBcDeFgHiz"

        values = []
        for i in range(0, 10000, 1000):
            a = ArithAlphanumeric(numberToAlphanum(i), max_len=10)
            if not a and isinstance(self.connection, db.Oracle):
                # Skip empty string, because Oracle treats it as NULL ..
                continue

            values.append((str(a), str(i)))

        queries = [
            src_table.insert_rows(values),
            table(self.table_dst_path).create(src_table),
            src_table.insert_row(self.new_alphanum, "This one is different"),
            commit,
        ]

        for query in queries:
            self.connection.query(query, None)

        self.a = table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

    def test_alphanum_keys(self):
        differ = HashDiffer(bisection_factor=2, bisection_threshold=3)
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.new_alphanum), "This one is different"))])

        self.connection.query([self.src_table.insert_row("@@@", "<-- this bad value should not break us"), commit])

        self.a = table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

        self.assertRaises(NotImplementedError, list, differ.diff_tables(self.a, self.b))


@test_each_database_in_list(TEST_DATABASES - {db.MySQL})
class TestVaryingAlphanumericKeys(DiffTestCase):
    src_schema = {"id": str, "text_comment": str}

    def setUp(self):
        super().setUp()

        src_table = self.src_table

        values = []
        for i in range(0, 10000, 1000):
            a = ArithAlphanumeric(numberToAlphanum(i * i))
            if not a and isinstance(self.connection, db.Oracle):
                # Skip empty string, because Oracle treats it as NULL ..
                continue

            values.append((str(a), str(i)))

        self.new_alphanum = "aBcDeFgHiJ"

        queries = [
            src_table.insert_rows(values),
            table(self.table_dst_path).create(src_table),
            src_table.insert_row(self.new_alphanum, "This one is different"),
            commit,
        ]

        self.connection.query(queries)

        self.a = table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

    def test_varying_alphanum_keys(self):
        # Test the class itself
        values = ["---", "0123", "Z9", "ZZ", "_", "a", "a-", "a123", "a_"]
        alphanums = [ArithAlphanumeric(v) for v in values]
        alphanums.sort()
        self.assertEqual(values, [str(i) for i in alphanums])

        for a in alphanums:
            assert a - a == 0

        differ = HashDiffer(bisection_factor=2)
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.new_alphanum), "This one is different"))])

        self.connection.query(
            self.src_table.insert_row("@@@", "<-- this bad value should not break us"),
            commit,
        )

        self.a = table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

        self.assertRaises(NotImplementedError, list, differ.diff_tables(self.a, self.b))


@test_each_database
class TestTableSegment(DiffTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.table = table_segment(self.connection, self.table_src_path, "id", "timestamp", case_sensitive=False)
        self.table2 = table_segment(self.connection, self.table_dst_path, "id", "timestamp", case_sensitive=False)

    def test_table_segment(self):
        early = datetime(2021, 1, 1, 0, 0)
        late = datetime(2022, 1, 1, 0, 0)
        self.assertRaises(ValueError, attrs.evolve, self.table, min_update=late, max_update=early)

        self.assertRaises(ValueError, attrs.evolve, self.table, min_key=Vector((10,)), max_key=Vector((0,)))

    def test_case_awareness(self):
        src_table = table(self.table_src_path, schema={"id": int, "userid": int, "timestamp": datetime})

        cols = "id userid timestamp".split()
        time = "2022-01-01 00:00:00.000000"
        time_obj = datetime.fromisoformat(time)

        self.connection.query(
            [src_table.create(), src_table.insert_rows([[1, 9, time_obj], [2, 2, time_obj]], columns=cols), commit]
        )

        res = tuple(attrs.evolve(self.table, key_columns=("Id",), case_sensitive=False).with_schema().query_key_range())
        self.assertEqual(res, (("1",), ("2",)))

        self.assertRaises(
            KeyError, attrs.evolve(self.table, key_columns=("Id",), case_sensitive=True).with_schema().query_key_range
        )


@test_each_database
class TestTableUUID(DiffTestCase):
    src_schema = {"id": str, "text_comment": str}

    def setUp(self):
        super().setUp()

        src_table = self.src_table

        values = []
        for i in range(10):
            uuid_value = uuid.uuid1(i)
            values.append((uuid_value, uuid_value))

        self.null_uuid = uuid.uuid1(32132131)

        self.connection.query(
            [
                src_table.insert_rows(values),
                table(self.table_dst_path).create(src_table),
                src_table.insert_row(self.null_uuid, None),
                commit,
            ]
        )

        self.a = table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

    def test_uuid_column_with_nulls(self):
        differ = HashDiffer(bisection_factor=2)
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.null_uuid), None))])


@test_each_database
class TestTableNullRowChecksum(DiffTestCase):
    src_schema = {"id": str, "text_comment": str}

    def setUp(self):
        super().setUp()

        src_table = self.src_table

        self.null_uuid = uuid.uuid1(1)
        self.connection.query(
            [
                src_table.insert_row(uuid.uuid1(1), "1"),
                table(self.table_dst_path).create(src_table),
                src_table.insert_row(self.null_uuid, None),  # Add a row where a column has NULL value
                commit,
            ]
        )

        self.a = table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

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
class TestConcatMultipleColumnWithNulls(DiffTestCase):
    src_schema = {"id": str, "c1": str, "c2": str}
    dst_schema = {"id": str, "c1": str, "c2": str}

    def setUp(self):
        super().setUp()

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
                self.src_table.insert_rows(src_values),
                self.dst_table.insert_rows(dst_values),
                commit,
            ]
        )

        self.a = table_segment(
            self.connection, self.table_src_path, "id", extra_columns=("c1", "c2"), case_sensitive=False
        )
        self.b = table_segment(
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
class TestTableTableEmpty(DiffTestCase):
    src_schema = {"id": str, "text_comment": str}
    dst_schema = {"id": str, "text_comment": str}

    def setUp(self):
        super().setUp()

        self.null_uuid = uuid.uuid1(1)

        diffs = [(uuid.uuid1(i), str(i)) for i in range(100)]
        self.connection.query([self.src_table.insert_rows(diffs), commit])

        self.a = table_segment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = table_segment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

        self.differ = HashDiffer(bisection_factor=2)

    def test_right_table_empty(self):
        # NotImplementedError: Cannot use a column of type Text(_notes=[]) as a key
        self.assertRaises(NotImplementedError, list, self.differ.diff_tables(self.a, self.b))

    def test_left_table_empty(self):
        # NotImplementedError: Cannot use a column of type Text(_notes=[]) as a key
        self.assertRaises(NotImplementedError, list, self.differ.diff_tables(self.a, self.b))


class TestInfoTree(DiffTestCase):
    db_cls = db.MySQL
    src_schema = dst_schema = dict(id=int)

    def test_info_tree_root(self):
        db = self.connection
        db.query(
            [
                self.src_table.insert_rows([i] for i in range(1000)),
                self.dst_table.insert_rows([i] for i in range(2000)),
            ]
        )

        ts1 = TableSegment(db, self.src_table.path, ("id",))
        ts2 = TableSegment(db, self.dst_table.path, ("id",))

        for differ in (HashDiffer(bisection_threshold=64), JoinDiffer(True)):
            diff_res = differ.diff_tables(ts1, ts2)
            diff = list(diff_res)
            info_tree = diff_res.info_tree
            assert info_tree.info.is_diff
            assert info_tree.info.diff_count == 1000
            self.assertEqual(info_tree.info.rowcounts, {1: 1000, 2: 2000})


class TestDuplicateTables(DiffTestCase):
    db_cls = db.MySQL

    src_schema = {"id": int, "data": str}
    dst_schema = {"id": int, "data": str}

    def setUp(self):
        """
        table 1:
            (12, 'ABCDE'),
            (12, 'ABCDE');
        table 2:
            (4,'ABCDEF'),
            (4,'ABCDE'),
            (4,'ABCDE'),
            (6,'ABCDE'),
            (6,'ABCDE'),
            (6,'ABCDE');
        """

        super().setUp()

        src_values = [(12, "ABCDE"), (12, "ABCDE")]
        dst_values = [(4, "ABCDEF"), (4, "ABCDE"), (4, "ABCDE"), (6, "ABCDE"), (6, "ABCDE"), (6, "ABCDE")]

        self.diffs = [("-", (str(r[0]), r[1])) for r in src_values] + [("+", (str(r[0]), r[1])) for r in dst_values]

        self.connection.query([self.src_table.insert_rows(src_values), self.dst_table.insert_rows(dst_values), commit])

        self.a = table_segment(
            self.connection, self.table_src_path, "id", extra_columns=("data",), case_sensitive=False
        )
        self.b = table_segment(
            self.connection, self.table_dst_path, "id", extra_columns=("data",), case_sensitive=False
        )

    def test_duplicates(self):
        """If there are duplicates in data, we want to return them as well"""

        differ = HashDiffer(bisection_factor=2, bisection_threshold=4)
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, self.diffs)


@test_each_database
class TestCompoundKeySimple1(DiffTestCase):
    src_schema = {"id": int, "id2": int}
    dst_schema = {"id": int, "id2": int}

    def test_simple1(self):
        N = 1000
        K = N + 1
        V1 = N + 1
        V2 = N * 1000 + 2

        diffs = [(i, i + N) for i in range(N)]
        self.connection.query(
            [
                self.src_table.insert_rows(diffs + [(K, V1)]),
                self.dst_table.insert_rows(diffs + [(K, V2)]),
                commit,
            ]
        )

        expected = {("-", (str(K), str(V1))), ("+", (str(K), str(V2)))}
        differ = HashDiffer()

        a = TableSegment(self.connection, self.src_table.path, ("id",), extra_columns=("id2",))
        b = TableSegment(self.connection, self.dst_table.path, ("id",), extra_columns=("id2",))
        diff = set(differ.diff_tables(a, b))
        self.assertEqual(diff, expected)

        aa = TableSegment(self.connection, self.src_table.path, ("id", "id2"))
        bb = TableSegment(self.connection, self.dst_table.path, ("id", "id2"))
        diff = set(differ.diff_tables(aa, bb))
        self.assertEqual(diff, expected)


@test_each_database
class TestCompoundKeySimple2(DiffTestCase):
    src_schema = {"id": int, "id2": int}
    dst_schema = {"id": int, "id2": int}

    def test_simple2(self):
        N = 1000
        K = N + 1
        V1 = N + 1
        V2 = N * 1000 + 2

        diffs = [(i + 1, i + N) for i in range(N)]  # pk=[1..1000], no dupes
        self.connection.query(
            [
                self.src_table.insert_rows(diffs + [(K, V1)]),  # exclusive pk=1001
                self.dst_table.insert_rows(diffs + [(0, V2)]),  # exclusive pk=0
                commit,
            ]
        )

        expected = {("-", (str(K), str(V1))), ("+", (str(0), str(V2)))}
        differ = HashDiffer()

        a = TableSegment(self.connection, self.src_table.path, ("id",), extra_columns=("id2",))
        b = TableSegment(self.connection, self.dst_table.path, ("id",), extra_columns=("id2",))
        diff = set(differ.diff_tables(a, b))
        self.assertEqual(diff, expected)

        aa = TableSegment(self.connection, self.src_table.path, ("id", "id2"))
        bb = TableSegment(self.connection, self.dst_table.path, ("id", "id2"))
        diff = set(differ.diff_tables(aa, bb))
        self.assertEqual(diff, expected)


@test_each_database
class TestCompoundKeySimple3(DiffTestCase):
    src_schema = {"id": int, "id2": int}
    dst_schema = {"id": int, "id2": int}

    def test_negative_keys(self):
        N = 1000
        K = -N + 1
        V1 = N + 1
        V2 = -N * 1000 + 2

        diffs = [(i, i + N) for i in range(N)]
        self.connection.query(
            [
                self.src_table.insert_rows(diffs + [(K, V1)]),
                self.dst_table.insert_rows(diffs + [(K, V2)]),
                commit,
            ]
        )

        expected = {("-", (str(K), str(V1))), ("+", (str(K), str(V2)))}
        differ = HashDiffer()

        a = TableSegment(self.connection, self.src_table.path, ("id",), extra_columns=("id2",))
        b = TableSegment(self.connection, self.dst_table.path, ("id",), extra_columns=("id2",))
        diff = set(differ.diff_tables(a, b))
        self.assertEqual(diff, expected)

        aa = TableSegment(self.connection, self.src_table.path, ("id", "id2"))
        bb = TableSegment(self.connection, self.dst_table.path, ("id", "id2"))
        diff = set(differ.diff_tables(aa, bb))
        self.assertEqual(diff, expected)


@test_each_database
class TestCompoundKeyAlphanum(DiffTestCase):
    src_schema = {"id": str, "id2": int, "comment": str}
    dst_schema = {"id": str, "id2": int, "comment": str}

    def setUp(self):
        super().setUp()

        rows = [(uuid.uuid1(i), i, str(i)) for i in range(100)]
        rows2 = list(rows)
        x = rows2[9]
        rows2[9] = (x[0], 9000, x[2])
        self.connection.query(
            [
                self.src_table.insert_rows(rows),
                self.dst_table.insert_rows(rows2),
                commit,
            ]
        )

    def test_compound_key(self):
        a = TableSegment(self.connection, self.src_table.path, ("id",), extra_columns=("id2", "comment"))
        b = TableSegment(self.connection, self.dst_table.path, ("id",), extra_columns=("id2", "comment"))

        differ = HashDiffer()
        diff = list(differ.diff_tables(a, b))
        uuid = diff[0][1][0]
        self.assertEqual(diff, [("-", (uuid, "9", "9")), ("+", (uuid, "9000", "9"))])

        aa = TableSegment(self.connection, self.src_table.path, ("id", "id2"), "comment")
        bb = TableSegment(self.connection, self.dst_table.path, ("id", "id2"), "comment")
        diff = list(differ.diff_tables(aa, bb))
        uuid = diff[0][1][0]
        self.assertEqual(diff, [("-", (uuid, "9", "9")), ("+", (uuid, "9000", "9"))])

        self.assertRaises(ValueError, list, differ.diff_tables(aa, a))
