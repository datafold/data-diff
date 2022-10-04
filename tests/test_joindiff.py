from parameterized import parameterized_class

from data_diff.databases.connect import connect
from data_diff.table_segment import TableSegment, split_space
from data_diff import databases as db
from data_diff.joindiff_tables import JoinDiffer

from .test_diff_tables import TestPerDatabase, _get_float_type, _get_text_type, _commit, _insert_row, _insert_rows

from .common import (
    str_to_checksum,
    CONN_STRINGS,
    N_THREADS,
)

DATABASE_INSTANCES = None
DATABASE_URIS = {k.__name__: v for k, v in CONN_STRINGS.items()}


def init_instances():
    global DATABASE_INSTANCES
    if DATABASE_INSTANCES is not None:
        return

    DATABASE_INSTANCES = {k.__name__: connect(v, N_THREADS) for k, v in CONN_STRINGS.items()}


TEST_DATABASES = {
    x.__name__
    for x in (
        db.PostgreSQL,
        db.Snowflake,
        db.MySQL,
        db.BigQuery,
        db.Presto,
        db.Vertica,
        db.Trino,
        db.Oracle,
        db.Redshift,
    )
}

_class_per_db_dec = parameterized_class(
    ("name", "db_name"), [(name, name) for name in DATABASE_URIS if name in TEST_DATABASES]
)


def test_per_database(cls):
    return _class_per_db_dec(cls)


@test_per_database
class TestJoindiff(TestPerDatabase):
    def setUp(self):
        super().setUp()

        float_type = _get_float_type(self.connection)

        self.connection.query(
            f"create table {self.table_src}(id int, userid int, movieid int, rating {float_type}, timestamp timestamp)",
            None,
        )
        self.connection.query(
            f"create table {self.table_dst}(id int, userid int, movieid int, rating {float_type}, timestamp timestamp)",
            None,
        )
        _commit(self.connection)

        self.table = TableSegment(self.connection, self.table_src_path, "id", "timestamp", case_sensitive=False)
        self.table2 = TableSegment(self.connection, self.table_dst_path, "id", "timestamp", case_sensitive=False)

        self.differ = JoinDiffer()

    def test_diff_small_tables(self):
        time = "2022-01-01 00:00:00"
        time_str = f"timestamp '{time}'"

        cols = "id userid movieid rating timestamp".split()
        _insert_rows(self.connection, self.table_src, cols, [[1, 1, 1, 9, time_str], [2, 2, 2, 9, time_str]])
        _insert_rows(self.connection, self.table_dst, cols, [[1, 1, 1, 9, time_str]])
        _commit(self.connection)
        diff = list(self.differ.diff_tables(self.table, self.table2))
        expected = [("-", ("2", time + ".000000"))]
        self.assertEqual(expected, diff)
        self.assertEqual(2, self.differ.stats["table1_count"])
        self.assertEqual(1, self.differ.stats["table2_count"])

    def test_diff_table_above_bisection_threshold(self):
        time = "2022-01-01 00:00:00"
        time_str = f"timestamp '{time}'"

        cols = "id userid movieid rating timestamp".split()
        _insert_rows(
            self.connection,
            self.table_src,
            cols,
            [
                [1, 1, 1, 9, time_str],
                [2, 2, 2, 9, time_str],
                [3, 3, 3, 9, time_str],
                [4, 4, 4, 9, time_str],
                [5, 5, 5, 9, time_str],
            ],
        )

        _insert_rows(
            self.connection,
            self.table_dst,
            cols,
            [
                [1, 1, 1, 9, time_str],
                [2, 2, 2, 9, time_str],
                [3, 3, 3, 9, time_str],
                [4, 4, 4, 9, time_str],
            ],
        )
        _commit(self.connection)

        diff = list(self.differ.diff_tables(self.table, self.table2))
        expected = [("-", ("5", time + ".000000"))]
        self.assertEqual(expected, diff)
        self.assertEqual(5, self.differ.stats["table1_count"])
        self.assertEqual(4, self.differ.stats["table2_count"])

    def test_return_empty_array_when_same(self):
        time = "2022-01-01 00:00:00"
        time_str = f"timestamp '{time}'"

        cols = "id userid movieid rating timestamp".split()

        _insert_row(self.connection, self.table_src, cols, [1, 1, 1, 9, time_str])
        _insert_row(self.connection, self.table_dst, cols, [1, 1, 1, 9, time_str])

        diff = list(self.differ.diff_tables(self.table, self.table2))
        self.assertEqual([], diff)

    def test_diff_sorted_by_key(self):
        time = "2022-01-01 00:00:00"
        time2 = "2021-01-01 00:00:00"

        time_str = f"timestamp '{time}'"
        time_str2 = f"timestamp '{time2}'"

        cols = "id userid movieid rating timestamp".split()

        _insert_rows(
            self.connection,
            self.table_src,
            cols,
            [
                [1, 1, 1, 9, time_str],
                [2, 2, 2, 9, time_str2],
                [3, 3, 3, 9, time_str],
                [4, 4, 4, 9, time_str2],
                [5, 5, 5, 9, time_str],
            ],
        )

        _insert_rows(
            self.connection,
            self.table_dst,
            cols,
            [
                [1, 1, 1, 9, time_str],
                [2, 2, 2, 9, time_str],
                [3, 3, 3, 9, time_str],
                [4, 4, 4, 9, time_str],
                [5, 5, 5, 9, time_str],
            ],
        )
        _commit(self.connection)

        diff = list(self.differ.diff_tables(self.table, self.table2))
        expected = [
            ("-", ("2", time2 + ".000000")),
            ("+", ("2", time + ".000000")),
            ("-", ("4", time2 + ".000000")),
            ("+", ("4", time + ".000000")),
        ]
        self.assertEqual(expected, diff)

    def test_dup_pks(self):
        time = "2022-01-01 00:00:00"
        time_str = f"timestamp '{time}'"

        cols = "id rating timestamp".split()

        _insert_row(self.connection, self.table_src, cols, [1, 9, time_str])
        _insert_row(self.connection, self.table_src, cols, [1, 10, time_str])
        _insert_row(self.connection, self.table_dst, cols, [1, 9, time_str])

        x = self.differ.diff_tables(self.table, self.table2)
        self.assertRaises(ValueError, list, x)

    def test_null_pks(self):
        time = "2022-01-01 00:00:00"
        time_str = f"timestamp '{time}'"

        cols = "id rating timestamp".split()

        _insert_row(self.connection, self.table_src, cols, ["null", 9, time_str])
        _insert_row(self.connection, self.table_dst, cols, [1, 9, time_str])

        x = self.differ.diff_tables(self.table, self.table2)
        self.assertRaises(ValueError, list, x)
