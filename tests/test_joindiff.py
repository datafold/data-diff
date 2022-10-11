from typing import List

from data_diff.queries.ast_classes import TablePath
from data_diff.table_segment import TableSegment
from data_diff import databases as db
from data_diff.joindiff_tables import JoinDiffer

from .test_diff_tables import TestPerDatabase, _get_float_type, _commit, _insert_row, _insert_rows

from .common import (
    random_table_suffix,
    test_each_database_in_list,
)


TEST_DATABASES = {
    db.PostgreSQL,
    db.Snowflake,
    db.MySQL,
    db.BigQuery,
    db.Presto,
    db.Vertica,
    db.Trino,
    db.Oracle,
    db.Redshift,
}

test_each_database = test_each_database_in_list(TEST_DATABASES)


@test_each_database_in_list({db.Snowflake, db.BigQuery})
class TestCompositeKey(TestPerDatabase):
    def setUp(self):
        super().setUp()

        float_type = _get_float_type(self.connection)

        self.connection.query(
            f"create table {self.table_src}(id int, userid int, movieid int, rating {float_type}, timestamp timestamp)",
        )
        self.connection.query(
            f"create table {self.table_dst}(id int, userid int, movieid int, rating {float_type}, timestamp timestamp)",
        )
        _commit(self.connection)

        self.differ = JoinDiffer()

    def test_composite_key(self):
        time = "2022-01-01 00:00:00"
        time_str = f"timestamp '{time}'"

        cols = "id userid movieid rating timestamp".split()
        _insert_rows(self.connection, self.table_src, cols, [[1, 1, 1, 9, time_str], [2, 2, 2, 9, time_str]])
        _insert_rows(self.connection, self.table_dst, cols, [[1, 1, 1, 9, time_str], [2, 3, 2, 9, time_str]])
        _commit(self.connection)

        # Sanity
        table1 = TableSegment(
            self.connection, self.table_src_path, ("id",), "timestamp", ("userid",), case_sensitive=False
        )
        table2 = TableSegment(
            self.connection, self.table_dst_path, ("id",), "timestamp", ("userid",), case_sensitive=False
        )
        diff = list(self.differ.diff_tables(table1, table2))
        assert len(diff) == 2
        assert self.differ.stats["exclusive_count"] == 0

        # Test pks diffed, by checking exclusive_count
        table1 = TableSegment(self.connection, self.table_src_path, ("id", "userid"), "timestamp", case_sensitive=False)
        table2 = TableSegment(self.connection, self.table_dst_path, ("id", "userid"), "timestamp", case_sensitive=False)
        diff = list(self.differ.diff_tables(table1, table2))
        assert len(diff) == 2
        assert self.differ.stats["exclusive_count"] == 2


@test_each_database
class TestJoindiff(TestPerDatabase):
    def setUp(self):
        super().setUp()

        float_type = _get_float_type(self.connection)

        self.connection.query(
            f"create table {self.table_src}(id int, userid int, movieid int, rating {float_type}, timestamp timestamp)",
        )
        self.connection.query(
            f"create table {self.table_dst}(id int, userid int, movieid int, rating {float_type}, timestamp timestamp)",
        )
        _commit(self.connection)

        self.table = TableSegment(self.connection, self.table_src_path, ("id",), "timestamp", case_sensitive=False)
        self.table2 = TableSegment(self.connection, self.table_dst_path, ("id",), "timestamp", case_sensitive=False)

        self.differ = JoinDiffer()

    def test_diff_small_tables(self):
        time = "2022-01-01 00:00:00"
        time_str = f"timestamp '{time}'"

        cols = "id userid movieid rating timestamp".split()
        _insert_rows(self.connection, self.table_src, cols, [[1, 1, 1, 9, time_str], [2, 2, 2, 9, time_str]])
        _insert_rows(self.connection, self.table_dst, cols, [[1, 1, 1, 9, time_str]])
        _commit(self.connection)
        diff = list(self.differ.diff_tables(self.table, self.table2))
        expected_row = ("2", time + ".000000")
        expected = [("-", expected_row)]
        self.assertEqual(expected, diff)
        self.assertEqual(2, self.differ.stats["table1_count"])
        self.assertEqual(1, self.differ.stats["table2_count"])
        self.assertEqual(3, self.differ.stats["table1_sum_id"])
        self.assertEqual(1, self.differ.stats["table2_sum_id"])

        # Test materialize
        materialize_path = self.connection.parse_table_name(f"test_mat_{random_table_suffix()}")
        mdiffer = self.differ.replace(materialize_to_table=materialize_path)
        diff = list(mdiffer.diff_tables(self.table, self.table2))
        self.assertEqual(expected, diff)

        t = TablePath(materialize_path)
        rows = self.connection.query(t.select(), List[tuple])
        self.connection.query(t.drop())
        # is_xa, is_xb, is_diff1, is_diff2, row1, row2
        assert rows == [(1, 0, 1, 1) + expected_row + (None, None)], rows

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
