from typing import List
from datetime import datetime

import attrs

from data_diff.queries.ast_classes import TablePath
from data_diff.queries.api import table, commit
from data_diff.table_segment import TableSegment
from data_diff import databases as db
from data_diff.joindiff_tables import JoinDiffer

from tests.test_diff_tables import DiffTestCase

from tests.common import (
    random_table_suffix,
    test_each_database_in_list,
)


TEST_DATABASES = {
    db.PostgreSQL,
    db.MySQL,
    db.Snowflake,
    db.BigQuery,
    db.DuckDB,
    db.Oracle,
    db.Redshift,
    db.Presto,
    db.Trino,
    db.Vertica,
}

test_each_database = test_each_database_in_list(TEST_DATABASES)


@test_each_database_in_list({db.Snowflake, db.BigQuery, db.DuckDB})
class TestCompositeKey(DiffTestCase):
    src_schema = {"id": int, "userid": int, "movieid": int, "rating": float, "timestamp": datetime}
    dst_schema = {"id": int, "userid": int, "movieid": int, "rating": float, "timestamp": datetime}

    def setUp(self):
        super().setUp()

        self.differ = JoinDiffer()

    def test_composite_key(self):
        time = "2022-01-01 00:00:00"
        time_obj = datetime.fromisoformat(time)

        cols = "id userid movieid rating timestamp".split()

        self.connection.query(
            [
                self.src_table.insert_rows([[1, 1, 1, 9, time_obj], [2, 2, 2, 9, time_obj]], columns=cols),
                self.dst_table.insert_rows([[1, 1, 1, 9, time_obj], [2, 3, 2, 9, time_obj]], columns=cols),
                commit,
            ]
        )

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
class TestJoindiff(DiffTestCase):
    src_schema = {"id": int, "userid": int, "movieid": int, "rating": float, "timestamp": datetime}
    dst_schema = {"id": int, "userid": int, "movieid": int, "rating": float, "timestamp": datetime}

    def setUp(self):
        super().setUp()

        self.table = TableSegment(self.connection, self.table_src_path, ("id",), "timestamp", case_sensitive=False)
        self.table2 = TableSegment(self.connection, self.table_dst_path, ("id",), "timestamp", case_sensitive=False)

        self.differ = JoinDiffer()

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

        expected_row = ("2", time + ".000000")
        expected = [("-", expected_row)]
        self.assertEqual(expected, diff)
        self.assertEqual(2, info.rowcounts[1])
        self.assertEqual(1, info.rowcounts[2])
        # self.assertEqual(2, self.differ.stats["table1_max_id"])
        # self.assertEqual(1, self.differ.stats["table2_min_id"])

        # Test materialize
        materialize_path = self.connection.dialect.parse_table_name(f"test_mat_{random_table_suffix()}")
        mdiffer = attrs.evolve(self.differ, materialize_to_table=materialize_path)
        diff = list(mdiffer.diff_tables(self.table, self.table2))
        self.assertEqual(expected, diff)

        t = TablePath(materialize_path)
        rows = self.connection.query(t.select(), List[tuple])
        # is_xa, is_xb, is_diff1, is_diff2, row1, row2
        # assert rows == [(1, 0, 1, 1) + expected_row + (None, None)], rows
        assert rows == [(1, 0, 1, 1) + (expected_row[0], None, expected_row[1], None)], rows
        self.connection.query(t.drop())

        # Test materialize all rows
        mdiffer = attrs.evolve(mdiffer, materialize_all_rows=True)
        diff = list(mdiffer.diff_tables(self.table, self.table2))
        self.assertEqual(expected, diff)
        rows = self.connection.query(t.select(), List[tuple])
        assert len(rows) == 2, len(rows)
        self.connection.query(t.drop())

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

        diff = list(self.differ.diff_tables(self.table, self.table2))
        expected = {
            ("-", ("2", time2 + ".000000")),
            ("+", ("2", time + ".000000")),
            ("-", ("4", time2 + ".000000")),
            ("+", ("4", time + ".000000")),
        }
        self.assertEqual(expected, set(diff))
        keys = [k for _, (k, _) in diff]
        assert keys[0] == keys[1] and keys[2] == keys[3]  # same keys

    def test_dup_pks(self):
        time = "2022-01-01 00:00:00"
        time_obj = datetime.fromisoformat(time)

        cols = "id rating timestamp".split()

        self.connection.query(
            [
                self.src_table.insert_rows([[1, 9, time_obj], [1, 10, time_obj]], columns=cols),
                self.dst_table.insert_row(1, 9, time_obj, columns=cols),
            ]
        )

        x = self.differ.diff_tables(self.table, self.table2)
        self.assertRaises(ValueError, list, x)

    def test_null_pks(self):
        time = "2022-01-01 00:00:00"
        time_obj = datetime.fromisoformat(time)

        cols = "id rating timestamp".split()

        self.connection.query(
            [
                self.src_table.insert_row(None, 9, time_obj, columns=cols),
                self.dst_table.insert_row(1, 9, time_obj, columns=cols),
            ]
        )

        x = self.differ.diff_tables(self.table, self.table2)
        self.assertRaises(ValueError, list, x)


@test_each_database_in_list(
    d for d in TEST_DATABASES if d.DIALECT_CLASS.SUPPORTS_PRIMARY_KEY and d.SUPPORTS_UNIQUE_CONSTAINT
)
class TestUniqueConstraint(DiffTestCase):
    def setUp(self):
        super().setUp()

        self.src_table = table(
            self.table_src_path,
            schema={"id": int, "userid": int, "movieid": int, "rating": float},
        )
        self.dst_table = table(
            self.table_dst_path,
            schema={"id": int, "userid": int, "movieid": int, "rating": float},
        )

        self.connection.query(
            [self.src_table.create(primary_keys=["id"]), self.dst_table.create(primary_keys=["id", "userid"]), commit]
        )

        self.differ = JoinDiffer()

    def test_unique_constraint(self):
        self.connection.query(
            [
                self.src_table.insert_rows([[1, 1, 1, 9], [2, 2, 2, 9]]),
                self.dst_table.insert_rows([[1, 1, 1, 9], [2, 2, 2, 9]]),
                commit,
            ]
        )

        # Test no active validation
        table = TableSegment(self.connection, self.table_src_path, ("id",), case_sensitive=False)
        table2 = TableSegment(self.connection, self.table_dst_path, ("id",), case_sensitive=False)

        res = list(self.differ.diff_tables(table, table2))
        assert not res
        assert "validated_unique_keys" not in self.differ.stats

        # Test active validation
        table = TableSegment(self.connection, self.table_src_path, ("userid",), case_sensitive=False)
        table2 = TableSegment(self.connection, self.table_dst_path, ("userid",), case_sensitive=False)

        res = list(self.differ.diff_tables(table, table2))
        assert not res
        self.assertEqual(self.differ.stats["validated_unique_keys"], [["userid"]])
