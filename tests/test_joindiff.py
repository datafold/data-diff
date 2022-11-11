from typing import List
from datetime import datetime

from data_diff.queries.ast_classes import TablePath
from data_diff.queries import table, commit
from data_diff.table_segment import TableSegment
from data_diff import databases as db
from data_diff.joindiff_tables import JoinDiffer

from .test_diff_tables import TestPerDatabase

from .common import (
    random_table_suffix,
    test_each_database_in_list,
)


TEST_DATABASES = {
    db.PostgreSQL,
    db.MySQL,
    db.Snowflake,
    db.BigQuery,
    db.Oracle,
    db.Redshift,
    db.Presto,
    db.Trino,
    db.Vertica,
}

test_each_database = test_each_database_in_list(TEST_DATABASES)


@test_each_database_in_list({db.Snowflake, db.BigQuery})
class TestCompositeKey(TestPerDatabase):
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
class TestJoindiff(TestPerDatabase):
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
        # is_xa, is_xb, is_diff1, is_diff2, row1, row2
        # assert rows == [(1, 0, 1, 1) + expected_row + (None, None)], rows
        assert rows == [(1, 0, 1, 1) + (expected_row[0], None, expected_row[1], None)], rows
        self.connection.query(t.drop())

        # Test materialize all rows
        mdiffer = mdiffer.replace(materialize_all_rows=True)
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
        expected = [
            ("-", ("2", time2 + ".000000")),
            ("+", ("2", time + ".000000")),
            ("-", ("4", time2 + ".000000")),
            ("+", ("4", time + ".000000")),
        ]
        self.assertEqual(expected, diff)

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


@test_each_database_in_list(d for d in TEST_DATABASES if d.dialect.SUPPORTS_PRIMARY_KEY and d.SUPPORTS_UNIQUE_CONSTAINT)
class TestUniqueConstraint(TestPerDatabase):
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
