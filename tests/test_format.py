import unittest
from data_diff.diff_tables import DiffResultWrapper, InfoTree, SegmentInfo, TableSegment
from data_diff.format import jsonify
from data_diff.abcs.database_types import Integer
from tests.test_query import MockDatabase


class TestFormat(unittest.TestCase):
    maxDiff = None

    def test_jsonify_diff(self):
        diff = DiffResultWrapper(
            info_tree=InfoTree(
                info=SegmentInfo(
                    tables=[
                        TableSegment(
                            table_path=("db", "schema", "table1"), key_columns=("id",), database=MockDatabase()
                        ),
                        TableSegment(
                            table_path=("db", "schema", "table2"), key_columns=("id",), database=MockDatabase()
                        ),
                    ],
                    diff_schema=(
                        ("is_exclusive_a", bool),
                        ("is_exclusive_b", bool),
                        ("is_diff_id", int),
                        ("is_diff_value", int),
                        ("id_a", str),
                        ("id_b", str),
                        ("value_a", str),
                        ("value_b", str),
                    ),
                    diff=[
                        (False, False, 0, 1, "1", "1", "3", "201"),
                        (True, False, 1, 1, "2", None, "4", None),
                        (False, True, 1, 1, None, "3", None, "202"),
                    ],
                )
            ),
            diff=[],
            stats={},
        )
        json_diff = jsonify(
            diff,
            dbt_model="my_model",
            dataset1_columns=[
                ("id", "NUMBER", Integer()),
                ("value", "NUMBER", Integer()),
            ],
            dataset2_columns=[
                ("id", "NUMBER", Integer()),
                ("value", "NUMBER", Integer()),
            ],
            columns_diff={
                "added": [],
                "removed": [],
                "typeChanged": [],
            },
        )

        self.assertEqual(
            json_diff,
            {
                "version": "1.1.0",
                "status": "success",
                "result": "different",
                "model": "my_model",
                "dataset1": ["db", "schema", "table1"],
                "dataset2": ["db", "schema", "table2"],
                "rows": {
                    "exclusive": {
                        "dataset1": [{"id": {"isPK": True, "value": "2"}, "value": {"isPK": False, "value": "4"}}],
                        "dataset2": [{"id": {"isPK": True, "value": "3"}, "value": {"isPK": False, "value": "202"}}],
                    },
                    "diff": [
                        {
                            "id": {"isPK": True, "dataset1": "1", "dataset2": "1", "isDiff": False},
                            "value": {"isPK": False, "dataset1": "3", "dataset2": "201", "isDiff": True},
                        },
                    ],
                },
                "columns": {
                    "dataset1": [
                        {"name": "id", "type": "NUMBER", "kind": "integer"},
                        {"name": "value", "type": "NUMBER", "kind": "integer"},
                    ],
                    "dataset2": [
                        {"name": "id", "type": "NUMBER", "kind": "integer"},
                        {"name": "value", "type": "NUMBER", "kind": "integer"},
                    ],
                    "primaryKey": ["id"],
                    "exclusive": {
                        "dataset1": [],
                        "dataset2": [],
                    },
                    "typeChanged": [],
                },
                "summary": None,
            },
        )

    def test_jsonify_no_stats(self):
        diff = DiffResultWrapper(
            info_tree=InfoTree(
                info=SegmentInfo(
                    tables=[
                        TableSegment(
                            table_path=("db", "schema", "table1"), key_columns=("id",), database=MockDatabase()
                        ),
                        TableSegment(
                            table_path=("db", "schema", "table2"), key_columns=("id",), database=MockDatabase()
                        ),
                    ],
                    diff_schema=(
                        ("is_exclusive_a", bool),
                        ("is_exclusive_b", bool),
                        ("is_diff_id", int),
                        ("is_diff_value", int),
                        ("id_a", str),
                        ("id_b", str),
                        ("value_a", str),
                        ("value_b", str),
                    ),
                    diff=[
                        (False, False, 0, 1, "1", "1", "3", "201"),
                        (True, False, 1, 1, "2", None, "4", None),
                        (False, True, 1, 1, None, "3", None, "202"),
                    ],
                )
            ),
            diff=[],
            stats={},
        )
        json_diff = jsonify(
            diff,
            dbt_model="my_model",
            dataset1_columns=[
                ("id", "NUMBER", Integer()),
                ("value", "NUMBER", Integer()),
            ],
            dataset2_columns=[
                ("id", "NUMBER", Integer()),
                ("value", "NUMBER", Integer()),
            ],
            columns_diff={
                "added": [],
                "removed": [],
                "typeChanged": [],
            },
            stats_only=True,
        )

        self.assertEqual(
            json_diff,
            {
                "version": "1.1.0",
                "status": "success",
                "result": "different",
                "model": "my_model",
                "dataset1": ["db", "schema", "table1"],
                "dataset2": ["db", "schema", "table2"],
                "rows": None,
                "columns": {
                    "dataset1": [
                        {"name": "id", "type": "NUMBER", "kind": "integer"},
                        {"name": "value", "type": "NUMBER", "kind": "integer"},
                    ],
                    "dataset2": [
                        {"name": "id", "type": "NUMBER", "kind": "integer"},
                        {"name": "value", "type": "NUMBER", "kind": "integer"},
                    ],
                    "primaryKey": ["id"],
                    "exclusive": {
                        "dataset1": [],
                        "dataset2": [],
                    },
                    "typeChanged": [],
                },
                "summary": None,
            },
        )

    def test_jsonify_diff_no_difeference(self):
        diff = DiffResultWrapper(
            info_tree=InfoTree(
                info=SegmentInfo(
                    tables=[
                        TableSegment(
                            table_path=("db", "schema", "table1"), key_columns=("id",), database=MockDatabase()
                        ),
                        TableSegment(
                            table_path=("db", "schema", "table2"), key_columns=("id",), database=MockDatabase()
                        ),
                    ],
                    diff_schema=(
                        ("is_exclusive_a", bool),
                        ("is_exclusive_b", bool),
                        ("is_diff_id", int),
                        ("is_diff_value", int),
                        ("id_a", str),
                        ("id_b", str),
                        ("value_a", str),
                        ("value_b", str),
                    ),
                    diff=[],
                )
            ),
            diff=[],
            stats={},
        )
        json_diff = jsonify(
            diff,
            dbt_model="model",
            dataset1_columns=[
                ("id", "NUMBER", Integer()),
                ("value", "NUMBER", Integer()),
            ],
            dataset2_columns=[
                ("id", "NUMBER", Integer()),
                ("value", "NUMBER", Integer()),
            ],
            columns_diff={
                "added": [],
                "removed": [],
                "changed": [],
            },
        )
        self.assertEqual(
            json_diff,
            {
                "version": "1.1.0",
                "status": "success",
                "result": "identical",
                "model": "model",
                "dataset1": ["db", "schema", "table1"],
                "dataset2": ["db", "schema", "table2"],
                "rows": {
                    "exclusive": {"dataset1": [], "dataset2": []},
                    "diff": [],
                },
                "columns": {
                    "primaryKey": ["id"],
                    "dataset1": [
                        {"name": "id", "type": "NUMBER", "kind": "integer"},
                        {"name": "value", "type": "NUMBER", "kind": "integer"},
                    ],
                    "dataset2": [
                        {"name": "id", "type": "NUMBER", "kind": "integer"},
                        {"name": "value", "type": "NUMBER", "kind": "integer"},
                    ],
                    "exclusive": {
                        "dataset1": [],
                        "dataset2": [],
                    },
                    "typeChanged": [],
                },
                "summary": None,
            },
        )

    def test_jsonify_column_suffix_fix(self):
        diff = DiffResultWrapper(
            info_tree=InfoTree(
                info=SegmentInfo(
                    tables=[
                        TableSegment(
                            table_path=("db", "schema", "table1"), key_columns=("id_a",), database=MockDatabase()
                        ),
                        TableSegment(
                            table_path=("db", "schema", "table2"), key_columns=("id_a",), database=MockDatabase()
                        ),
                    ],
                    diff_schema=(
                        ("is_exclusive_a", bool),
                        ("is_exclusive_b", bool),
                        ("is_diff_id_a", int),
                        ("is_diff_value_b", int),
                        ("id_a_a", str),
                        ("id_a_b", str),
                        ("value_b_a", str),
                        ("value_b_b", str),
                    ),
                    diff=[
                        (False, False, 0, 1, "1", "1", "3", "201"),
                        (True, False, 1, 1, "2", None, "4", None),
                        (False, True, 1, 1, None, "3", None, "202"),
                    ],
                )
            ),
            diff=[],
            stats={},
        )
        json_diff = jsonify(
            diff,
            dbt_model="my_model",
            dataset1_columns=[
                ("id_a", "NUMBER", Integer()),
                ("value_b", "NUMBER", Integer()),
            ],
            dataset2_columns=[
                ("id_a", "NUMBER", Integer()),
                ("value_b", "NUMBER", Integer()),
            ],
            columns_diff={
                "added": [],
                "removed": [],
                "typeChanged": [],
            },
        )
        self.assertEqual(
            json_diff,
            {
                "version": "1.1.0",
                "status": "success",
                "result": "different",
                "model": "my_model",
                "dataset1": ["db", "schema", "table1"],
                "dataset2": ["db", "schema", "table2"],
                "rows": {
                    "exclusive": {
                        "dataset1": [{"id_a": {"isPK": True, "value": "2"}, "value_b": {"isPK": False, "value": "4"}}],
                        "dataset2": [
                            {"id_a": {"isPK": True, "value": "3"}, "value_b": {"isPK": False, "value": "202"}}
                        ],
                    },
                    "diff": [
                        {
                            "id_a": {"isPK": True, "dataset1": "1", "dataset2": "1", "isDiff": False},
                            "value_b": {"isPK": False, "dataset1": "3", "dataset2": "201", "isDiff": True},
                        },
                    ],
                },
                "summary": None,
                "columns": {
                    "dataset1": [
                        {"name": "id_a", "type": "NUMBER", "kind": "integer"},
                        {"name": "value_b", "type": "NUMBER", "kind": "integer"},
                    ],
                    "dataset2": [
                        {"name": "id_a", "type": "NUMBER", "kind": "integer"},
                        {"name": "value_b", "type": "NUMBER", "kind": "integer"},
                    ],
                    "primaryKey": ["id_a"],
                    "exclusive": {
                        "dataset1": [],
                        "dataset2": [],
                    },
                    "typeChanged": [],
                },
            },
        )
