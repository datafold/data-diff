import unittest
from data_diff.databases import duckdb as duckdb_differ
import os
import uuid

test_duckdb_filepath = str(uuid.uuid4()) + ".duckdb"


class TestDuckDBTableSchemaMethods(unittest.TestCase):
    def setUp(self):
        # Create a new duckdb file
        self.duckdb_conn = duckdb_differ.DuckDB(filepath=test_duckdb_filepath)

    def tearDown(self):
        # Optional: delete file after tests
        os.remove(test_duckdb_filepath)

    def test_normalize_table_path(self):
        self.assertEqual(self.duckdb_conn._normalize_table_path(("test_table",)), (None, "main", "test_table"))
        self.assertEqual(
            self.duckdb_conn._normalize_table_path(("test_schema", "test_table")), (None, "test_schema", "test_table")
        )
        self.assertEqual(
            self.duckdb_conn._normalize_table_path(("test_database", "test_schema", "test_table")),
            ("test_database", "test_schema", "test_table"),
        )

        with self.assertRaises(ValueError):
            self.duckdb_conn._normalize_table_path(("test_database", "test_schema", "test_table", "extra"))

    def test_select_table_schema(self):
        db_path = ("test_table",)
        expected_sql = "SELECT column_name, data_type, datetime_precision, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = 'test_table' AND table_schema = 'main' and table_catalog = current_catalog()"
        self.assertEqual(self.duckdb_conn.select_table_schema(db_path), expected_sql)

        db_path = ("custom_schema", "test_table")
        expected_sql = "SELECT column_name, data_type, datetime_precision, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = 'test_table' AND table_schema = 'custom_schema' and table_catalog = current_catalog()"
        self.assertEqual(self.duckdb_conn.select_table_schema(db_path), expected_sql)

        db_path = ("custom_db", "custom_schema", "test_table")
        expected_sql = "SELECT column_name, data_type, datetime_precision, numeric_precision, numeric_scale FROM custom_db.information_schema.columns WHERE table_name = 'test_table' AND table_schema = 'custom_schema' and table_catalog = 'custom_db'"
        self.assertEqual(self.duckdb_conn.select_table_schema(db_path), expected_sql)
