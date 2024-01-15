import unittest
from datetime import datetime
from typing import Callable, List, Tuple

import attrs
import pytz

from data_diff import connect, Database
from data_diff import databases as dbs
from data_diff.abcs.database_types import TimestampTZ
from data_diff.queries.api import table, current_timestamp
from data_diff.queries.extras import NormalizeAsString
from data_diff.schema import create_schema
from tests.common import (
    TEST_MYSQL_CONN_STRING,
    test_each_database_in_list,
    get_conn,
    str_to_checksum,
    random_table_suffix,
)

TEST_DATABASES = {
    dbs.MySQL,
    dbs.PostgreSQL,
    dbs.Oracle,
    dbs.Redshift,
    dbs.Snowflake,
    dbs.DuckDB,
    dbs.BigQuery,
    dbs.Presto,
    dbs.Trino,
    dbs.Vertica,
    dbs.MsSQL,
}

test_each_database: Callable = test_each_database_in_list(TEST_DATABASES)


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.mysql = connect(TEST_MYSQL_CONN_STRING)

    def test_connect_to_db(self):
        self.assertEqual(1, self.mysql.query("SELECT 1", int))


class TestMD5(unittest.TestCase):
    def test_md5_as_int(self):
        self.mysql = connect(TEST_MYSQL_CONN_STRING)

        message = "hello world"
        query_fragment = self.mysql.dialect.md5_as_int(f"'{message}'")
        query = f"SELECT {query_fragment}"

        self.assertEqual(str_to_checksum(message), self.mysql.query(query, int))


class TestConnect(unittest.TestCase):
    def test_bad_uris(self):
        self.assertRaises(ValueError, connect, "p")
        self.assertRaises(ValueError, connect, "postgresql:///bla/foo")
        self.assertRaises(ValueError, connect, "snowflake://user:pass@foo/bar/TEST1")
        self.assertRaises(ValueError, connect, "snowflake://user:pass@foo/bar/TEST1?warehouse=ha&schema=dup")


@test_each_database
class TestQueries(unittest.TestCase):
    def test_current_timestamp(self):
        db = get_conn(self.db_cls)
        res = db.query(current_timestamp(), datetime)
        assert isinstance(res, datetime), (res, type(res))

    def test_correct_timezone(self):
        if self.db_cls in [dbs.MsSQL]:
            self.skipTest("No support for session tz.")
        name = "tbl_" + random_table_suffix()

        db_connection = get_conn(self.db_cls)
        with db_connection:
            tbl = table(name, schema={"id": int, "created_at": TimestampTZ(9), "updated_at": TimestampTZ(9)})

            db_connection.query(tbl.create())

            tz = pytz.timezone("Europe/Berlin")

            now = datetime.now(tz)
            if isinstance(db_connection, dbs.Presto):
                ms = now.microsecond // 1000 * 1000  # Presto max precision is 3
                now = now.replace(microsecond=ms)

            db_connection.query(table(name).insert_row(1, now, now))
            db_connection.query(db_connection.dialect.set_timezone_to_utc())

            table_object = table(name)
            raw_schema = db_connection.query_table_schema(table_object.path)
            schema = db_connection._process_table_schema(table_object.path, raw_schema)
            schema = create_schema(db_connection.name, table_object, schema, case_sensitive=True)
            table_object = attrs.evolve(table_object, schema=schema)
            table_object.schema["created_at"] = attrs.evolve(
                table_object.schema["created_at"], precision=table_object.schema["created_at"].precision
            )

            tbl = table(name, schema=table_object.schema)

            results = db_connection.query(
                tbl.select(NormalizeAsString(tbl[c]) for c in ["created_at", "updated_at"]), List[Tuple]
            )

            created_at = results[0][1]
            updated_at = results[0][1]

            utc = now.astimezone(pytz.UTC)
            expected = utc.__format__("%Y-%m-%d %H:%M:%S.%f")

            self.assertEqual(created_at, expected)
            self.assertEqual(updated_at, expected)

            db_connection.query(tbl.drop())


@test_each_database
class TestThreePartIds(unittest.TestCase):
    def test_three_part_support(self):
        if self.db_cls not in [dbs.PostgreSQL, dbs.Redshift, dbs.Snowflake, dbs.DuckDB, dbs.MsSQL]:
            self.skipTest("Limited support for 3 part ids")

        table_name = "tbl_" + random_table_suffix()
        db_connection = get_conn(self.db_cls)
        with db_connection:
            db_res = db_connection.query(f"SELECT {db_connection.dialect.current_database()}")
            schema_res = db_connection.query(f"SELECT {db_connection.dialect.current_schema()}")
            db_name = db_res.rows[0][0]
            schema_name = schema_res.rows[0][0]

            table_one_part = table((table_name,), schema={"id": int})
            table_two_part = table((schema_name, table_name), schema={"id": int})
            table_three_part = table((db_name, schema_name, table_name), schema={"id": int})

            for part in (table_one_part, table_two_part, table_three_part):
                db_connection.query(part.create())
                schema = db_connection.query_table_schema(part.path)
                assert len(schema) == 1
                db_connection.query(part.drop())


@test_each_database
class TestNumericPrecisionParsing(unittest.TestCase):
    def test_specified_precision(self):
        name = "tbl_" + random_table_suffix()
        db_connection = get_conn(self.db_cls)
        with db_connection:
            table_object = table(name, schema={"value": "DECIMAL(10, 2)"})
            db_connection.query(table_object.create())
            table_object = table(name)
            raw_schema = db_connection.query_table_schema(table_object.path)
            schema = db_connection._process_table_schema(table_object.path, raw_schema)
            self.assertEqual(schema["value"].precision, 2)

    def test_specified_zero_precision(self):
        name = "tbl_" + random_table_suffix()
        db_connection = get_conn(self.db_cls)
        with db_connection:
            table_object = table(name, schema={"value": "DECIMAL(10)"})
            db_connection.query(table_object.create())
            table_object = table(name)
            raw_schema = db_connection.query_table_schema(table_object.path)
            schema = db_connection._process_table_schema(table_object.path, raw_schema)
            self.assertEqual(schema["value"].precision, 0)

    def test_default_precision(self):
        name = "tbl_" + random_table_suffix()
        db_connection = get_conn(self.db_cls)
        with db_connection:
            table_object = table(name, schema={"value": "DECIMAL"})
            db_connection.query(table_object.create())
            table_object = table(name)
            raw_schema = db_connection.query_table_schema(table_object.path)
            schema = db_connection._process_table_schema(table_object.path, raw_schema)
            self.assertEqual(schema["value"].precision, db_connection.dialect.DEFAULT_NUMERIC_PRECISION)


# Skip presto as it doesn't support a close method:
# https://github.com/prestodb/presto-python-client/blob/be2610e524fa8400c9f2baa41ba0159d44ac2b11/prestodb/dbapi.py#L130
closeable_databases = TEST_DATABASES.copy()
closeable_databases.discard(dbs.Presto)

test_closeable_databases: Callable = test_each_database_in_list(closeable_databases)


@test_closeable_databases
class TestCloseMethod(unittest.TestCase):
    def test_close_connection(self):
        database: Database = get_conn(self.db_cls)

        # Perform a query to verify the connection is established
        with database:
            database.query("SELECT 1")

        # Now the connection should be closed, and trying to execute a query should fail.
        with self.assertRaises(Exception):  # Catch any type of exception.
            database.query("SELECT 1")
