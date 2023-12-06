import unittest
from datetime import datetime
from typing import Callable, List, Tuple

import attrs
import pytz

from data_diff import connect
from data_diff import databases as dbs
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
from data_diff.abcs.database_types import TimestampTZ

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

        str = "hello world"
        query_fragment = self.mysql.dialect.md5_as_int("'{0}'".format(str))
        query = f"SELECT {query_fragment}"

        self.assertEqual(str_to_checksum(str), self.mysql.query(query, int))


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
        db = get_conn(self.db_cls)
        tbl = table(name, schema={"id": int, "created_at": TimestampTZ(9), "updated_at": TimestampTZ(9)})

        db.query(tbl.create())

        tz = pytz.timezone("Europe/Berlin")

        now = datetime.now(tz)
        if isinstance(db, dbs.Presto):
            ms = now.microsecond // 1000 * 1000  # Presto max precision is 3
            now = now.replace(microsecond=ms)

        db.query(table(name).insert_row(1, now, now))
        db.query(db.dialect.set_timezone_to_utc())

        t = table(name)
        raw_schema = db.query_table_schema(t.path)
        schema = db._process_table_schema(t.path, raw_schema)
        schema = create_schema(db.name, t, schema, case_sensitive=True)
        t = attrs.evolve(t, schema=schema)
        t.schema["created_at"] = attrs.evolve(t.schema["created_at"], precision=t.schema["created_at"].precision)

        tbl = table(name, schema=t.schema)

        results = db.query(tbl.select(NormalizeAsString(tbl[c]) for c in ["created_at", "updated_at"]), List[Tuple])

        created_at = results[0][1]
        updated_at = results[0][1]

        utc = now.astimezone(pytz.UTC)
        expected = utc.__format__("%Y-%m-%d %H:%M:%S.%f")

        self.assertEqual(created_at, expected)
        self.assertEqual(updated_at, expected)

        db.query(tbl.drop())


@test_each_database
class TestThreePartIds(unittest.TestCase):
    def test_three_part_support(self):
        if self.db_cls not in [dbs.PostgreSQL, dbs.Redshift, dbs.Snowflake, dbs.DuckDB, dbs.MsSQL]:
            self.skipTest("Limited support for 3 part ids")

        table_name = "tbl_" + random_table_suffix()
        db = get_conn(self.db_cls)
        db_res = db.query(f"SELECT {db.dialect.current_database()}")
        schema_res = db.query(f"SELECT {db.dialect.current_schema()}")
        db_name = db_res.rows[0][0]
        schema_name = schema_res.rows[0][0]

        table_one_part = table((table_name,), schema={"id": int})
        table_two_part = table((schema_name, table_name), schema={"id": int})
        table_three_part = table((db_name, schema_name, table_name), schema={"id": int})

        for part in (table_one_part, table_two_part, table_three_part):
            db.query(part.create())
            d = db.query_table_schema(part.path)
            assert len(d) == 1
            db.query(part.drop())
