from typing import Callable, List
from datetime import datetime
import unittest

from .common import str_to_checksum, TEST_MYSQL_CONN_STRING
from .common import str_to_checksum, test_each_database_in_list, get_conn, random_table_suffix

from sqeleton.queries import table, current_timestamp

from sqeleton import databases as dbs
from sqeleton import connect


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
}

test_each_database: Callable = test_each_database_in_list(TEST_DATABASES)


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.mysql = connect(TEST_MYSQL_CONN_STRING)

    def test_connect_to_db(self):
        self.assertEqual(1, self.mysql.query("SELECT 1", int))

class TestMD5(unittest.TestCase):
    def test_md5_as_int(self):
        class MD5Dialect(dbs.mysql.Dialect, dbs.mysql.Mixin_MD5):
            pass

        self.mysql = connect(TEST_MYSQL_CONN_STRING)
        self.mysql.dialect = MD5Dialect()

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
class TestSchema(unittest.TestCase):
    def test_table_list(self):
        name = "tbl_" + random_table_suffix()
        db = get_conn(self.db_cls)
        tbl = table(db.parse_table_name(name), schema={"id": int})
        q = db.dialect.list_tables(db.default_schema, name)
        assert not db.query(q)

        db.query(tbl.create())
        self.assertEqual(db.query(q, List[str]), [name])

        db.query(tbl.drop())
        assert not db.query(q)


@test_each_database
class TestQueries(unittest.TestCase):
    def test_current_timestamp(self):
        db = get_conn(self.db_cls)
        res = db.query(current_timestamp(), datetime)
        assert isinstance(res, datetime), (res, type(res))
