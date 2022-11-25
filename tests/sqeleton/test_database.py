from typing import Callable, List
import unittest

from ..common import str_to_checksum, TEST_MYSQL_CONN_STRING
from ..common import str_to_checksum, test_each_database_in_list, DiffTestCase, get_conn, random_table_suffix

from data_diff.sqeleton.queries import table

from data_diff import databases as dbs
from data_diff.databases import connect


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

    def test_md5_as_int(self):
        str = "hello world"
        query_fragment = self.mysql.dialect.md5_as_int("'{0}'".format(str))
        query = f"SELECT {query_fragment}"

        self.assertEqual(str_to_checksum(str), self.mysql.query(query, int))


class TestConnect(unittest.TestCase):
    def test_bad_uris(self):
        self.assertRaises(ValueError, connect, "p")
        self.assertRaises(ValueError, connect, "postgresql:///bla/foo")
        self.assertRaises(ValueError, connect, "snowflake://user:pass@bya42734/xdiffdev/TEST1")
        self.assertRaises(ValueError, connect, "snowflake://user:pass@bya42734/xdiffdev/TEST1?warehouse=ha&schema=dup")


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
