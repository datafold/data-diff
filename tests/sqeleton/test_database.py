import unittest

from ..common import str_to_checksum, TEST_MYSQL_CONN_STRING
from data_diff.databases import connect


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
