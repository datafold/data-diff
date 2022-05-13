import unittest

from .common import str_to_checksum, TEST_CONN_STRING
from data_diff.database import connect_to_uri


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = connect_to_uri(TEST_CONN_STRING)

    def test_connect_to_db(self):
        self.assertEqual(1, self.db.query("SELECT 1", int))

    def test_md5_to_int(self):
        str = "hello world"
        query_fragment = self.db.md5_to_int("'{0}'".format(str))
        query = f"SELECT {query_fragment}"

        self.assertEqual(str_to_checksum(str), self.db.query(query, int))
