import unittest

from .common import str_to_checksum
from xdiff.database import connect_to_uri


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.mysql = connect_to_uri("mysql://xdiff:xdiff@localhost/xdiff")

    def test_connect_to_db(self):
        self.assertEqual(1, self.mysql.query("SELECT 1", int))

    def test_md5_to_int(self):
        str = 'hello world'
        query_fragment = self.mysql.md5_to_int("'{0}'".format(str))
        query = f"SELECT {query_fragment}"

        self.assertEqual(
            str_to_checksum(str),
            self.mysql.query(query, int)
        )
