import unittest

from data_diff.database import connect_to_uri
from data_diff.sql import Compiler, Select, TableName

from .common import TEST_MYSQL_CONN_STRING


class TestSQL(unittest.TestCase):
    def setUp(self):
        self.mysql = connect_to_uri(TEST_MYSQL_CONN_STRING)
        self.compiler = Compiler(self.mysql)

    def test_compile_string(self):
        self.assertEqual("SELECT 1", self.compiler.compile("SELECT 1"))

    def test_compile_int(self):
        self.assertEqual("1", self.compiler.compile(1))

    def test_compile_table_name(self):
        self.assertEqual("`marine_mammals.walrus`", self.compiler.compile(
            TableName(("marine_mammals", "walrus"))))

    def test_compile_select(self):
        expected_sql = "SELECT name FROM `marine_mammals.walrus`"
        self.assertEqual(expected_sql, self.compiler.compile(
            Select(
                ["name"],
                TableName(("marine_mammals", "walrus")),
            ))
        )
