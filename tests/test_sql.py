import unittest

from data_diff.database import connect_to_uri
from data_diff.sql import Checksum, Compare, Compiler, Count, Enum, Explain, In, Select, TableName

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
        self.assertEqual("`marine_mammals.walrus`", self.compiler.compile(TableName(("marine_mammals", "walrus"))))

    def test_compile_select(self):
        expected_sql = "SELECT name FROM `marine_mammals.walrus`"
        self.assertEqual(
            expected_sql,
            self.compiler.compile(
                Select(
                    ["name"],
                    TableName(("marine_mammals", "walrus")),
                )
            ),
        )

    def test_enum(self):
        expected_sql = "(SELECT *, (row_number() over (ORDER BY id)) as idx FROM `walrus` ORDER BY id) tmp"
        self.assertEqual(
            expected_sql,
            self.compiler.compile(
                Enum(
                    ("walrus",),
                    "id",
                )
            ),
        )

    def test_checksum(self):
        expected_sql = "SELECT name, sum(cast(conv(substring(md5(concat(cast(id as char), cast(timestamp as char))), 18), 16, 10) as unsigned)) FROM `marine_mammals.walrus`"
        self.assertEqual(
            expected_sql,
            self.compiler.compile(
                Select(
                    ["name", Checksum(["id", "timestamp"])],
                    TableName(("marine_mammals", "walrus")),
                )
            ),
        )

    def test_compare(self):
        expected_sql = "SELECT name FROM `marine_mammals.walrus` WHERE (id <= 1000) AND (id > 1)"
        self.assertEqual(
            expected_sql,
            self.compiler.compile(
                Select(
                    ["name"],
                    TableName(("marine_mammals", "walrus")),
                    [Compare("<=", "id", "1000"), Compare(">", "id", "1")],
                )
            ),
        )

    def test_in(self):
        expected_sql = "SELECT name FROM `marine_mammals.walrus` WHERE (id IN (1, 2, 3))"
        self.assertEqual(
            expected_sql,
            self.compiler.compile(Select(["name"], TableName(("marine_mammals", "walrus")), [In("id", [1, 2, 3])])),
        )

    def test_count(self):
        expected_sql = "SELECT count(*) FROM `marine_mammals.walrus` WHERE (id IN (1, 2, 3))"
        self.assertEqual(
            expected_sql,
            self.compiler.compile(Select([Count()], TableName(("marine_mammals", "walrus")), [In("id", [1, 2, 3])])),
        )

    def test_count_with_column(self):
        expected_sql = "SELECT count(id) FROM `marine_mammals.walrus` WHERE (id IN (1, 2, 3))"
        self.assertEqual(
            expected_sql,
            self.compiler.compile(
                Select([Count("id")], TableName(("marine_mammals", "walrus")), [In("id", [1, 2, 3])])
            ),
        )

    def test_explain(self):
        expected_sql = "EXPLAIN SELECT count(id) FROM `marine_mammals.walrus` WHERE (id IN (1, 2, 3))"
        self.assertEqual(
            expected_sql,
            self.compiler.compile(
                Explain(Select([Count("id")], TableName(("marine_mammals", "walrus")), [In("id", [1, 2, 3])]))
            ),
        )
