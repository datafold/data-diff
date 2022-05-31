from os import wait
import unittest

from data_diff.database import connect_to_uri
from data_diff.sql import Checksum, Compare, Compiler, Count, Enum, Explain, In, Select, TableName
from parameterized import parameterized, parameterized_class

from .common import TEST_MYSQL_CONN_STRING

@parameterized_class([
    { "db": connect_to_uri(TEST_MYSQL_CONN_STRING) },
    { "db": connect_to_uri("postgres://postgres:Password1@localhost/postgres") }
])
class TestSQL(unittest.TestCase):
    def setUp(self):
        self.compiler = Compiler(self.db)
        self.quoted_table_name = self.compiler.compile(TableName(("marine_mammals", "walrus")))

    def test_compile_string(self):
        self.assertEqual("SELECT 1", self.compiler.compile("SELECT 1"))

    def test_compile_int(self):
        self.assertEqual("1", self.compiler.compile(1))

    def test_compile_table_name(self):
        quoted_table_regex = r"^(`|\")marine_mammals(`|\").(`|\")walrus(`|\")"
        self.assertRegex(self.quoted_table_name, quoted_table_regex)

    def test_compile_select(self):
        expected_sql = f"SELECT name FROM {self.quoted_table_name}"
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
        expected_sql = f"(SELECT *, (row_number() over (ORDER BY id)) as idx FROM {self.quoted_table_name} ORDER BY id) tmp"
        self.assertEqual(
            expected_sql,
            self.compiler.compile(
                Enum(
                    ("marine_mammals", "walrus"),
                    "id",
                )
            ),
        )

    def test_checksum(self):
        # The actual checksumming depends heavily on the database, and these
        # tests are generic over them.
        expected_sql = rf"SELECT name, sum(.*) FROM {self.quoted_table_name}"
        self.assertRegex(
            self.compiler.compile(
                Select(
                    ["name", Checksum(["id", "timestamp"])],
                    TableName(("marine_mammals", "walrus")),
                )
            ),
            expected_sql,
        )

    def test_compare(self):
        expected_sql = f"SELECT name FROM {self.quoted_table_name} WHERE (id <= 1000) AND (id > 1)"
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
        expected_sql = f"SELECT name FROM {self.quoted_table_name} WHERE (id IN (1, 2, 3))"
        self.assertEqual(
            expected_sql,
            self.compiler.compile(Select(["name"], TableName(("marine_mammals", "walrus")), [In("id", [1, 2, 3])])),
        )

    def test_count(self):
        expected_sql = f"SELECT count(*) FROM {self.quoted_table_name} WHERE (id IN (1, 2, 3))"
        self.assertEqual(
            expected_sql,
            self.compiler.compile(Select([Count()], TableName(("marine_mammals", "walrus")), [In("id", [1, 2, 3])])),
        )

    def test_count_with_column(self):
        expected_sql = f"SELECT count(id) FROM {self.quoted_table_name} WHERE (id IN (1, 2, 3))"
        self.assertEqual(
            expected_sql,
            self.compiler.compile(
                Select([Count("id")], TableName(("marine_mammals", "walrus")), [In("id", [1, 2, 3])])
            ),
        )

    def test_explain(self):
        expected_sql = f"EXPLAIN SELECT count(id) FROM {self.quoted_table_name} WHERE (id IN (1, 2, 3))"
        self.assertEqual(
            expected_sql,
            self.compiler.compile(
                Explain(Select([Count("id")], TableName(("marine_mammals", "walrus")), [In("id", [1, 2, 3])]))
            ),
        )
