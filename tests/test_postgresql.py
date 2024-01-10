import unittest
from copy import deepcopy
from urllib.parse import quote

from data_diff import TableSegment, HashDiffer, Database
from data_diff import connect_to_table
from data_diff import databases as db
from data_diff.queries.api import table, commit
from tests.common import get_conn, random_table_suffix, connect


class TestUUID(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = get_conn(db.PostgreSQL)

        table_suffix = random_table_suffix()

        self.table_src_name = f"src{table_suffix}"
        self.table_dst_name = f"dst{table_suffix}"

        self.table_src = table(self.table_src_name)
        self.table_dst = table(self.table_dst_name)

    def test_uuid(self):
        self.connection.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";', None)

        queries = [
            self.table_src.drop(True),
            self.table_dst.drop(True),
            f"CREATE TABLE {self.table_src_name} (id uuid DEFAULT uuid_generate_v4 (), comment VARCHAR, PRIMARY KEY (id))",
            commit,
            self.table_src.insert_rows([[i] for i in range(100)], columns=["comment"]),
            commit,
            self.table_dst.create(self.table_src),
            commit,
            self.table_src.insert_row("This one is different", columns=["comment"]),
            commit,
        ]

        for query in queries:
            self.connection.query(query)

        a = TableSegment(self.connection, self.table_src.path, ("id",), "comment")
        b = TableSegment(self.connection, self.table_dst.path, ("id",), "comment")

        differ = HashDiffer()
        diff = list(differ.diff_tables(a, b))
        uuid = diff[0][1][0]
        self.assertEqual(diff, [("-", (uuid, "This one is different"))])

        # Compare with MySql
        mysql_conn = get_conn(db.MySQL)

        rows = self.connection.query(self.table_src.select(), list)

        queries = [
            f"CREATE TABLE {self.table_dst_name} (id VARCHAR(128), comment VARCHAR(128))",
            commit,
            self.table_dst.insert_rows(rows, columns=["id", "comment"]),
            commit,
        ]

        for q in queries:
            mysql_conn.query(q)

        c = TableSegment(mysql_conn, (self.table_dst_name,), ("id",), "comment")
        diff = list(differ.diff_tables(a, c))
        assert not diff, diff
        diff = list(differ.diff_tables(c, a))
        assert not diff, diff

        self.connection.query(self.table_src.drop(True))
        self.connection.query(self.table_dst.drop(True))
        mysql_conn.query(self.table_dst.drop(True))


class Test100Fields(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = get_conn(db.PostgreSQL)

        table_suffix = random_table_suffix()

        self.table_src_name = f"src{table_suffix}"
        self.table_dst_name = f"dst{table_suffix}"

        self.table_src = table(self.table_src_name)
        self.table_dst = table(self.table_dst_name)

    def test_100_fields(self):
        self.connection.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";', None)

        columns = [f"col{i}" for i in range(100)]
        fields = " ,".join(f'"{field}" TEXT' for field in columns)

        queries = [
            self.table_src.drop(True),
            self.table_dst.drop(True),
            f"CREATE TABLE {self.table_src_name} (id uuid DEFAULT uuid_generate_v4 (), {fields})",
            commit,
            self.table_src.insert_rows([[f"{x * y}" for x in range(100)] for y in range(10)], columns=columns),
            commit,
            self.table_dst.create(self.table_src),
            commit,
            self.table_src.insert_rows([[1 for x in range(100)]], columns=columns),
            commit,
        ]

        for query in queries:
            self.connection.query(query)

        a = TableSegment(self.connection, self.table_src.path, ("id",), extra_columns=tuple(columns))
        b = TableSegment(self.connection, self.table_dst.path, ("id",), extra_columns=tuple(columns))

        differ = HashDiffer()
        diff = list(differ.diff_tables(a, b))
        id_ = diff[0][1][0]
        result = (id_,) + tuple("1" for x in range(100))
        self.assertEqual(diff, [("-", result)])


class TestSpecialCharacterPassword(unittest.TestCase):
    username: str = "test"
    password: str = "passw!!!@rd"

    def setUp(self) -> None:
        self.connection: Database = get_conn(db.PostgreSQL)
        self.table_name = f"table{random_table_suffix()}"

        # Setup user with special character '@' in password
        self.connection.query(f"DROP USER IF EXISTS {self.username};", None)
        self.connection.query(f"CREATE USER {self.username} WITH PASSWORD '{self.password}';", None)

    def tearDown(self):
        self.connection.query(f"DROP USER IF EXISTS {self.username};", None)
        self.connection.close()

    def test_special_char_password(self):
        db_config = deepcopy(self.connection._args)
        db_config.update(
            {
                "driver": "postgresql",
                "dbname": db_config.pop("database"),
                "user": self.username,
                "password": quote(self.password),
            }
        )

        # verify pythonic connection method
        connect_to_table(db_config, self.table_name)

        # verify connection method with URL string unquoted after it's verified
        db_url = (
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:"
            f"{db_config.get('port', 5432)}/{db_config['dbname']}"
        )

        with connect(db_url) as connection_verified:
            assert connection_verified._args.get("password") == self.password
