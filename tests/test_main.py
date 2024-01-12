import unittest

from data_diff import Database, JoinDiffer, HashDiffer
from data_diff import databases as db
from data_diff.__main__ import _get_dbs, _set_age, _get_table_differ, _get_expanded_columns, _get_threads
from data_diff.databases.mysql import MySQL
from data_diff.diff_tables import TableDiffer
from tests.common import CONN_STRINGS, get_conn, DiffTestCase


class TestGetDBS(unittest.TestCase):
    def test__get_dbs(self) -> None:
        db1: Database
        db2: Database
        db1_str: str = CONN_STRINGS[db.PostgreSQL]
        db2_str: str = CONN_STRINGS[db.PostgreSQL]

        # no threads and 2 threads1
        db1, db2 = _get_dbs(0, db1_str, 2, db2_str, 0, False)
        with db1, db2:
            assert db1 == db2
            assert db1.thread_count == 2

        # 3 threads and 0 threads1
        db1, db2 = _get_dbs(3, db1_str, 0, db2_str, 0, False)
        with db1, db2:
            assert db1 == db2
            assert db1.thread_count == 3

        # not interactive
        db1, db2 = _get_dbs(1, db1_str, 0, db2_str, 0, False)
        with db1, db2:
            assert db1 == db2
            assert not db1._interactive

        # interactive
        db1, db2 = _get_dbs(1, db1_str, 0, db2_str, 0, True)
        with db1, db2:
            assert db1 == db2
            assert db1._interactive

        db2_str: str = CONN_STRINGS[db.MySQL]

        # no threads and 1 threads1 and 2 thread2
        db1, db2 = _get_dbs(0, db1_str, 1, db2_str, 2, False)
        with db1, db2:
            assert db1 != db2
            assert db1.thread_count == 1
            assert db2.thread_count == 2

        # 3 threads and 0 threads1 and 0 thread2
        db1, db2 = _get_dbs(3, db1_str, 0, db2_str, 0, False)
        with db1, db2:
            assert db1 != db2
            assert db1.thread_count == 3
            assert db2.thread_count == 3
            assert db1.thread_count == db2.thread_count

        # not interactive
        db1, db2 = _get_dbs(1, db1_str, 0, db2_str, 0, False)
        with db1, db2:
            assert db1 != db2
            assert not db1._interactive
            assert not db2._interactive

        # interactive
        db1, db2 = _get_dbs(1, db1_str, 0, db2_str, 0, True)
        with db1, db2:
            assert db1 != db2
            assert db1._interactive
            assert db2._interactive

    def test_database_connection_failure(self) -> None:
        """Test when database connection fails."""
        with self.assertRaises(Exception):  # Assuming that connect() raises Exception on connection failure
            _get_dbs(1, "db1_str", 0, "db2_str", 0, False)

    def test_invalid_inputs(self) -> None:
        """Test invalid inputs."""
        with self.assertRaises(Exception):  # Assuming that connect() raises Exception on failure
            _get_dbs(0, "", 0, "", 0, False)  # Empty connection strings

    def test_database_object(self) -> None:
        """Test returned database objects are valid and not None."""
        db1_str: str = CONN_STRINGS[db.PostgreSQL]
        db2_str: str = CONN_STRINGS[db.PostgreSQL]
        db1, db2 = _get_dbs(1, db1_str, 0, db2_str, 0, False)
        self.assertIsNotNone(db1)
        self.assertIsNotNone(db2)
        self.assertIsInstance(db1, Database)
        self.assertIsInstance(db2, Database)

    def test_databases_are_different(self) -> None:
        """Test separate connections for different databases."""
        db1_str: str = CONN_STRINGS[db.PostgreSQL]
        db2_str: str = CONN_STRINGS[db.MySQL]
        db1, db2 = _get_dbs(0, db1_str, 1, db2_str, 2, False)
        with db1, db2:
            self.assertIsNot(db1, db2)  # Check that db1 and db2 are not the same object


class TestSetAge(unittest.TestCase):
    def setUp(self) -> None:
        self.database: Database = get_conn(db.PostgreSQL)

    def tearDown(self):
        self.database.close()

    def test__set_age(self):
        options = {}
        _set_age(options, None, None, self.database)
        assert len(options) == 0

        options = {}
        _set_age(options, "1d", None, self.database)
        assert len(options) == 1
        assert options.get("max_update") is not None

        options = {}
        _set_age(options, None, "1d", self.database)
        assert len(options) == 1
        assert options.get("min_update") is not None

        options = {}
        _set_age(options, "1d", "1d", self.database)
        assert len(options) == 2
        assert options.get("max_update") is not None
        assert options.get("min_update") is not None

    def test__set_age_db_query_failure(self):
        with self.assertRaises(Exception):
            options = {}
            _set_age(options, "1d", "1d", self.mock_database)


class TestGetTableDiffer(unittest.TestCase):
    def test__get_table_differ(self):
        db1: Database
        db2: Database
        db1_str: str = CONN_STRINGS[db.PostgreSQL]
        db2_str: str = CONN_STRINGS[db.PostgreSQL]

        db1, db2 = _get_dbs(1, db1_str, 0, db2_str, 0, False)
        with db1, db2:
            assert db1 == db2
            table_differ: TableDiffer = self._get_differ("auto", db1, db2)
            assert isinstance(table_differ, JoinDiffer)

            table_differ: TableDiffer = self._get_differ("joindiff", db1, db2)
            assert isinstance(table_differ, JoinDiffer)

            table_differ: TableDiffer = self._get_differ("hashdiff", db1, db2)
            assert isinstance(table_differ, HashDiffer)

        db2_str: str = CONN_STRINGS[db.MySQL]
        db1, db2 = _get_dbs(1, db1_str, 0, db2_str, 0, False)
        with db1, db2:
            assert db1 != db2
            table_differ: TableDiffer = self._get_differ("auto", db1, db2)
            assert isinstance(table_differ, HashDiffer)

            table_differ: TableDiffer = self._get_differ("joindiff", db1, db2)
            assert isinstance(table_differ, JoinDiffer)

            table_differ: TableDiffer = self._get_differ("hashdiff", db1, db2)
            assert isinstance(table_differ, HashDiffer)

    @staticmethod
    def _get_differ(algorithm, db1, db2):
        return _get_table_differ(algorithm, db1, db2, False, 1, False, False, False, 1, None, None, None)


class TestGetExpandedColumns(DiffTestCase):
    db_cls = MySQL

    def setUp(self):
        super().setUp()

    def test__get_expanded_columns(self):
        columns = ["user_id", "movie_id", "rating"]
        kwargs = {
            "db1": self.connection,
            "schema1": self.src_schema,
            "table1": self.table_src_name,
            "db2": self.connection,
            "schema2": self.dst_schema,
            "table2": self.table_dst_name,
        }
        expanded_columns = _get_expanded_columns(columns, False, set(columns), **kwargs)

        assert len(expanded_columns) == 3
        assert len(set(expanded_columns) & set(columns)) == 3

    def test__get_expanded_columns_case_sensitive(self):
        columns = ["UserID", "MovieID", "Rating"]
        kwargs = {
            "db1": self.connection,
            "schema1": self.src_schema,
            "table1": self.table_src_name,
            "db2": self.connection,
            "schema2": self.dst_schema,
            "table2": self.table_dst_name,
        }
        expanded_columns = _get_expanded_columns(columns, True, set(columns), **kwargs)

        assert len(expanded_columns) == 3
        assert len(set(expanded_columns) & set(columns)) == 3


class TestGetThreads(unittest.TestCase):
    def test__get_threads(self):
        threaded, threads = _get_threads(None, None, None)
        assert threaded
        assert threads == 1

        threaded, threads = _get_threads(None, 2, 3)
        assert threaded
        assert threads == 1

        threaded, threads = _get_threads("serial", None, None)
        assert not threaded
        assert threads == 1

        with self.assertRaises(AssertionError):
            _get_threads("serial", 1, 2)

        threaded, threads = _get_threads("4", None, None)
        assert threaded
        assert threads == 4

        with self.assertRaises(ValueError) as value_error:
            _get_threads("auto", None, None)
        assert str(value_error.exception) == "invalid literal for int() with base 10: 'auto'"

        threaded, threads = _get_threads(5, None, None)
        assert threaded
        assert threads == 5

        threaded, threads = _get_threads(6, 7, 8)
        assert threaded
        assert threads == 6

        with self.assertRaises(ValueError) as value_error:
            _get_threads(0, None, None)
        assert str(value_error.exception) == "Error: threads must be >= 1"

        with self.assertRaises(ValueError) as value_error:
            _get_threads(-1, None, None)
        assert str(value_error.exception) == "Error: threads must be >= 1"
