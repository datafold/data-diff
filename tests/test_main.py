import unittest

from pydantic_core._pydantic_core import PydanticCustomError, ValidationError

from data_diff import Database, JoinDiffer, HashDiffer
from data_diff import databases as db
from data_diff.__main__ import _get_dbs, _set_age, _get_table_differ, _get_expanded_columns
from data_diff.cli_options import CliOptions
from data_diff.databases.mysql import MySQL
from data_diff.diff_tables import TableDiffer
from tests.common import CONN_STRINGS, get_conn, DiffTestCase, get_cli_options


class TestGetDBS(unittest.TestCase):
    def test__get_dbs(self) -> None:
        db1: Database
        db2: Database

        # no threads and 2 threads1
        cli_options: CliOptions = get_cli_options(
            database1=CONN_STRINGS[db.PostgreSQL],
            database2=CONN_STRINGS[db.PostgreSQL],
            threads=1,
            threads1=2,
            threads2=None,
            interactive=False,
        )
        db1, db2 = _get_dbs(cli_options)
        with db1, db2:
            assert db1 == db2
            assert db1.thread_count == 2

        # 3 threads and 0 threads1
        cli_options.threads = 3
        cli_options.threads1 = 0
        db1, db2 = _get_dbs(cli_options)
        with db1, db2:
            assert db1 == db2
            assert db1.thread_count == 3

        # not interactive
        db1, db2 = _get_dbs(cli_options)
        with db1, db2:
            assert db1 == db2
            assert not db1._interactive

        # interactive
        cli_options.interactive = True
        db1, db2 = _get_dbs(cli_options)
        with db1, db2:
            assert db1 == db2
            assert db1._interactive

        db2_str: str = CONN_STRINGS[db.MySQL]

        # no threads and 1 threads1 and 2 thread2
        cli_options.database2 = db2_str
        cli_options.threads = 0
        cli_options.threads1 = 1
        cli_options.threads2 = 2
        cli_options.interactive = False
        db1, db2 = _get_dbs(cli_options)
        with db1, db2:
            assert db1 != db2
            assert db1.thread_count == 1
            assert db2.thread_count == 2

        # 3 threads and 0 threads1 and 0 thread2
        cli_options.threads = 3
        cli_options.threads1 = 0
        cli_options.threads2 = 0
        db1, db2 = _get_dbs(cli_options)
        with db1, db2:
            assert db1 != db2
            assert db1.thread_count == 3
            assert db2.thread_count == 3
            assert db1.thread_count == db2.thread_count

        # not interactive
        db1, db2 = _get_dbs(cli_options)
        with db1, db2:
            assert db1 != db2
            assert not db1._interactive
            assert not db2._interactive

        # interactive
        cli_options.interactive = True
        db1, db2 = _get_dbs(cli_options)
        with db1, db2:
            assert db1 != db2
            assert db1._interactive
            assert db2._interactive

    def test_database_connection_failure(self) -> None:
        """Test when database connection fails."""
        cli_options: CliOptions = get_cli_options()
        cli_options.database1 = "db1_str"
        cli_options.database2 = "db2_str"
        with self.assertRaises(Exception):  # Assuming that connect() raises Exception on connection failure
            _get_dbs(cli_options)

    def test_invalid_inputs(self) -> None:
        """Test invalid inputs."""
        cli_options: CliOptions = get_cli_options()
        cli_options.database1 = ""
        cli_options.database2 = ""
        with self.assertRaises(Exception):  # Assuming that connect() raises Exception on failure
            _get_dbs(cli_options)  # Empty connection strings

    def test_database_object(self) -> None:
        """Test returned database objects are valid and not None."""
        cli_options: CliOptions = get_cli_options()
        cli_options.database1 = CONN_STRINGS[db.PostgreSQL]
        cli_options.database2 = CONN_STRINGS[db.PostgreSQL]
        db1, db2 = _get_dbs(cli_options)
        self.assertIsNotNone(db1)
        self.assertIsNotNone(db2)
        self.assertIsInstance(db1, Database)
        self.assertIsInstance(db2, Database)

    def test_databases_are_different(self) -> None:
        """Test separate connections for different databases."""
        cli_options: CliOptions = get_cli_options()
        cli_options.database1 = CONN_STRINGS[db.PostgreSQL]
        cli_options.database2 = CONN_STRINGS[db.MySQL]
        db1, db2 = _get_dbs(cli_options)
        with db1, db2:
            self.assertIsNot(db1, db2)  # Check that db1 and db2 are not the same object


class TestSetAge(unittest.TestCase):
    def setUp(self) -> None:
        self.database: Database = get_conn(db.PostgreSQL)

    def tearDown(self):
        self.database.close()

    def test__set_age(self):
        options = {}
        cli_options: CliOptions = get_cli_options()
        _set_age(options, cli_options, self.database)
        assert len(options) == 0

        options = {}
        cli_options.min_age = "1d"
        _set_age(options, cli_options, self.database)
        assert len(options) == 1
        assert options.get("max_update") is not None

        options = {}
        cli_options.min_age = None
        cli_options.max_age = "1d"
        _set_age(options, cli_options, self.database)
        assert len(options) == 1
        assert options.get("min_update") is not None

        options = {}
        cli_options.min_age = "1d"
        _set_age(options, cli_options, self.database)
        assert len(options) == 2
        assert options.get("max_update") is not None
        assert options.get("min_update") is not None

    def test__set_age_db_query_failure(self):
        cli_options: CliOptions = get_cli_options()
        cli_options.min_age = "1d"
        cli_options.max_age = "1d"
        with self.assertRaises(Exception):
            options = {}
            _set_age(options, cli_options, self.mock_database)


class TestGetTableDiffer(unittest.TestCase):
    def test__get_table_differ(self):
        db1: Database
        db2: Database

        cli_options: CliOptions = get_cli_options()
        cli_options.database1 = CONN_STRINGS[db.PostgreSQL]
        cli_options.database2 = CONN_STRINGS[db.PostgreSQL]
        cli_options.threads = 1
        cli_options.threads1 = 0
        cli_options.threads2 = 0
        cli_options.threaded = False
        cli_options.interactive = False
        cli_options.assume_unique_key = False
        cli_options.sample_exclusive_rows = False
        cli_options.materialize_all_rows = False
        cli_options.materialize_to_table = None

        db1, db2 = _get_dbs(cli_options)
        with db1, db2:
            assert db1 == db2
            cli_options.algorithm = "auto"
            table_differ: TableDiffer = _get_table_differ(cli_options, db1, db2)
            assert isinstance(table_differ, JoinDiffer)

            cli_options.algorithm = "joindiff"
            table_differ: TableDiffer = _get_table_differ(cli_options, db1, db2)
            assert isinstance(table_differ, JoinDiffer)

            cli_options.algorithm = "hashdiff"
            table_differ: TableDiffer = _get_table_differ(cli_options, db1, db2)
            assert isinstance(table_differ, HashDiffer)

        cli_options.database2 = CONN_STRINGS[db.MySQL]
        db1, db2 = _get_dbs(cli_options)
        with db1, db2:
            assert db1 != db2
            cli_options.algorithm = "auto"
            table_differ: TableDiffer = _get_table_differ(cli_options, db1, db2)
            assert isinstance(table_differ, HashDiffer)

            cli_options.algorithm = "joindiff"
            table_differ: TableDiffer = _get_table_differ(cli_options, db1, db2)
            assert isinstance(table_differ, JoinDiffer)

            cli_options.algorithm = "hashdiff"
            table_differ: TableDiffer = _get_table_differ(cli_options, db1, db2)
            assert isinstance(table_differ, HashDiffer)


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
        cli_options: CliOptions = get_cli_options(thread1=None, threads2=None)
        assert cli_options.threaded
        assert cli_options.threads == 1

        cli_options: CliOptions = get_cli_options(thread1=2, threads2=3)
        assert cli_options.threaded
        assert cli_options.threads == 1

        cli_options: CliOptions = get_cli_options(threads="serial", thread1=None, threads2=None)
        assert not cli_options.threaded
        assert cli_options.threads == 1

        with self.assertRaises(ValueError):
            get_cli_options(threads="serial", thread1=1, threads2=2)

        with self.assertRaises(ValidationError):
            get_cli_options(threads="auto", thread1=None, threads2=None)

        cli_options: CliOptions = get_cli_options(threads="4", thread1=None, threads2=None)
        assert cli_options.threaded
        assert cli_options.threads == 4

        cli_options: CliOptions = get_cli_options(threads=5, thread1=None, threads2=None)
        assert cli_options.threaded
        assert cli_options.threads == 5

        cli_options: CliOptions = get_cli_options(threads=6, thread1=7, threads2=8)
        assert cli_options.threaded
        assert cli_options.threads == 6

        with self.assertRaises(ValidationError):
            get_cli_options(threads=0, thread1=None, threads2=None)

        with self.assertRaises(ValidationError):
            get_cli_options(threads=-1, thread1=None, threads2=None)
