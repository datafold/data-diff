import hashlib
import os
import re
import string
import random
from typing import Callable
import unittest
import logging
import subprocess

from parameterized import parameterized_class

from data_diff.queries.api import table
from data_diff.databases.base import Database

from data_diff import databases as db
from data_diff import tracking
from data_diff import connect
from data_diff.table_segment import TableSegment
from data_diff.query_utils import drop_table

tracking.disable_tracking()

# We write 'or None' because Github sometimes creates empty env vars for secrets
TEST_MYSQL_CONN_STRING: str = "mysql://mysql:Password1@localhost/mysql"
TEST_POSTGRESQL_CONN_STRING: str = "postgresql://postgres:Password1@localhost/postgres"
TEST_SNOWFLAKE_CONN_STRING: str = os.environ.get("DATADIFF_SNOWFLAKE_URI") or None
TEST_PRESTO_CONN_STRING: str = os.environ.get("DATADIFF_PRESTO_URI") or None
TEST_BIGQUERY_CONN_STRING: str = os.environ.get("DATADIFF_BIGQUERY_URI") or None
TEST_REDSHIFT_CONN_STRING: str = os.environ.get("DATADIFF_REDSHIFT_URI") or None
TEST_ORACLE_CONN_STRING: str = None
TEST_DATABRICKS_CONN_STRING: str = os.environ.get("DATADIFF_DATABRICKS_URI")
TEST_TRINO_CONN_STRING: str = os.environ.get("DATADIFF_TRINO_URI") or None
# clickhouse uri for provided docker - "clickhouse://clickhouse:Password1@localhost:9000/clickhouse"
TEST_CLICKHOUSE_CONN_STRING: str = os.environ.get("DATADIFF_CLICKHOUSE_URI")
# vertica uri provided for docker - "vertica://vertica:Password1@localhost:5433/vertica"
TEST_VERTICA_CONN_STRING: str = os.environ.get("DATADIFF_VERTICA_URI")
TEST_DUCKDB_CONN_STRING: str = "duckdb://main:@:memory:"
TEST_MSSQL_CONN_STRING: str = os.environ.get("DATADIFF_MSSQL_URI")


DEFAULT_N_SAMPLES = 50
N_SAMPLES = int(os.environ.get("N_SAMPLES", DEFAULT_N_SAMPLES))
BENCHMARK = os.environ.get("BENCHMARK", False)
N_THREADS = int(os.environ.get("N_THREADS", 1))
TEST_ACROSS_ALL_DBS = os.environ.get("TEST_ACROSS_ALL_DBS", True)  # Should we run the full db<->db test suite?


def get_git_revision_short_hash() -> str:
    return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode("ascii").strip()


GIT_REVISION = get_git_revision_short_hash()

level = logging.ERROR
if os.environ.get("LOG_LEVEL", False):
    level = getattr(logging, os.environ["LOG_LEVEL"].upper())

logging.basicConfig(level=level)
logging.getLogger("hashdiff_tables").setLevel(level)
logging.getLogger("joindiff_tables").setLevel(level)
logging.getLogger("diff_tables").setLevel(level)
logging.getLogger("table_segment").setLevel(level)
logging.getLogger("database").setLevel(level)

try:
    from tests.local_settings import *
except ImportError:
    pass  # No local settings


CONN_STRINGS = {
    db.BigQuery: TEST_BIGQUERY_CONN_STRING,
    db.MySQL: TEST_MYSQL_CONN_STRING,
    db.PostgreSQL: TEST_POSTGRESQL_CONN_STRING,
    db.Snowflake: TEST_SNOWFLAKE_CONN_STRING,
    db.Redshift: TEST_REDSHIFT_CONN_STRING,
    db.Oracle: TEST_ORACLE_CONN_STRING,
    db.Presto: TEST_PRESTO_CONN_STRING,
    db.Databricks: TEST_DATABRICKS_CONN_STRING,
    db.Trino: TEST_TRINO_CONN_STRING,
    db.Clickhouse: TEST_CLICKHOUSE_CONN_STRING,
    db.Vertica: TEST_VERTICA_CONN_STRING,
    db.DuckDB: TEST_DUCKDB_CONN_STRING,
    db.MsSQL: TEST_MSSQL_CONN_STRING,
}


def get_conn(cls: type) -> Database:
    return connect(CONN_STRINGS[cls], N_THREADS)


def _print_used_dbs():
    used = {k.__name__ for k, v in CONN_STRINGS.items() if v is not None}
    unused = {k.__name__ for k, v in CONN_STRINGS.items() if v is None}

    print(f"Testing databases: {', '.join(used)}")
    if unused:
        logging.info(f"Connection not configured; skipping tests for: {', '.join(unused)}")
    if TEST_ACROSS_ALL_DBS:
        logging.info(
            f"Full tests enabled (every db<->db). May take very long when many dbs are involved. ={TEST_ACROSS_ALL_DBS}"
        )


_print_used_dbs()
CONN_STRINGS = {k: v for k, v in CONN_STRINGS.items() if v is not None}


def random_table_suffix() -> str:
    char_set = string.ascii_lowercase + string.digits
    suffix = "_"
    suffix += "".join(random.choice(char_set) for _ in range(5))
    return suffix


def str_to_checksum(str: str):
    # hello world
    #   => 5eb63bbbe01eeed093cb22bb8f5acdc3
    #   =>                   cb22bb8f5acdc3
    #   => 273350391345368515 - offset (see db.CHECKSUM_OFFSET)
    m = hashlib.md5()
    m.update(str.encode("utf-8"))  # encode to binary
    md5 = m.hexdigest()
    # 0-indexed, unlike DBs which are 1-indexed here, so +1 in dbs
    half_pos = db.MD5_HEXDIGITS - db.CHECKSUM_HEXDIGITS
    return int(md5[half_pos:], 16) - db.CHECKSUM_OFFSET


class DiffTestCase(unittest.TestCase):
    """Sets up two tables for diffing"""

    db_cls = None
    src_schema = None
    dst_schema = None

    def setUp(self):
        assert self.db_cls, self.db_cls

        self.connection = get_conn(self.db_cls)

        table_suffix = random_table_suffix()
        self.table_src_name = f"src{table_suffix}"
        self.table_dst_name = f"dst{table_suffix}"

        self.table_src_path = self.connection.dialect.parse_table_name(self.table_src_name)
        self.table_dst_path = self.connection.dialect.parse_table_name(self.table_dst_name)

        drop_table(self.connection, self.table_src_path)
        drop_table(self.connection, self.table_dst_path)

        self.src_table = table(self.table_src_path, schema=self.src_schema)
        self.dst_table = table(self.table_dst_path, schema=self.dst_schema)
        if self.src_schema:
            self.connection.query(self.src_table.create())
        if self.dst_schema:
            self.connection.query(self.dst_table.create())

        return super().setUp()

    def tearDown(self):
        drop_table(self.connection, self.table_src_path)
        drop_table(self.connection, self.table_dst_path)


def _parameterized_class_per_conn(test_databases):
    test_databases = set(test_databases)
    names = [(cls.__name__, cls) for cls in CONN_STRINGS if cls in test_databases]
    return parameterized_class(("name", "db_cls"), names)


def test_each_database_in_list(databases) -> Callable:
    def _test_per_database(cls):
        return _parameterized_class_per_conn(databases)(cls)

    return _test_per_database


def table_segment(database, table_path, key_columns, *args, **kw):
    if isinstance(key_columns, str):
        key_columns = (key_columns,)
    return TableSegment(database, table_path, key_columns, *args, **kw)


def ansi_stdout_cleanup(ansi_input) -> str:
    return re.sub(r"\x1B\[[0-?]*[ -/]*[@-~]", "", ansi_input)
