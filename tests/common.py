from contextlib import suppress
import hashlib
import os
import string
import random

from data_diff import databases as db
from data_diff import tracking
import logging
import subprocess

tracking.disable_tracking()

# We write 'or None' because Github sometimes creates empty env vars for secrets
TEST_MYSQL_CONN_STRING: str = "mysql://mysql:Password1@localhost/mysql"
TEST_POSTGRESQL_CONN_STRING: str = "postgresql://postgres:Password1@localhost/postgres"
TEST_SNOWFLAKE_CONN_STRING: str = os.environ.get("DATADIFF_SNOWFLAKE_URI") or None
TEST_PRESTO_CONN_STRING: str = os.environ.get("DATADIFF_PRESTO_URI") or None
TEST_BIGQUERY_CONN_STRING: str = None
TEST_REDSHIFT_CONN_STRING: str = None
TEST_ORACLE_CONN_STRING: str = None
TEST_DATABRICKS_CONN_STRING: str = os.environ.get("DATADIFF_DATABRICKS_URI")
TEST_TRINO_CONN_STRING: str = os.environ.get("DATADIFF_TRINO_URI") or None
TEST_CLICKHOUSE_CONN_STRING: str = "clickhouse://clickhouse:Password1@localhost:9000/clickhouse"

DEFAULT_N_SAMPLES = 50
N_SAMPLES = int(os.environ.get("N_SAMPLES", DEFAULT_N_SAMPLES))
BENCHMARK = os.environ.get("BENCHMARK", False)
N_THREADS = int(os.environ.get("N_THREADS", 1))


def get_git_revision_short_hash() -> str:
    return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode("ascii").strip()


GIT_REVISION = get_git_revision_short_hash()

level = logging.ERROR
if os.environ.get("LOG_LEVEL", False):
    level = getattr(logging, os.environ["LOG_LEVEL"].upper())

logging.basicConfig(level=level)
logging.getLogger("diff_tables").setLevel(level)
logging.getLogger("table_segment").setLevel(level)
logging.getLogger("database").setLevel(level)

try:
    from .local_settings import *
except ImportError:
    pass  # No local settings

if TEST_BIGQUERY_CONN_STRING and TEST_SNOWFLAKE_CONN_STRING:
    # TODO Fix this. Seems to have something to do with pyarrow
    raise RuntimeError("Using BigQuery at the same time as Snowflake causes an error!!")

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
}


def _print_used_dbs():
    used = {k.__name__ for k, v in CONN_STRINGS.items() if v is not None}
    unused = {k.__name__ for k, v in CONN_STRINGS.items() if v is None}

    logging.info(f"Testing databases: {', '.join(used)}")
    if unused:
        logging.info(f"Connection not configured; skipping tests for: {', '.join(unused)}")


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
    #   => 273350391345368515
    m = hashlib.md5()
    m.update(str.encode("utf-8"))  # encode to binary
    md5 = m.hexdigest()
    # 0-indexed, unlike DBs which are 1-indexed here, so +1 in dbs
    half_pos = db.MD5_HEXDIGITS - db.CHECKSUM_HEXDIGITS
    return int(md5[half_pos:], 16)


def _drop_table_if_exists(conn, table):
    with suppress(db.QueryError):
        if isinstance(conn, db.Oracle):
            conn.query(f"DROP TABLE {table}", None)
            conn.query(f"DROP TABLE {table}", None)
        else:
            conn.query(f"DROP TABLE IF EXISTS {table}", None)
            if not isinstance(conn, (db.BigQuery, db.Databricks, db.Clickhouse)):
                conn.query("COMMIT", None)
