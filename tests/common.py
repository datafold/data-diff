import hashlib
import os

from data_diff import databases as db
import logging
import subprocess

TEST_MYSQL_CONN_STRING: str = "mysql://mysql:Password1@localhost/mysql"
TEST_POSTGRESQL_CONN_STRING: str = None
TEST_SNOWFLAKE_CONN_STRING: str = None
TEST_BIGQUERY_CONN_STRING: str = None
TEST_REDSHIFT_CONN_STRING: str = None
TEST_ORACLE_CONN_STRING: str = None
TEST_PRESTO_CONN_STRING: str = None

DEFAULT_N_SAMPLES = 50
N_SAMPLES = int(os.environ.get("N_SAMPLES", DEFAULT_N_SAMPLES))
BENCHMARK = os.environ.get("BENCHMARK", False)

def get_git_revision_short_hash() -> str:
    return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('ascii').strip()

GIT_REVISION=get_git_revision_short_hash()

level = logging.ERROR
if os.environ.get("LOG_LEVEL", False):
    level = getattr(logging, os.environ["LOG_LEVEL"].upper())

logging.basicConfig(level=level)
logging.getLogger("diff_tables").setLevel(level)
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
}

for k, v in CONN_STRINGS.items():
    if v is None:
        logging.warn(f"Connection to {k} not configured")
    else:
        logging.info(f"Testing database: {k}")

CONN_STRINGS = {k: v for k, v in CONN_STRINGS.items() if v is not None}


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
