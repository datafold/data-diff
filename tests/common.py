import hashlib

from data_diff import database as db
import logging

logging.basicConfig(level=logging.WARN)

TEST_MYSQL_CONN_STRING: str = "mysql://mysql:Password1@localhost/mysql"
TEST_POSTGRES_CONN_STRING: str = None
TEST_SNOWFLAKE_CONN_STRING: str = None
TEST_BIGQUERY_CONN_STRING: str = None
TEST_REDSHIFT_CONN_STRING: str = None
TEST_ORACLE_CONN_STRING: str = None


try:
    from .local_settings import *
except ImportError:
    pass  # No local settings

CONN_STRINGS = {
    # db.BigQuery: TEST_BIGQUERY_CONN_STRING,     # TODO BigQuery before/after Snowflake causes an error!
    db.MySQL: TEST_MYSQL_CONN_STRING,
    db.Postgres: TEST_POSTGRES_CONN_STRING,
    db.Snowflake: TEST_SNOWFLAKE_CONN_STRING,
    db.Redshift: TEST_REDSHIFT_CONN_STRING,
    db.Oracle: TEST_ORACLE_CONN_STRING,
}

for k, v in CONN_STRINGS.items():
    if v is None:
        print(f"Warning: Connection to {k} not configured")

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
