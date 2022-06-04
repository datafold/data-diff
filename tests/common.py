import hashlib

from data_diff import database as db
import logging

logging.basicConfig(level=logging.WARN)

TEST_MYSQL_CONN_STRING: str = None
TEST_POSTGRES_CONN_STRING: str = None
TEST_SNOWFLAKE_CONN_STRING: str = None

try:
    from .local_settings import *
except ImportError:
    pass  # No local settings

assert TEST_MYSQL_CONN_STRING and TEST_POSTGRES_CONN_STRING and TEST_SNOWFLAKE_CONN_STRING

CONN_STRINGS = {
    db.MySQL: TEST_MYSQL_CONN_STRING,
    db.Postgres: TEST_POSTGRES_CONN_STRING,
    db.Snowflake: TEST_SNOWFLAKE_CONN_STRING,
}


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
