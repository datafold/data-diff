import hashlib

from data_diff.database import CHECKSUM_HEXDIGITS, MD5_HEXDIGITS
import logging

logging.basicConfig(level=logging.WARN)

TEST_MYSQL_CONN_STRING = "mysql://mysql:Password1@localhost/mysql"

def str_to_checksum(str: str):
    # hello world
    #   => 5eb63bbbe01eeed093cb22bb8f5acdc3
    #   =>                   cb22bb8f5acdc3
    #   => 273350391345368515
    m = hashlib.md5()
    m.update(str.encode('utf-8'))  # encode to binary
    md5 = m.hexdigest()
    # 0-indexed, unlike DBs which are 1-indexed here, so +1 in dbs
    half_pos = MD5_HEXDIGITS - CHECKSUM_HEXDIGITS
    return int(md5[half_pos:], 16)
