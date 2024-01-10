import unittest

from data_diff import Database
from data_diff import databases as db
from data_diff.__main__ import _get_dbs
from tests.common import CONN_STRINGS


class TestMain(unittest.TestCase):
    def test__get_dbs(self):
        db1: Database
        db2: Database
        db1_str: str = CONN_STRINGS[db.PostgreSQL]
        db2_str: str = CONN_STRINGS[db.PostgreSQL]

        # no threads and 2 threads1 with no interactive
        db1, db2 = _get_dbs(0, db1_str, 2, db2_str, 0, False)
        with db1, db2:
            assert db1 == db2
            assert db1.thread_count == 2
            assert not db1._interactive

        # 3 threads and 0 threads1 with interactive
        db1, db2 = _get_dbs(3, db1_str, 0, db2_str, 0, True)
        with db1, db2:
            assert db1 == db2
            assert db1.thread_count == 3
            assert db1._interactive

        db2_str: str = CONN_STRINGS[db.MySQL]

        # no threads and 1 threads1 and 2 thread2 with no interactive
        db1, db2 = _get_dbs(0, db1_str, 1, db2_str, 2, False)
        with db1, db2:
            assert db1 != db2
            assert db1.thread_count == 1
            assert db2.thread_count == 2
            assert not db1._interactive

        # 3 threads and 0 threads1 and 0 thread2 with interactive
        db1, db2 = _get_dbs(3, db1_str, 0, db2_str, 0, True)
        with db1, db2:
            assert db1 != db2
            assert db1.thread_count == 3
            assert db2.thread_count == 3
            assert db1.thread_count == db2.thread_count
            assert db1._interactive
