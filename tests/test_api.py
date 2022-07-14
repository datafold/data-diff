import unittest
import preql
import arrow

from data_diff import diff_tables, connect_to_table

from .common import TEST_MYSQL_CONN_STRING


class TestApi(unittest.TestCase):
    def setUp(self) -> None:
        self.preql = preql.Preql(TEST_MYSQL_CONN_STRING)
        self.preql(
            r"""
            table test_api {
                datetime: datetime
                comment: string
            }
            commit()

            func add(date, comment) {
                new test_api(date, comment)
            }
        """
        )
        self.now = now = arrow.get(self.preql.now())
        self.preql.add(now, "now")
        self.preql.add(now, self.now.shift(seconds=-10))
        self.preql.add(now, self.now.shift(seconds=-7))
        self.preql.add(now, self.now.shift(seconds=-6))

        self.preql(
            r"""
            const table test_api_2 = test_api
            commit()
        """
        )

        self.preql.add(self.now.shift(seconds=-3), "3 seconds ago")
        self.preql.commit()

    def tearDown(self) -> None:
        self.preql.run_statement("drop table if exists test_api")
        self.preql.run_statement("drop table if exists test_api_2")
        self.preql.commit()
        self.preql.close()

        return super().tearDown()

    def test_api(self):
        t1 = connect_to_table(TEST_MYSQL_CONN_STRING, "test_api")
        t2 = connect_to_table(TEST_MYSQL_CONN_STRING, ("test_api_2",))
        diff = list(diff_tables(t1, t2))
        assert len(diff) == 1

        t1.database.close()
        t2.database.close()

        # test where
        diff_id = diff[0][1][0]
        where = f"id != {diff_id}"

        t1 = connect_to_table(TEST_MYSQL_CONN_STRING, "test_api", where=where)
        t2 = connect_to_table(TEST_MYSQL_CONN_STRING, "test_api_2", where=where)
        diff = list(diff_tables(t1, t2))
        assert len(diff) == 0

        t1.database.close()
        t2.database.close()
