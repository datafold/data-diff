import unittest

from data_diff.config import apply_config_from_string, ConfigParseError


class TestConfig(unittest.TestCase):
    def test_basic(self):
        config = r"""
            [database.test_postgresql]
            driver = "postgresql"
            user = "postgres"
            password = "Password1"

            [run.default]
            update_column = "timestamp"
            verbose = true
            threads = 2

            [run.pg_pg]
            threads = 4
            1.database = "test_postgresql"
            1.table = "rating"
            1.threads = 11
            2.database = "postgresql://postgres:Password1@/"
            2.table = "rating_del1"
            2.threads = 22
        """
        self.assertRaises(ConfigParseError, apply_config_from_string, config, "bla", {})  # No such run

        res = apply_config_from_string(config, "pg_pg", {})
        assert res["update_column"] == "timestamp"  # default
        assert res["verbose"] is True
        assert res["threads"] == 4  # overwritten by pg_pg
        assert res["database1"] == {"driver": "postgresql", "user": "postgres", "password": "Password1"}
        assert res["database2"] == "postgresql://postgres:Password1@/"
        assert res["table1"] == "rating"
        assert res["table2"] == "rating_del1"
        assert res["threads1"] == 11
        assert res["threads2"] == 22

        res = apply_config_from_string(config, "pg_pg", {"update_column": "foo", "table2": "bar"})
        assert res["update_column"] == "foo"
        assert res["table2"] == "bar"
