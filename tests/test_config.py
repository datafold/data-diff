import os
import unittest

from data_diff.config import apply_config_from_string, ConfigParseError
from data_diff.utils import remove_password_from_url


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

    def test_remove_password(self):
        replace_with = "*****"
        urls = [
            "d://host/",
            "d://host:123/",
            "d://user@host:123/",
            "d://user:PASS@host:123/",
            "d://:PASS@host:123/",
            "d://:PASS@host:123/path",
            "d://:PASS@host:123/path?whatever#blabla",
        ]
        for url in urls:
            removed = remove_password_from_url(url, replace_with)
            expected = url.replace("PASS", replace_with)
            removed = remove_password_from_url(url, replace_with)
            self.assertEqual(removed, expected)

    def test_embed_env(self):
        env = {
            "DRIVER": "postgresql",
            "USER": "postgres",
            "PASSWORD": "Password1",
            "RUN_PG_1_DATABASE": "test_postgresql",
            "RUN_PG_1_TABLE": "rating",
            "RUN_PG_2_DATABASE": "postgresql://postgres:Password1@/",
            "RUN_PG_2_TABLE": "rating_del1",
        }
        config = r"""
            [database.test_postgresql]
            driver = "${DRIVER}"
            user = "${USER}"
            password = "${PASSWORD}"

            [run.default]
            update_column = "${UPDATE_COLUMN}"
            verbose = true
            threads = 2

            [run.pg_pg]
            threads = 4
            1.database = "${RUN_PG_1_DATABASE}"
            1.table = "${RUN_PG_1_TABLE}"
            1.threads = 11
            2.database = "${RUN_PG_2_DATABASE}"
            2.table = "${RUN_PG_2_TABLE}"
            2.threads = 22
        """

        os.environ.update(env)
        res = apply_config_from_string(config, "pg_pg", {})
        assert res["update_column"] == ""  # missing env var
        assert res["verbose"] is True
        assert res["threads"] == 4  # overwritten by pg_pg
        assert res["database1"] == {"driver": "postgresql", "user": "postgres", "password": "Password1"}
        assert res["database2"] == "postgresql://postgres:Password1@/"
        assert res["table1"] == "rating"
        assert res["table2"] == "rating_del1"
        assert res["threads1"] == 11
        assert res["threads2"] == 22
