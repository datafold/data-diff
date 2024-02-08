import os
import unittest

from data_diff.cli_options import CliOptions
from data_diff.config import apply_config_from_string, ConfigParseError
from data_diff.utils import remove_password_from_url
from tests.common import get_cli_options


class TestConfig(unittest.TestCase):
    def test_basic(self):
        config = r"""
            [database.test_postgresql]
            driver = "postgresql"
            user = "postgres"
            password = "Password1"

            [run.default]
            update_column = "timestamp"
            key_columns = ["id"]
            columns = ["name", "age"]
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
        cli_options: CliOptions = get_cli_options()
        self.assertRaises(ConfigParseError, apply_config_from_string, config, "bla", cli_options)  # No such run

        cli_options: CliOptions = get_cli_options()
        print(config)
        apply_config_from_string(config, "pg_pg", cli_options)
        assert cli_options.update_column == "timestamp"  # default
        assert cli_options.verbose is True
        print(cli_options.threads)
        assert cli_options.threads == 4  # overwritten by pg_pg
        assert cli_options.database1 == {"driver": "postgresql", "user": "postgres", "password": "Password1"}
        assert cli_options.database2 == "postgresql://postgres:Password1@/"
        assert cli_options.table1 == "rating"
        assert cli_options.table2 == "rating_del1"
        assert cli_options.threads1 == 11
        assert cli_options.threads2 == 22
        assert cli_options.key_columns == ("id",)
        assert cli_options.columns == ("name", "age")

        cli_options: CliOptions = get_cli_options()
        cli_options.update_column = "foo"
        cli_options.table2 = "bar"
        apply_config_from_string(config, "pg_pg", cli_options)
        assert cli_options.update_column == "foo"
        assert cli_options.table2 == "bar"

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
        cli_options: CliOptions = get_cli_options()
        apply_config_from_string(config, "pg_pg", cli_options)
        assert cli_options.update_column == ""  # missing env var
        assert cli_options.verbose is True
        assert cli_options.threads == 4  # overwritten by pg_pg
        assert cli_options.database1 == {"driver": "postgresql", "user": "postgres", "password": "Password1"}
        assert cli_options.database2 == "postgresql://postgres:Password1@/"
        assert cli_options.table1 == "rating"
        assert cli_options.table2 == "rating_del1"
        assert cli_options.threads1 == 11
        assert cli_options.threads2 == 22
