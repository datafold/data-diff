import os

from pathlib import Path
import yaml
from data_diff.diff_tables import Algorithm
from .test_cli import run_datadiff_cli

from data_diff.dbt import (
    _get_diff_vars,
    dbt_diff,
    _local_diff,
    _cloud_diff,
    DbtParser,
    RUN_RESULTS_PATH,
    MANIFEST_PATH,
    PROJECT_FILE,
    DiffVars,
)
import unittest
from unittest.mock import MagicMock, Mock, mock_open, patch, ANY


class TestDbtParser(unittest.TestCase):
    def test_get_datadiff_variables(self):
        expected_dict = {"some_key": "some_value"}
        full_dict = {"vars": {"data_diff": expected_dict}}

        mock_self = Mock()
        mock_self.project_dict = full_dict
        returned_dict = DbtParser.get_datadiff_variables(mock_self)

        self.assertEqual(expected_dict, returned_dict)

    def test_get_datadiff_variables_none(self):
        none_dict = None

        mock_self = Mock()
        mock_self.project_dict = none_dict

        with self.assertRaises(Exception):
            DbtParser.get_datadiff_variables(mock_self)

    def test_get_datadiff_variables_empty(self):
        empty_dict = {}

        mock_self = Mock()
        mock_self.project_dict = empty_dict

        with self.assertRaises(Exception):
            DbtParser.get_datadiff_variables(mock_self)

    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    def test_get_models(self, mock_open):
        mock_model = {"success_unique_id": "expected_value"}
        mock_self = Mock()
        mock_self.project_dir = Path()
        mock_run_results = Mock()
        mock_success_result = Mock()
        mock_failed_result = Mock()
        mock_self.parse_run_results.return_value = mock_run_results
        mock_run_results.metadata.dbt_version = "1.0.0"
        mock_success_result.unique_id = "success_unique_id"
        mock_failed_result.unique_id = "failed_unique_id"
        mock_success_result.status.name = "success"
        mock_failed_result.status.name = "failed"
        mock_run_results.results = [mock_success_result, mock_failed_result]
        mock_self.manifest_obj.nodes.get.return_value = mock_model

        models = DbtParser.get_models(mock_self)

        self.assertEqual(mock_model, models[0])
        mock_open.assert_any_call(Path(RUN_RESULTS_PATH))
        mock_self.parse_run_results.assert_called_once_with(run_results={})

    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    def test_get_models_bad_lower_dbt_version(self, mock_open):
        mock_self = Mock()
        mock_self.project_dir = Path()
        mock_run_results = Mock()
        mock_self.parse_run_results.return_value = mock_run_results
        mock_run_results.metadata.dbt_version = "0.19.0"

        with self.assertRaises(Exception) as ex:
            DbtParser.get_models(mock_self)

        mock_open.assert_called_once_with(Path(RUN_RESULTS_PATH))
        mock_self.parse_run_results.assert_called_once_with(run_results={})
        mock_self.parse_manifest.assert_not_called()
        self.assertIn("version to be", ex.exception.args[0])

    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    def test_get_models_bad_upper_dbt_version(self, mock_open):
        mock_self = Mock()
        mock_self.project_dir = Path()
        mock_run_results = Mock()
        mock_self.parse_run_results.return_value = mock_run_results
        mock_run_results.metadata.dbt_version = "1.5.1"

        with self.assertRaises(Exception) as ex:
            DbtParser.get_models(mock_self)

        mock_open.assert_called_once_with(Path(RUN_RESULTS_PATH))
        mock_self.parse_run_results.assert_called_once_with(run_results={})
        mock_self.parse_manifest.assert_not_called()
        self.assertIn("version to be", ex.exception.args[0])

    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    def test_get_models_no_success(self, mock_open):
        mock_self = Mock()
        mock_self.project_dir = Path()
        mock_run_results = Mock()
        mock_success_result = Mock()
        mock_failed_result = Mock()
        mock_self.parse_run_results.return_value = mock_run_results
        mock_run_results.metadata.dbt_version = "1.0.0"
        mock_failed_result.unique_id = "failed_unique_id"
        mock_success_result.status.name = "success"
        mock_failed_result.status.name = "failed"
        mock_run_results.results = [mock_failed_result]

        with self.assertRaises(Exception):
            DbtParser.get_models(mock_self)

        mock_open.assert_any_call(Path(RUN_RESULTS_PATH))
        mock_self.parse_run_results.assert_called_once_with(run_results={})

    @patch("builtins.open", new_callable=mock_open, read_data="key:\n  value")
    def test_get_project_dict(self, mock_open):
        expected_dict = {"key1": "value1"}
        mock_self = Mock()

        mock_self.project_dir = Path()
        mock_self.yaml.safe_load.return_value = expected_dict
        project_dict = DbtParser.get_project_dict(mock_self)

        self.assertEqual(project_dict, expected_dict)
        mock_open.assert_called_once_with(Path(PROJECT_FILE))

    def test_set_connection_snowflake_success_password(self):
        expected_driver = "snowflake"
        expected_credentials = {"user": "user", "password": "password"}
        mock_self = Mock()
        mock_self._get_connection_creds.return_value = (expected_credentials, expected_driver)

        DbtParser.set_connection(mock_self)

        self.assertIsInstance(mock_self.connection, dict)
        self.assertEqual(mock_self.connection.get("driver"), expected_driver)
        self.assertEqual(mock_self.connection.get("user"), expected_credentials["user"])
        self.assertEqual(mock_self.connection.get("password"), expected_credentials["password"])
        self.assertEqual(mock_self.connection.get("key"), None)
        self.assertEqual(mock_self.requires_upper, True)

    def test_set_connection_snowflake_success_key(self):
        expected_driver = "snowflake"
        expected_credentials = {"user": "user", "private_key_path": "private_key_path"}
        mock_self = Mock()
        mock_self._get_connection_creds.return_value = (expected_credentials, expected_driver)

        DbtParser.set_connection(mock_self)

        self.assertIsInstance(mock_self.connection, dict)
        self.assertEqual(mock_self.connection.get("driver"), expected_driver)
        self.assertEqual(mock_self.connection.get("user"), expected_credentials["user"])
        self.assertEqual(mock_self.connection.get("password"), None)
        self.assertEqual(mock_self.connection.get("key"), expected_credentials["private_key_path"])
        self.assertEqual(mock_self.requires_upper, True)

    def test_set_connection_snowflake_success_key_and_passphrase(self):
        expected_driver = "snowflake"
        expected_credentials = {"user": "user", "private_key_path": "private_key_path", "private_key_passphrase": "private_key_passphrase"}
        mock_self = Mock()
        mock_self._get_connection_creds.return_value = (expected_credentials, expected_driver)

        DbtParser.set_connection(mock_self)

        self.assertIsInstance(mock_self.connection, dict)
        self.assertEqual(mock_self.connection.get("driver"), expected_driver)
        self.assertEqual(mock_self.connection.get("user"), expected_credentials["user"])
        self.assertEqual(mock_self.connection.get("password"), None)
        self.assertEqual(mock_self.connection.get("key"), expected_credentials["private_key_path"])
        self.assertEqual(mock_self.connection.get("private_key_passphrase"), expected_credentials["private_key_passphrase"])
        self.assertEqual(mock_self.requires_upper, True)

    def test_set_connection_snowflake_no_key_or_password(self):
        expected_driver = "snowflake"
        expected_credentials = {"user": "user"}
        mock_self = Mock()
        mock_self._get_connection_creds.return_value = (expected_credentials, expected_driver)

        with self.assertRaises(Exception):
            DbtParser.set_connection(mock_self)

        self.assertNotIsInstance(mock_self.connection, dict)
    
    def test_set_connection_snowflake_authenticator(self):
        expected_driver = "snowflake"
        expected_credentials = {"user": "user", "authenticator": "authenticator"}
        mock_self = Mock()
        mock_self._get_connection_creds.return_value = (expected_credentials, expected_driver)

        with self.assertRaises(Exception):
            DbtParser.set_connection(mock_self)

        self.assertNotIsInstance(mock_self.connection, dict)

    def test_set_connection_snowflake_key_and_password(self):
        expected_driver = "snowflake"
        expected_credentials = {"user": "user", "private_key_path": "private_key_path", "password": "password"}
        mock_self = Mock()
        mock_self._get_connection_creds.return_value = (expected_credentials, expected_driver)

        with self.assertRaises(Exception):
            DbtParser.set_connection(mock_self)

        self.assertNotIsInstance(mock_self.connection, dict)

    def test_set_connection_bigquery_success(self):
        expected_driver = "bigquery"
        expected_credentials = {
            "method": "oauth",
            "project": "a_project",
            "dataset": "a_dataset",
        }
        mock_self = Mock()
        mock_self._get_connection_creds.return_value = (expected_credentials, expected_driver)

        DbtParser.set_connection(mock_self)

        self.assertIsInstance(mock_self.connection, dict)
        self.assertEqual(mock_self.connection.get("driver"), expected_driver)
        self.assertEqual(mock_self.connection.get("project"), expected_credentials["project"])
        self.assertEqual(mock_self.connection.get("dataset"), expected_credentials["dataset"])

    def test_set_connection_bigquery_not_oauth(self):
        expected_driver = "bigquery"
        expected_credentials = {
            "method": "not_oauth",
            "project": "a_project",
            "dataset": "a_dataset",
        }

        mock_self = Mock()
        mock_self._get_connection_creds.return_value = (expected_credentials, expected_driver)
        with self.assertRaises(Exception):
            DbtParser.set_connection(mock_self)

        self.assertNotIsInstance(mock_self.connection, dict)

    def test_set_connection_not_implemented(self):
        expected_driver = "unimplemented_provider"

        mock_self = Mock()
        mock_self._get_connection_creds.return_value = (None, expected_driver)
        with self.assertRaises(NotImplementedError):
            DbtParser.set_connection(mock_self)

        self.assertNotIsInstance(mock_self.connection, dict)

    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_connection_creds_success(self, mock_open):
        profiles_dict = {
            "a_profile": {
                "outputs": {
                    "a_target": {"type": "TYPE1", "credential_1": "credential_1", "credential_2": "credential_2"}
                },
                "target": "a_target",
            }
        }
        profile = profiles_dict["a_profile"]
        expected_credentials = profiles_dict["a_profile"]["outputs"]["a_target"]
        mock_self = Mock()
        mock_self.profiles_dir = Path()
        mock_self.project_dict = {"profile": "a_profile"}
        mock_self.yaml.safe_load.return_value = profiles_dict
        mock_self.ProfileRenderer().render_data.return_value = profile
        credentials, conn_type = DbtParser._get_connection_creds(mock_self)
        self.assertEqual(credentials, expected_credentials)
        self.assertEqual(conn_type, "type1")

    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_connection_no_matching_profile(self, mock_open):
        profiles_dict = {"a_profile": {}}
        mock_self = Mock()
        mock_self.profiles_dir = Path()
        mock_self.project_dict = {"profile": "wrong_profile"}
        mock_self.yaml.safe_load.return_value = profiles_dict
        profile = profiles_dict["a_profile"]
        mock_self.ProfileRenderer().render_data.return_value = profile
        with self.assertRaises(ValueError):
            _, _ = DbtParser._get_connection_creds(mock_self)

    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_connection_no_target(self, mock_open):
        profiles_dict = {
            "a_profile": {
                "outputs": {
                    "a_target": {"type": "TYPE1", "credential_1": "credential_1", "credential_2": "credential_2"}
                },
            }
        }
        mock_self = Mock()
        mock_self.profiles_dir = Path()
        profile = profiles_dict["a_profile"]
        mock_self.ProfileRenderer().render_data.return_value = profile
        mock_self.project_dict = {"profile": "a_profile"}
        mock_self.yaml.safe_load.return_value = profiles_dict
        with self.assertRaises(ValueError):
            _, _ = DbtParser._get_connection_creds(mock_self)

    profile_yaml_no_outputs = """
    a_profile:
      target: a_target
    """

    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_connection_no_outputs(self, mock_open):
        profiles_dict = {"a_profile": {"target": "a_target"}}
        mock_self = Mock()
        mock_self.profiles_dir = Path()
        mock_self.project_dict = {"profile": "a_profile"}
        profile = profiles_dict["a_profile"]
        mock_self.ProfileRenderer().render_data.return_value = profile
        mock_self.yaml.safe_load.return_value = profiles_dict
        with self.assertRaises(ValueError):
            _, _ = DbtParser._get_connection_creds(mock_self)

    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_connection_no_credentials(self, mock_open):
        profiles_dict = {
            "a_profile": {
                "outputs": {"a_target": {}},
                "target": "a_target",
            }
        }
        mock_self = Mock()
        mock_self.profiles_dir = Path()
        mock_self.project_dict = {"profile": "a_profile"}
        mock_self.yaml.safe_load.return_value = profiles_dict
        profile = profiles_dict["a_profile"]
        mock_self.ProfileRenderer().render_data.return_value = profile
        with self.assertRaises(ValueError):
            _, _ = DbtParser._get_connection_creds(mock_self)

    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_connection_no_target_credentials(self, mock_open):
        profiles_dict = {
            "a_profile": {
                "outputs": {
                    "a_target": {"type": "TYPE1", "credential_1": "credential_1", "credential_2": "credential_2"}
                },
                "target": "a_different_target",
            }
        }
        mock_self = Mock()
        mock_self.profiles_dir = Path()
        mock_self.project_dict = {"profile": "a_profile"}
        profile = profiles_dict["a_profile"]
        mock_self.ProfileRenderer().render_data.return_value = profile
        mock_self.yaml.safe_load.return_value = profiles_dict
        with self.assertRaises(ValueError):
            _, _ = DbtParser._get_connection_creds(mock_self)

    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_connection_no_type(self, mock_open):
        profiles_dict = {
            "a_profile": {
                "outputs": {"a_target": {"credential_1": "credential_1", "credential_2": "credential_2"}},
                "target": "a_target",
            }
        }
        mock_self = Mock()
        mock_self.profiles_dir = Path()
        mock_self.project_dict = {"profile": "a_profile"}
        mock_self.yaml.safe_load.return_value = profiles_dict
        profile = profiles_dict["a_profile"]
        mock_self.ProfileRenderer().render_data.return_value = profile
        with self.assertRaises(ValueError):
            _, _ = DbtParser._get_connection_creds(mock_self)


EXAMPLE_DIFF_RESULTS = {
    "pks": {"exclusives": [5, 3]},
    "values": {
        "rows_with_differences": 2,
        "total_rows": 10,
        "columns_diff_stats": [
            {"column_name": "name", "match": 80.0},
            {"column_name": "age", "match": 100.0},
            {"column_name": "city", "match": 0.0},
            {"column_name": "country", "match": 100.0},
        ],
    },
}


class TestDbtDiffer(unittest.TestCase):
    # Set DATA_DIFF_DBT_PROJ to use your own dbt project, otherwise uses the duckdb project in tests/dbt_artifacts
    def test_integration_basic_dbt(self):
        artifacts_path = os.getcwd() + "/tests/dbt_artifacts"
        test_project_path = os.environ.get("DATA_DIFF_DBT_PROJ") or artifacts_path
        diff = run_datadiff_cli(
            "--dbt", "--dbt-project-dir", test_project_path, "--dbt-profiles-dir", test_project_path
        )
        assert "Diffs Complete!" in "\n".join(d.decode("utf-8") for d in diff)

        # assertions for the diff that exists in tests/dbt_artifacts/jaffle_shop.duckdb
        if test_project_path == artifacts_path:
            diff_string = b"".join(diff).decode("utf-8")
            # 5 diffs were ran
            assert diff_string.count("<>") == 5
            # 4 with no diffs
            assert diff_string.count("No row differences") == 4
            # 1 with a diff
            assert diff_string.count("| Rows Added    | Rows Removed") == 1

    def test_integration_cloud_dbt(self):
        project_dir = os.environ.get("DATA_DIFF_DBT_PROJ")
        if project_dir is not None:
            diff = run_datadiff_cli("--dbt", "--cloud", "--dbt-project-dir", project_dir)
            assert diff[-1].decode("utf-8") == "Diffs Complete!"
        else:
            pass

    @patch("data_diff.dbt.diff_tables")
    def test_local_diff(self, mock_diff_tables):
        mock_connection = Mock()
        mock_table1 = Mock()
        column_set = {"col1", "col2"}
        mock_table1.get_schema.return_value = column_set
        mock_table2 = Mock()
        mock_table2.get_schema.return_value = column_set
        mock_diff = MagicMock()
        mock_diff_tables.return_value = mock_diff
        mock_diff.__iter__.return_value = [1, 2, 3]
        dev_qualified_list = ["dev_db", "dev_schema", "dev_table"]
        prod_qualified_list = ["prod_db", "prod_schema", "prod_table"]
        expected_keys = ["key"]
        diff_vars = DiffVars(dev_qualified_list, prod_qualified_list, expected_keys, mock_connection, None)
        with patch("data_diff.dbt.connect_to_table", side_effect=[mock_table1, mock_table2]) as mock_connect:
            _local_diff(diff_vars)

        mock_diff_tables.assert_called_once_with(
            mock_table1, mock_table2, threaded=True, algorithm=Algorithm.JOINDIFF, extra_columns=ANY
        )
        self.assertEqual(len(mock_diff_tables.call_args[1]["extra_columns"]), 2)
        self.assertEqual(mock_connect.call_count, 2)
        mock_connect.assert_any_call(mock_connection, ".".join(dev_qualified_list), tuple(expected_keys), None)
        mock_connect.assert_any_call(mock_connection, ".".join(prod_qualified_list), tuple(expected_keys), None)
        mock_diff.get_stats_string.assert_called_once()

    @patch("data_diff.dbt.diff_tables")
    def test_local_diff_no_diffs(self, mock_diff_tables):
        mock_connection = Mock()
        column_set = {"col1", "col2"}
        mock_table1 = Mock()
        mock_table1.get_schema.return_value = column_set
        mock_table2 = Mock()
        mock_table2.get_schema.return_value = column_set
        mock_diff = MagicMock()
        mock_diff_tables.return_value = mock_diff
        mock_diff.__iter__.return_value = []
        dev_qualified_list = ["dev_db", "dev_schema", "dev_table"]
        prod_qualified_list = ["prod_db", "prod_schema", "prod_table"]
        expected_keys = ["primary_key_column"]
        diff_vars = DiffVars(dev_qualified_list, prod_qualified_list, expected_keys, mock_connection, None)
        with patch("data_diff.dbt.connect_to_table", side_effect=[mock_table1, mock_table2]) as mock_connect:
            _local_diff(diff_vars)

        mock_diff_tables.assert_called_once_with(
            mock_table1, mock_table2, threaded=True, algorithm=Algorithm.JOINDIFF, extra_columns=ANY
        )
        self.assertEqual(len(mock_diff_tables.call_args[1]["extra_columns"]), 2)
        self.assertEqual(mock_connect.call_count, 2)
        mock_connect.assert_any_call(mock_connection, ".".join(dev_qualified_list), tuple(expected_keys), None)
        mock_connect.assert_any_call(mock_connection, ".".join(prod_qualified_list), tuple(expected_keys), None)
        mock_diff.get_stats_string.assert_not_called()

    @patch("data_diff.dbt._cloud_poll_and_get_summary_results")
    @patch("data_diff.dbt._cloud_submit_diff")
    @patch("data_diff.dbt.rich.print")
    def test_cloud_diff(self, mock_print, mock_submit_diff, mock_poll_results):
        mock_submit_diff.return_value = 123
        mock_poll_results.return_value = EXAMPLE_DIFF_RESULTS
        dev_qualified_list = ["dev_db", "dev_schema", "dev_table"]
        prod_qualified_list = ["prod_db", "prod_schema", "prod_table"]
        datasource_id = 1
        expected_primary_keys = ["primary_key_column"]
        diff_vars = DiffVars(dev_qualified_list, prod_qualified_list, expected_primary_keys, None, None)
        datafold_host = "a_host"
        url = "a_url"
        api_key = "an_api_key"
        _cloud_diff(diff_vars, datasource_id, datafold_host, url, api_key)

        mock_print.assert_called_once()
        mock_submit_diff.assert_called_once()
        mock_poll_results.assert_called_once()

    @patch("data_diff.dbt._setup_cloud_diff")
    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    def test_diff_is_cloud(
        self, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars, mock_setup_cloud_diff
    ):
        mock_dbt_parser_inst = Mock()
        mock_model = Mock()
        expected_dbt_vars_dict = {
            "prod_database": "prod_db",
            "prod_schema": "prod_schema",
            "datasource_id": 1,
        }
        host = "a_host"
        url = "a_url"
        api_key = "a_api_key"
        setup_tuple = (host, url, api_key)
        mock_setup_cloud_diff.return_value = setup_tuple

        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_variables.return_value = expected_dbt_vars_dict
        expected_diff_vars = DiffVars(["dev"], ["prod"], ["pks"], None, None)
        mock_get_diff_vars.return_value = expected_diff_vars
        dbt_diff(is_cloud=True)
        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_not_called()

        mock_setup_cloud_diff.assert_called_once()
        mock_cloud_diff.assert_called_once_with(expected_diff_vars, 1, host, url, api_key)
        mock_local_diff.assert_not_called()
        mock_print.assert_called_once()

    @patch("data_diff.dbt._setup_cloud_diff")
    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    def test_diff_is_cloud_no_ds_id(
        self, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars, mock_setup_cloud_diff
    ):
        mock_dbt_parser_inst = Mock()
        mock_model = Mock()
        expected_dbt_vars_dict = {
            "prod_database": "prod_db",
            "prod_schema": "prod_schema",
        }
        host = "a_host"
        url = "a_url"
        api_key = "a_api_key"
        mock_setup_cloud_diff.return_value = (host, url, api_key)

        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_variables.return_value = expected_dbt_vars_dict
        expected_diff_vars = DiffVars(["dev"], ["prod"], ["pks"], None, None)
        mock_get_diff_vars.return_value = expected_diff_vars
        with self.assertRaises(ValueError):
            dbt_diff(is_cloud=True)
        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_not_called()

        mock_setup_cloud_diff.assert_not_called()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_not_called()
        mock_print.assert_not_called()

    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    def test_diff_is_not_cloud(self, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars):
        mock_dbt_parser_inst = Mock()
        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_model = Mock()
        expected_dbt_vars_dict = {
            "prod_database": "prod_db",
            "prod_schema": "prod_schema",
        }
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_variables.return_value = expected_dbt_vars_dict
        expected_diff_vars = DiffVars(["dev"], ["prod"], ["pks"], None, None)
        mock_get_diff_vars.return_value = expected_diff_vars
        dbt_diff(is_cloud=False)

        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_called_once()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_called_once_with(expected_diff_vars)
        mock_print.assert_called_once()

    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    def test_diff_no_prod_configs(
        self, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars
    ):
        mock_dbt_parser_inst = Mock()
        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_model = Mock()
        expected_dbt_vars_dict = {
            "datasource_id": 1,
        }

        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_variables.return_value = expected_dbt_vars_dict
        expected_diff_vars = DiffVars(["dev"], ["prod"], ["pks"], None, None)
        mock_get_diff_vars.return_value = expected_diff_vars
        with self.assertRaises(ValueError):
            dbt_diff(is_cloud=False)

        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_called_once()
        mock_dbt_parser_inst.get_primary_keys.assert_not_called()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_not_called()
        mock_print.assert_not_called()

    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    def test_diff_only_prod_db(self, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars):
        mock_dbt_parser_inst = Mock()
        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_model = Mock()
        expected_dbt_vars_dict = {
            "prod_database": "prod_db",
            "datasource_id": 1,
        }
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_variables.return_value = expected_dbt_vars_dict
        expected_diff_vars = DiffVars(["dev"], ["prod"], ["pks"], None, None)
        mock_get_diff_vars.return_value = expected_diff_vars
        dbt_diff(is_cloud=False)

        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_called_once()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_called_once_with(expected_diff_vars)
        mock_print.assert_called_once()

    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    def test_diff_only_prod_schema(
        self, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars
    ):
        mock_dbt_parser_inst = Mock()
        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_model = Mock()
        expected_dbt_vars_dict = {
            "datasource_id": 1,
            "prod_schema": "prod_schema",
        }

        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_variables.return_value = expected_dbt_vars_dict
        expected_diff_vars = DiffVars(["dev"], ["prod"], ["pks"], None, None)
        mock_get_diff_vars.return_value = expected_diff_vars
        with self.assertRaises(ValueError):
            dbt_diff(is_cloud=False)

        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_called_once()
        mock_dbt_parser_inst.get_primary_keys.assert_not_called()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_not_called()
        mock_print.assert_not_called()

    @patch("data_diff.dbt._setup_cloud_diff")
    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    def test_diff_is_cloud_no_pks(
        self, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars, mock_setup_cloud_diff
    ):
        mock_dbt_parser_inst = Mock()
        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_model = Mock()
        expected_dbt_vars_dict = {
            "prod_database": "prod_db",
            "prod_schema": "prod_schema",
            "datasource_id": 1,
        }
        host = "a_host"
        url = "a_url"
        api_key = "a_api_key"
        mock_setup_cloud_diff.return_value = (host, url, api_key)

        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_variables.return_value = expected_dbt_vars_dict
        expected_diff_vars = DiffVars(["dev"], ["prod"], [], None, None)
        mock_get_diff_vars.return_value = expected_diff_vars
        dbt_diff(is_cloud=True)

        mock_setup_cloud_diff.assert_called_once()
        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_not_called()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_not_called()
        self.assertEqual(mock_print.call_count, 2)

    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    def test_diff_not_is_cloud_no_pks(
        self, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars
    ):
        mock_dbt_parser_inst = Mock()
        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_model = Mock()
        expected_dbt_vars_dict = {
            "prod_database": "prod_db",
            "prod_schema": "prod_schema",
            "datasource_id": 1,
        }
        host = "a_host"
        url = "a_url"
        api_key = "a_api_key"

        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_variables.return_value = expected_dbt_vars_dict

        expected_diff_vars = DiffVars(["dev"], ["prod"], [], None, None)
        mock_get_diff_vars.return_value = expected_diff_vars
        dbt_diff(is_cloud=False)
        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_called_once()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_not_called()
        self.assertEqual(mock_print.call_count, 2)

    def test_get_diff_vars_custom_schemas_prod_db_and_schema(self):
        mock_model = Mock()
        prod_database = "a_prod_db"
        prod_schema = "a_prod_schema"
        primary_keys = ["a_primary_key"]
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_custom_dev_schema"
        mock_model.config.schema_ = mock_model.schema_
        mock_model.alias = "a_model_name"
        mock_dbt_parser = Mock()
        mock_dbt_parser.get_pk_from_model.return_value = primary_keys
        mock_dbt_parser.requires_upper = False

        diff_vars = _get_diff_vars(mock_dbt_parser, "a_prod_db", "a_prod_schema", mock_model, custom_schemas=True)

        assert diff_vars.dev_path == [mock_model.database, mock_model.schema_, mock_model.alias]
        assert diff_vars.prod_path == [prod_database, prod_schema + "_" + mock_model.schema_, mock_model.alias]
        assert diff_vars.primary_keys == primary_keys
        assert diff_vars.connection == mock_dbt_parser.connection
        assert diff_vars.threads == mock_dbt_parser.threads
        mock_dbt_parser.get_pk_from_model.assert_called_once()

    def test_get_diff_vars_false_custom_schemas_prod_db_and_schema(self):
        mock_model = Mock()
        prod_database = "a_prod_db"
        prod_schema = "a_prod_schema"
        primary_keys = ["a_primary_key"]
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_custom_dev_schema"
        mock_model.config.schema_ = mock_model.schema_
        mock_model.alias = "a_model_name"
        mock_dbt_parser = Mock()
        mock_dbt_parser.get_pk_from_model.return_value = ["a_primary_key"]
        mock_dbt_parser.requires_upper = False

        diff_vars = _get_diff_vars(mock_dbt_parser, "a_prod_db", "a_prod_schema", mock_model, custom_schemas=False)

        assert diff_vars.dev_path == [mock_model.database, mock_model.schema_, mock_model.alias]
        assert diff_vars.prod_path == [prod_database, prod_schema, mock_model.alias]
        assert diff_vars.primary_keys == primary_keys
        assert diff_vars.connection == mock_dbt_parser.connection
        assert diff_vars.threads == mock_dbt_parser.threads
        mock_dbt_parser.get_pk_from_model.assert_called_once()

    def test_get_diff_vars_false_custom_schemas_prod_db(self):
        mock_model = Mock()
        prod_database = "a_prod_db"
        primary_keys = ["a_primary_key"]
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_custom_dev_schema"
        mock_model.config.schema_ = mock_model.schema_
        mock_model.alias = "a_model_name"
        mock_dbt_parser = Mock()
        mock_dbt_parser.get_pk_from_model.return_value = ["a_primary_key"]
        mock_dbt_parser.requires_upper = False

        diff_vars = _get_diff_vars(mock_dbt_parser, "a_prod_db", None, mock_model, custom_schemas=False)

        assert diff_vars.dev_path == [mock_model.database, mock_model.schema_, mock_model.alias]
        assert diff_vars.prod_path == [prod_database, mock_model.schema_, mock_model.alias]
        assert diff_vars.primary_keys == primary_keys
        assert diff_vars.connection == mock_dbt_parser.connection
        assert diff_vars.threads == mock_dbt_parser.threads
        mock_dbt_parser.get_pk_from_model.assert_called_once()
