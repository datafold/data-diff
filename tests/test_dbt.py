import json
import os

from data_diff.diff_tables import Algorithm
from .test_cli import run_datadiff_cli

from data_diff.dbt import (
    dbt_diff,
    _local_diff,
    _cloud_diff,
    DbtParser,
    RUN_RESULTS_PATH,
    MANIFEST_PATH,
    PROFILES_FILE,
    PROJECT_FILE,
    DiffVars,
)
import unittest
from unittest.mock import MagicMock, Mock, mock_open, patch


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
    @patch("data_diff.dbt.parse_run_results")
    @patch("data_diff.dbt.parse_manifest")
    def test_get_models(self, mock_manifest_parser, mock_run_parser, mock_open):
        expected_value = "expected_value"
        mock_self = Mock()
        mock_self.project_dir = ""
        mock_run_results = Mock()
        mock_success_result = Mock()
        mock_failed_result = Mock()
        mock_manifest = Mock()
        mock_run_parser.return_value = mock_run_results
        mock_run_results.metadata.dbt_version = "1.0.0"
        mock_success_result.unique_id = "success_unique_id"
        mock_failed_result.unique_id = "failed_unique_id"
        mock_success_result.status.name = "success"
        mock_failed_result.status.name = "failed"
        mock_run_results.results = [mock_success_result, mock_failed_result]
        mock_manifest_parser.return_value = mock_manifest
        mock_manifest.nodes = {"success_unique_id": expected_value}

        models = DbtParser.get_models(mock_self)

        self.assertEqual(expected_value, models[0])
        mock_open.assert_any_call(RUN_RESULTS_PATH)
        mock_open.assert_any_call(MANIFEST_PATH)
        mock_run_parser.assert_called_once_with(run_results={})
        mock_manifest_parser.assert_called_once_with(manifest={})

    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    @patch("data_diff.dbt.parse_run_results")
    @patch("data_diff.dbt.parse_manifest")
    def test_get_models_bad_lower_dbt_version(self, mock_manifest_parser, mock_run_parser, mock_open):
        mock_self = Mock()
        mock_self.project_dir = ""
        mock_run_results = Mock()
        mock_run_parser.return_value = mock_run_results
        mock_run_results.metadata.dbt_version = "0.19.0"

        with self.assertRaises(Exception) as ex:
            DbtParser.get_models(mock_self)

        mock_open.assert_called_once_with(RUN_RESULTS_PATH)
        mock_run_parser.assert_called_once_with(run_results={})
        mock_manifest_parser.assert_not_called()
        self.assertIn("version to be", ex.exception.args[0])
        
    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    @patch("data_diff.dbt.parse_run_results")
    @patch("data_diff.dbt.parse_manifest")
    def test_get_models_bad_upper_dbt_version(self, mock_manifest_parser, mock_run_parser, mock_open):
        mock_self = Mock()
        mock_self.project_dir = ""
        mock_run_results = Mock()
        mock_run_parser.return_value = mock_run_results
        mock_run_results.metadata.dbt_version = "1.5.1"

        with self.assertRaises(Exception) as ex:
            DbtParser.get_models(mock_self)

        mock_open.assert_called_once_with(RUN_RESULTS_PATH)
        mock_run_parser.assert_called_once_with(run_results={})
        mock_manifest_parser.assert_not_called()
        self.assertIn("version to be", ex.exception.args[0])

    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    @patch("data_diff.dbt.parse_run_results")
    @patch("data_diff.dbt.parse_manifest")
    def test_get_models_no_success(self, mock_manifest_parser, mock_run_parser, mock_open):
        mock_self = Mock()
        mock_self.project_dir = ""
        mock_run_results = Mock()
        mock_success_result = Mock()
        mock_failed_result = Mock()
        mock_manifest = Mock()
        mock_run_parser.return_value = mock_run_results
        mock_run_results.metadata.dbt_version = "1.0.0"
        mock_failed_result.unique_id = "failed_unique_id"
        mock_success_result.status.name = "success"
        mock_failed_result.status.name = "failed"
        mock_run_results.results = [mock_failed_result]
        mock_manifest_parser.return_value = mock_manifest
        mock_manifest.nodes = {"success_unique_id": "a_unique_id"}

        with self.assertRaises(Exception):
            DbtParser.get_models(mock_self)

        mock_open.assert_any_call(RUN_RESULTS_PATH)
        mock_open.assert_any_call(MANIFEST_PATH)
        mock_run_parser.assert_called_once_with(run_results={})
        mock_manifest_parser.assert_called_once_with(manifest={})

    @patch("data_diff.dbt.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open, read_data="key:\n  value")
    def test_set_project_dict(self, mock_open, mock_yaml_parse):
        expected_dict = {"key1": "value1"}
        mock_self = Mock()
        mock_self.project_dir = ""
        mock_yaml_parse.return_value = expected_dict
        DbtParser.set_project_dict(mock_self)

        self.assertEqual(mock_self.project_dict, expected_dict)
        mock_open.assert_called_once_with(PROJECT_FILE)

    @patch("data_diff.dbt.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open, read_data="key:\n  value")
    def test_set_connection_snowflake(self, mock_open_file, mock_yaml_parse):
        expected_driver = "snowflake"
        expected_password = "password_value"
        profiles_dict = {
            "profile_name": {
                "outputs": {
                    "connection1": {
                        "type": expected_driver,
                        "password": expected_password,
                    }
                },
                "target": "connection1",
            }
        }

        mock_self = Mock()
        mock_self.profiles_dir = ""
        mock_self.project_dict = {"profile": "profile_name"}
        mock_yaml_parse.return_value = profiles_dict
        DbtParser.set_connection(mock_self)

        self.assertIsInstance(mock_self.connection, dict)
        self.assertEqual(mock_self.connection.get("driver"), expected_driver)
        self.assertEqual(mock_self.connection.get("password"), expected_password)
        self.assertEqual(mock_self.requires_upper, True)
        mock_open_file.assert_called_once_with(PROFILES_FILE)
        mock_yaml_parse.assert_called_once_with(mock_open_file())

    @patch("data_diff.dbt.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open, read_data="key:\n  value")
    def test_set_connection_snowflake_no_password(self, mock_open_file, mock_yaml_parse):
        expected_driver = "snowflake"
        profiles_dict = {
            "profile_name": {
                "outputs": {"connection1": {"type": expected_driver}},
                "target": "connection1",
            }
        }

        mock_self = Mock()
        mock_self.profiles_dir = ""
        mock_self.project_dict = {"profile": "profile_name"}
        mock_yaml_parse.return_value = profiles_dict

        with self.assertRaises(Exception):
            DbtParser.set_connection(mock_self)

        mock_open_file.assert_called_once_with(PROFILES_FILE)
        mock_yaml_parse.assert_called_once_with(mock_open_file())
        self.assertNotIsInstance(mock_self.connection, dict)

    @patch("data_diff.dbt.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open, read_data="key:\n  value")
    def test_set_connection_bigquery(self, mock_open_file, mock_yaml_parse):
        expected_driver = "bigquery"
        expected_method = "oauth"
        expected_project = "a_project"
        expected_dataset = "a_dataset"
        profiles_dict = {
            "profile_name": {
                "outputs": {
                    "connection1": {
                        "type": expected_driver,
                        "method": expected_method,
                        "project": expected_project,
                        "dataset": expected_dataset,
                    }
                },
                "target": "connection1",
            }
        }

        mock_self = Mock()
        mock_self.profiles_dir = ""
        mock_self.project_dict = {"profile": "profile_name"}
        mock_yaml_parse.return_value = profiles_dict
        DbtParser.set_connection(mock_self)

        self.assertIsInstance(mock_self.connection, dict)
        self.assertEqual(mock_self.connection.get("driver"), expected_driver)
        self.assertEqual(mock_self.connection.get("project"), expected_project)
        self.assertEqual(mock_self.connection.get("dataset"), expected_dataset)
        mock_open_file.assert_called_once_with(PROFILES_FILE)
        mock_yaml_parse.assert_called_once_with(mock_open_file())

    @patch("data_diff.dbt.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open, read_data="key:\n  value")
    def test_set_connection_bigquery_not_oauth(self, mock_open_file, mock_yaml_parse):
        expected_driver = "bigquery"
        expected_method = "not_oauth"
        expected_project = "a_project"
        expected_dataset = "a_dataset"
        profiles_dict = {
            "profile_name": {
                "outputs": {
                    "connection1": {
                        "type": expected_driver,
                        "method": expected_method,
                        "project": expected_project,
                        "dataset": expected_dataset,
                    }
                },
                "target": "connection1",
            }
        }

        mock_self = Mock()
        mock_self.profiles_dir = ""
        mock_self.project_dict = {"profile": "profile_name"}
        mock_yaml_parse.return_value = profiles_dict
        with self.assertRaises(Exception):
            DbtParser.set_connection(mock_self)

        mock_open_file.assert_called_once_with(PROFILES_FILE)
        mock_yaml_parse.assert_called_once_with(mock_open_file())
        self.assertNotIsInstance(mock_self.connection, dict)

    @patch("data_diff.dbt.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open, read_data="key:\n  value")
    def test_set_connection_key_error(self, mock_open_file, mock_yaml_parse):
        profiles_dict = {
            "profile_name": {
                "outputs": {
                    "connection1": {
                        "type": "a_driver",
                        "password": "a_password",
                    }
                },
                "target": "connection1",
            }
        }

        mock_self = Mock()
        mock_self.profiles_dir = ""
        mock_self.project_dir = ""
        mock_self.project_dict = {"profile": "bad_key"}
        mock_yaml_parse.return_value = profiles_dict
        with self.assertRaises(Exception):
            DbtParser.set_connection(mock_self)

        mock_open_file.assert_called_once_with(PROFILES_FILE)
        mock_yaml_parse.assert_called_once_with(mock_open_file())
        self.assertNotIsInstance(mock_self.connection, dict)

    @patch("data_diff.dbt.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open, read_data="key:\n  value")
    def test_set_connection_not_implemented(self, mock_open_file, mock_yaml_parse):
        expected_driver = "not_implemented"
        profiles_dict = {
            "profile_name": {
                "outputs": {
                    "connection1": {
                        "type": expected_driver,
                    }
                },
                "target": "connection1",
            }
        }

        mock_self = Mock()
        mock_self.profiles_dir = ""
        mock_self.project_dir = ""
        mock_self.project_dict = {"profile": "profile_name"}
        mock_yaml_parse.return_value = profiles_dict
        with self.assertRaises(NotImplementedError):
            DbtParser.set_connection(mock_self)

        mock_open_file.assert_called_once_with(PROFILES_FILE)
        mock_yaml_parse.assert_called_once_with(mock_open_file())
        self.assertNotIsInstance(mock_self.connection, dict)


class TestDbtDiffer(unittest.TestCase):
    # These two integration tests can be used to test a real diff
    # export DATA_DIFF_DBT_PROJ=/path/to/a/dbt/project
    # Expects a valid dbt project using a ~/.dbt/profiles.yml with run results
    def test_integration_basic_dbt(self):
        project_dir = os.environ.get("DATA_DIFF_DBT_PROJ")
        if project_dir is not None:
            diff = run_datadiff_cli("--dbt", "--dbt-project-dir", project_dir)
            assert diff[-1].decode("utf-8") == "Diffs Complete!"
        else:
            pass

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
        expected_key = "key"
        diff_vars = DiffVars(dev_qualified_list, prod_qualified_list, [expected_key], None, mock_connection)
        with patch("data_diff.dbt.connect_to_table", side_effect=[mock_table1, mock_table2]) as mock_connect:
            _local_diff(diff_vars)

        mock_diff_tables.assert_called_once_with(
            mock_table1, mock_table2, threaded=True, algorithm=Algorithm.JOINDIFF, extra_columns=tuple(column_set)
        )
        self.assertEqual(mock_connect.call_count, 2)
        mock_connect.assert_any_call(mock_connection, ".".join(dev_qualified_list), expected_key)
        mock_connect.assert_any_call(mock_connection, ".".join(prod_qualified_list), expected_key)
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
        expected_key = "primary_key_column"
        diff_vars = DiffVars(dev_qualified_list, prod_qualified_list, [expected_key], None, mock_connection)
        with patch("data_diff.dbt.connect_to_table", side_effect=[mock_table1, mock_table2]) as mock_connect:
            _local_diff(diff_vars)

        mock_diff_tables.assert_called_once_with(
            mock_table1, mock_table2, threaded=True, algorithm=Algorithm.JOINDIFF, extra_columns=tuple(column_set)
        )
        self.assertEqual(mock_connect.call_count, 2)
        mock_connect.assert_any_call(mock_connection, ".".join(dev_qualified_list), expected_key)
        mock_connect.assert_any_call(mock_connection, ".".join(prod_qualified_list), expected_key)
        mock_diff.get_stats_string.assert_not_called()

    @patch("data_diff.dbt.rich.print")
    @patch("data_diff.dbt.os.environ")
    @patch("data_diff.dbt.requests.request")
    def test_cloud_diff(self, mock_request, mock_os_environ, mock_print):
        expected_api_key = "an_api_key"
        mock_response = Mock()
        mock_response.json.return_value = {"id": 123}
        mock_request.return_value = mock_response
        mock_os_environ.get.return_value = expected_api_key
        dev_qualified_list = ["dev_db", "dev_schema", "dev_table"]
        prod_qualified_list = ["prod_db", "prod_schema", "prod_table"]
        expected_datasource_id = 1
        expected_primary_keys = ["primary_key_column"]
        diff_vars = DiffVars(
            dev_qualified_list, prod_qualified_list, expected_primary_keys, expected_datasource_id, None
        )
        _cloud_diff(diff_vars)

        mock_request.assert_called_once()
        mock_print.assert_called_once()
        request_data_dict = mock_request.call_args[1]["json"]
        self.assertEqual(
            mock_request.call_args[1]["headers"]["Authorization"],
            "Key " + expected_api_key,
        )
        self.assertEqual(request_data_dict["data_source1_id"], expected_datasource_id)
        self.assertEqual(request_data_dict["data_source2_id"], expected_datasource_id)
        self.assertEqual(request_data_dict["table1"], dev_qualified_list)
        self.assertEqual(request_data_dict["table2"], prod_qualified_list)
        self.assertEqual(request_data_dict["pk_columns"], expected_primary_keys)

    @patch("data_diff.dbt.rich.print")
    @patch("data_diff.dbt.os.environ")
    @patch("data_diff.dbt.requests.request")
    def test_cloud_diff_ds_id_none(self, mock_request, mock_os_environ, mock_print):
        expected_api_key = "an_api_key"
        mock_response = Mock()
        mock_response.json.return_value = {"id": 123}
        mock_request.return_value = mock_response
        mock_os_environ.get.return_value = expected_api_key
        dev_qualified_list = ["dev_db", "dev_schema", "dev_table"]
        prod_qualified_list = ["prod_db", "prod_schema", "prod_table"]
        expected_datasource_id = None
        primary_keys = ["primary_key_column"]
        diff_vars = DiffVars(dev_qualified_list, prod_qualified_list, primary_keys, expected_datasource_id, None)
        with self.assertRaises(ValueError):
            _cloud_diff(diff_vars)

        mock_request.assert_not_called()
        mock_print.assert_not_called()

    @patch("data_diff.dbt.rich.print")
    @patch("data_diff.dbt.os.environ")
    @patch("data_diff.dbt.requests.request")
    def test_cloud_diff_api_key_none(self, mock_request, mock_os_environ, mock_print):
        expected_api_key = None
        mock_response = Mock()
        mock_response.json.return_value = {"id": 123}
        mock_request.return_value = mock_response
        mock_os_environ.get.return_value = expected_api_key
        dev_qualified_list = ["dev_db", "dev_schema", "dev_table"]
        prod_qualified_list = ["prod_db", "prod_schema", "prod_table"]
        expected_datasource_id = 1
        primary_keys = ["primary_key_column"]
        diff_vars = DiffVars(dev_qualified_list, prod_qualified_list, primary_keys, expected_datasource_id, None)
        with self.assertRaises(ValueError):
            _cloud_diff(diff_vars)

        mock_request.assert_not_called()
        mock_print.assert_not_called()

    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    def test_diff_is_cloud(self, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars):
        mock_dbt_parser_inst = Mock()
        mock_model = Mock()
        expected_dbt_vars_dict = {
            "prod_database": "prod_db",
            "prod_schema": "prod_schema",
            "datasource_id": 1,
        }

        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_variables.return_value = expected_dbt_vars_dict
        expected_diff_vars = DiffVars(["dev"], ["prod"], ["pks"], 123, None)
        mock_get_diff_vars.return_value = expected_diff_vars
        dbt_diff(is_cloud=True)
        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_project_dict.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_not_called()

        mock_cloud_diff.assert_called_once_with(expected_diff_vars)
        mock_local_diff.assert_not_called()
        mock_print.assert_called_once()

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
            "datasource_id": 1,
        }
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_variables.return_value = expected_dbt_vars_dict
        expected_diff_vars = DiffVars(["dev"], ["prod"], ["pks"], 123, None)
        mock_get_diff_vars.return_value = expected_diff_vars
        dbt_diff(is_cloud=False)

        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_project_dict.assert_called_once()
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
        expected_diff_vars = DiffVars(["dev"], ["prod"], ["pks"], 123, None)
        mock_get_diff_vars.return_value = expected_diff_vars
        with self.assertRaises(ValueError):
            dbt_diff(is_cloud=False)

        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_project_dict.assert_called_once()
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
    def test_diff_is_cloud_no_pks(
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

        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_variables.return_value = expected_dbt_vars_dict
        expected_diff_vars = DiffVars(["dev"], ["prod"], [], 123, None)
        mock_get_diff_vars.return_value = expected_diff_vars
        dbt_diff(is_cloud=True)

        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_project_dict.assert_called_once()
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

        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_variables.return_value = expected_dbt_vars_dict

        expected_diff_vars = DiffVars(["dev"], ["prod"], [], 123, None)
        mock_get_diff_vars.return_value = expected_diff_vars
        dbt_diff(is_cloud=False)
        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_project_dict.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_called_once()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_not_called()
        self.assertEqual(mock_print.call_count, 2)
