import os

from pathlib import Path
from data_diff.cloud.datafold_api import TCloudApiDataSource
from data_diff.cloud.datafold_api import TCloudApiOrgMeta
from data_diff.diff_tables import Algorithm
from data_diff.errors import (
    DataDiffDbtBigQueryUnsupportedMethodError,
    DataDiffCustomSchemaNoConfigError,
    DataDiffDbtConnectionNotImplementedError,
    DataDiffDbtCoreNoRunnerError,
    DataDiffDbtNoSuccessfulModelsInRunError,
    DataDiffDbtProfileNotFoundError,
    DataDiffDbtProjectVarsNotFoundError,
    DataDiffDbtRedshiftPasswordOnlyError,
    DataDiffDbtRunResultsVersionError,
    DataDiffDbtSelectVersionTooLowError,
    DataDiffDbtSnowflakeSetConnectionError,
    DataDiffNoAPIKeyError,
    DataDiffNoDatasourceIdError,
)
from .test_cli import run_datadiff_cli

from data_diff.dbt import (
    _get_diff_vars,
    _get_prod_path_from_config,
    _get_prod_path_from_manifest,
    dbt_diff,
    _local_diff,
    _cloud_diff,
    DbtParser,
    TDiffVars,
    DatafoldAPI,
)
from data_diff.dbt_parser import (
    RUN_RESULTS_PATH,
    PROJECT_FILE,
    TDatadiffConfig,
)
import unittest
from unittest.mock import MagicMock, Mock, create_autospec, mock_open, patch, ANY


class TestDbtParser(unittest.TestCase):
    def test_get_datadiff_config(self):
        project_dict = {"vars": {"data_diff": {"prod_database": "a_prod_database"}}}

        mock_self = Mock()
        mock_self.project_dict = project_dict
        config = DbtParser.get_datadiff_config(mock_self)

        self.assertEqual(project_dict["vars"]["data_diff"]["prod_database"], config.prod_database)
        self.assertEqual(config.prod_schema, None)

    def test_get_datadiff_config_no_config(self):
        project_dict = {"key": {"key": "value"}}

        mock_self = Mock()
        mock_self.project_dict = project_dict

        config = DbtParser.get_datadiff_config(mock_self)
        self.assertEqual(config, TDatadiffConfig())

    def test_get_models(self):
        mock_self = Mock()
        mock_self.project_dir = Path()
        mock_self.dbt_version = "1.5.0"
        selection = "model+"
        mock_return_value = Mock()
        mock_self.get_dbt_selection_models.return_value = mock_return_value

        models = DbtParser.get_models(mock_self, selection)
        mock_self.get_dbt_selection_models.assert_called_once_with(selection)
        self.assertEqual(models, mock_return_value)

    def test_get_models_unsupported_manifest_version(self):
        mock_self = Mock()
        mock_self.project_dir = Path()
        mock_self.dbt_version = "1.4.0"
        selection = "model+"
        mock_return_value = Mock()
        mock_self.get_dbt_selection_models.return_value = mock_return_value

        with self.assertRaises(DataDiffDbtSelectVersionTooLowError):
            _ = DbtParser.get_models(mock_self, selection)
        mock_self.get_dbt_selection_models.assert_not_called()

    def test_get_models_no_runner(self):
        mock_self = Mock()
        mock_self.project_dir = Path()
        mock_self.dbt_version = "1.5.0"
        mock_self.dbt_runner = None
        selection = "model+"
        mock_return_value = Mock()
        mock_self.get_dbt_selection_models.return_value = mock_return_value

        with self.assertRaises(DataDiffDbtCoreNoRunnerError):
            _ = DbtParser.get_models(mock_self, selection)
        mock_self.get_dbt_selection_models.assert_not_called()

    def test_get_models_no_selection(self):
        mock_self = Mock()
        mock_self.project_dir = Path()
        mock_self.dbt_version = "1.5.0"
        selection = None
        mock_return_value = Mock()
        mock_self.get_run_results_models.return_value = mock_return_value

        models = DbtParser.get_models(mock_self, selection)
        mock_self.get_dbt_selection_models.assert_not_called()
        mock_self.get_run_results_models.assert_called()
        self.assertEqual(models, mock_return_value)

    @patch("data_diff.dbt_parser.parse_run_results")
    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    def test_get_run_results_models(self, mock_open, mock_artifact_parser):
        mock_model = {"success_unique_id": "expected_value"}
        mock_self = Mock()
        mock_self.project_dir = Path()
        mock_run_results = Mock()
        mock_success_result = Mock()
        mock_failed_result = Mock()
        mock_artifact_parser.return_value = mock_run_results
        mock_run_results.metadata.dbt_version = "1.0.0"
        mock_success_result.unique_id = "success_unique_id"
        mock_failed_result.unique_id = "failed_unique_id"
        mock_success_result.status.name = "success"
        mock_failed_result.status.name = "failed"
        mock_run_results.results = [mock_success_result, mock_failed_result]
        mock_self.dev_manifest_obj.nodes.get.return_value = mock_model

        models = DbtParser.get_run_results_models(mock_self)

        self.assertEqual(mock_model, models[0])
        mock_open.assert_any_call(Path(RUN_RESULTS_PATH))
        mock_artifact_parser.assert_called_once_with(run_results={})

    @patch("data_diff.dbt_parser.parse_run_results")
    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    def test_get_run_results_models_bad_lower_dbt_version(self, mock_open, mock_artifact_parser):
        mock_self = Mock()
        mock_self.project_dir = Path()
        mock_run_results = Mock()
        mock_artifact_parser.return_value = mock_run_results
        mock_run_results.metadata.dbt_version = "0.19.0"

        with self.assertRaises(DataDiffDbtRunResultsVersionError) as ex:
            DbtParser.get_run_results_models(mock_self)

        mock_open.assert_called_once_with(Path(RUN_RESULTS_PATH))
        mock_artifact_parser.assert_called_once_with(run_results={})
        mock_self.parse_manifest.assert_not_called()
        self.assertIn("version to be", ex.exception.args[0])

    @patch("data_diff.dbt_parser.parse_run_results")
    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    def test_get_run_results_models_no_success(self, mock_open, mock_artifact_parser):
        mock_self = Mock()
        mock_self.project_dir = Path()
        mock_run_results = Mock()
        mock_success_result = Mock()
        mock_failed_result = Mock()
        mock_artifact_parser.return_value = mock_run_results
        mock_run_results.metadata.dbt_version = "1.0.0"
        mock_failed_result.unique_id = "failed_unique_id"
        mock_success_result.status.name = "success"
        mock_failed_result.status.name = "failed"
        mock_run_results.results = [mock_failed_result]

        with self.assertRaises(DataDiffDbtNoSuccessfulModelsInRunError):
            DbtParser.get_run_results_models(mock_self)

        mock_open.assert_any_call(Path(RUN_RESULTS_PATH))
        mock_artifact_parser.assert_called_once_with(run_results={})

    @patch("data_diff.dbt_parser.yaml")
    @patch("builtins.open", new_callable=mock_open, read_data="key:\n  value")
    def test_get_project_dict(self, mock_open, mock_yaml):
        expected_dict = {"key1": "value1"}
        mock_self = Mock()

        mock_self.project_dir = Path()
        mock_yaml.safe_load.return_value = expected_dict
        project_dict = DbtParser.get_project_dict(mock_self)

        self.assertEqual(project_dict, expected_dict)
        mock_open.assert_called_once_with(Path(PROJECT_FILE))

    def test_set_connection_snowflake_success_password(self):
        expected_driver = "snowflake"
        expected_credentials = {"user": "user", "password": "password"}
        mock_self = Mock()
        mock_self.get_connection_creds.return_value = (expected_credentials, expected_driver)

        DbtParser.set_connection(mock_self)

        mock_self.set_casing_policy_for.assert_called_once_with("snowflake")
        self.assertIsInstance(mock_self.connection, dict)
        self.assertEqual(mock_self.connection.get("driver"), expected_driver)
        self.assertEqual(mock_self.connection.get("user"), expected_credentials["user"])
        self.assertEqual(mock_self.connection.get("password"), expected_credentials["password"])
        self.assertEqual(mock_self.connection.get("key"), None)

    def test_set_connection_snowflake_success_key(self):
        expected_driver = "snowflake"
        expected_credentials = {"user": "user", "private_key_path": "private_key_path"}
        mock_self = Mock()
        mock_self.get_connection_creds.return_value = (expected_credentials, expected_driver)

        DbtParser.set_connection(mock_self)

        mock_self.set_casing_policy_for.assert_called_once_with("snowflake")
        self.assertIsInstance(mock_self.connection, dict)
        self.assertEqual(mock_self.connection.get("driver"), expected_driver)
        self.assertEqual(mock_self.connection.get("user"), expected_credentials["user"])
        self.assertEqual(mock_self.connection.get("password"), None)
        self.assertEqual(mock_self.connection.get("key"), expected_credentials["private_key_path"])

    def test_set_connection_snowflake_success_key_and_passphrase(self):
        expected_driver = "snowflake"
        expected_credentials = {
            "user": "user",
            "private_key_path": "private_key_path",
            "private_key_passphrase": "private_key_passphrase",
        }
        mock_self = Mock()
        mock_self.get_connection_creds.return_value = (expected_credentials, expected_driver)

        DbtParser.set_connection(mock_self)

        mock_self.set_casing_policy_for.assert_called_once_with("snowflake")
        self.assertIsInstance(mock_self.connection, dict)
        self.assertEqual(mock_self.connection.get("driver"), expected_driver)
        self.assertEqual(mock_self.connection.get("user"), expected_credentials["user"])
        self.assertEqual(mock_self.connection.get("password"), None)
        self.assertEqual(mock_self.connection.get("key"), expected_credentials["private_key_path"])
        self.assertEqual(
            mock_self.connection.get("private_key_passphrase"), expected_credentials["private_key_passphrase"]
        )

    def test_set_connection_snowflake_no_key_or_password(self):
        expected_driver = "snowflake"
        expected_credentials = {"user": "user"}
        mock_self = Mock()
        mock_self.get_connection_creds.return_value = (expected_credentials, expected_driver)

        with self.assertRaises(DataDiffDbtSnowflakeSetConnectionError):
            DbtParser.set_connection(mock_self)

        self.assertNotIsInstance(mock_self.connection, dict)

    def test_set_connection_snowflake_authenticator(self):
        expected_driver = "snowflake"
        expected_credentials = {"user": "user", "authenticator": "authenticator"}
        mock_self = Mock()
        mock_self.get_connection_creds.return_value = (expected_credentials, expected_driver)

        DbtParser.set_connection(mock_self)

        self.assertIsInstance(mock_self.connection, dict)
        self.assertEqual(mock_self.connection.get("driver"), expected_driver)
        self.assertEqual(mock_self.connection.get("authenticator"), expected_credentials["authenticator"])
        self.assertEqual(mock_self.connection.get("user"), expected_credentials["user"])

    def test_set_connection_snowflake_key_and_password(self):
        expected_driver = "snowflake"
        expected_credentials = {"user": "user", "private_key_path": "private_key_path", "password": "password"}
        mock_self = Mock()
        mock_self.get_connection_creds.return_value = (expected_credentials, expected_driver)

        with self.assertRaises(DataDiffDbtSnowflakeSetConnectionError):
            DbtParser.set_connection(mock_self)

        self.assertNotIsInstance(mock_self.connection, dict)

    def test_set_connection_bigquery_oauth(self):
        expected_driver = "bigquery"
        expected_credentials = {
            "method": "oauth",
            "project": "a_project",
            "dataset": "a_dataset",
        }
        mock_self = Mock()
        mock_self.get_connection_creds.return_value = (expected_credentials, expected_driver)

        DbtParser.set_connection(mock_self)

        self.assertIsInstance(mock_self.connection, dict)
        self.assertEqual(mock_self.connection.get("driver"), expected_driver)
        self.assertEqual(mock_self.connection.get("project"), expected_credentials["project"])
        self.assertEqual(mock_self.connection.get("dataset"), expected_credentials["dataset"])

    def test_set_connection_bigquery_svc_account(self):
        expected_driver = "bigquery"
        expected_credentials = {
            "method": "service-account",
            "project": "a_project",
            "dataset": "a_dataset",
            "keyfile": "/some/path",
        }
        mock_self = Mock()
        mock_self.get_connection_creds.return_value = (expected_credentials, expected_driver)

        DbtParser.set_connection(mock_self)

        self.assertIsInstance(mock_self.connection, dict)
        self.assertEqual(mock_self.connection.get("driver"), expected_driver)
        self.assertEqual(mock_self.connection.get("project"), expected_credentials["project"])
        self.assertEqual(mock_self.connection.get("dataset"), expected_credentials["dataset"])
        self.assertEqual(mock_self.connection.get("keyfile"), expected_credentials["keyfile"])

    def test_set_connection_bigquery_not_supported(self):
        expected_driver = "bigquery"
        expected_credentials = {
            "method": "not_supported",
            "project": "a_project",
            "dataset": "a_dataset",
        }

        mock_self = Mock()
        mock_self.get_connection_creds.return_value = (expected_credentials, expected_driver)
        with self.assertRaises(DataDiffDbtBigQueryUnsupportedMethodError):
            DbtParser.set_connection(mock_self)

        self.assertNotIsInstance(mock_self.connection, dict)

    def test_set_connection_redshift_not_password(self):
        driver = "redshift"
        credentials = {
            "method": "not_password",
        }

        mock_self = Mock()
        mock_self.get_connection_creds.return_value = (credentials, driver)
        with self.assertRaises(DataDiffDbtRedshiftPasswordOnlyError):
            DbtParser.set_connection(mock_self)

        self.assertNotIsInstance(mock_self.connection, dict)

    def test_set_connection_not_implemented(self):
        expected_driver = "unimplemented_provider"

        mock_self = Mock()
        mock_self.get_connection_creds.return_value = (None, expected_driver)
        with self.assertRaises(DataDiffDbtConnectionNotImplementedError):
            DbtParser.set_connection(mock_self)

        self.assertNotIsInstance(mock_self.connection, dict)

    @patch("data_diff.dbt_parser.yaml")
    @patch("data_diff.dbt_parser.ProfileRenderer")
    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_connection_creds_success(self, mock_open, mock_profile_renderer, mock_yaml):
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
        mock_yaml.safe_load.return_value = profiles_dict
        mock_profile_renderer().render_data.return_value = profile
        credentials, conn_type = DbtParser.get_connection_creds(mock_self)
        self.assertEqual(credentials, expected_credentials)
        self.assertEqual(conn_type, "type1")

    @patch("data_diff.dbt_parser.yaml")
    @patch("data_diff.dbt_parser.ProfileRenderer")
    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_connection_no_matching_profile(self, mock_open, mock_profile_renderer, mock_yaml):
        profiles_dict = {"a_profile": {}}
        mock_self = Mock()
        mock_self.profiles_dir = Path()
        mock_self.project_dict = {"profile": "wrong_profile"}
        mock_yaml.safe_load.return_value = profiles_dict
        profile = profiles_dict["a_profile"]
        mock_profile_renderer().render_data.return_value = profile
        with self.assertRaises(DataDiffDbtProfileNotFoundError):
            _, _ = DbtParser.get_connection_creds(mock_self)

    @patch("data_diff.dbt_parser.yaml")
    @patch("data_diff.dbt_parser.ProfileRenderer")
    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_connection_no_target(self, mock_open, mock_profile_renderer, mock_yaml):
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
        mock_profile_renderer().render_data.return_value = profile
        mock_self.project_dict = {"profile": "a_profile"}
        mock_yaml.safe_load.return_value = profiles_dict
        with self.assertRaises(DataDiffDbtProfileNotFoundError):
            _, _ = DbtParser.get_connection_creds(mock_self)

    profile_yaml_no_outputs = """
    a_profile:
      target: a_target
    """

    @patch("data_diff.dbt_parser.yaml")
    @patch("data_diff.dbt_parser.ProfileRenderer")
    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_connection_no_outputs(self, mock_open, mock_profile_renderer, mock_yaml):
        profiles_dict = {"a_profile": {"target": "a_target"}}
        mock_self = Mock()
        mock_self.profiles_dir = Path()
        mock_self.project_dict = {"profile": "a_profile"}
        profile = profiles_dict["a_profile"]
        mock_profile_renderer().render_data.return_value = profile
        mock_yaml.safe_load.return_value = profiles_dict
        with self.assertRaises(DataDiffDbtProfileNotFoundError):
            _, _ = DbtParser.get_connection_creds(mock_self)

    @patch("data_diff.dbt_parser.yaml")
    @patch("data_diff.dbt_parser.ProfileRenderer")
    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_connection_no_credentials(self, mock_open, mock_profile_renderer, mock_yaml):
        profiles_dict = {
            "a_profile": {
                "outputs": {"a_target": {}},
                "target": "a_target",
            }
        }
        mock_self = Mock()
        mock_self.profiles_dir = Path()
        mock_self.project_dict = {"profile": "a_profile"}
        mock_yaml.safe_load.return_value = profiles_dict
        profile = profiles_dict["a_profile"]
        mock_profile_renderer().render_data.return_value = profile
        with self.assertRaises(DataDiffDbtProfileNotFoundError):
            _, _ = DbtParser.get_connection_creds(mock_self)

    @patch("data_diff.dbt_parser.yaml")
    @patch("data_diff.dbt_parser.ProfileRenderer")
    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_connection_no_target_credentials(self, mock_open, mock_profile_renderer, mock_yaml):
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
        mock_profile_renderer().render_data.return_value = profile
        mock_yaml.safe_load.return_value = profiles_dict
        with self.assertRaises(DataDiffDbtProfileNotFoundError):
            _, _ = DbtParser.get_connection_creds(mock_self)

    @patch("data_diff.dbt_parser.yaml")
    @patch("data_diff.dbt_parser.ProfileRenderer")
    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_get_connection_no_type(self, mock_open, mock_profile_renderer, mock_yaml):
        profiles_dict = {
            "a_profile": {
                "outputs": {"a_target": {"credential_1": "credential_1", "credential_2": "credential_2"}},
                "target": "a_target",
            }
        }
        mock_self = Mock()
        mock_self.profiles_dir = Path()
        mock_self.project_dict = {"profile": "a_profile"}
        mock_yaml.safe_load.return_value = profiles_dict
        profile = profiles_dict["a_profile"]
        mock_profile_renderer().render_data.return_value = profile
        with self.assertRaises(DataDiffDbtProfileNotFoundError):
            _, _ = DbtParser.get_connection_creds(mock_self)


class TestDbtDiffer(unittest.TestCase):
    # Set DATA_DIFF_DBT_PROJ to use your own dbt project, otherwise uses the duckdb project in tests/dbt_artifacts
    def test_integration_basic_dbt(self):
        artifacts_path = os.getcwd() + "/tests/dbt_artifacts"
        test_project_path = os.environ.get("DATA_DIFF_DBT_PROJ") or artifacts_path
        diff = run_datadiff_cli(
            "--dbt", "--dbt-project-dir", test_project_path, "--dbt-profiles-dir", test_project_path
        )

        # assertions for the diff that exists in tests/dbt_artifacts/jaffle_shop.duckdb
        if test_project_path == artifacts_path:
            diff_string = b"".join(diff).decode("utf-8")
            # 5 diffs were ran
            assert diff_string.count("<>") == 5
            # 4 with no diffs
            assert diff_string.count("No row differences") == 4
            # 1 with a diff
            assert diff_string.count("  Rows Added    Rows Removed") == 1

    def test_integration_cloud_dbt(self):
        project_dir = os.environ.get("DATA_DIFF_DBT_PROJ")
        if project_dir is not None:
            diff = run_datadiff_cli("--dbt", "--cloud", "--dbt-project-dir", project_dir)
            assert diff[-1].decode("utf-8") == "Diffs Complete!"
        else:
            pass

    @patch("data_diff.dbt.diff_tables")
    def test_local_diff(self, mock_diff_tables):
        connection = {}
        mock_table1 = Mock()
        column_dictionary = {"col1": ("col1", "type"), "col2": ("col2", "type")}
        mock_table1.get_schema.return_value = column_dictionary
        mock_table2 = Mock()
        mock_table2.get_schema.return_value = column_dictionary
        mock_diff = MagicMock()
        mock_diff_tables.return_value = mock_diff
        mock_diff.__iter__.return_value = [1, 2, 3]
        threads = None
        where = "a_string"
        dev_qualified_list = ["dev_db", "dev_schema", "dev_table"]
        prod_qualified_list = ["prod_db", "prod_schema", "prod_table"]
        expected_primary_keys = ["key"]
        diff_vars = TDiffVars(
            dev_path=dev_qualified_list,
            prod_path=prod_qualified_list,
            primary_keys=expected_primary_keys,
            connection=connection,
            threads=threads,
            where_filter=where,
            include_columns=[],
            exclude_columns=[],
        )
        with patch("data_diff.dbt.connect_to_table", side_effect=[mock_table1, mock_table2]) as mock_connect:
            _local_diff(diff_vars)

        mock_diff_tables.assert_called_once_with(
            mock_table1,
            mock_table2,
            threaded=True,
            algorithm=Algorithm.JOINDIFF,
            extra_columns=ANY,
            where=where,
            skip_null_keys=True,
        )
        self.assertEqual(len(mock_diff_tables.call_args[1]["extra_columns"]), 2)
        self.assertEqual(mock_connect.call_count, 2)
        mock_connect.assert_any_call(connection, ".".join(dev_qualified_list), tuple(expected_primary_keys), threads)
        mock_connect.assert_any_call(connection, ".".join(prod_qualified_list), tuple(expected_primary_keys), threads)
        mock_diff.get_stats_string.assert_called_once()

    @patch("data_diff.dbt.diff_tables")
    def test_local_diff_types_differ(self, mock_diff_tables):
        connection = {}
        mock_table1 = Mock()
        mock_table2 = Mock()
        table1_column_dictionary = {"col1": ("col1", "type"), "col2": ("col2", "type")}
        table2_column_dictionary = {"col1": ("col1", "type"), "col2": ("col2", "differing_type")}
        mock_table1.get_schema.return_value = table1_column_dictionary
        mock_table2.get_schema.return_value = table2_column_dictionary
        mock_diff = MagicMock()
        mock_diff_tables.return_value = mock_diff
        mock_diff.__iter__.return_value = [1, 2, 3]
        threads = None
        where = "a_string"
        dev_qualified_list = ["dev_db", "dev_schema", "dev_table"]
        prod_qualified_list = ["prod_db", "prod_schema", "prod_table"]
        expected_primary_keys = ["key"]
        diff_vars = TDiffVars(
            dev_path=dev_qualified_list,
            prod_path=prod_qualified_list,
            primary_keys=expected_primary_keys,
            connection=connection,
            threads=threads,
            where_filter=where,
            include_columns=[],
            exclude_columns=[],
        )
        with patch("data_diff.dbt.connect_to_table", side_effect=[mock_table1, mock_table2]) as mock_connect:
            _local_diff(diff_vars)

        mock_diff_tables.assert_called_once_with(
            mock_table1,
            mock_table2,
            threaded=True,
            algorithm=Algorithm.JOINDIFF,
            extra_columns=ANY,
            where=where,
            skip_null_keys=True,
        )
        self.assertEqual(len(mock_diff_tables.call_args[1]["extra_columns"]), 1)
        self.assertEqual(mock_connect.call_count, 2)
        mock_diff.get_stats_string.assert_called_once()

    @patch("data_diff.dbt.diff_tables")
    def test_local_diff_no_diffs(self, mock_diff_tables):
        connection = {}
        column_dictionary = {"col1": ("col1", "type"), "col2": ("col2", "type")}
        mock_table1 = Mock()
        mock_table1.get_schema.return_value = column_dictionary
        mock_table2 = Mock()
        mock_table2.get_schema.return_value = column_dictionary
        mock_diff = MagicMock()
        mock_diff_tables.return_value = mock_diff
        mock_diff.__iter__.return_value = []
        dev_qualified_list = ["dev_db", "dev_schema", "dev_table"]
        prod_qualified_list = ["prod_db", "prod_schema", "prod_table"]
        expected_primary_keys = ["primary_key_column"]
        threads = None
        where = "a_string"
        diff_vars = TDiffVars(
            dev_path=dev_qualified_list,
            prod_path=prod_qualified_list,
            primary_keys=expected_primary_keys,
            connection=connection,
            threads=threads,
            where_filter=where,
            include_columns=[],
            exclude_columns=[],
        )
        with patch("data_diff.dbt.connect_to_table", side_effect=[mock_table1, mock_table2]) as mock_connect:
            _local_diff(diff_vars)

        mock_diff_tables.assert_called_once_with(
            mock_table1,
            mock_table2,
            threaded=True,
            algorithm=Algorithm.JOINDIFF,
            extra_columns=ANY,
            where=where,
            skip_null_keys=True,
        )
        self.assertEqual(len(mock_diff_tables.call_args[1]["extra_columns"]), 2)
        self.assertEqual(mock_connect.call_count, 2)
        mock_connect.assert_any_call(connection, ".".join(dev_qualified_list), tuple(expected_primary_keys), None)
        mock_connect.assert_any_call(connection, ".".join(prod_qualified_list), tuple(expected_primary_keys), None)
        mock_diff.get_stats_string.assert_not_called()

    @patch("data_diff.dbt.rich.print")
    @patch("data_diff.dbt.os.environ")
    @patch("data_diff.dbt.DatafoldAPI")
    def test_cloud_diff(self, mock_api, mock_os_environ, mock_print):
        org_meta = TCloudApiOrgMeta(org_id=1, org_name="", user_id=1)
        expected_api_key = "an_api_key"
        dev_qualified_list = ["dev_db", "dev_schema", "dev_table"]
        prod_qualified_list = ["prod_db", "prod_schema", "prod_table"]
        expected_datasource_id = 1
        expected_primary_keys = ["primary_key_column"]
        threads = None
        where = "a_string"
        connection = {}
        mock_api.create_data_diff.return_value = {"id": 123}
        mock_os_environ.get.return_value = expected_api_key

        diff_vars = TDiffVars(
            dev_path=dev_qualified_list,
            prod_path=prod_qualified_list,
            primary_keys=expected_primary_keys,
            connection=connection,
            threads=threads,
            where_filter=where,
            include_columns=[],
            exclude_columns=[],
        )

        _cloud_diff(diff_vars, expected_datasource_id, org_meta=org_meta, api=mock_api)

        mock_api.create_data_diff.assert_called_once()
        self.assertEqual(mock_print.call_count, 2)

        payload = mock_api.create_data_diff.call_args[1]["payload"]
        self.assertEqual(payload.data_source1_id, expected_datasource_id)
        self.assertEqual(payload.data_source2_id, expected_datasource_id)
        self.assertEqual(payload.table1, prod_qualified_list)
        self.assertEqual(payload.table2, dev_qualified_list)
        self.assertEqual(payload.pk_columns, expected_primary_keys)
        self.assertEqual(payload.filter1, where)
        self.assertEqual(payload.filter2, where)

    @patch("data_diff.dbt._initialize_api")
    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt_parser.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    @patch("data_diff.dbt.DatafoldAPI")
    def test_diff_is_cloud(
        self,
        mock_api,
        mock_print,
        mock_dbt_parser,
        mock_cloud_diff,
        mock_local_diff,
        mock_get_diff_vars,
        mock_initialize_api,
    ):
        org_meta = TCloudApiOrgMeta(org_id=1, org_name="", user_id=1)
        connection = {}
        threads = None
        where = "a_string"
        config = TDatadiffConfig(prod_database="prod_db", prod_schema="prod_schema", datasource_id=1)
        mock_dbt_parser_inst = Mock()
        mock_model = Mock()
        mock_api.get_data_source.return_value = TCloudApiDataSource(id=1, type="snowflake", name="snowflake")
        mock_initialize_api.return_value = mock_api
        mock_api.get_org_meta.return_value = org_meta

        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_config.return_value = config

        diff_vars = TDiffVars(
            dev_path=["dev"],
            prod_path=["prod"],
            primary_keys=["pks"],
            connection=connection,
            threads=threads,
            where_filter=where,
            include_columns=[],
            exclude_columns=[],
        )
        mock_get_diff_vars.return_value = diff_vars
        dbt_diff(is_cloud=True)
        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_not_called()
        mock_dbt_parser_inst.set_casing_policy_for.assert_called_once()

        mock_initialize_api.assert_called_once()
        mock_api.get_data_source.assert_called_once_with(1)
        mock_cloud_diff.assert_called_once_with(diff_vars, 1, mock_api, org_meta)
        mock_local_diff.assert_not_called()
        mock_print.assert_called_once()

    @patch("data_diff.dbt._initialize_api")
    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt_parser.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    @patch("builtins.input", return_value="n")
    def test_diff_is_cloud_no_ds_id(
        self, _, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars, mock_initialize_api
    ):
        org_meta = TCloudApiOrgMeta(org_id=1, org_name="", user_id=1)
        connection = {}
        threads = None
        where = "a_string"
        mock_dbt_parser_inst = Mock()
        mock_model = Mock()
        config = TDatadiffConfig(prod_database="prod_db", prod_schema="prod_schema")
        mock_api = Mock()
        mock_initialize_api.return_value = mock_api
        mock_api.get_org_meta.return_value = org_meta

        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_config.return_value = config

        diff_vars = TDiffVars(
            dev_path=["dev"],
            prod_path=["prod"],
            primary_keys=["pks"],
            connection=connection,
            threads=threads,
            where_filter=where,
            include_columns=[],
            exclude_columns=[],
        )
        mock_get_diff_vars.return_value = diff_vars

        with self.assertRaises(DataDiffNoDatasourceIdError):
            dbt_diff(is_cloud=True)
        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_not_called()

        mock_initialize_api.assert_called_once()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_not_called()
        mock_print.assert_called_once()

    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt_parser.DbtParser.__new__")
    @patch("builtins.input", return_value="n")
    def test_diff_is_cloud_no_api_key(
        self, _, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars
    ):
        mock_dbt_parser_inst = Mock()
        mock_model = Mock()
        config = TDatadiffConfig(prod_database="prod_db", prod_schema="prod_schema")

        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_config.return_value = config

        with self.assertRaises(DataDiffNoAPIKeyError):
            dbt_diff(is_cloud=True)
        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_not_called()

        mock_get_diff_vars.assert_not_called()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_not_called()

    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt_parser.DbtParser.__new__")
    def test_diff_no_state_no_config(self, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars):
        mock_dbt_parser_inst = Mock()
        mock_model = Mock()
        config = TDatadiffConfig()

        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_config.return_value = config

        with self.assertRaises(DataDiffDbtProjectVarsNotFoundError):
            dbt_diff()
        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.get_datadiff_config.assert_called_once()

        mock_get_diff_vars.assert_not_called()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_not_called()

    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt_parser.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    def test_diff_is_not_cloud(self, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars):
        config = TDatadiffConfig(prod_database="prod_db", prod_schema="prod_schema")
        connection = {}
        threads = None
        where = "a_string"
        mock_dbt_parser_inst = Mock()
        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_model = Mock()
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_config.return_value = config

        diff_vars = TDiffVars(
            dev_path=["dev"],
            prod_path=["prod"],
            primary_keys=["pks"],
            connection=connection,
            threads=threads,
            where_filter=where,
            include_columns=[],
            exclude_columns=[],
        )
        mock_get_diff_vars.return_value = diff_vars
        dbt_diff(is_cloud=False)

        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_called_once()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_called_once_with(diff_vars)
        mock_print.assert_not_called()

    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt_parser.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    def test_diff_state_model_dne(
        self, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars
    ):
        config = TDatadiffConfig(prod_database="prod_db", prod_schema="prod_schema")
        connection = {}
        threads = None
        where = "a_string"
        mock_dbt_parser_inst = Mock()
        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_model = Mock()
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_config.return_value = config
        mock_dbt_parser_inst.get_datadiff_config.return_value = TDatadiffConfig()

        diff_vars = TDiffVars(
            dev_path=["dev_db", "dev_schema", "model"],
            prod_path=["model"],
            primary_keys=["pks"],
            connection=connection,
            threads=threads,
            where_filter=where,
            include_columns=[],
            exclude_columns=[],
        )
        mock_get_diff_vars.return_value = diff_vars
        dbt_diff(is_cloud=False, state="/manifest_path.json")

        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_called_once()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_not_called()
        self.assertTrue("nothing to diff" in mock_print.call_args[0][0])
        mock_print.assert_called_once()

    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt_parser.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    def test_diff_only_prod_db(self, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars):
        config = TDatadiffConfig(prod_database="prod_db")
        connection = {}
        threads = None
        where = "a_string"
        mock_dbt_parser_inst = Mock()
        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_model = Mock()
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_config.return_value = config

        diff_vars = TDiffVars(
            dev_path=["dev"],
            prod_path=["prod"],
            primary_keys=["pks"],
            connection=connection,
            threads=threads,
            where_filter=where,
            include_columns=[],
            exclude_columns=[],
        )
        mock_get_diff_vars.return_value = diff_vars
        dbt_diff(is_cloud=False)

        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_called_once()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_called_once_with(diff_vars)
        mock_print.assert_not_called()

    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt_parser.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    def test_diff_only_prod_schema(
        self, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars
    ):
        config = TDatadiffConfig(prod_schema="prod_schema")
        connection = {}
        threads = None
        where = "a_string"
        mock_dbt_parser_inst = Mock()
        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_model = Mock()
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_config.return_value = config

        diff_vars = TDiffVars(
            dev_path=["dev"],
            prod_path=["prod"],
            primary_keys=["pks"],
            connection=connection,
            threads=threads,
            where_filter=where,
            include_columns=[],
            exclude_columns=[],
        )
        mock_get_diff_vars.return_value = diff_vars
        dbt_diff(is_cloud=False)

        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_called_once()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_called_once_with(diff_vars)
        mock_print.assert_not_called()

    @patch("data_diff.dbt._initialize_api")
    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt_parser.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    @patch("data_diff.dbt.DatafoldAPI")
    def test_diff_is_cloud_no_pks(
        self,
        mock_api,
        mock_print,
        mock_dbt_parser,
        mock_cloud_diff,
        mock_local_diff,
        mock_get_diff_vars,
        mock_initialize_api,
    ):
        mock_dbt_parser_inst = Mock()
        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_model = Mock()
        connection = {}
        threads = None
        where = "a_string"
        config = TDatadiffConfig(prod_database="prod_db", prod_schema="prod_schema", datasource_id=1)
        mock_api = Mock()
        mock_initialize_api.return_value = mock_api

        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_config.return_value = config
        diff_vars = TDiffVars(
            dev_path=["dev"],
            prod_path=["prod"],
            primary_keys=[],
            connection=connection,
            threads=threads,
            where_filter=where,
            include_columns=[],
            exclude_columns=[],
        )
        mock_get_diff_vars.return_value = diff_vars
        dbt_diff(is_cloud=True)

        mock_initialize_api.assert_called_once()
        mock_api.get_data_source.assert_called_once_with(1)
        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_not_called()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_not_called()
        self.assertEqual(mock_print.call_count, 2)

    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt_parser.DbtParser.__new__")
    @patch("data_diff.dbt.rich.print")
    def test_diff_not_is_cloud_no_pks(
        self, mock_print, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars
    ):
        config = TDatadiffConfig(prod_database="prod_db", prod_schema="prod_schema")
        connection = {}
        threads = None
        where = "a_string"
        mock_dbt_parser_inst = Mock()
        mock_dbt_parser.return_value = mock_dbt_parser_inst
        mock_model = Mock()
        mock_dbt_parser_inst.get_models.return_value = [mock_model]
        mock_dbt_parser_inst.get_datadiff_config.return_value = config

        diff_vars = TDiffVars(
            dev_path=["dev"],
            prod_path=["prod"],
            primary_keys=[],
            connection=connection,
            threads=threads,
            where_filter=where,
            include_columns=[],
            exclude_columns=[],
        )
        mock_get_diff_vars.return_value = diff_vars
        dbt_diff(is_cloud=False)
        mock_dbt_parser_inst.get_models.assert_called_once()
        mock_dbt_parser_inst.set_connection.assert_called_once()
        mock_cloud_diff.assert_not_called()
        mock_local_diff.assert_not_called()
        self.assertEqual(mock_print.call_count, 1)

    def test_get_prod_path_from_config_replace_custom_schema(self):
        config = TDatadiffConfig(
            prod_database="prod_db", prod_schema="prod_schema", prod_custom_schema="prod_<custom_schema>"
        )
        mock_model = Mock()
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_custom_schema"
        mock_model.config.schema_ = mock_model.schema_
        mock_model.config.database = None
        mock_model.alias = "a_model_name"
        mock_model.meta = None

        prod_database, prod_schema = _get_prod_path_from_config(
            config, mock_model, mock_model.database, mock_model.schema_
        )

        self.assertEqual(prod_schema, "prod_" + mock_model.schema_)
        self.assertEqual(prod_database, config.prod_database)

    def test_get_prod_path_from_config_static_custom_schema(self):
        config = TDatadiffConfig(prod_database="prod_db", prod_schema="prod_schema", prod_custom_schema="prod")
        mock_model = Mock()
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_custom_schema"
        mock_model.config.database = None
        mock_model.config.schema_ = mock_model.schema_
        mock_model.alias = "a_model_name"
        mock_model.meta = None

        prod_database, prod_schema = _get_prod_path_from_config(
            config, mock_model, mock_model.database, mock_model.schema_
        )

        self.assertEqual(prod_schema, config.prod_custom_schema)
        self.assertEqual(prod_database, config.prod_database)

    def test_get_prod_path_from_config_no_custom_schema_on_model(self):
        config = TDatadiffConfig(prod_database="prod_db", prod_schema="prod_schema", prod_custom_schema="prod")
        mock_model = Mock()
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_custom_schema"
        mock_model.config.schema_ = None
        mock_model.config.database = None
        mock_model.alias = "a_model_name"
        mock_model.meta = None

        prod_database, prod_schema = _get_prod_path_from_config(
            config, mock_model, mock_model.database, mock_model.schema_
        )

        self.assertEqual(prod_schema, config.prod_schema)
        self.assertEqual(prod_database, config.prod_database)

    def test_get_prod_path_from_config_match_dev_schema(self):
        config = TDatadiffConfig(prod_database="prod_db")
        mock_model = Mock()
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_schema"
        mock_model.config.schema_ = None
        mock_model.config.database = None
        mock_model.alias = "a_model_name"
        mock_model.meta = None

        prod_database, prod_schema = _get_prod_path_from_config(
            config, mock_model, mock_model.database, mock_model.schema_
        )

        self.assertEqual(prod_schema, mock_model.schema_)
        self.assertEqual(prod_database, config.prod_database)

    def test_get_prod_path_from_manifest_model_exists(self):
        mock_model = Mock()
        mock_model.unique_id = "unique_model_id"
        mock_prod_manifest = Mock()
        mock_prod_model = Mock()
        mock_prod_manifest.nodes.get.return_value = mock_prod_model
        mock_prod_model.database = "prod_db"
        mock_prod_model.schema_ = "prod_schema"
        prod_database, prod_schema = _get_prod_path_from_manifest(mock_model, mock_prod_manifest)
        self.assertEqual(prod_database, mock_prod_model.database)
        self.assertEqual(prod_schema, mock_prod_model.schema_)

    def test_get_prod_path_from_manifest_model_not_exists(self):
        mock_model = Mock()
        mock_model.unique_id = "unique_model_id"
        mock_prod_manifest = Mock()
        mock_prod_model = Mock()
        mock_prod_manifest.nodes.get.return_value = None
        mock_prod_model.database = "prod_db"
        mock_prod_model.schema_ = "prod_schema"
        prod_database, prod_schema = _get_prod_path_from_manifest(mock_model, mock_prod_manifest)
        self.assertEqual(prod_database, None)
        self.assertEqual(prod_schema, None)

    def test_get_diff_custom_schema_no_config_exception(self):
        config = TDatadiffConfig(prod_database="prod_db", prod_schema="prod_schema")
        mock_model = Mock()
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_schema"
        mock_model.config.schema_ = "a_custom_schema"
        mock_model.alias = "a_model_name"

        with self.assertRaises(DataDiffCustomSchemaNoConfigError):
            _get_prod_path_from_config(config, mock_model, mock_model.database, mock_model.schema_)

    @patch("data_diff.dbt._get_prod_path_from_config")
    @patch("data_diff.dbt._get_prod_path_from_manifest")
    def test_get_diff_vars_meta_where(self, mock_prod_path_from_manifest, mock_prod_path_from_config):
        config = TDatadiffConfig(prod_database="prod_db")
        mock_model = Mock()
        primary_keys = ["a_primary_key"]
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_schema"
        mock_model.config.schema_ = None
        mock_model.config.database = None
        mock_model.alias = "a_model_name"
        mock_tdatadiffmodelconfig = Mock()
        mock_tdatadiffmodelconfig.where_filter = "where"
        mock_tdatadiffmodelconfig.include_columns = ["include"]
        mock_tdatadiffmodelconfig.exclude_columns = ["exclude"]
        mock_dbt_parser = Mock()
        mock_dbt_parser.get_datadiff_model_config.return_value = mock_tdatadiffmodelconfig
        mock_dbt_parser.connection = {}
        mock_dbt_parser.threads = 0
        mock_dbt_parser.get_pk_from_model.return_value = primary_keys
        mock_dbt_parser.requires_upper = False
        mock_dbt_parser.prod_manifest_obj = None
        mock_prod_path_from_config.return_value = ("prod_db", "prod_schema")

        diff_vars = _get_diff_vars(mock_dbt_parser, config, mock_model)

        self.assertEqual(diff_vars.primary_keys, primary_keys)
        self.assertEqual(diff_vars.connection, mock_dbt_parser.connection)
        self.assertEqual(diff_vars.threads, mock_dbt_parser.threads)
        self.assertEqual(diff_vars.where_filter, mock_tdatadiffmodelconfig.where_filter)
        mock_dbt_parser.get_pk_from_model.assert_called_once()
        mock_prod_path_from_config.assert_called_once_with(config, mock_model, mock_model.database, mock_model.schema_)
        mock_prod_path_from_manifest.assert_not_called()
        self.assertEqual(diff_vars.prod_path[0], mock_prod_path_from_config.return_value[0])
        self.assertEqual(diff_vars.prod_path[1], mock_prod_path_from_config.return_value[1])

    @patch("data_diff.dbt._get_prod_path_from_config")
    @patch("data_diff.dbt._get_prod_path_from_manifest")
    def test_get_diff_vars_meta_unrelated(self, mock_prod_path_from_manifest, mock_prod_path_from_config):
        config = TDatadiffConfig(prod_database="prod_db")
        mock_model = Mock()
        primary_keys = ["a_primary_key"]
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_schema"
        mock_model.config.schema_ = None
        mock_model.config.database = None
        mock_model.alias = "a_model_name"
        mock_tdatadiffmodelconfig = Mock()
        mock_tdatadiffmodelconfig.where_filter = "where"
        mock_tdatadiffmodelconfig.include_columns = ["include"]
        mock_tdatadiffmodelconfig.exclude_columns = ["exclude"]
        mock_dbt_parser = Mock()
        mock_dbt_parser.get_datadiff_model_config.return_value = mock_tdatadiffmodelconfig
        mock_dbt_parser.connection = {}
        mock_dbt_parser.threads = 0
        mock_dbt_parser.get_pk_from_model.return_value = primary_keys
        mock_dbt_parser.requires_upper = False
        mock_dbt_parser.prod_manifest_obj = None
        mock_prod_path_from_config.return_value = ("prod_db", "prod_schema")

        diff_vars = _get_diff_vars(mock_dbt_parser, config, mock_model)

        self.assertEqual(diff_vars.primary_keys, primary_keys)
        self.assertEqual(diff_vars.connection, mock_dbt_parser.connection)
        self.assertEqual(diff_vars.threads, mock_dbt_parser.threads)
        self.assertEqual(diff_vars.where_filter, mock_tdatadiffmodelconfig.where_filter)
        mock_dbt_parser.get_pk_from_model.assert_called_once()
        mock_prod_path_from_config.assert_called_once_with(config, mock_model, mock_model.database, mock_model.schema_)
        mock_prod_path_from_manifest.assert_not_called()
        self.assertEqual(diff_vars.prod_path[0], mock_prod_path_from_config.return_value[0])
        self.assertEqual(diff_vars.prod_path[1], mock_prod_path_from_config.return_value[1])

    @patch("data_diff.dbt._get_prod_path_from_config")
    @patch("data_diff.dbt._get_prod_path_from_manifest")
    def test_get_diff_vars_meta_none(self, mock_prod_path_from_manifest, mock_prod_path_from_config):
        config = TDatadiffConfig(prod_database="prod_db")
        mock_model = Mock()
        primary_keys = ["a_primary_key"]
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_schema"
        mock_model.config.schema_ = None
        mock_model.config.database = None
        mock_model.alias = "a_model_name"
        mock_tdatadiffmodelconfig = Mock()
        mock_tdatadiffmodelconfig.where_filter = "where"
        mock_tdatadiffmodelconfig.include_columns = ["include"]
        mock_tdatadiffmodelconfig.exclude_columns = ["exclude"]
        mock_dbt_parser = Mock()
        mock_dbt_parser.get_datadiff_model_config.return_value = mock_tdatadiffmodelconfig
        mock_dbt_parser.connection = {}
        mock_dbt_parser.threads = 0
        mock_dbt_parser.get_pk_from_model.return_value = primary_keys
        mock_dbt_parser.requires_upper = False
        mock_model.meta = None
        mock_dbt_parser.prod_manifest_obj = None
        mock_prod_path_from_config.return_value = ("prod_db", "prod_schema")

        diff_vars = _get_diff_vars(mock_dbt_parser, config, mock_model)

        assert diff_vars.primary_keys == primary_keys
        assert diff_vars.connection == mock_dbt_parser.connection
        assert diff_vars.threads == mock_dbt_parser.threads
        self.assertEqual(diff_vars.where_filter, mock_tdatadiffmodelconfig.where_filter)
        mock_dbt_parser.get_pk_from_model.assert_called_once()
        mock_prod_path_from_config.assert_called_once_with(config, mock_model, mock_model.database, mock_model.schema_)
        mock_prod_path_from_manifest.assert_not_called()
        self.assertEqual(diff_vars.prod_path[0], mock_prod_path_from_config.return_value[0])
        self.assertEqual(diff_vars.prod_path[1], mock_prod_path_from_config.return_value[1])

    @patch("data_diff.dbt._get_prod_path_from_config")
    @patch("data_diff.dbt._get_prod_path_from_manifest")
    def test_get_diff_vars_custom_db(self, mock_prod_path_from_manifest, mock_prod_path_from_config):
        config = TDatadiffConfig(prod_database="prod_db")
        mock_model = Mock()
        primary_keys = ["a_primary_key"]
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_schema"
        mock_model.config.schema_ = None
        mock_model.config.database = "custom_database"
        mock_model.alias = "a_model_name"
        mock_tdatadiffmodelconfig = Mock()
        mock_tdatadiffmodelconfig.where_filter = "where"
        mock_tdatadiffmodelconfig.include_columns = ["include"]
        mock_tdatadiffmodelconfig.exclude_columns = ["exclude"]
        mock_dbt_parser = Mock()
        mock_dbt_parser.get_datadiff_model_config.return_value = mock_tdatadiffmodelconfig
        mock_dbt_parser.connection = {}
        mock_dbt_parser.threads = 0
        mock_dbt_parser.get_pk_from_model.return_value = primary_keys
        mock_dbt_parser.requires_upper = False
        mock_model.meta = None
        mock_dbt_parser.prod_manifest_obj = None
        mock_prod_path_from_config.return_value = ("prod_db", "prod_schema")

        diff_vars = _get_diff_vars(mock_dbt_parser, config, mock_model)

        assert diff_vars.primary_keys == primary_keys
        assert diff_vars.connection == mock_dbt_parser.connection
        assert diff_vars.threads == mock_dbt_parser.threads
        self.assertEqual(diff_vars.where_filter, mock_tdatadiffmodelconfig.where_filter)
        mock_dbt_parser.get_pk_from_model.assert_called_once()
        mock_prod_path_from_config.assert_called_once_with(config, mock_model, mock_model.database, mock_model.schema_)
        mock_prod_path_from_manifest.assert_not_called()
        self.assertEqual(diff_vars.prod_path[0], mock_prod_path_from_config.return_value[0])
        self.assertEqual(diff_vars.prod_path[1], mock_prod_path_from_config.return_value[1])

    @patch("data_diff.dbt._get_prod_path_from_config")
    @patch("data_diff.dbt._get_prod_path_from_manifest")
    def test_get_diff_vars_upper(self, mock_prod_path_from_manifest, mock_prod_path_from_config):
        config = TDatadiffConfig(prod_database="prod_db")
        mock_model = Mock()
        primary_keys = ["a_primary_key"]
        upper_primary_keys = [x.upper() for x in primary_keys]
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_schema"
        mock_model.config.schema_ = None
        mock_model.config.database = "custom_database"
        mock_model.alias = "a_model_name"
        mock_tdatadiffmodelconfig = Mock()
        mock_tdatadiffmodelconfig.where_filter = "where"
        mock_tdatadiffmodelconfig.include_columns = ["include"]
        mock_tdatadiffmodelconfig.exclude_columns = ["exclude"]
        mock_dbt_parser = Mock()
        mock_dbt_parser.get_datadiff_model_config.return_value = mock_tdatadiffmodelconfig
        mock_dbt_parser.connection = {}
        mock_dbt_parser.threads = 0
        mock_dbt_parser.get_pk_from_model.return_value = primary_keys
        mock_dbt_parser.requires_upper = True
        mock_model.meta = None
        mock_dbt_parser.prod_manifest_obj = None
        mock_prod_path_from_config.return_value = ("prod_db", "prod_schema")

        diff_vars = _get_diff_vars(mock_dbt_parser, config, mock_model)

        self.assertEqual(diff_vars.primary_keys, upper_primary_keys)
        self.assertEqual(diff_vars.connection, mock_dbt_parser.connection)
        self.assertEqual(diff_vars.threads, mock_dbt_parser.threads)
        self.assertEqual(diff_vars.where_filter, mock_tdatadiffmodelconfig.where_filter)
        mock_dbt_parser.get_pk_from_model.assert_called_once()
        mock_prod_path_from_config.assert_called_once_with(config, mock_model, mock_model.database, mock_model.schema_)
        mock_prod_path_from_manifest.assert_not_called()
        self.assertEqual(diff_vars.prod_path[0], mock_prod_path_from_config.return_value[0].upper())
        self.assertEqual(diff_vars.prod_path[1], mock_prod_path_from_config.return_value[1].upper())

    @patch("data_diff.dbt._get_prod_path_from_config")
    @patch("data_diff.dbt._get_prod_path_from_manifest")
    def test_get_diff_vars_call_get_prod_path_from_manifest(
        self, mock_prod_path_from_manifest, mock_prod_path_from_config
    ):
        config = TDatadiffConfig(prod_database="prod_db")
        mock_model = Mock()
        primary_keys = ["a_primary_key"]
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_schema"
        mock_model.config.schema_ = None
        mock_model.config.database = "custom_database"
        mock_model.alias = "a_model_name"
        mock_tdatadiffmodelconfig = Mock()
        mock_tdatadiffmodelconfig.where_filter = "where"
        mock_tdatadiffmodelconfig.include_columns = ["include"]
        mock_tdatadiffmodelconfig.exclude_columns = ["exclude"]
        mock_dbt_parser = Mock()
        mock_dbt_parser.get_datadiff_model_config.return_value = mock_tdatadiffmodelconfig
        mock_dbt_parser.connection = {}
        mock_dbt_parser.threads = 0
        mock_dbt_parser.get_pk_from_model.return_value = primary_keys
        mock_dbt_parser.requires_upper = False
        mock_model.meta = None
        mock_dbt_parser.prod_manifest_obj = {"manifest_key": "manifest_value"}
        mock_prod_path_from_manifest.return_value = ("prod_db", "prod_schema")

        diff_vars = _get_diff_vars(mock_dbt_parser, config, mock_model)

        mock_prod_path_from_manifest.assert_called_once_with(mock_model, mock_dbt_parser.prod_manifest_obj)
        self.assertEqual(diff_vars.prod_path[0], mock_prod_path_from_manifest.return_value[0])
        self.assertEqual(diff_vars.prod_path[1], mock_prod_path_from_manifest.return_value[1])
