from pathlib import Path
import unittest
from unittest.mock import Mock, mock_open, patch

from data_diff.errors import (
    DataDiffDbtBigQueryUnsupportedMethodError,
    DataDiffDbtConnectionNotImplementedError,
    DataDiffDbtCoreNoRunnerError,
    DataDiffDbtNoSuccessfulModelsInRunError,
    DataDiffDbtProfileNotFoundError,
    DataDiffDbtRedshiftPasswordOnlyError,
    DataDiffDbtRunResultsVersionError,
    DataDiffDbtSnowflakeSetConnectionError,
)

from data_diff.dbt import (
    DbtParser,
)
from data_diff.dbt_parser import (
    RUN_RESULTS_PATH,
    PROJECT_FILE,
    TDatadiffConfig,
)


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

    def test_get_models_simple_select(self):
        mock_self = Mock()
        mock_self.project_dir = Path()
        mock_self.dbt_version = "1.4.0"
        selection = "model+"
        mock_return_value = Mock()
        mock_self.get_simple_model_selection.return_value = mock_return_value

        models = DbtParser.get_models(mock_self, selection)
        mock_self.get_dbt_selection_models.assert_not_called()
        mock_self.get_simple_model_selection.assert_called_with(selection)
        self.assertEqual(models, mock_return_value)

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

    @patch("data_diff.dbt_parser.RunResultsJsonConfig.parse_obj")
    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    def test_get_run_results_models(self, mock_open, mock_artifact_parser):
        mock_model = {"success_unique_id": "expected_value"}
        mock_self = Mock()
        mock_self.project_dir = Path()
        mock_run_results = Mock()
        mock_success_result = Mock()
        mock_success_result.status = mock_success_result.Status.success
        mock_fail_result = Mock()
        mock_fail_result.status = mock_fail_result.Status.fail
        mock_artifact_parser.return_value = mock_run_results
        mock_run_results.metadata.dbt_version = "1.0.0"
        mock_success_result.unique_id = "success_unique_id"
        mock_fail_result.unique_id = "fail_unique_id"
        mock_run_results.results = [mock_success_result, mock_fail_result]

        mock_self.dev_manifest_obj.nodes.get.return_value = mock_model
        models = DbtParser.get_run_results_models(mock_self)
        self.assertEqual(mock_model, models[0])
        mock_open.assert_called_with(Path(RUN_RESULTS_PATH))
        mock_artifact_parser.assert_called_once_with({})

    @patch("data_diff.dbt_parser.RunResultsJsonConfig.parse_obj")
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
        mock_artifact_parser.assert_called_once_with({})
        self.assertIn("version to be", ex.exception.args[0])

    @patch("data_diff.dbt_parser.RunResultsJsonConfig.parse_obj")
    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    def test_get_run_results_models_no_success(self, mock_open, mock_artifact_parser):
        mock_self = Mock()
        mock_self.project_dir = Path()
        mock_run_results = Mock()
        mock_fail_result = Mock()
        mock_fail_result.status = mock_fail_result.Status.fail
        mock_artifact_parser.return_value = mock_run_results
        mock_run_results.metadata.dbt_version = "1.0.0"
        mock_fail_result.unique_id = "fail_unique_id"
        mock_run_results.results = [mock_fail_result]

        with self.assertRaises(DataDiffDbtNoSuccessfulModelsInRunError):
            DbtParser.get_run_results_models(mock_self)

        mock_open.assert_any_call(Path(RUN_RESULTS_PATH))
        mock_artifact_parser.assert_called_once_with({})

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

    def test_set_connection_bigquery_oauth_sa_impersonation(self):
        expected_driver = "bigquery"
        expected_credentials = {
            "method": "oauth",
            "project": "a_project",
            "dataset": "a_dataset",
            "impersonate_service_account": "a_service_account@yourproject.iam.gserviceaccount.com",
        }
        mock_self = Mock()
        mock_self.get_connection_creds.return_value = (expected_credentials, expected_driver)

        DbtParser.set_connection(mock_self)

        self.assertIsInstance(mock_self.connection, dict)
        self.assertEqual(mock_self.connection.get("driver"), expected_driver)
        self.assertEqual(mock_self.connection.get("project"), expected_credentials["project"])
        self.assertEqual(mock_self.connection.get("dataset"), expected_credentials["dataset"])
        self.assertEqual(
            mock_self.connection.get("impersonate_service_account"),
            expected_credentials["impersonate_service_account"],
        )

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
        target = profiles_dict["a_profile"]["target"]
        expected_credentials = profiles_dict["a_profile"]["outputs"]["a_target"]
        mock_self = Mock()
        mock_self.profiles_dir = Path()
        mock_self.project_dict = {"profile": "a_profile"}
        mock_yaml.safe_load.return_value = profiles_dict
        mock_profile_renderer().render_data.side_effect = [target, expected_credentials]
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
        profile_target = profiles_dict["a_profile"]["target"]
        mock_profile_renderer().render_data.return_value = profile_target
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
        profile_target = profiles_dict["a_profile"]["target"]
        mock_profile_renderer().render_data.return_value = profile_target
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
        profile_target = profiles_dict["a_profile"]["target"]
        mock_profile_renderer().render_data.return_value = profile_target
        with self.assertRaises(DataDiffDbtProfileNotFoundError):
            _, _ = DbtParser.get_connection_creds(mock_self)
