import os
import unittest
from unittest.mock import MagicMock, Mock, patch, ANY

from data_diff.cloud.datafold_api import TCloudApiDataSource, TCloudApiOrgMeta
from data_diff.diff_tables import Algorithm
from data_diff.errors import (
    DataDiffCustomSchemaNoConfigError,
    DataDiffDbtProjectVarsNotFoundError,
    DataDiffNoAPIKeyError,
    DataDiffNoDatasourceIdError,
)
from data_diff.dbt import (
    _get_diff_vars,
    _get_prod_path_from_config,
    _get_prod_path_from_manifest,
    dbt_diff,
    _local_diff,
    _cloud_diff,
    TDiffVars,
)
from data_diff.dbt_parser import (
    TDatadiffConfig,
)
from data_diff.schema import RawColumnInfo
from tests.test_cli import run_datadiff_cli


class TestDbtDiffer(unittest.TestCase):
    # Set DATA_DIFF_DBT_PROJ to use your own dbt project, otherwise uses the duckdb project in tests/dbt_artifacts
    def test_integration_basic_dbt(self):
        artifacts_path = os.getcwd() + "/tests/dbt_artifacts"
        test_project_path = os.environ.get("DATA_DIFF_DBT_PROJ") or artifacts_path
        test_profiles_path = os.environ.get("DATA_DIFF_DBT_PROFILES") or artifacts_path
        diff = run_datadiff_cli(
            "--dbt", "--dbt-project-dir", test_project_path, "--dbt-profiles-dir", test_profiles_path
        )

        orders_expected_output = """
        jaffle_shop.prod.orders <> jaffle_shop.dev.orders 
        Primary Keys: ['order_id'] 
        Where Filter: 'amount >= 0' 
        Included Columns: ['order_id', 'customer_id', 'order_date', 'amount', 'credit_card_amount', 'coupon_amount', 
        'bank_transfer_amount', 'gift_card_amount'] 
        Excluded Columns: ['new_column'] 
        Columns removed [-1]: {'status'}
        Columns added [+1]: {'new_column'}
        Type changed [1]: {'order_date'}

        rows       PROD    <>            DEV
        ---------  ------  ------------  -----------------
        Total      10                    11 [+1]
        Added              +2
        Removed            -1
        Different          9
        Unchanged          0

        columns                 # diff values
        --------------------  ---------------
        amount                              8
        bank_transfer_amount                3
        coupon_amount                       3
        credit_card_amount                  6
        customer_id                         9
        gift_card_amount                    2 
        """

        stg_payments_expected_output = """
        jaffle_shop.prod.stg_payments <> jaffle_shop.dev.stg_payments 
        Primary Keys: ['payment_id'] 
        No row differences
        """

        stg_customers_expected_output = """
        jaffle_shop.prod.stg_customers <> jaffle_shop.dev.stg_customers 
        Primary Keys: ['customer_id'] 
        No row differences
        """

        stg_orders_expected_output = """
        jaffle_shop.prod.stg_orders <> jaffle_shop.dev.stg_orders 
        Primary Keys: ['order_id'] 
        No row differences
        """

        customers_expected_output = """
        jaffle_shop.prod.customers <> jaffle_shop.dev.customers 
        Primary Keys: ['customer_id'] 
        No row differences
        """

        expected_outputs = [
            orders_expected_output,
            stg_payments_expected_output,
            stg_customers_expected_output,
            stg_orders_expected_output,
            customers_expected_output,
        ]

        if test_project_path == artifacts_path:
            actual_output_stripped = b"".join(diff).decode("utf-8").strip().replace(" ", "")

            for expected_output in expected_outputs:
                expected_output_stripped = "".join(line.strip() for line in expected_output.split("\n")).replace(
                    " ", ""
                )
                assert expected_output_stripped in actual_output_stripped

    @unittest.skipIf(
        not os.environ.get("MOTHERDUCK_TOKEN"),
        "MOTHERDUCK_TOKEN doesn't exist or is empty if this is run from a forked branch pull request",
    )
    def test_integration_motherduck_dbt(self):
        artifacts_path = os.getcwd() + "/tests/dbt_artifacts"
        test_project_path = os.environ.get("DATA_DIFF_DBT_PROJ") or artifacts_path
        test_profiles_path = os.environ.get("DATA_DIFF_DBT_PROJ") or artifacts_path + "/motherduck"
        diff = run_datadiff_cli(
            "--dbt", "--dbt-project-dir", test_project_path, "--dbt-profiles-dir", test_profiles_path
        )

        # assertions for the diff that exists in tests/dbt_artifacts/jaffle_shop.duckdb
        if test_project_path == artifacts_path:
            diff_string = b"".join(diff).decode("utf-8")
            # 4 with no diffs
            assert diff_string.count("No row differences") == 4
            # 1 with a diff
            assert diff_string.count("PROD") == 1
            assert diff_string.count("DEV") == 1
            assert diff_string.count("Primary Keys") == 5
            assert diff_string.count("Where Filter") == 1
            assert diff_string.count("Type Changed") == 0
            assert diff_string.count("Total") == 1
            assert diff_string.count("Added") == 1
            assert diff_string.count("Removed") == 1
            assert diff_string.count("Different") == 1
            assert diff_string.count("Unchanged") == 1

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
        column_dictionary = {
            "col1": RawColumnInfo(column_name="col1", data_type="type"),
            "col2": RawColumnInfo(column_name="col2", data_type="type"),
        }
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
        mock_connect.assert_any_call(connection, ".".join(dev_qualified_list), tuple(expected_primary_keys))
        mock_connect.assert_any_call(connection, ".".join(prod_qualified_list), tuple(expected_primary_keys))
        mock_diff.get_stats_string.assert_called_once()

    @patch("data_diff.dbt.diff_tables")
    def test_local_diff_types_differ(self, mock_diff_tables):
        connection = {}
        mock_table1 = Mock()
        mock_table2 = Mock()
        table1_column_dictionary = {
            "col1": RawColumnInfo(column_name="col1", data_type="type"),
            "col2": RawColumnInfo(column_name="col2", data_type="type"),
        }
        table2_column_dictionary = {
            "col1": RawColumnInfo(column_name="col1", data_type="type"),
            "col2": RawColumnInfo(column_name="col2", data_type="differing_type"),
        }
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
        column_dictionary = {
            "col1": RawColumnInfo(column_name="col1", data_type="type"),
            "col2": RawColumnInfo(column_name="col2", data_type="type"),
        }
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
        mock_connect.assert_any_call(connection, ".".join(dev_qualified_list), tuple(expected_primary_keys))
        mock_connect.assert_any_call(connection, ".".join(prod_qualified_list), tuple(expected_primary_keys))
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
        include_columns = ["created_at", "num_users", "sub_created_at", "sub_plan"]
        exclude_columns = ["new_column"]
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
            include_columns=include_columns,
            exclude_columns=exclude_columns,
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
        self.assertEqual(payload.include_columns, include_columns)
        self.assertEqual(payload.exclude_columns, exclude_columns)

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
        mock_dbt_parser_inst.threads = threads
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
        mock_cloud_diff.assert_called_once_with(diff_vars, 1, mock_api, org_meta, None)
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

    @patch("data_diff.dbt.keyring.get_password")
    @patch("data_diff.dbt._get_diff_vars")
    @patch("data_diff.dbt._local_diff")
    @patch("data_diff.dbt._cloud_diff")
    @patch("data_diff.dbt_parser.DbtParser.__new__")
    @patch("builtins.input", return_value="n")
    def test_diff_is_cloud_no_api_key(
        self, _, mock_dbt_parser, mock_cloud_diff, mock_local_diff, mock_get_diff_vars, mock_get_password
    ):
        mock_get_password.return_value = None
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
        mock_dbt_parser_inst.threads = threads
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
        mock_local_diff.assert_called_once_with(diff_vars, False, None)
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
        mock_dbt_parser_inst.threads = threads
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
        mock_dbt_parser_inst.threads = threads
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
        mock_local_diff.assert_called_once_with(diff_vars, False, None)
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
        mock_dbt_parser_inst.threads = threads
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
        mock_local_diff.assert_called_once_with(diff_vars, False, None)
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
        mock_dbt_parser_inst.threads = threads
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
        mock_dbt_parser_inst.threads = threads
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
        mock_prod_model.alias = "prod_alias"
        prod_database, prod_schema, prod_alias = _get_prod_path_from_manifest(mock_model, mock_prod_manifest)
        self.assertEqual(prod_database, mock_prod_model.database)
        self.assertEqual(prod_schema, mock_prod_model.schema_)
        self.assertEqual(prod_alias, mock_prod_model.alias)

    def test_get_prod_path_from_manifest_model_not_exists(self):
        mock_model = Mock()
        mock_model.unique_id = "unique_model_id"
        mock_prod_manifest = Mock()
        mock_prod_model = Mock()
        mock_prod_manifest.nodes.get.return_value = None
        mock_prod_model.database = "prod_db"
        mock_prod_model.schema_ = "prod_schema"
        mock_prod_model.alias = "prod_alias"
        prod_database, prod_schema, prod_alias = _get_prod_path_from_manifest(mock_model, mock_prod_manifest)
        self.assertEqual(prod_database, None)
        self.assertEqual(prod_schema, None)
        self.assertEqual(prod_alias, None)

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
        mock_model.unique_id = "unique_id"
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
        mock_model.unique_id = "unique_id"
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
        mock_model.unique_id = "unique_id"
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
        mock_model.unique_id = "unique_id"
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
        mock_model.unique_id = "unique_id"
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
        mock_model.unique_id = "unique_id"
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
        mock_prod_path_from_manifest.return_value = ("prod_db", "prod_schema", "prod_alias")

        diff_vars = _get_diff_vars(mock_dbt_parser, config, mock_model)

        mock_prod_path_from_manifest.assert_called_once_with(mock_model, mock_dbt_parser.prod_manifest_obj)
        self.assertEqual(diff_vars.prod_path[0], mock_prod_path_from_manifest.return_value[0])
        self.assertEqual(diff_vars.prod_path[1], mock_prod_path_from_manifest.return_value[1])

    @patch("data_diff.dbt._get_prod_path_from_config")
    @patch("data_diff.dbt._get_prod_path_from_manifest")
    def test_get_diff_vars_cli_columns(self, mock_prod_path_from_manifest, mock_prod_path_from_config):
        config = TDatadiffConfig(prod_database="prod_db")
        mock_model = Mock()
        primary_keys = ["a_primary_key"]
        mock_model.database = "a_dev_db"
        mock_model.schema_ = "a_schema"
        mock_model.config.schema_ = None
        mock_model.config.database = None
        mock_model.alias = "a_model_name"
        mock_model.unique_id = "unique_id"
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
        cli_columns = ("col1", "col2")
        production_database_flag_override = "prod_db_override"
        production_schema_flag_override = "prod_schema_override"

        diff_vars = _get_diff_vars(
            mock_dbt_parser,
            config,
            mock_model,
            where_flag=None,
            columns_flag=cli_columns,
            production_database_flag=production_database_flag_override,
            production_schema_flag=production_schema_flag_override,
        )

        mock_dbt_parser.get_pk_from_model.assert_called_once()
        mock_prod_path_from_config.assert_called_once_with(config, mock_model, mock_model.database, mock_model.schema_)
        mock_prod_path_from_manifest.assert_not_called()
        self.assertEqual(diff_vars.include_columns, list(cli_columns))
        self.assertEqual(diff_vars.exclude_columns, [])
        self.assertEqual(diff_vars.prod_path[0], production_database_flag_override)
        self.assertEqual(diff_vars.prod_path[1], production_schema_flag_override)
