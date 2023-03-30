import json
from pathlib import Path
from parameterized import parameterized
import unittest
from unittest.mock import Mock, patch

from data_diff.cloud.datafold_api import (
    TCloudApiDataSourceConfigSchema, TCloudApiDataSourceSchema, TCloudApiDataSource, TCloudApiDataSourceTestResult,
    TCloudDataSourceTestResult, TDsConfig, TestDataSourceStatus
)
from data_diff.cloud.data_source import TDataSourceTestStage, TestDataSourceStatus, create_ds_config, _check_data_source_exists, _test_data_source


DATA_SOURCE_CONFIGS = [
    TDsConfig(
        name='ds_name',
        type='snowflake',
        options={
            'account': 'account',
            'user': 'user',
            'password': 'password',
            'warehouse': 'warehouse',
            'role': 'role',
            'default_db': 'database',
        },
        float_tolerance=0.000001,
        temp_schema='database.temp_schema',
    ),
    TDsConfig(
        name='ds_name',
        type='pg',
        options={
            'host': 'host',
            'port': 5432,
            'user': 'user',
            'password': 'password',
            'dbname': 'database',
        },
        float_tolerance=0.000001,
        temp_schema='database.temp_schema',
    ),
    TDsConfig(
        name='ds_name',
        type='bigquery',
        options={
            'projectId': 'project_id',
            'jsonKeyFile': 'some_string',
            'location': 'US',
        },
        float_tolerance=0.000001,
        temp_schema='database.temp_schema',
    ),
    TDsConfig(
        name='ds_name',
        type='databricks',
        options={
            'host': 'host',
            'http_path': 'some_http_path',
            'http_password': 'password',
            'database': 'database',
        },
        float_tolerance=0.000001,
        temp_schema='database.temp_schema',
    ),
    TDsConfig(
        name='ds_name',
        type='redshift',
        options={
            'host': 'host',
            'port': 5432,
            'user': 'user',
            'password': 'password',
            'dbname': 'database',
        },
        float_tolerance=0.000001,
        temp_schema='database.temp_schema',
    ),
    TDsConfig(
        name='ds_name',
        type='postgres_aurora',
        options={
            'host': 'host',
            'port': 5432,
            'user': 'user',
            'password': 'password',
            'dbname': 'database',
        },
        float_tolerance=0.000001,
        temp_schema='database.temp_schema',
    ),
    TDsConfig(
        name='ds_name',
        type='postgres_aws_rds',
        options={
            'host': 'host',
            'port': 5432,
            'user': 'user',
            'password': 'password',
            'dbname': 'database',
        },
        float_tolerance=0.000001,
        temp_schema='database.temp_schema',
    ),
]


def format_data_source_config_test(testcase_func, param_num, param):
    config, = param.args
    return f'{testcase_func.__name__}_{config.type}'


class TestDataSource(unittest.TestCase):
    def setUp(self) -> None:
        with open(Path(__file__).parent / 'files/data_source_schema_config_response.json', 'r') as file:
            self.data_source_schema = [
                TCloudApiDataSourceConfigSchema(
                    name=item['name'],
                    db_type=item['type'],
                    config_schema=TCloudApiDataSourceSchema(
                        title=item['configuration_schema']['title'],
                        properties=item['configuration_schema']['properties'],
                        required=item['configuration_schema']['required'],
                        secret=item['configuration_schema']['secret'],
                    )
                )
                for item in json.load(file)
            ]

        self.db_type_data_source_schemas = {
            ds_schema.db_type: ds_schema
            for ds_schema in self.data_source_schema
        }

        with open(Path(__file__).parent / 'files/data_source_list_response.json', 'r') as file:
            self.data_sources = [TCloudApiDataSource(**item) for item in json.load(file)]

        self.api = Mock()
        self.api.get_data_source_schema_config.return_value = self.data_source_schema
        self.api.get_data_sources.return_value = self.data_sources

    @parameterized.expand([(c,) for c in DATA_SOURCE_CONFIGS], name_func=format_data_source_config_test)
    def test_create_ds_config(self, config: TDsConfig):
        inputs = list(config.options.values()) + [config.temp_schema, config.float_tolerance]
        with patch('rich.prompt.Console.input', side_effect=map(str, inputs)):
            actual_config = create_ds_config(
                ds_config=self.db_type_data_source_schemas[config.type],
                data_source_name=config.name,
            )
            self.assertEqual(actual_config, config)

    def test_check_data_source_exists(self):
        self.assertEqual(_check_data_source_exists(self.data_sources, self.data_sources[0].name), self.data_sources[0])

    def test_check_data_source_not_exists(self):
        self.assertEqual(_check_data_source_exists(self.data_sources, 'ds_with_this_name_does_not_exist'), None)

    @patch('data_diff.cloud.data_source.DatafoldAPI')
    def test_data_source_all_tests_ok(self, mock_api: Mock):
        mock_api.test_data_source.return_value = 1
        mock_api.check_data_source_test_results.return_value = [
            TCloudApiDataSourceTestResult(
                name='lineage_download',
                status='done',
                result=TCloudDataSourceTestResult(
                    status=TestDataSourceStatus.SUCCESS,
                    message='No lineage downloader for this data source',
                    outcome='skipped',
                ),
            ),
            TCloudApiDataSourceTestResult(
                name='schema_download',
                status='done',
                result=TCloudDataSourceTestResult(
                    status=TestDataSourceStatus.SUCCESS, message='Discovered 6 tables', outcome='success'
                ),
            ),
            TCloudApiDataSourceTestResult(
                name='temp_schema',
                status='done',
                result=TCloudDataSourceTestResult(
                    status=TestDataSourceStatus.FAILED, message='Created table "database"."schema"',  outcome='failed'
                ),
            ), TCloudApiDataSourceTestResult(
                name='connection',
                status='done',
                result=TCloudDataSourceTestResult(
                    status=TestDataSourceStatus.SUCCESS, message='Connected to the database', outcome='success'
                ),
            ),
        ]

        expected_results = [
            TDataSourceTestStage(
                name='schema_download', status=TestDataSourceStatus.SUCCESS, description='Discovered 6 tables'
            ),
            TDataSourceTestStage(
                name='temp_schema', status=TestDataSourceStatus.FAILED, description='Created table "database"."schema"'
            ),
            TDataSourceTestStage(
                name='connection', status=TestDataSourceStatus.SUCCESS, description='Connected to the database'
            ),
        ]

        self.assertEqual(_test_data_source(api=mock_api, data_source_id=1), expected_results)

    @patch('data_diff.cloud.data_source.DatafoldAPI')
    def test_data_source_one_test_failed(self, mock_api: Mock):
        mock_api.test_data_source.return_value = 1
        mock_api.check_data_source_test_results.return_value = [
            TCloudApiDataSourceTestResult(
                name='lineage_download',
                status='done',
                result=TCloudDataSourceTestResult(
                    status=TestDataSourceStatus.SUCCESS,
                    message='No lineage downloader for this data source',
                    outcome='skipped',
                ),
            ),
            TCloudApiDataSourceTestResult(
                name='schema_download',
                status='done',
                result=TCloudDataSourceTestResult(
                    status=TestDataSourceStatus.SUCCESS,
                    message='Discovered 6 tables',
                    outcome='success'),
            ),
            TCloudApiDataSourceTestResult(
                name='temp_schema',
                status='done',
                result=TCloudDataSourceTestResult(
                    status=TestDataSourceStatus.FAILED,
                    message='Unable to create table "database"."schema"',
                    outcome='failed',
                ),
            ), TCloudApiDataSourceTestResult(
                name='connection',
                status='done',
                result=TCloudDataSourceTestResult(
                    status=TestDataSourceStatus.SUCCESS,
                    message='Connected to the database',
                    outcome='success'),
            ),
        ]

        expected_results = [
            TDataSourceTestStage(
                name='schema_download', status=TestDataSourceStatus.SUCCESS, description='Discovered 6 tables'
            ),
            TDataSourceTestStage(
                name='temp_schema',
                status=TestDataSourceStatus.FAILED,
                description='Unable to create table "database"."schema"',
            ),
            TDataSourceTestStage(
                name='connection', status=TestDataSourceStatus.SUCCESS, description='Connected to the database'
            ),
        ]

        self.assertEqual(_test_data_source(api=mock_api, data_source_id=1), expected_results)
