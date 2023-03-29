import json
from pathlib import Path
from parameterized import parameterized
import unittest
from unittest.mock import Mock, patch

from data_diff.cloud.datafold_api import TCloudApiDataSourceConfigSchema, TCloudApiDataSourceSchema, TCloudApiDataSource, TDsConfig
from data_diff.cloud.data_source import create_ds_config, _check_data_source_exists


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
