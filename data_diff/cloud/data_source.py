from typing import List, Optional

import rich
from rich.table import Table
from rich.prompt import Confirm, Prompt, FloatPrompt, IntPrompt, InvalidResponse

from .datafold_api import DatafoldAPI, TCloudApiDataSourceConfigSchema, TCloudApiDataSource, TDsConfig


DATA_SOURCE_TYPES_REQUIRED_SETTINGS = {
    'bigquery': {'projectId', 'jsonKeyFile', 'location'},
    'databricks': {'host', 'http_password', 'database', 'http_pats'},
    'mysql': {'host', 'user', 'passwd', 'db'},
    'pg': {'host', 'user', 'port', 'password', 'dbname'},
    'postgres_aurora': {'host', 'user', 'port', 'password', 'dbname'},
    'postgres_aws_rds': {'host', 'user', 'port', 'password', 'dbname'},
    'redshift': {'host', 'user', 'port', 'password', 'dbname'},
    'snowflake': {'account', 'user', 'password', 'warehouse', 'role', 'default_db'},
}


class TemporarySchemaPrompt(Prompt):
    response_type = str
    validate_error_message = "[prompt.invalid]Please enter Y or N"

    def process_response(self, value: str) -> str:
        """Convert choices to a bool."""

        if len(value.split('.')) != 2:
            raise InvalidResponse('Temporary schema should has a format <database>.<schema>')
        return value


def _validate_temp_schema(temp_schema: str):
    if len(temp_schema.split('.')) != 2:
        raise ValueError('Temporary schema should has a format <database>.<schema>')


def create_ds_config(ds_config: TCloudApiDataSourceConfigSchema, ds_name: str) -> TDsConfig:
    options = _parse_ds_credentials(ds_config=ds_config, only_basic_settings=True)

    temp_schema = TemporarySchemaPrompt.ask('Temporary schema (<database>.<schema>)')
    float_tolerance = FloatPrompt.ask('Float tolerance', default=0.000001)

    return TDsConfig(
        name=ds_name,
        type=ds_config.db_type,
        temp_schema=temp_schema,
        float_tolerance=float_tolerance,
        options=options,
    )


def _parse_ds_credentials(ds_config: TCloudApiDataSourceConfigSchema, only_basic_settings: bool = True):
    ds_options = {}
    basic_required_fields = DATA_SOURCE_TYPES_REQUIRED_SETTINGS.get(ds_config.db_type)
    for param_name, param_data in ds_config.config_schema.properties.items():
        if only_basic_settings and param_name not in basic_required_fields:
            continue

        title = param_data['title']
        default_value = param_data.get('default')
        is_password = bool(param_data.get('format'))

        type_ = param_data['type']
        if type_ == 'integer':
            value = IntPrompt.ask(title, default=default_value if default_value is not None else None)
        elif type_ == 'boolean':
            value = Confirm.ask(title)
        else:
            value = Prompt.ask(
                title,
                default=default_value if default_value is not None else None,
                password=is_password,
            )

        ds_options[param_name] = value
    return ds_options


def _check_data_source_exists(
    data_sources: List[TCloudApiDataSource],
    data_source_name: str,
) -> Optional[TCloudApiDataSource]:
    for ds in data_sources:
        if ds.name == data_source_name:
            return ds
    return None


def _render_table(data_source: TCloudApiDataSource, title: str) -> None:
    table = Table(title=title)
    table.add_column("Parameter", justify="center", style="cyan", no_wrap=True)
    table.add_column("Value", justify="center", style="magenta", no_wrap=True)
    table.add_row("ID", str(data_source.id))
    table.add_row("Name", data_source.name)
    table.add_row("Type", data_source.type)
    rich.print(table)


def get_or_create_data_source(api: DatafoldAPI) -> int:
    ds_configs = api.get_data_source_schema_config()
    config_names = [ds_config.name for ds_config in ds_configs]

    for i, db_type in enumerate(config_names, start=1):
        rich.print(f'{i}) {db_type}')
    db_type_num = IntPrompt.ask(
        'What data source type you want to create? Please, select a number',
        choices=list(map(str, range(1, len(config_names) + 1))),
        show_choices=False
    )

    ds_config = ds_configs[db_type_num - 1]
    default_ds_name = ds_config.name
    ds_name = Prompt.ask("Data source name", default=default_ds_name)

    data_sources = api.get_data_sources()
    ds = _check_data_source_exists(data_sources=data_sources, data_source_name=ds_name)
    if ds is not None:
        _render_table(data_source=ds, title=f'Found data source with name "{ds.name}"')
        use_existing_ds = Confirm.ask("Would you like to continue with existing data source?")
        if not use_existing_ds:
            return get_or_create_data_source(api)
        return ds.id

    ds_config = create_ds_config(ds_config, ds_name)
    ds = api.create_data_source(ds_config)
    _render_table(data_source=ds, title='Create data a new data source')
    return ds.id
