import json
import time
from typing import List, Optional, Union, overload

import pydantic
import rich
from rich.table import Table
from rich.prompt import Confirm, Prompt, FloatPrompt, IntPrompt, InvalidResponse
from typing_extensions import Literal

from data_diff.cloud.datafold_api import (
    DatafoldAPI,
    TCloudApiDataSourceConfigSchema,
    TCloudApiDataSource,
    TDsConfig,
    TestDataSourceStatus,
)
from data_diff.dbt_parser import DbtParser


UNKNOWN_VALUE = "unknown_value"


class TDataSourceTestStage(pydantic.BaseModel):
    name: str
    status: TestDataSourceStatus
    description: str = ""


class TemporarySchemaPrompt(Prompt):
    response_type = str

    def process_response(self, value: str) -> str:
        """Convert choices to a bool."""

        if len(value.split(".")) != 2:
            raise InvalidResponse("Temporary schema should have a format <database>.<schema>")
        return value


class ValueRequiredPrompt(Prompt):
    def process_response(self, value: str) -> str:
        value = super().process_response(value)
        if value == UNKNOWN_VALUE or value is None or value == "":
            raise InvalidResponse("Parameter must not be empty")
        return value


def _validate_temp_schema(temp_schema: str):
    if len(temp_schema.split(".")) != 2:
        raise ValueError("Temporary schema should have a format <database>.<schema>")


def _get_temp_schema(dbt_parser: DbtParser, db_type: str) -> Optional[str]:
    config = dbt_parser.get_datadiff_config()
    config_prod_database = config.prod_database
    config_prod_schema = config.prod_schema
    if config_prod_database is not None and config_prod_schema is not None:
        temp_schema = f"{config_prod_database}.{config_prod_schema}"
        if db_type == "snowflake":
            return temp_schema.upper()
        elif db_type in {"pg", "postgres_aurora", "postgres_aws_rds", "redshift"}:
            return temp_schema.lower()
        return temp_schema
    return


def create_ds_config(
    ds_config: TCloudApiDataSourceConfigSchema,
    data_source_name: str,
    dbt_parser: Optional[DbtParser] = None,
) -> TDsConfig:
    options = _parse_ds_credentials(ds_config=ds_config, only_basic_settings=True, dbt_parser=dbt_parser)

    temp_schema = _get_temp_schema(dbt_parser=dbt_parser, db_type=ds_config.db_type) if dbt_parser else None
    if temp_schema:
        temp_schema = TemporarySchemaPrompt.ask("Temporary schema", default=temp_schema)
    else:
        temp_schema = TemporarySchemaPrompt.ask("Temporary schema (<database>.<schema>)")

    float_tolerance = FloatPrompt.ask("Float tolerance", default=0.000001)

    return TDsConfig(
        name=data_source_name,
        type=ds_config.db_type,
        temp_schema=temp_schema,
        float_tolerance=float_tolerance,
        options=options,
    )


@overload
def _cast_value(value: str, type_: Literal["integer"]) -> int:
    ...


@overload
def _cast_value(value: str, type_: Literal["boolean"]) -> bool:
    ...


@overload
def _cast_value(value: str, type_: Literal["string"]) -> str:
    ...


def _cast_value(value: str, type_: str) -> Union[bool, int, str]:
    if type_ == "integer":
        return int(value)
    elif type_ == "boolean":
        return bool(value)
    return value


def _get_data_from_bigquery_json(path: str):
    with open(path, "r") as file:
        return json.load(file)


def _align_dbt_cred_params_with_datafold_params(dbt_creds: dict) -> dict:
    db_type = dbt_creds["type"]
    if db_type == "bigquery":
        method = dbt_creds["method"]
        if method == "service-account":
            data = _get_data_from_bigquery_json(path=dbt_creds["keyfile"])
            dbt_creds["jsonKeyFile"] = json.dumps(data)
        elif method == "service-account-json":
            dbt_creds["jsonKeyFile"] = json.dumps(dbt_creds["keyfile_json"])
        else:
            rich.print(
                f'[red]Cannot extract bigquery credentials from dbt_project.yml for "{method}" type. '
                f"If you want to provide credentials via dbt_project.yml, "
                f'please, use "service-account" or "service-account-json" '
                f"(more in docs: https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup). "
                f"Otherwise, you can provide a path to a json key file or a json key file data as an input."
            )
        dbt_creds["projectId"] = dbt_creds["project"]
    elif db_type == "snowflake":
        dbt_creds["default_db"] = dbt_creds["database"]
    elif db_type == "databricks":
        dbt_creds["http_password"] = dbt_creds["token"]
        dbt_creds["database"] = dbt_creds.get("catalog")
    return dbt_creds


def _parse_ds_credentials(
    ds_config: TCloudApiDataSourceConfigSchema, only_basic_settings: bool = True, dbt_parser: Optional[DbtParser] = None
):
    creds = {}
    use_dbt_data = False
    if dbt_parser is not None:
        use_dbt_data = Confirm.ask("Would you like to extract database credentials from dbt profiles.yml?")
        try:
            creds = dbt_parser.get_connection_creds()[0]
            creds = _align_dbt_cred_params_with_datafold_params(dbt_creds=creds)
        except Exception as e:
            rich.print(f"[red]Cannot parse database credentials from dbt profiles.yml. Reason: {e}")

    ds_options = {}
    basic_required_fields = set(ds_config.config_schema.required)
    for param_name, param_data in ds_config.config_schema.properties.items():
        if only_basic_settings and param_name not in basic_required_fields:
            continue

        default_value = param_data.get("default", UNKNOWN_VALUE)
        is_password = bool(param_data.get("format"))

        title = param_data["title"]
        type_ = param_data["type"]
        input_values = {
            "prompt": title,
            "password": is_password,
        }
        if default_value != UNKNOWN_VALUE:
            input_values["default"] = default_value

        if use_dbt_data:
            value = creds.get(param_name, UNKNOWN_VALUE)
            if value == UNKNOWN_VALUE:
                rich.print(f'[red]Cannot extract "{param_name}" from dbt profiles.yml. Please, type it manually')
            else:
                ds_options[param_name] = _cast_value(value, type_)
                continue

        if type_ == "integer":
            value = IntPrompt.ask(**input_values)
        elif type_ == "boolean":
            value = Confirm.ask(title)
        else:
            value = ValueRequiredPrompt.ask(**input_values)

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


def _test_data_source(api: DatafoldAPI, data_source_id: int, timeout: int = 64) -> List[TDataSourceTestStage]:
    job_id = api.test_data_source(data_source_id)

    checked_tests = {"connection", "temp_schema", "schema_download"}
    seconds = 1
    start = time.monotonic()
    results = []
    while True:
        tests = api.check_data_source_test_results(job_id)
        for test in tests:
            if test.name not in checked_tests:
                continue

            if test.status == "done":
                checked_tests.remove(test.name)
                results.append(
                    TDataSourceTestStage(name=test.name, status=test.result.status, description=test.result.message)
                )

        if not checked_tests:
            break

        if time.monotonic() - start > timeout:
            for test_name in checked_tests:
                results.append(
                    TDataSourceTestStage(
                        name=test_name,
                        status=TestDataSourceStatus.SKIP,
                        description=f"Does not complete in {timeout} seconds",
                    )
                )
            break
        time.sleep(seconds)
        seconds *= 2

    return results


def _render_data_source(data_source: TCloudApiDataSource, title: str = "") -> None:
    table = Table(title=title, min_width=80)
    table.add_column("Parameter", justify="center", style="cyan")
    table.add_column("Value", justify="center", style="magenta")
    table.add_row("ID", str(data_source.id))
    table.add_row("Name", data_source.name)
    table.add_row("Type", data_source.type)
    rich.print(table)


def _render_available_data_sources(data_source_schema_configs: List[TCloudApiDataSourceConfigSchema]) -> None:
    config_names = [ds_config.name for ds_config in data_source_schema_configs]

    table = Table()
    table.add_column("", justify="center", style="cyan")
    table.add_column("Available data sources", style="magenta")
    for i, db_type in enumerate(config_names, start=1):
        table.add_row(str(i), db_type)
    rich.print(table)


def _render_data_source_test_results(test_results: List[TDataSourceTestStage]) -> None:
    table = Table(title="Test results", min_width=80)
    table.add_column(
        "Test",
        justify="center",
        style="cyan",
    )
    table.add_column("Status", justify="center", style="magenta")
    table.add_column("Description", justify="center", style="magenta")
    for result in test_results:
        table.add_row(result.name, result.status, result.description)
    rich.print(table)


def get_or_create_data_source(api: DatafoldAPI, dbt_parser: Optional[DbtParser] = None) -> int:
    ds_configs = api.get_data_source_schema_config()
    data_sources = api.get_data_sources()

    _render_available_data_sources(data_source_schema_configs=ds_configs)
    db_type_num = IntPrompt.ask(
        prompt="What data source type do you want to create? Please, select a number",
        choices=list(map(str, range(1, len(ds_configs) + 1))),
        show_choices=False,
    )

    ds_config = ds_configs[db_type_num - 1]
    default_ds_name = ds_config.name
    rich.print("Press enter to accept the (Default value)")
    ds_name = Prompt.ask("Data source name", default=default_ds_name)

    ds = _check_data_source_exists(data_sources=data_sources, data_source_name=ds_name)
    if ds is not None:
        _render_data_source(data_source=ds, title=f'Found existing data source for name "{ds.name}"')
        use_existing_ds = Confirm.ask("Would you like to continue with the existing data source?")
        if not use_existing_ds:
            return get_or_create_data_source(api=api, dbt_parser=dbt_parser)
        return ds.id

    ds_config = create_ds_config(ds_config=ds_config, data_source_name=ds_name, dbt_parser=dbt_parser)
    ds = api.create_data_source(ds_config)
    data_source_url = f"{api.host}/settings/integrations/dwh/{ds.type}/{ds.id}"
    _render_data_source(data_source=ds, title=f"Created a new data source with ID = {ds.id} ({data_source_url})")

    rich.print(
        "We recommend to run tests for a new data source. "
        "It requires some time but makes sure that the data source is configured correctly."
    )
    run_tests = Confirm.ask("Would you like to run tests?")
    if run_tests:
        test_results = _test_data_source(api=api, data_source_id=ds.id)
        _render_data_source_test_results(test_results=test_results)
        if any(result.status == TestDataSourceStatus.FAILED for result in test_results):
            raise ValueError(
                f"Data source tests failed. Please, try to update or test data source in the UI: {data_source_url}"
            )

    return ds.id
