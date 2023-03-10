import json
import logging
import os
import time
import rich
from dataclasses import dataclass
from packaging.version import parse as parse_version
from typing import List, Optional, Dict, Tuple
from pathlib import Path

import requests


def import_dbt():
    try:
        from dbt_artifacts_parser.parser import parse_run_results, parse_manifest
        from dbt.config.renderer import ProfileRenderer
        import yaml
    except ImportError:
        raise RuntimeError("Could not import 'dbt' package. You can install it using: pip install 'data-diff[dbt]'.")

    return parse_run_results, parse_manifest, ProfileRenderer, yaml


from .tracking import (
    set_entrypoint_name,
    create_end_event_json,
    create_start_event_json,
    send_event_json,
    is_tracking_enabled,
)
from .utils import get_from_dict_with_raise, run_as_daemon, truncate_error
from . import connect_to_table, diff_tables, Algorithm

RUN_RESULTS_PATH = "target/run_results.json"
MANIFEST_PATH = "target/manifest.json"
PROJECT_FILE = "dbt_project.yml"
PROFILES_FILE = "profiles.yml"
LOWER_DBT_V = "1.0.0"
UPPER_DBT_V = "1.4.5"


# https://github.com/dbt-labs/dbt-core/blob/c952d44ec5c2506995fbad75320acbae49125d3d/core/dbt/cli/resolvers.py#L6
def default_project_dir() -> Path:
    paths = list(Path.cwd().parents)
    paths.insert(0, Path.cwd())
    return next((x for x in paths if (x / PROJECT_FILE).exists()), Path.cwd())


# https://github.com/dbt-labs/dbt-core/blob/c952d44ec5c2506995fbad75320acbae49125d3d/core/dbt/cli/resolvers.py#L12
def default_profiles_dir() -> Path:
    return Path.cwd() if (Path.cwd() / PROFILES_FILE).exists() else Path.home() / ".dbt"


def legacy_profiles_dir() -> Path:
    return Path.home() / ".dbt"


@dataclass
class DiffVars:
    dev_path: List[str]
    prod_path: List[str]
    primary_keys: List[str]
    datasource_id: str
    connection: Dict[str, str]
    threads: Optional[int]


def dbt_diff(
    profiles_dir_override: Optional[str] = None, project_dir_override: Optional[str] = None, is_cloud: bool = False
) -> None:
    set_entrypoint_name("CLI-dbt")
    dbt_parser = DbtParser(profiles_dir_override, project_dir_override, is_cloud)
    models = dbt_parser.get_models()
    dbt_parser.set_project_dict()
    datadiff_variables = dbt_parser.get_datadiff_variables()
    config_prod_database = datadiff_variables.get("prod_database")
    config_prod_schema = datadiff_variables.get("prod_schema")
    datasource_id = datadiff_variables.get("datasource_id")
    custom_schemas = datadiff_variables.get("custom_schemas")
    # custom schemas is default dbt behavior, so default to True if the var doesn't exist
    custom_schemas = True if custom_schemas is None else custom_schemas

    if not is_cloud:
        dbt_parser.set_connection()

    if config_prod_database is None:
        raise ValueError(
            "Expected a value for prod_database: OR prod_database: AND prod_schema: under \nvars:\n  data_diff: "
        )

    for model in models:
        diff_vars = _get_diff_vars(
            dbt_parser, config_prod_database, config_prod_schema, model, datasource_id, custom_schemas
        )

        if is_cloud and len(diff_vars.primary_keys) > 0:
            _cloud_diff(diff_vars)
        elif not is_cloud and len(diff_vars.primary_keys) > 0:
            _local_diff(diff_vars)
        else:
            rich.print(
                "[red]"
                + ".".join(diff_vars.prod_path)
                + " <> "
                + ".".join(diff_vars.dev_path)
                + "[/] \n"
                + "Skipped due to missing primary-key tag(s).\n"
            )

    rich.print("Diffs Complete!")


def _get_diff_vars(
    dbt_parser: "DbtParser",
    config_prod_database: Optional[str],
    config_prod_schema: Optional[str],
    model,
    datasource_id: int,
    custom_schemas: bool,
) -> DiffVars:
    dev_database = model.database
    dev_schema = model.schema_
    primary_keys = dbt_parser.get_primary_keys(model)

    prod_database = config_prod_database if config_prod_database else dev_database
    prod_schema = config_prod_schema if config_prod_schema else dev_schema

    # if project has custom schemas (default)
    # need to construct the prod schema as <prod_target_schema>_<custom_schema>
    # https://docs.getdbt.com/docs/build/custom-schemas
    if custom_schemas and model.config.schema_:
        prod_schema = prod_schema + "_" + model.config.schema_

    if dbt_parser.requires_upper:
        dev_qualified_list = [x.upper() for x in [dev_database, dev_schema, model.alias]]
        prod_qualified_list = [x.upper() for x in [prod_database, prod_schema, model.alias]]
        primary_keys = [x.upper() for x in primary_keys]
    else:
        dev_qualified_list = [dev_database, dev_schema, model.alias]
        prod_qualified_list = [prod_database, prod_schema, model.alias]

    return DiffVars(
        dev_qualified_list, prod_qualified_list, primary_keys, datasource_id, dbt_parser.connection, dbt_parser.threads
    )


def _local_diff(diff_vars: DiffVars) -> None:
    column_diffs_str = ""
    dev_qualified_string = ".".join(diff_vars.dev_path)
    prod_qualified_string = ".".join(diff_vars.prod_path)

    table1 = connect_to_table(
        diff_vars.connection, dev_qualified_string, tuple(diff_vars.primary_keys), diff_vars.threads
    )
    table2 = connect_to_table(
        diff_vars.connection, prod_qualified_string, tuple(diff_vars.primary_keys), diff_vars.threads
    )

    table1_columns = list(table1.get_schema())
    try:
        table2_columns = list(table2.get_schema())
    # Not ideal, but we don't have more specific exceptions yet
    except Exception as ex:
        logging.info(ex)
        rich.print(
            "[red]"
            + prod_qualified_string
            + " <> "
            + dev_qualified_string
            + "[/] \n"
            + column_diffs_str
            + "[green]New model or no access to prod table.[/] \n"
        )
        return

    mutual_set = set(table1_columns) & set(table2_columns)
    table1_set_diff = list(set(table1_columns) - set(table2_columns))
    table2_set_diff = list(set(table2_columns) - set(table1_columns))

    if table1_set_diff:
        column_diffs_str += "Column(s) added: " + str(table1_set_diff) + "\n"

    if table2_set_diff:
        column_diffs_str += "Column(s) removed: " + str(table2_set_diff) + "\n"

    mutual_set = mutual_set - set(diff_vars.primary_keys)
    extra_columns = tuple(mutual_set)

    diff = diff_tables(table1, table2, threaded=True, algorithm=Algorithm.JOINDIFF, extra_columns=extra_columns)

    if list(diff):
        rich.print(
            "[red]"
            + prod_qualified_string
            + " <> "
            + dev_qualified_string
            + "[/] \n"
            + column_diffs_str
            + diff.get_stats_string(is_dbt=True)
            + "\n"
        )
    else:
        rich.print(
            "[red]"
            + prod_qualified_string
            + " <> "
            + dev_qualified_string
            + "[/] \n"
            + column_diffs_str
            + "[green]No row differences[/] \n"
        )


def _cloud_diff(diff_vars: DiffVars) -> None:
    api_key = os.environ.get("DATAFOLD_API_KEY")

    if diff_vars.datasource_id is None:
        raise ValueError(
            "Datasource ID not found, include it as a dbt variable in the dbt_project.yml. \nvars:\n data_diff:\n   datasource_id: 1234"
        )
    if api_key is None:
        raise ValueError("API key not found, add it as an environment variable called DATAFOLD_API_KEY.")

    url = "https://app.datafold.com/api/v1/datadiffs"

    payload = {
        "data_source1_id": diff_vars.datasource_id,
        "data_source2_id": diff_vars.datasource_id,
        "table1": diff_vars.prod_path,
        "table2": diff_vars.dev_path,
        "pk_columns": diff_vars.primary_keys,
    }

    headers = {
        "Authorization": f"Key {api_key}",
        "Content-Type": "application/json",
    }
    if is_tracking_enabled():
        event_json = create_start_event_json({"is_cloud": True, "datasource_id": diff_vars.datasource_id})
        run_as_daemon(send_event_json, event_json)

    start = time.monotonic()
    error = None
    diff_id = None
    try:
        response = requests.request("POST", url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        diff_id = data["id"]
        # TODO in future we should support self hosted datafold
        diff_url = f"https://app.datafold.com/datadiffs/{diff_id}/overview"
        rich.print(
            "[red]"
            + ".".join(diff_vars.prod_path)
            + " <> "
            + ".".join(diff_vars.dev_path)
            + "[/] \n    Diff in progress: \n    "
            + diff_url
            + "\n"
        )
    except BaseException as ex:  # Catch KeyboardInterrupt too
        error = ex
    finally:
        # we don't currently have much of this information
        # but I imagine a future iteration of this _cloud method
        # will poll for results
        if is_tracking_enabled():
            err_message = truncate_error(repr(error))
            event_json = create_end_event_json(
                is_success=error is None,
                runtime_seconds=time.monotonic() - start,
                data_source_1_type="",
                data_source_2_type="",
                table1_count=0,
                table2_count=0,
                diff_count=0,
                error=err_message,
                diff_id=diff_id,
                is_cloud=True,
            )
            send_event_json(event_json)

        if error:
            raise error


class DbtParser:
    def __init__(self, profiles_dir_override: str, project_dir_override: str, is_cloud: bool) -> None:
        self.profiles_dir = Path(profiles_dir_override or default_profiles_dir())
        self.project_dir = Path(project_dir_override or default_project_dir())
        self.is_cloud = is_cloud
        self.connection = None
        self.project_dict = None
        self.requires_upper = False
        self.threads = None

        self.parse_run_results, self.parse_manifest, self.ProfileRenderer, self.yaml = import_dbt()

    def get_datadiff_variables(self) -> dict:
        return self.project_dict.get("vars").get("data_diff")

    def get_models(self):
        with open(self.project_dir / RUN_RESULTS_PATH) as run_results:
            run_results_dict = json.load(run_results)
            run_results_obj = self.parse_run_results(run_results=run_results_dict)

        dbt_version = parse_version(run_results_obj.metadata.dbt_version)

        if dbt_version < parse_version("1.3.0"):
            self.profiles_dir = legacy_profiles_dir()

        if dbt_version < parse_version(LOWER_DBT_V) or dbt_version >= parse_version(UPPER_DBT_V):
            raise Exception(
                f"Found dbt: v{dbt_version} Expected the dbt project's version to be >= {LOWER_DBT_V} and < {UPPER_DBT_V}"
            )

        with open(self.project_dir / MANIFEST_PATH) as manifest:
            manifest_dict = json.load(manifest)
            manifest_obj = self.parse_manifest(manifest=manifest_dict)

        success_models = [x.unique_id for x in run_results_obj.results if x.status.name == "success"]
        models = [manifest_obj.nodes.get(x) for x in success_models]
        if not models:
            raise ValueError("Expected > 0 successful models runs from the last dbt command.")

        rich.print(f"Found {str(len(models))} successful model runs from the last dbt command.")
        return models

    def get_primary_keys(self, model):
        return list((x.name for x in model.columns.values() if "primary-key" in x.tags))

    def set_project_dict(self):
        with open(self.project_dir / PROJECT_FILE) as project:
            self.project_dict = self.yaml.safe_load(project)

    def _get_connection_creds(self) -> Tuple[Dict[str, str], str]:
        profiles_path = self.profiles_dir / PROFILES_FILE
        with open(profiles_path) as profiles:
            profiles = self.yaml.safe_load(profiles)

        dbt_profile_var = self.project_dict.get("profile")

        profile = get_from_dict_with_raise(
            profiles, dbt_profile_var, f"No profile '{dbt_profile_var}' found in '{profiles_path}'."
        )
        # values can contain env_vars
        rendered_profile = self.ProfileRenderer().render_data(profile)
        profile_target = get_from_dict_with_raise(
            rendered_profile, "target", f"No target found in profile '{dbt_profile_var}' in '{profiles_path}'."
        )
        outputs = get_from_dict_with_raise(
            rendered_profile, "outputs", f"No outputs found in profile '{dbt_profile_var}' in '{profiles_path}'."
        )
        credentials = get_from_dict_with_raise(
            outputs,
            profile_target,
            f"No credentials found for target '{profile_target}' in profile '{dbt_profile_var}' in '{profiles_path}'.",
        )
        conn_type = get_from_dict_with_raise(
            credentials,
            "type",
            f"No type found for target '{profile_target}' in profile '{dbt_profile_var}' in '{profiles_path}'.",
        )
        conn_type = conn_type.lower()

        return credentials, conn_type

    def set_connection(self):
        credentials, conn_type = self._get_connection_creds()

        if conn_type == "snowflake":
            if credentials.get("password") is None or credentials.get("private_key_path") is not None:
                raise Exception("Only password authentication is currently supported for Snowflake.")
            conn_info = {
                "driver": conn_type,
                "user": credentials.get("user"),
                "password": credentials.get("password"),
                "account": credentials.get("account"),
                "database": credentials.get("database"),
                "warehouse": credentials.get("warehouse"),
                "role": credentials.get("role"),
                "schema": credentials.get("schema"),
            }
            self.threads = credentials.get("threads")
            self.requires_upper = True
        elif conn_type == "bigquery":
            method = credentials.get("method")
            # there are many connection types https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#oauth-via-gcloud
            # this assumes that the user is auth'd via `gcloud auth application-default login`
            if method is None or method != "oauth":
                raise Exception("Oauth is the current method supported for Big Query.")
            conn_info = {
                "driver": conn_type,
                "project": credentials.get("project"),
                "dataset": credentials.get("dataset"),
            }
            self.threads = credentials.get("threads")
        elif conn_type == "duckdb":
            conn_info = {
                "driver": conn_type,
                "filepath": credentials.get("path"),
            }
        elif conn_type == "redshift":
            if credentials.get("password") is None or credentials.get("method") == "iam":
                raise Exception("Only password authentication is currently supported for Redshift.")
            conn_info = {
                "driver": conn_type,
                "host": credentials.get("host"),
                "user": credentials.get("user"),
                "password": credentials.get("password"),
                "port": credentials.get("port"),
                "dbname": credentials.get("dbname"),
            }
            self.threads = credentials.get("threads")
        elif conn_type == "databricks":
            conn_info = {
                "driver": conn_type,
                "catalog": credentials.get("catalog"),
                "server_hostname": credentials.get("host"),
                "http_path": credentials.get("http_path"),
                "schema": credentials.get("schema"),
                "access_token": credentials.get("token"),
            }
            self.threads = credentials.get("threads")
        elif conn_type == "postgres":
            conn_info = {
                "driver": "postgresql",
                "host": credentials.get("host"),
                "user": credentials.get("user"),
                "password": credentials.get("password"),
                "port": credentials.get("port"),
                "dbname": credentials.get("dbname") or credentials.get("database"),
            }
            self.threads = credentials.get("threads")
        else:
            raise NotImplementedError(f"Provider {conn_type} is not yet supported for dbt diffs")

        self.connection = conn_info
