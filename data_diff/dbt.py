import json
import os
import time
import webbrowser
import rich
from rich.prompt import Confirm

from collections import defaultdict
from dataclasses import dataclass
from packaging.version import parse as parse_version
from typing import List, Optional, Dict, Tuple, Set
from .utils import dbt_diff_string_template, getLogger
from .version import __version__
from pathlib import Path

import requests

logger = getLogger(__name__)


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
    set_dbt_user_id,
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
UPPER_DBT_V = "1.4.6"


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
    connection: Dict[str, str]
    threads: Optional[int]


def dbt_diff(
    profiles_dir_override: Optional[str] = None, project_dir_override: Optional[str] = None, is_cloud: bool = False
) -> None:
    diff_threads = []
    set_entrypoint_name("CLI-dbt")
    dbt_parser = DbtParser(profiles_dir_override, project_dir_override)
    models = dbt_parser.get_models()
    datadiff_variables = dbt_parser.get_datadiff_variables()
    config_prod_database = datadiff_variables.get("prod_database")
    config_prod_schema = datadiff_variables.get("prod_schema")
    datasource_id = datadiff_variables.get("datasource_id")
    custom_schemas = datadiff_variables.get("custom_schemas")
    # custom schemas is default dbt behavior, so default to True if the var doesn't exist
    custom_schemas = True if custom_schemas is None else custom_schemas
    set_dbt_user_id(dbt_parser.dbt_user_id)

    if is_cloud:
        if datasource_id is None:
            raise ValueError(
                "Datasource ID not found, include it as a dbt variable in the dbt_project.yml. \nvars:\n data_diff:\n   datasource_id: 1234"
            )
        datafold_host, url, api_key = _setup_cloud_diff()

        # exit so the user can set the key
        if not api_key:
            return
    else:
        dbt_parser.set_connection()

    if config_prod_database is None:
        raise ValueError(
            "Expected a value for prod_database: OR prod_database: AND prod_schema: under \nvars:\n  data_diff: "
        )

    for model in models:
        diff_vars = _get_diff_vars(dbt_parser, config_prod_database, config_prod_schema, model, custom_schemas)

        if diff_vars.primary_keys:
            if is_cloud:
                diff_thread = run_as_daemon(_cloud_diff, diff_vars, datasource_id, datafold_host, url, api_key)
                diff_threads.append(diff_thread)
            else:
                _local_diff(diff_vars)
        else:
            rich.print(
                "[red]"
                + ".".join(diff_vars.prod_path)
                + " <> "
                + ".".join(diff_vars.dev_path)
                + "[/] \n"
                + "Skipped due to unknown primary key. Add uniqueness tests, meta, or tags.\n"
            )

    # wait for all threads
    if diff_threads:
        for thread in diff_threads:
            thread.join()

    rich.print("Diffs Complete!")


def _get_diff_vars(
    dbt_parser: "DbtParser",
    config_prod_database: Optional[str],
    config_prod_schema: Optional[str],
    model,
    custom_schemas: bool,
) -> DiffVars:
    dev_database = model.database
    dev_schema = model.schema_

    primary_keys = dbt_parser.get_pk_from_model(model, dbt_parser.unique_columns, "primary-key")

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

    return DiffVars(dev_qualified_list, prod_qualified_list, primary_keys, dbt_parser.connection, dbt_parser.threads)


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
        logger.debug(ex)
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


def _cloud_diff(diff_vars: DiffVars, datasource_id: int, datafold_host: str, url: str, api_key: str) -> None:
    payload = {
        "data_source1_id": datasource_id,
        "data_source2_id": datasource_id,
        "table1": diff_vars.prod_path,
        "table2": diff_vars.dev_path,
        "pk_columns": diff_vars.primary_keys,
    }

    headers = {
        "Authorization": f"Key {api_key}",
        "Content-Type": "application/json",
    }
    if is_tracking_enabled():
        event_json = create_start_event_json({"is_cloud": True, "datasource_id": datasource_id})
        run_as_daemon(send_event_json, event_json)

    start = time.monotonic()
    error = None
    diff_id = None
    diff_url = None
    try:
        diff_id = _cloud_submit_diff(url, payload, headers)
        summary_url = f"{url}/{diff_id}/summary_results"
        diff_results = _cloud_poll_and_get_summary_results(summary_url, headers)

        diff_url = f"{datafold_host}/datadiffs/{diff_id}/overview"

        rows_added_count = diff_results["pks"]["exclusives"][1]
        rows_removed_count = diff_results["pks"]["exclusives"][0]

        rows_updated = diff_results["values"]["rows_with_differences"]
        total_rows = diff_results["values"]["total_rows"]
        rows_unchanged = int(total_rows) - int(rows_updated)
        diff_percent_list = {
            x["column_name"]: str(x["match"]) + "%"
            for x in diff_results["values"]["columns_diff_stats"]
            if x["match"] != 100.0
        }

        if any([rows_added_count, rows_removed_count, rows_updated]):
            diff_output = dbt_diff_string_template(
                rows_added_count,
                rows_removed_count,
                rows_updated,
                str(rows_unchanged),
                diff_percent_list,
                "Value Match Percent:",
            )
            rich.print(
                "[red]"
                + ".".join(diff_vars.prod_path)
                + " <> "
                + ".".join(diff_vars.dev_path)
                + f"[/]\n{diff_url}\n"
                + diff_output
                + "\n"
            )
        else:
            rich.print(
                "[red]"
                + ".".join(diff_vars.prod_path)
                + " <> "
                + ".".join(diff_vars.dev_path)
                + f"[/]\n{diff_url}\n"
                + "[green]No row differences[/] \n"
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
            rich.print(
                "[red]"
                + ".".join(diff_vars.prod_path)
                + " <> "
                + ".".join(diff_vars.dev_path) + "[/]\n"
            )
            if diff_id:
                diff_url = f"{datafold_host}/datadiffs/{diff_id}/overview"
                rich.print(f"{diff_url} \n")
            logger.error(error)


def _setup_cloud_diff() -> Tuple[Optional[str]]:
    datafold_host = os.environ.get("DATAFOLD_HOST")
    if datafold_host is None:
        datafold_host = "https://app.datafold.com"
    datafold_host = datafold_host.rstrip("/")
    rich.print(f"Cloud datafold host: {datafold_host}\n")
    url = f"{datafold_host}/api/v1/datadiffs"

    api_key = os.environ.get("DATAFOLD_API_KEY")
    if not api_key:
        rich.print("[red]API key not found, add it as an environment variable called DATAFOLD_API_KEY.")
        yes_or_no = Confirm.ask("Would you like to generate a new API key?")
        if yes_or_no:
            webbrowser.open(f"{datafold_host}/login?next={datafold_host}/users/me")
            return None, None, None
        else:
            raise ValueError("Cannot diff because the API key is not provided")

    return datafold_host, url, api_key


def _cloud_submit_diff(url, payload, headers) -> str:
    response = requests.request("POST", url, headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    response_json = response.json()
    diff_id = str(response_json["id"])

    if diff_id is None:
        raise Exception(f"Api response did not contain a diff_id: {str(response_json)}")
    return diff_id


def _cloud_poll_and_get_summary_results(url, headers):
    summary_results = None
    start_time = time.time()
    sleep_interval = 5  # starts at 5 sec
    max_sleep_interval = 60
    max_wait_time = 300

    while not summary_results:
        response = requests.request("GET", url, headers=headers, timeout=30)
        response.raise_for_status()
        response_json = response.json()

        if response_json["status"] == "success":
            summary_results = response_json
        elif response_json["status"] == "failed":
            raise Exception(f"Diff failed: {str(response_json)}")

        if time.time() - start_time > max_wait_time:
            raise Exception("Timed out waiting for diff results")

        time.sleep(sleep_interval)
        sleep_interval = min(sleep_interval * 2, max_sleep_interval)

    return summary_results


class DbtParser:
    def __init__(self, profiles_dir_override: str, project_dir_override: str) -> None:
        self.parse_run_results, self.parse_manifest, self.ProfileRenderer, self.yaml = import_dbt()
        self.profiles_dir = Path(profiles_dir_override or default_profiles_dir())
        self.project_dir = Path(project_dir_override or default_project_dir())
        self.connection = None
        self.project_dict = self.get_project_dict()
        self.manifest_obj = self.get_manifest_obj()
        self.dbt_user_id = self.manifest_obj.metadata.user_id
        self.requires_upper = False
        self.threads = None
        self.unique_columns = self.get_unique_columns()

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

        success_models = [x.unique_id for x in run_results_obj.results if x.status.name == "success"]
        models = [self.manifest_obj.nodes.get(x) for x in success_models]
        if not models:
            raise ValueError("Expected > 0 successful models runs from the last dbt command.")

        print(f"Running with data-diff={__version__}\n")
        return models

    def get_manifest_obj(self):
        with open(self.project_dir / MANIFEST_PATH) as manifest:
            manifest_dict = json.load(manifest)
            manifest_obj = self.parse_manifest(manifest=manifest_dict)
        return manifest_obj

    def get_project_dict(self):
        with open(self.project_dir / PROJECT_FILE) as project:
            project_dict = self.yaml.safe_load(project)
        return project_dict

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
            if credentials.get("authenticator") is not None:
                raise Exception("Federated authentication is not yet supported for Snowflake.")
            conn_info = {
                "driver": conn_type,
                "user": credentials.get("user"),
                "account": credentials.get("account"),
                "database": credentials.get("database"),
                "warehouse": credentials.get("warehouse"),
                "role": credentials.get("role"),
                "schema": credentials.get("schema"),
            }
            self.threads = credentials.get("threads")
            self.requires_upper = True

            if credentials.get("private_key_path") is not None:
                if credentials.get("password") is not None:
                    raise Exception("Cannot use password and key at the same time")
                conn_info["key"] = credentials.get("private_key_path")
                conn_info["private_key_passphrase"] = credentials.get("private_key_passphrase")
            elif credentials.get("password") is not None:
                conn_info["password"] = credentials.get("password")
            else:
                raise Exception("Password or key authentication not provided.")
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

    def get_pk_from_model(self, node, unique_columns: dict, pk_tag: str) -> List[str]:
        try:
            # Get a set of all the column names
            column_names = {name for name, params in node.columns.items()}
            # Check if the tag is present on a table level
            if pk_tag in node.meta:
                # Get all the PKs that are also present as a column
                pks = [pk for pk in pk_tag in node.meta[pk_tag] if pk in column_names]
                if pks:
                    # If there are any left, return it
                    logger.debug("Found PKs via Table META: " + str(pks))
                    return pks

            from_meta = [name for name, params in node.columns.items() if pk_tag in params.meta] or None
            if from_meta:
                logger.debug("Found PKs via META: " + str(from_meta))
                return from_meta

            from_tags = [name for name, params in node.columns.items() if pk_tag in params.tags] or None
            if from_tags:
                logger.debug("Found PKs via Tags: " + str(from_tags))
                return from_tags

            if node.unique_id in unique_columns:
                from_uniq = unique_columns.get(node.unique_id)
                if from_uniq is not None:
                    logger.debug("Found PKs via Uniqueness tests: " + str(from_uniq))
                    return list(from_uniq)

        except (KeyError, IndexError, TypeError) as e:
            raise e

        logger.debug("Found no PKs")
        return []

    def get_unique_columns(self) -> Dict[str, Set[str]]:
        manifest = self.manifest_obj
        cols_by_uid = defaultdict(set)
        for node in manifest.nodes.values():
            try:
                if not (node.resource_type.value == "test" and hasattr(node, "test_metadata")):
                    continue

                if node.depends_on is None or node.depends_on.nodes is []:
                    continue

                uid = node.depends_on.nodes[0]
                model_node = manifest.nodes[uid]

                if node.test_metadata.name == "unique":
                    column_name: str = node.test_metadata.kwargs["column_name"]
                    for col in self._parse_concat_pk_definition(column_name):
                        if model_node is None or col in model_node.columns:
                            # skip anything that is not a column.
                            # for example, string literals used in concat
                            # like "pk1 || '-' || pk2"
                            cols_by_uid[uid].add(col)

                if node.test_metadata.name == "unique_combination_of_columns":
                    for col in node.test_metadata.kwargs["combination_of_columns"]:
                        cols_by_uid[uid].add(col)

            except (KeyError, IndexError, TypeError) as e:
                logger.warning("Failure while finding unique cols: %s", e)

        return cols_by_uid

    def _parse_concat_pk_definition(self, definition: str) -> List[str]:
        definition = definition.strip()
        if definition.lower().startswith("concat(") and definition.endswith(")"):
            definition = definition[7:-1]  # Removes concat( and )
            columns = definition.split(",")
        else:
            columns = definition.split("||")

        stripped_columns = [col.strip('" ()') for col in columns]
        return stripped_columns
