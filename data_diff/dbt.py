from contextlib import nullcontext
import json
import os
import re
import time
from typing import List, Optional, Dict, Tuple, Union
import keyring
import pydantic
import rich
from rich.prompt import Prompt

from data_diff.errors import (
    DataDiffCustomSchemaNoConfigError,
    DataDiffDbtProjectVarsNotFoundError,
    DataDiffNoAPIKeyError,
    DataDiffNoDatasourceIdError,
)

from . import connect_to_table, diff_tables, Algorithm
from .cloud import DatafoldAPI, TCloudApiDataDiff, TCloudApiOrgMeta
from .dbt_parser import DbtParser, TDatadiffConfig
from .diff_tables import DiffResultWrapper
from .format import jsonify, jsonify_error
from .tracking import (
    bool_ask_for_email,
    create_email_signup_event_json,
    set_entrypoint_name,
    set_dbt_user_id,
    set_dbt_version,
    set_dbt_project_id,
    create_end_event_json,
    create_start_event_json,
    send_event_json,
    is_tracking_enabled,
)
from .utils import (
    dbt_diff_string_template,
    getLogger,
    columns_added_template,
    columns_removed_template,
    no_differences_template,
    columns_type_changed_template,
    run_as_daemon,
    truncate_error,
    print_version_info,
    LogStatusHandler,
)

logger = getLogger(__name__)
CLOUD_DOC_URL = "https://docs.datafold.com/development_testing/cloud"


class TDiffVars(pydantic.BaseModel):
    dev_path: List[str]
    prod_path: List[str]
    primary_keys: List[str]
    connection: Dict[str, Optional[str]]
    threads: Optional[int] = None
    where_filter: Optional[str] = None
    include_columns: List[str]
    exclude_columns: List[str]
    dbt_model: Optional[str] = None


def dbt_diff(
    profiles_dir_override: Optional[str] = None,
    project_dir_override: Optional[str] = None,
    is_cloud: bool = False,
    dbt_selection: Optional[str] = None,
    json_output: bool = False,
    state: Optional[str] = None,
    log_status_handler: Optional[LogStatusHandler] = None,
    where_flag: Optional[str] = None,
    columns_flag: Optional[Tuple[str]] = None,
) -> None:
    print_version_info()
    diff_threads = []
    set_entrypoint_name(os.getenv("DATAFOLD_TRIGGERED_BY", "CLI-dbt"))
    dbt_parser = DbtParser(profiles_dir_override, project_dir_override, state)
    models = dbt_parser.get_models(dbt_selection)
    config = dbt_parser.get_datadiff_config()
    _initialize_events(dbt_parser.dbt_user_id, dbt_parser.dbt_version, dbt_parser.dbt_project_id)

    if not state and not (config.prod_database or config.prod_schema):
        doc_url = "https://docs.datafold.com/development_testing/open_source#configure-your-dbt-project"
        raise DataDiffDbtProjectVarsNotFoundError(
            f"""vars: data_diff: section not found in dbt_project.yml.\n\nTo solve this, please configure your dbt project: \n{doc_url}\n\nOr specify a production manifest using the `--state` flag."""
        )

    if is_cloud:
        api = _initialize_api()
        # exit so the user can set the key
        if not api:
            return
        org_meta = api.get_org_meta()
        if config.datasource_id is None:
            rich.print("[red]Data source ID not found in dbt_project.yml")
            raise DataDiffNoDatasourceIdError(
                f"Datasource ID not found. Please include it as a dbt variable in the dbt_project.yml. \nInstructions: {CLOUD_DOC_URL}\n\nvars:\n data_diff:\n   datasource_id: 1234"
            )

        data_source = api.get_data_source(config.datasource_id)
        dbt_parser.set_casing_policy_for(connection_type=data_source.type)
        rich.print("[green][bold]\nDiffs in progress...[/][/]\n")

    else:
        dbt_parser.set_connection()

    with log_status_handler.status if log_status_handler else nullcontext():
        for model in models:
            if log_status_handler:
                log_status_handler.set_prefix(f"Diffing {model.alias} \n")

            diff_vars = _get_diff_vars(dbt_parser, config, model, where_flag, columns_flag)

            # we won't always have a prod path when using state
            # when the model DNE in prod manifest, skip the model diff
            if (
                state and len(diff_vars.prod_path) < 2
            ):  # < 2 because some providers like databricks can legitimately have *only* 2
                diff_output_str = _diff_output_base(".".join(diff_vars.dev_path), ".".join(diff_vars.prod_path))
                diff_output_str += "[green]New model: nothing to diff![/] \n"
                rich.print(diff_output_str)
                continue

            if diff_vars.primary_keys:
                if is_cloud:
                    diff_thread = run_as_daemon(
                        _cloud_diff, diff_vars, config.datasource_id, api, org_meta, log_status_handler
                    )
                    diff_threads.append(diff_thread)
                else:
                    _local_diff(diff_vars, json_output)
            else:
                if json_output:
                    print(
                        json.dumps(
                            jsonify_error(
                                table1=diff_vars.prod_path,
                                table2=diff_vars.dev_path,
                                dbt_model=diff_vars.dbt_model,
                                error="No primary key found. Add uniqueness tests, meta, or tags.",
                            )
                        ),
                        flush=True,
                    )
                else:
                    rich.print(
                        _diff_output_base(".".join(diff_vars.dev_path), ".".join(diff_vars.prod_path))
                        + "Skipped due to unknown primary key. Add uniqueness tests, meta, or tags.\n"
                    )

        # wait for all threads
        if diff_threads:
            for thread in diff_threads:
                thread.join()


def _get_diff_vars(
    dbt_parser: "DbtParser",
    config: TDatadiffConfig,
    model,
    where_flag: Optional[str] = None,
    columns_flag: Optional[Tuple[str]] = None,
) -> TDiffVars:
    cli_columns = list(columns_flag) if columns_flag else []
    dev_database = model.database
    dev_schema = model.schema_
    dev_alias = prod_alias = model.alias
    primary_keys = dbt_parser.get_pk_from_model(model, dbt_parser.unique_columns, "primary-key")

    # prod path is constructed via configuration or the prod manifest via --state
    if dbt_parser.prod_manifest_obj:
        prod_database, prod_schema, prod_alias = _get_prod_path_from_manifest(model, dbt_parser.prod_manifest_obj)
    else:
        prod_database, prod_schema = _get_prod_path_from_config(config, model, dev_database, dev_schema)

    if dbt_parser.requires_upper:
        dev_qualified_list = [x.upper() for x in [dev_database, dev_schema, dev_alias] if x]
        prod_qualified_list = [x.upper() for x in [prod_database, prod_schema, prod_alias] if x]
        primary_keys = [x.upper() for x in primary_keys]
    else:
        dev_qualified_list = [x for x in [dev_database, dev_schema, dev_alias] if x]
        prod_qualified_list = [x for x in [prod_database, prod_schema, prod_alias] if x]

    datadiff_model_config = dbt_parser.get_datadiff_model_config(model.meta)

    return TDiffVars(
        dbt_model=model.unique_id,
        dev_path=dev_qualified_list,
        prod_path=prod_qualified_list,
        primary_keys=primary_keys,
        connection=dbt_parser.connection,
        threads=dbt_parser.threads,
        # cli flags take precedence over any model level config
        where_filter=where_flag or datadiff_model_config.where_filter,
        include_columns=cli_columns or datadiff_model_config.include_columns,
        exclude_columns=[] if cli_columns else datadiff_model_config.exclude_columns,
    )


def _get_prod_path_from_config(config, model, dev_database, dev_schema) -> Tuple[str, str]:
    # "custom" dbt config database
    if model.config.database:
        prod_database = model.config.database
    elif config.prod_database:
        prod_database = config.prod_database
    else:
        prod_database = dev_database

    # prod schema name differs from dev schema name
    if config.prod_schema:
        custom_schema = model.config.schema_

        # the model has a custom schema config(schema='some_schema')
        if custom_schema:
            if not config.prod_custom_schema:
                raise DataDiffCustomSchemaNoConfigError(
                    f"Found a custom schema on model {model.name}, but no value for\nvars:\n  data_diff:\n    prod_custom_schema:\nPlease set a value or utilize the `--state` flag!\n\n"
                    + "For more details see: https://docs.datafold.com/development_testing/open_source"
                )
            prod_schema = config.prod_custom_schema.replace("<custom_schema>", custom_schema)
            # no custom schema, use the default
        else:
            prod_schema = config.prod_schema
    else:
        prod_schema = dev_schema
    return prod_database, prod_schema


def _get_prod_path_from_manifest(model, prod_manifest) -> Union[Tuple[str, str, str], Tuple[None, None, None]]:
    prod_database = None
    prod_schema = None
    prod_alias = None
    prod_model = prod_manifest.nodes.get(model.unique_id, None)
    if prod_model:
        prod_database = prod_model.database
        prod_schema = prod_model.schema_
        prod_alias = prod_model.alias
    return prod_database, prod_schema, prod_alias


def _local_diff(diff_vars: TDiffVars, json_output: bool = False) -> None:
    dev_qualified_str = ".".join(diff_vars.dev_path)
    prod_qualified_str = ".".join(diff_vars.prod_path)
    diff_output_str = _diff_output_base(dev_qualified_str, prod_qualified_str)

    table1 = connect_to_table(
        diff_vars.connection, prod_qualified_str, tuple(diff_vars.primary_keys), diff_vars.threads
    )
    table2 = connect_to_table(diff_vars.connection, dev_qualified_str, tuple(diff_vars.primary_keys), diff_vars.threads)

    try:
        table1_columns = table1.get_schema()
    # Not ideal, but we don't have more specific exceptions yet
    except Exception as ex:
        logger.debug(ex)
        diff_output_str += "[red]New model or no access to prod table.[/] \n"
        rich.print(diff_output_str)
        return

    table2_columns = table2.get_schema()

    table1_column_names = set(table1_columns.keys())
    table2_column_names = set(table2_columns.keys())
    column_set = table1_column_names.intersection(table2_column_names)
    columns_added = table2_column_names.difference(table1_column_names)
    columns_removed = table1_column_names.difference(table2_column_names)
    # col type is i = 1 in tuple
    columns_type_changed = {
        k for k, v in table2_columns.items() if k in table1_columns and v[1] != table1_columns[k][1]
    }

    if columns_added:
        diff_output_str += columns_added_template(columns_added)

    if columns_removed:
        diff_output_str += columns_removed_template(columns_removed)

    if columns_type_changed:
        diff_output_str += columns_type_changed_template(columns_type_changed)
        column_set = column_set.difference(columns_type_changed)

    column_set = column_set - set(diff_vars.primary_keys)

    if diff_vars.include_columns:
        column_set = {x for x in column_set if x.upper() in [y.upper() for y in diff_vars.include_columns]}

    if diff_vars.exclude_columns:
        column_set = {x for x in column_set if x.upper() not in [y.upper() for y in diff_vars.exclude_columns]}

    extra_columns = tuple(column_set)

    diff: DiffResultWrapper = diff_tables(
        table1,
        table2,
        threaded=True,
        algorithm=Algorithm.JOINDIFF,
        extra_columns=extra_columns,
        where=diff_vars.where_filter,
        skip_null_keys=True,
    )
    if json_output:
        # drain the iterator to get accumulated stats in diff.info_tree
        try:
            list(diff)
        except Exception as e:
            print(
                json.dumps(
                    jsonify_error(list(table1.table_path), list(table2.table_path), diff_vars.dbt_model, str(e))
                ),
                flush=True,
            )
            return

        dataset1_columns = [
            (name, type_, table1.database.dialect.parse_type(table1.table_path, name, type_, *other))
            for (name, type_, *other) in table1_columns.values()
        ]
        dataset2_columns = [
            (name, type_, table2.database.dialect.parse_type(table2.table_path, name, type_, *other))
            for (name, type_, *other) in table2_columns.values()
        ]
        print(
            json.dumps(
                jsonify(
                    diff,
                    dbt_model=diff_vars.dbt_model,
                    dataset1_columns=dataset1_columns,
                    dataset2_columns=dataset2_columns,
                    with_summary=True,
                    columns_diff={
                        "added": columns_added,
                        "removed": columns_removed,
                        "changed": columns_type_changed,
                    },
                )
            ),
            flush=True,
        )
        return

    if list(diff):
        diff_output_str += f"{diff.get_stats_string(is_dbt=True)} \n"
        rich.print(diff_output_str)
    else:
        diff_output_str += no_differences_template()
        rich.print(diff_output_str)


def _initialize_api() -> Optional[DatafoldAPI]:
    datafold_host = os.environ.get("DATAFOLD_HOST")
    if datafold_host is None:
        datafold_host = "https://app.datafold.com"
    datafold_host = datafold_host.rstrip("/")
    rich.print(f"Cloud datafold host: {datafold_host}")

    api_key = os.environ.get("DATAFOLD_API_KEY")
    if not api_key:
        rich.print("[red]API key not found. Getting from the keyring service")
        api_key = keyring.get_password("data-diff", "DATAFOLD_API_KEY")
        if not api_key:
            raise DataDiffNoAPIKeyError(
                f"API key not found. Please follow the steps at {CLOUD_DOC_URL} to use the --cloud flag."
            )
    rich.print("Saving the API key to the system keyring service")
    try:
        keyring.set_password("data-diff", "DATAFOLD_API_KEY", api_key)
    except Exception as e:
        rich.print(f"[red]Failed when saving the API key to the system keyring service. Reason: {e}")

    return DatafoldAPI(api_key=api_key, host=datafold_host)


def _cloud_diff(
    diff_vars: TDiffVars,
    datasource_id: int,
    api: DatafoldAPI,
    org_meta: TCloudApiOrgMeta,
    log_status_handler: Optional[LogStatusHandler] = None,
) -> None:
    if log_status_handler:
        log_status_handler.cloud_diff_started(diff_vars.dev_path[-1])
    diff_output_str = _diff_output_base(".".join(diff_vars.dev_path), ".".join(diff_vars.prod_path))
    payload = TCloudApiDataDiff(
        data_source1_id=datasource_id,
        data_source2_id=datasource_id,
        table1=diff_vars.prod_path,
        table2=diff_vars.dev_path,
        pk_columns=diff_vars.primary_keys,
        filter1=diff_vars.where_filter,
        filter2=diff_vars.where_filter,
        include_columns=diff_vars.include_columns,
        exclude_columns=diff_vars.exclude_columns,
    )

    if is_tracking_enabled():
        event_json = create_start_event_json({"is_cloud": True, "datasource_id": datasource_id})
        run_as_daemon(send_event_json, event_json)

    start = time.monotonic()
    error = None
    diff_id = None
    diff_url = None
    try:
        diff_id = api.create_data_diff(payload=payload)
        diff_url = f"{api.host}/datadiffs/{diff_id}/overview"
        rich.print(f"{diff_vars.dev_path[-1]}: {diff_url}")

        if diff_id is None:
            raise Exception(f"Api response did not contain a diff_id")

        diff_results = api.poll_data_diff_results(diff_id)

        rows_added_count = diff_results.pks.exclusives[1]
        rows_removed_count = diff_results.pks.exclusives[0]

        rows_updated = diff_results.values.rows_with_differences
        total_rows = diff_results.values.total_rows
        rows_unchanged = int(total_rows) - int(rows_updated)
        diff_percent_list = {
            x.column_name: str(x.match) + "%" for x in diff_results.values.columns_diff_stats if x.match != 100.0
        }
        columns_added = diff_results.schema_.exclusive_columns[1]
        columns_removed = diff_results.schema_.exclusive_columns[0]
        column_type_changes = diff_results.schema_.column_type_differs

        if columns_added:
            diff_output_str += columns_added_template(columns_added)

        if columns_removed:
            diff_output_str += columns_removed_template(columns_removed)

        if column_type_changes:
            diff_output_str += columns_type_changed_template(column_type_changes)

        if any([rows_added_count, rows_removed_count, rows_updated]):
            diff_output = dbt_diff_string_template(
                rows_added_count,
                rows_removed_count,
                rows_updated,
                str(rows_unchanged),
                diff_percent_list,
                "Value Match Percent:",
            )
            diff_output_str += f"\n{diff_url}\n {diff_output} \n"
            rich.print(diff_output_str)
        else:
            diff_output_str += f"\n{diff_url}\n{no_differences_template()}\n"
            rich.print(diff_output_str)

        if log_status_handler:
            log_status_handler.cloud_diff_finished(diff_vars.dev_path[-1])
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
                org_id=org_meta.org_id,
                org_name=org_meta.org_name,
                user_id=org_meta.user_id,
            )
            send_event_json(event_json)

        if error:
            rich.print(diff_output_str)
            if diff_id:
                diff_url = f"{api.host}/datadiffs/{diff_id}/overview"
                rich.print(f"{diff_url} \n")
            logger.error(error)


def _diff_output_base(dev_path: str, prod_path: str) -> str:
    return f"\n[green]{prod_path} <> {dev_path}[/] \n"


def _initialize_events(dbt_user_id: Optional[str], dbt_version: Optional[str], dbt_project_id: Optional[str]) -> None:
    set_dbt_user_id(dbt_user_id)
    set_dbt_version(dbt_version)
    set_dbt_project_id(dbt_project_id)
    _email_signup()


def _email_signup() -> None:
    email_regex = r"^[\w\.\+-]+@[\w\.-]+\.\w+$"
    prompt = "\nWould you like to be notified when a new data-diff version is available?\n\nEnter email or leave blank to opt out (we'll only ask once).\n"

    if bool_ask_for_email():
        while True:
            email_input = Prompt.ask(
                prompt=prompt,
                default="",
                show_default=False,
            )
            email = email_input.strip()

            if email == "" or re.match(email_regex, email):
                break

            prompt = ""
            rich.print("[red]Invalid email. Please enter a valid email or leave it blank to opt out.[/]")

        if email:
            event_json = create_email_signup_event_json(email)
            run_as_daemon(send_event_json, event_json)
