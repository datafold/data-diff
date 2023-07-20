from argparse import Namespace
from collections import defaultdict
import json
from pathlib import Path
from typing import List, Dict, Tuple, Set, Optional
import yaml

from packaging.version import parse as parse_version
import pydantic
from dbt_artifacts_parser.parser import parse_run_results, parse_manifest
from dbt.config.renderer import ProfileRenderer

from data_diff.errors import (
    DataDiffDbtBigQueryUnsupportedMethodError,
    DataDiffDbtConnectionNotImplementedError,
    DataDiffDbtCoreNoRunnerError,
    DataDiffDbtNoSuccessfulModelsInRunError,
    DataDiffDbtProfileNotFoundError,
    DataDiffDbtRedshiftPasswordOnlyError,
    DataDiffDbtRunResultsVersionError,
    DataDiffDbtSelectNoMatchingModelsError,
    DataDiffDbtSelectUnexpectedError,
    DataDiffDbtSelectVersionTooLowError,
    DataDiffDbtSnowflakeSetConnectionError,
)

from .utils import getLogger, get_from_dict_with_raise


logger = getLogger(__name__)


# getting this dbt_runner will only succeed in dbt-core>=1.5
# it's needed for `--select` functionality
def try_get_dbt_runner():
    try:
        from dbt.cli.main import dbtRunner
    except ImportError:
        dbtRunner = None

    if dbtRunner is not None:
        dbt_runner = dbtRunner()
    else:
        dbt_runner = None

    return dbt_runner


# ProfileRenderer.render_data() fails without instantiating global flag MACRO_DEBUGGING in dbt-core 1.5
# hacky but seems to be a bug on dbt's end
def try_set_dbt_flags():
    try:
        from dbt.flags import set_flags

        set_flags(Namespace(MACRO_DEBUGGING=False))
    except:
        pass


RUN_RESULTS_PATH = "target/run_results.json"
MANIFEST_PATH = "target/manifest.json"
PROJECT_FILE = "dbt_project.yml"
PROFILES_FILE = "profiles.yml"
LOWER_DBT_V = "1.0.0"
UPPER_DBT_V = "1.6.0"


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


class TDatadiffModelConfig(pydantic.BaseModel):
    where_filter: Optional[str] = None
    include_columns: List[str] = []
    exclude_columns: List[str] = []


class TDatadiffConfig(pydantic.BaseModel):
    prod_database: Optional[str] = None
    prod_schema: Optional[str] = None
    prod_custom_schema: Optional[str] = None
    datasource_id: Optional[int] = None


class DbtParser:
    def __init__(
        self,
        profiles_dir_override: Optional[str] = None,
        project_dir_override: Optional[str] = None,
        state: Optional[str] = None,
    ) -> None:
        try_set_dbt_flags()
        self.dbt_runner = try_get_dbt_runner()
        self.project_dir = Path(project_dir_override or default_project_dir())
        self.connection = {}
        self.project_dict = self.get_project_dict()
        self.dev_manifest_obj = self.get_manifest_obj(self.project_dir / MANIFEST_PATH)
        self.prod_manifest_obj = None
        if state:
            self.prod_manifest_obj = self.get_manifest_obj(Path(state))

        self.dbt_user_id = self.dev_manifest_obj.metadata.user_id
        self.dbt_version = self.dev_manifest_obj.metadata.dbt_version
        self.dbt_project_id = self.dev_manifest_obj.metadata.project_id
        self.requires_upper = False
        self.threads = None
        self.unique_columns = self.get_unique_columns()

        if profiles_dir_override:
            self.profiles_dir = Path(profiles_dir_override)
        elif parse_version(self.dbt_version) < parse_version("1.3.0"):
            self.profiles_dir = legacy_profiles_dir()
        else:
            self.profiles_dir = default_profiles_dir()

    def get_datadiff_config(self) -> TDatadiffConfig:
        data_diff_vars = self.project_dict.get("vars", {}).get("data_diff", {})
        prod_database = data_diff_vars.get("prod_database")
        prod_schema = data_diff_vars.get("prod_schema")
        prod_custom_schema = data_diff_vars.get("prod_custom_schema")
        datasource_id = data_diff_vars.get("datasource_id")
        config = TDatadiffConfig(
            prod_database=prod_database,
            prod_schema=prod_schema,
            prod_custom_schema=prod_custom_schema,
            datasource_id=datasource_id,
        )
        logger.info(f"config: {config}")
        return config

    def get_datadiff_model_config(self, model_meta: dict) -> TDatadiffModelConfig:
        where_filter = None
        include_columns = []
        exclude_columns = []

        if "datafold" in model_meta and "datadiff" in model_meta["datafold"]:
            config = model_meta["datafold"]["datadiff"]
            where_filter = config.get("filter")
            include_columns = config.get("include_columns") or []
            exclude_columns = config.get("exclude_columns") or []

        return TDatadiffModelConfig(
            where_filter=where_filter, include_columns=include_columns, exclude_columns=exclude_columns
        )

    def get_models(self, dbt_selection: Optional[str] = None):
        dbt_version = parse_version(self.dbt_version)
        if dbt_selection:
            if (dbt_version.major, dbt_version.minor) >= (1, 5):
                if self.dbt_runner:
                    return self.get_dbt_selection_models(dbt_selection)
                # edge case if running data-diff from a separate env than dbt (likely local development)
                else:
                    raise DataDiffDbtCoreNoRunnerError(
                        "data-diff is using a dbt-core version < 1.5, update the environment's dbt-core version via pip install 'dbt-core>=1.5' in order to use `--select`"
                    )
            else:
                raise DataDiffDbtSelectVersionTooLowError(
                    f"The `--select` feature requires dbt >= 1.5, but your project's manifest.json is from dbt v{dbt_version}. Please follow these steps to use the `--select` feature: \n 1. Update your dbt-core version via pip install 'dbt-core>=1.5'. Details: https://docs.getdbt.com/docs/core/pip-install#change-dbt-core-versions \n 2. Execute any `dbt` command (`run`, `compile`, `build`) to create a new manifest.json."
                )
        else:
            return self.get_run_results_models()

    def get_dbt_selection_models(self, dbt_selection: str) -> List[str]:
        # log level and format settings needed to prevent dbt from printing to stdout
        # ls command is used to get the list of model unique_ids
        results = self.dbt_runner.invoke(
            [
                "--log-format",
                "json",
                "--log-level",
                "none",
                "ls",
                "--select",
                dbt_selection,
                "--resource-type",
                "model",
                "--output",
                "json",
                "--output-keys",
                "unique_id",
                "--project-dir",
                self.project_dir,
            ]
        )
        if results.exception:
            raise results.exception

        if results.success and results.result:
            model_list = [json.loads(model)["unique_id"] for model in results.result]
            models = [self.dev_manifest_obj.nodes.get(x) for x in model_list]
            return models

        if not results.result:
            raise DataDiffDbtSelectNoMatchingModelsError(f"No dbt models found for `--select {dbt_selection}`")

        logger.debug(str(results))
        raise DataDiffDbtSelectUnexpectedError("Encountered an unexpected error while finding `--select` models")

    def get_run_results_models(self):
        with open(self.project_dir / RUN_RESULTS_PATH) as run_results:
            logger.info(f"Parsing file {RUN_RESULTS_PATH}")
            run_results_dict = json.load(run_results)
            run_results_obj = parse_run_results(run_results=run_results_dict)

        dbt_version = parse_version(run_results_obj.metadata.dbt_version)

        if dbt_version < parse_version(LOWER_DBT_V):
            raise DataDiffDbtRunResultsVersionError(
                f"Found dbt: v{dbt_version} Expected the dbt project's version to be >= {LOWER_DBT_V}"
            )
        if dbt_version >= parse_version(UPPER_DBT_V):
            logger.warning(
                f"{dbt_version} is a recent version of dbt and may not be fully tested with data-diff! \nPlease report any issues to https://github.com/datafold/data-diff/issues"
            )

        success_models = [x.unique_id for x in run_results_obj.results if x.status.name == "success"]
        models = [self.dev_manifest_obj.nodes.get(x) for x in success_models]
        if not models:
            raise DataDiffDbtNoSuccessfulModelsInRunError(
                "Expected > 0 successful models runs from the last dbt command."
            )

        return models

    def get_manifest_obj(self, path: Path):
        with open(path) as manifest:
            logger.info(f"Parsing file {path}")
            manifest_dict = json.load(manifest)
            manifest_obj = parse_manifest(manifest=manifest_dict)
        return manifest_obj

    def get_project_dict(self):
        with open(self.project_dir / PROJECT_FILE) as project:
            logger.info(f"Parsing file {PROJECT_FILE}")
            project_dict = yaml.safe_load(project)
        return project_dict

    def get_connection_creds(self) -> Tuple[Dict[str, str], str]:
        profiles_path = self.profiles_dir / PROFILES_FILE
        with open(profiles_path) as profiles:
            logger.info(f"Parsing file {profiles_path}")
            profiles = yaml.safe_load(profiles)

        dbt_profile_var = self.project_dict.get("profile")

        profile = get_from_dict_with_raise(
            profiles,
            dbt_profile_var,
            DataDiffDbtProfileNotFoundError(f"No profile '{dbt_profile_var}' found in '{profiles_path}'."),
        )
        profile_target = get_from_dict_with_raise(
            profile,
            "target",
            DataDiffDbtProfileNotFoundError(f"No target found in profile '{dbt_profile_var}' in '{profiles_path}'."),
        )

        # some use an env var in target:
        rendered_profile_target = ProfileRenderer().render_data(profile_target)

        outputs = get_from_dict_with_raise(
            profile,
            "outputs",
            DataDiffDbtProfileNotFoundError(f"No outputs found in profile '{dbt_profile_var}' in '{profiles_path}'."),
        )
        credentials = get_from_dict_with_raise(
            outputs,
            rendered_profile_target,
            DataDiffDbtProfileNotFoundError(
                f"No credentials found for target '{rendered_profile_target}' in profile '{dbt_profile_var}' in '{profiles_path}'."
            ),
        )
        conn_type = get_from_dict_with_raise(
            credentials,
            "type",
            DataDiffDbtProfileNotFoundError(
                f"No type found for target '{rendered_profile_target}' in profile '{dbt_profile_var}' in '{profiles_path}'."
            ),
        )
        conn_type = conn_type.lower()

        # resolve any jinja
        return ProfileRenderer().render_data(credentials), conn_type

    def set_connection(self):
        credentials, conn_type = self.get_connection_creds()
        self.set_casing_policy_for(conn_type)

        if conn_type == "snowflake":
            conn_info = {
                "driver": conn_type,
                "user": credentials.get("user"),
                "account": credentials.get("account"),
                "database": credentials.get("database"),
                "warehouse": credentials.get("warehouse"),
                "role": credentials.get("role"),
                "schema": credentials.get("schema"),
                "insecure_mode": credentials.get("insecure_mode", False),
                "client_session_keep_alive": credentials.get("client_session_keep_alive", False),
            }
            self.threads = credentials.get("threads")

            if credentials.get("private_key_path") is not None:
                if credentials.get("password") is not None:
                    raise DataDiffDbtSnowflakeSetConnectionError("Cannot use password and key at the same time")
                conn_info["key"] = credentials.get("private_key_path")
                conn_info["private_key_passphrase"] = credentials.get("private_key_passphrase")
            elif credentials.get("authenticator") is not None:
                conn_info["authenticator"] = credentials.get("authenticator")
                conn_info["password"] = credentials.get("password")
            elif credentials.get("password") is not None:
                conn_info["password"] = credentials.get("password")
            else:
                raise DataDiffDbtSnowflakeSetConnectionError("Snowflake: unsupported auth method")
        elif conn_type == "bigquery":
            supported_methods = ["oauth", "service-account"]
            method = credentials.get("method")
            # there are many connection types https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#oauth-via-gcloud
            # this assumes that the user is auth'd via `gcloud auth application-default login`
            if method not in supported_methods:
                raise DataDiffDbtBigQueryUnsupportedMethodError(
                    f"Method: {method} is not in the current methods supported for Big Query ({supported_methods})."
                )

            conn_info = {
                "driver": conn_type,
                "project": credentials.get("project"),
                "dataset": credentials.get("dataset"),
            }

            self.threads = credentials.get("threads")
            if method == supported_methods[1]:
                conn_info["keyfile"] = credentials.get("keyfile")

        elif conn_type == "duckdb":
            conn_info = {
                "driver": conn_type,
                "filepath": credentials.get("path"),
            }
        elif conn_type == "redshift":
            if (credentials.get("pass") is None and credentials.get("password") is None) or credentials.get(
                "method"
            ) == "iam":
                raise DataDiffDbtRedshiftPasswordOnlyError(
                    "Only password authentication is currently supported for Redshift."
                )
            conn_info = {
                "driver": conn_type,
                "host": credentials.get("host"),
                "user": credentials.get("user"),
                "password": credentials.get("password") or credentials.get("pass"),
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
            raise DataDiffDbtConnectionNotImplementedError(f"Provider {conn_type} is not yet supported for dbt diffs")

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
        manifest = self.dev_manifest_obj
        cols_by_uid = defaultdict(set)
        for node in manifest.nodes.values():
            try:
                if not (node.resource_type.value == "test" and hasattr(node, "test_metadata")):
                    continue

                if not node.depends_on or not node.depends_on.nodes:
                    continue

                uid = node.depends_on.nodes[0]

                # sources can have tests and are not in manifest.nodes
                # skip as source unique columns are not needed
                if uid.startswith("source."):
                    continue

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

    def set_casing_policy_for(self, connection_type: str):
        """
        Set casing policy for identifiers: database, schema, table, column, etc.
        Correct policy depends on the type of the database, because some databases (e.g. Snowflake)
        use upper case identifiers by default, while others (e.g. Postgres) use lower case.
        """
        self.requires_upper = connection_type == "snowflake"
