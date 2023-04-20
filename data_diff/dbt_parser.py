from collections import defaultdict
import json
from pathlib import Path
from typing import List, Dict, Tuple, Set

from packaging.version import parse as parse_version

from .utils import getLogger, get_from_dict_with_raise
from .version import __version__


logger = getLogger(__name__)


def import_dbt():
    try:
        from dbt_artifacts_parser.parser import parse_run_results, parse_manifest
        from dbt.config.renderer import ProfileRenderer
        import yaml
    except ImportError:
        raise RuntimeError("Could not import 'dbt' package. You can install it using: pip install 'data-diff[dbt]'.")

    return parse_run_results, parse_manifest, ProfileRenderer, yaml


RUN_RESULTS_PATH = "target/run_results.json"
MANIFEST_PATH = "target/manifest.json"
PROJECT_FILE = "dbt_project.yml"
PROFILES_FILE = "profiles.yml"
LOWER_DBT_V = "1.0.0"
UPPER_DBT_V = "1.4.7"


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


class DbtParser:
    def __init__(self, profiles_dir_override: str, project_dir_override: str) -> None:
        self.parse_run_results, self.parse_manifest, self.ProfileRenderer, self.yaml = import_dbt()
        self.profiles_dir = Path(profiles_dir_override or default_profiles_dir())
        self.project_dir = Path(project_dir_override or default_project_dir())
        self.connection = None
        self.project_dict = self.get_project_dict()
        self.manifest_obj = self.get_manifest_obj()
        self.dbt_user_id = self.manifest_obj.metadata.user_id
        self.dbt_version = self.manifest_obj.metadata.dbt_version
        self.dbt_project_id = self.manifest_obj.metadata.project_id
        self.requires_upper = False
        self.threads = None
        self.unique_columns = self.get_unique_columns()

    def get_datadiff_variables(self) -> dict:
        vars = get_from_dict_with_raise(self.project_dict, "vars", f"No vars: found in dbt_project.yml.")
        return get_from_dict_with_raise(vars, "data_diff", f"data_diff: section not found in dbt_project.yml vars:.")

    def get_models(self):
        with open(self.project_dir / RUN_RESULTS_PATH) as run_results:
            logger.info(f"Parsing file {RUN_RESULTS_PATH}")
            run_results_dict = json.load(run_results)
            run_results_obj = self.parse_run_results(run_results=run_results_dict)

        dbt_version = parse_version(run_results_obj.metadata.dbt_version)

        if dbt_version < parse_version("1.3.0"):
            self.profiles_dir = legacy_profiles_dir()

        if dbt_version < parse_version(LOWER_DBT_V):
            raise Exception(
                f"Found dbt: v{dbt_version} Expected the dbt project's version to be >= {LOWER_DBT_V}"
            )
        elif dbt_version >= parse_version(UPPER_DBT_V):
            logger.warning(f"{dbt_version} is a recent version of dbt and may not be fully tested with data-diff! \nPlease report any issues to https://github.com/datafold/data-diff/issues")

        success_models = [x.unique_id for x in run_results_obj.results if x.status.name == "success"]
        models = [self.manifest_obj.nodes.get(x) for x in success_models]
        if not models:
            raise ValueError("Expected > 0 successful models runs from the last dbt command.")

        print(f"Running with data-diff={__version__}\n")
        return models

    def get_manifest_obj(self):
        with open(self.project_dir / MANIFEST_PATH) as manifest:
            logger.info(f"Parsing file {MANIFEST_PATH}")
            manifest_dict = json.load(manifest)
            manifest_obj = self.parse_manifest(manifest=manifest_dict)
        return manifest_obj

    def get_project_dict(self):
        with open(self.project_dir / PROJECT_FILE) as project:
            logger.info(f"Parsing file {PROJECT_FILE}")
            project_dict = self.yaml.safe_load(project)
        return project_dict

    def get_connection_creds(self) -> Tuple[Dict[str, str], str]:
        profiles_path = self.profiles_dir / PROFILES_FILE
        with open(profiles_path) as profiles:
            logger.info(f"Parsing file {profiles_path}")
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
        credentials, conn_type = self.get_connection_creds()

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
            self.requires_upper = True

            if credentials.get("private_key_path") is not None:
                if credentials.get("password") is not None:
                    raise Exception("Cannot use password and key at the same time")
                conn_info["key"] = credentials.get("private_key_path")
                conn_info["private_key_passphrase"] = credentials.get("private_key_passphrase")
            elif credentials.get("authenticator") is not None:
                conn_info["authenticator"] = credentials.get("authenticator")
                conn_info["password"] = credentials.get("password")
            elif credentials.get("password") is not None:
                conn_info["password"] = credentials.get("password")
            else:
                raise Exception("Snowflake: unsupported auth method")
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
            if (credentials.get("pass") is None and credentials.get("password") is None) or credentials.get(
                "method"
            ) == "iam":
                raise Exception("Only password authentication is currently supported for Redshift.")
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
