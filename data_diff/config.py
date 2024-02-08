import re
import os
from typing import Any, Dict
import toml

from data_diff.cli_options import CliOptions

_ARRAY_FIELDS = (
    "key_columns",
    "columns",
)


class ConfigParseError(Exception):
    pass


def is_uri(s: str) -> bool:
    return "://" in s


def _apply_config(config: Dict[str, Any], run_name: str, cli_options: CliOptions):
    _resolve_env(config)

    # Load config
    databases = config.pop("database", {})
    runs = config.pop("run", {})
    if config:
        raise ConfigParseError(f"Unknown option(s): {config}")

    # Init run_args
    run_args = runs.get("default") or {}
    if run_name:
        if run_name not in runs:
            raise ConfigParseError(f"Cannot find run '{run_name}' in configuration.")
        run_args.update(runs[run_name])
    else:
        run_name = "default"

    # if db details are provided from cli that has precedence
    if cli_options.database1 is not None:
        for attr in ("table1", "database2", "table2"):
            if cli_options.__getattribute__(attr) is None:
                raise ValueError(
                    f"Specified database1 but not {attr}. "
                    f"Must specify all 4 arguments (database1, table1, database2, table2), or none."
                )

        for index in "12":
            run_args[index] = {attr: cli_options.__getattribute__(f"{attr}{index}") for attr in ("database", "table")}

    # Make sure array fields are decoded as list, since array fields in toml are decoded as list,
    # but TableSegment object requires tuple type.
    for field in _ARRAY_FIELDS:
        if isinstance(run_args.get(field), list):
            run_args[field] = tuple(run_args[field])

    # Process databases + tables
    for index in "12":
        try:
            args = run_args.pop(index)
        except KeyError:
            raise ConfigParseError(
                f"Could not find source #{index}: Expecting a key of '{index}' containing '.database' and '.table'."
            )
        for attr in ("database", "table"):
            if attr not in args:
                raise ConfigParseError(f"Running 'run.{run_name}': Connection #{index} is missing attribute '{attr}'.")

        database = args.pop("database")
        table = args.pop("table")
        threads = args.pop("threads", None)
        if args:
            raise ConfigParseError(f"Unexpected attributes for connection #{index}: {args}")

        if not is_uri(database):
            if database not in databases:
                raise ConfigParseError(
                    f"Database '{database}' not found in list of databases. Available: {list(databases)}."
                )
            database = dict(databases[database])
            assert isinstance(database, dict)
            if "driver" not in database:
                raise ConfigParseError(f"Database '{database}' did not specify a driver.")

        run_args[f"database{index}"] = database
        run_args[f"table{index}"] = table
        if threads is not None:
            run_args[f"threads{index}"] = int(threads)

    print(run_args)
    # Update keywords
    for new_key, new_value in run_args.items():
        print(new_key, new_value)
        cli_options.__setattr__(new_key, cli_options.__getattribute__(new_key) or new_value)
        print(cli_options.__getattribute__(new_key))

    cli_options.__conf__ = run_args


# There are no strict requirements for the environment variable name format.
# But most shells only allow alphanumeric characters and underscores.
# https://pubs.opengroup.org/onlinepubs/000095399/basedefs/xbd_chap08.html
# "Environment variable names (...) consist solely of uppercase letters, digits, and the '_' (underscore)"
_ENV_VAR_PATTERN = r"\$\{([A-Za-z0-9_]+)\}"


def _resolve_env(config: Dict[str, Any]) -> None:
    """
    Resolve environment variables referenced as ${ENV_VAR_NAME}.
    Missing environment variables are replaced with an empty string.
    """
    for key, value in config.items():
        if isinstance(value, dict):
            _resolve_env(value)
        elif isinstance(value, str):
            config[key] = re.sub(_ENV_VAR_PATTERN, _replace_match, value)


def _replace_match(match: re.Match) -> str:
    # Lookup referenced variable in environment.
    # Replace with empty string if not found
    referenced_var = match.group(1)  # group(0) is the whole string
    return os.environ.get(referenced_var, "")


def apply_config_from_file(path: str, run_name: str, cli_options: CliOptions):
    with open(path) as f:
        _apply_config(toml.load(f), run_name, cli_options)


def apply_config_from_string(toml_config: str, run_name: str, cli_options: CliOptions):
    _apply_config(toml.loads(toml_config), run_name, cli_options)
