import re
import os
from typing import Any, Dict
import toml


_ARRAY_FIELDS = (
    "key_columns",
    "columns",
)


class ConfigParseError(Exception):
    pass


def is_uri(s: str) -> bool:
    return "://" in s


def _apply_config(config: Dict[str, Any], run_name: str, kw: Dict[str, Any]):
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

    if kw.get("database1") is not None:
        for attr in ("table1", "database2", "table2"):
            if kw[attr] is None:
                raise ValueError(f"Specified database1 but not {attr}. Must specify all 4 arguments, or neither.")

        for index in "12":
            run_args[index] = {attr: kw.pop(f"{attr}{index}") for attr in ("database", "table")}

    # Make sure array fields are decoded as list, since array fields in toml are decoded as list, but TableSegment object requires tuple type.
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

    # Update keywords
    new_kw = dict(kw)  # Set defaults
    new_kw.update(run_args)  # Apply config
    new_kw.update({k: v for k, v in kw.items() if v})  # Apply non-empty defaults

    new_kw["__conf__"] = run_args

    return new_kw


# There are no strict requirements for the environment variable name format.
# But most shells only allow alphanumeric characters and underscores.
# https://pubs.opengroup.org/onlinepubs/000095399/basedefs/xbd_chap08.html
# "Environment variable names (...) consist solely of uppercase letters, digits, and the '_' (underscore)"
_ENV_VAR_PATTERN = r"\$\{([A-Za-z0-9_]+)\}"


def _resolve_env(config: Dict[str, Any]):
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


def apply_config_from_file(path: str, run_name: str, kw: Dict[str, Any]):
    with open(path) as f:
        return _apply_config(toml.load(f), run_name, kw)


def apply_config_from_string(toml_config: str, run_name: str, kw: Dict[str, Any]):
    return _apply_config(toml.loads(toml_config), run_name, kw)
