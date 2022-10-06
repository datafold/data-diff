from typing import Any, Dict
import toml


class ConfigParseError(Exception):
    pass


def is_uri(s: str) -> bool:
    return "://" in s


def _apply_config(config: Dict[str, Any], run_name: str, kw: Dict[str, Any]):
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

    if 'database1' in kw:
        for attr in ('table1', 'database2', 'table2'):
            if kw[attr] is None:
                raise ValueError(f"Specified database1 but not {attr}. Must specify all 4 arguments, or niether.")

        for index in "12":
            run_args[index] = {attr: kw.pop(f"{attr}{index}") for attr in ('database', 'table')}

    # Process databases + tables
    for index in "12":
        args = run_args.pop(index, {})
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


def apply_config_from_file(path: str, run_name: str, kw: Dict[str, Any]):
    with open(path) as f:
        return _apply_config(toml.load(f), run_name, kw)


def apply_config_from_string(toml_config: str, run_name: str, kw: Dict[str, Any]):
    return _apply_config(toml.loads(toml_config), run_name, kw)
