import json
import logging
import os
import sys
import time
from copy import deepcopy
from datetime import datetime
from itertools import islice
from typing import Dict, Optional, Tuple, List, Set

import click
import rich
from rich.logging import RichHandler

from data_diff import Database, DbPath
from data_diff.cli_options import CliOptions
from data_diff.config import apply_config_from_file
from data_diff.databases._connect import connect
from data_diff.dbt import dbt_diff
from data_diff.diff_tables import Algorithm, TableDiffer
from data_diff.hashdiff_tables import HashDiffer, DEFAULT_BISECTION_THRESHOLD, DEFAULT_BISECTION_FACTOR
from data_diff.joindiff_tables import TABLE_WRITE_LIMIT, JoinDiffer
from data_diff.parse_time import parse_time_before, UNITS_STR, ParseError
from data_diff.queries.api import current_timestamp
from data_diff.schema import RawColumnInfo, create_schema
from data_diff.table_segment import TableSegment
from data_diff.tracking import disable_tracking, set_entrypoint_name
from data_diff.utils import eval_name_template, remove_password_from_url, safezip, match_like, LogStatusHandler
from data_diff.version import __version__

COLOR_SCHEME = {
    "+": "green",
    "-": "red",
}

set_entrypoint_name(os.getenv("DATAFOLD_TRIGGERED_BY", "CLI"))


def _get_log_handlers(is_dbt: bool = False) -> Dict[str, logging.Handler]:
    handlers = {}
    date_format = "%H:%M:%S"
    log_format_rich = "%(message)s"

    # limits to 100 characters arbitrarily
    log_format_status = "%(message).100s"
    rich_handler = RichHandler(rich_tracebacks=True)
    rich_handler.setFormatter(logging.Formatter(log_format_rich, datefmt=date_format))
    rich_handler.setLevel(logging.WARN)
    handlers["rich_handler"] = rich_handler

    # only use log_status_handler in an interactive terminal session
    if rich_handler.console.is_interactive and is_dbt:
        log_status_handler = LogStatusHandler()
        log_status_handler.setFormatter(logging.Formatter(log_format_status, datefmt=date_format))
        log_status_handler.setLevel(logging.DEBUG)
        handlers["log_status_handler"] = log_status_handler

    return handlers


def _remove_passwords_in_dict(d: dict) -> None:
    for k, v in d.items():
        if k == "password":
            d[k] = "*" * len(v)
        elif k == "filepath":
            if "motherduck_token=" in v:
                d[k] = v.split("motherduck_token=")[0] + "motherduck_token=**********"
        elif isinstance(v, dict):
            _remove_passwords_in_dict(v)
        elif k.startswith("database"):
            d[k] = remove_password_from_url(v)


def _get_schema(pair: Tuple[Database, DbPath]) -> Dict[str, RawColumnInfo]:
    db, table_path = pair
    return db.query_table_schema(table_path)


def diff_schemas(table1, table2, schema1, schema2, columns) -> None:
    logging.info("Diffing schemas...")
    attrs = "name", "type", "datetime_precision", "numeric_precision", "numeric_scale"
    for c in columns:
        if c is None:  # Skip for convenience
            continue
        diffs = []

        if c not in schema1:
            cols = ", ".join(schema1)
            raise ValueError(f"Column '{c}' not found in table 1, named '{table1}'. Columns: {cols}")
        if c not in schema2:
            cols = ", ".join(schema1)
            raise ValueError(f"Column '{c}' not found in table 2, named '{table2}'. Columns: {cols}")

        col1 = schema1[c]
        col2 = schema2[c]

        for attr, v1, v2 in safezip(attrs, col1, col2):
            if v1 != v2:
                diffs.append(f"{attr}:({v1} != {v2})")
        if diffs:
            logging.warning(f"Schema mismatch in column '{c}': {', '.join(diffs)}")


class MyHelpFormatter(click.HelpFormatter):
    def __init__(self, **kwargs) -> None:
        super().__init__(self, **kwargs)
        self.indent_increment = 6

    def write_usage(self, prog: str, args: str = "", prefix: Optional[str] = None) -> None:
        self.write(f"data-diff v{__version__} - efficiently diff rows across database tables.\n\n")
        self.write("Usage:\n")
        self.write(f"  * In-db diff:    {prog} <database_a> <table_a> <table_b> [OPTIONS]\n")
        self.write(f"  * Cross-db diff: {prog} <database_a> <table_a> <database_b> <table_b> [OPTIONS]\n")
        self.write(f"  * Using config:  {prog} --conf PATH [--run NAME] [OPTIONS]\n")


click.Context.formatter_class = MyHelpFormatter


@click.command(no_args_is_help=True)
@click.argument("database1", required=False, type=str)
@click.argument("table1", required=False, type=str)
@click.argument("database2", required=False, type=str)
@click.argument("table2", required=False, type=str)
@click.option(
    "-k",
    "--key-columns",
    default=[],
    multiple=True,
    help="Names of primary key columns. Default='id'.",
    metavar="NAME",
    type=str,
)
@click.option(
    "-t", "--update-column", default=None, help="Name of updated_at/last_updated column", metavar="NAME", type=str
)
@click.option(
    "-c",
    "--columns",
    default=[],
    multiple=True,
    help="Names of extra columns to compare."
    "Can be used more than once in the same command. "
    "Accepts a name or a pattern like in SQL. Example: -c col% -c another_col",
    metavar="NAME",
    type=str,
)
@click.option("-l", "--limit", default=None, help="Maximum number of differences to find", metavar="NUM", type=int)
@click.option(
    "--bisection-factor",
    default=DEFAULT_BISECTION_FACTOR,
    help=f"Segments per iteration. Default={DEFAULT_BISECTION_FACTOR}.",
    metavar="NUM",
    type=int,
)
@click.option(
    "--bisection-threshold",
    default=DEFAULT_BISECTION_THRESHOLD,
    help=(
        f"Minimal bisection threshold. Below it, data-diff will download the data and compare it locally. "
        f"Default={DEFAULT_BISECTION_THRESHOLD}."
    ),
    metavar="NUM",
    type=int,
)
@click.option(
    "-m",
    "--materialize-to-table",
    default=None,
    metavar="TABLE_NAME",
    help=(
        "(joindiff only) Materialize the diff results into a new table in the database. "
        "If a table exists by that name, it will be replaced."
    ),
    type=str,
)
@click.option(
    "--min-age",
    default=None,
    help="Considers only rows older than specified. Useful for specifying replication lag."
    "Example: --min-age=5min ignores rows from the last 5 minutes. "
    f"\nValid units: {UNITS_STR}",
    metavar="AGE",
    type=str,
)
@click.option(
    "--max-age",
    default=None,
    help="Considers only rows younger than specified. See --min-age.",
    metavar="AGE",
    type=str,
)
@click.option("-s", "--stats", is_flag=True, help="Print stats instead of a detailed diff", type=bool)
@click.option("-d", "--debug", is_flag=True, help="Print debug info", type=bool)
@click.option("--json", "json_output", is_flag=True, help="Print JSONL output for machine readability", type=bool)
@click.option("-v", "--verbose", is_flag=True, help="Print extra info", type=bool)
@click.option("--version", is_flag=True, help="Print version info and exit", type=bool)
@click.option("-i", "--interactive", is_flag=True, help="Confirm queries, implies --debug", type=bool)
@click.option(
    "--no-tracking", is_flag=True, help="data-diff sends home anonymous usage data. Use this to disable it.", type=bool
)
@click.option(
    "--case-sensitive",
    is_flag=True,
    help="Column names are treated as case-sensitive. Otherwise, data-diff corrects their case according to schema.",
    type=bool,
)
@click.option(
    "--assume-unique-key",
    is_flag=True,
    help="Skip validating the uniqueness of the key column during joindiff, which is costly in non-cloud dbs.",
    type=bool,
)
@click.option(
    "--sample-exclusive-rows",
    is_flag=True,
    help="Sample several rows that only appear in one of the tables, but not the other. (joindiff only)",
    type=bool,
)
@click.option(
    "--materialize-all-rows",
    is_flag=True,
    help="Materialize every row, even if they are the same, instead of just the differing rows. (joindiff only)",
    type=bool,
)
@click.option(
    "--table-write-limit",
    default=TABLE_WRITE_LIMIT,
    help=(
        f"Maximum number of rows to write when creating materialized or sample tables, per thread. "
        f"Default={TABLE_WRITE_LIMIT}"
    ),
    type=int,
    metavar="COUNT",
)
@click.option(
    "-j",
    "--threads",
    default=1,
    help=(
        "Number of worker threads to use per database. Default=1. "
        "A higher number will increase performance, but take more capacity from your database. "
        "'serial' guarantees a single-threaded execution of the algorithm (useful for debugging)."
    ),
    metavar="COUNT",
    type=int,
)
@click.option(
    "-w",
    "--where",
    default=None,
    help="An additional 'where' expression to restrict the search space. Beware of SQL Injection!",
    metavar="EXPR",
    type=str,
)
@click.option("-a", "--algorithm", default=Algorithm.AUTO.value, type=click.Choice([i.value for i in Algorithm]))
@click.option(
    "--conf",
    default=None,
    help="Path to a configuration.toml file, to provide a default configuration, and a list of possible runs.",
    metavar="PATH",
    type=str,
)
@click.option(
    "--run",
    default=None,
    help="Name of run-configuration to run. If used, CLI arguments for database and table must be omitted.",
    metavar="NAME",
    type=str,
)
@click.option(
    "--dbt",
    is_flag=True,
    help="Run a diff using your local dbt project. Expects to be run from a dbt project folder by default.",
    type=bool,
)
@click.option(
    "--cloud",
    is_flag=True,
    help=(
        "Add this flag along with --dbt to run a diff using your local dbt project on Datafold cloud. "
        "Expects an api key on env var DATAFOLD_API_KEY."
    ),
    type=bool,
)
@click.option(
    "--dbt-profiles-dir",
    envvar="DBT_PROFILES_DIR",
    default=None,
    metavar="PATH",
    help=(
        "Which directory to look in for the profiles.yml file. If not set, "
        "we follow the default profiles.yml location for the dbt version being used. "
        "Can also be set via the DBT_PROFILES_DIR environment variable."
    ),
    type=str,
)
@click.option(
    "--dbt-project-dir",
    default=None,
    metavar="PATH",
    help=(
        "Which directory to look in for the dbt_project.yml file. "
        "Default is the current working directory and its parents."
    ),
    type=str,
)
@click.option(
    "--select",
    "-s",
    default=None,
    metavar="SELECTION or MODEL_NAME",
    help=(
        "--select dbt resources to compare using dbt selection syntax in dbt versions >= 1.5.\n"
        "In versions < 1.5, it will naively search for a model with MODEL_NAME as the name."
    ),
    type=str,
)
@click.option(
    "--state",
    "-s",
    default=None,
    metavar="PATH",
    help="Specify manifest to utilize for 'prod' comparison paths instead of using configuration.",
    type=str,
)
@click.option(
    "-pd",
    "--prod-database",
    "prod_database",
    default=None,
    help="Override the dbt production database configuration within dbt_project.yml",
    type=str,
)
@click.option(
    "-ps",
    "--prod-schema",
    "prod_schema",
    default=None,
    help="Override the dbt production schema configuration within dbt_project.yml",
    type=str,
)
def main(conf, run, **kwargs) -> None:
    cli_options: CliOptions = CliOptions(**kwargs)
    log_handlers = _get_log_handlers(cli_options.dbt)
    if cli_options.table2 is None and cli_options.database2:
        # Use the "database table table" form
        cli_options.table2 = cli_options.database2
        cli_options.database2 = cli_options.database1

    if cli_options.version:
        print(f"v{__version__}")
        return

    if conf:
        apply_config_from_file(conf, run, cli_options)

    if cli_options.no_tracking:
        disable_tracking()

    if cli_options.interactive:
        cli_options.debug = True

    if cli_options.debug:
        log_handlers["rich_handler"].setLevel(logging.DEBUG)
        logging.basicConfig(level=logging.DEBUG, handlers=list(log_handlers.values()))
        if cli_options.__conf__:
            __conf__ = deepcopy(cli_options.__conf__)
            _remove_passwords_in_dict(__conf__)
            logging.debug(f"Applied run configuration: {__conf__}")
    elif cli_options.verbose:
        log_handlers["rich_handler"].setLevel(logging.INFO)
        logging.basicConfig(level=logging.DEBUG, handlers=list(log_handlers.values()))
    else:
        log_handlers["rich_handler"].setLevel(logging.WARNING)
        logging.basicConfig(level=logging.DEBUG, handlers=list(log_handlers.values()))

    try:
        if cli_options.state:
            cli_options.state = os.path.expanduser(cli_options.state)
        if cli_options.dbt_profiles_dir:
            cli_options.dbt_profiles_dir = os.path.expanduser(cli_options.dbt_profiles_dir)
        if cli_options.dbt_project_dir:
            cli_options.dbt_project_dir = os.path.expanduser(cli_options.dbt_project_dir)

        if cli_options.dbt:
            dbt_diff(cli_options, log_status_handler=log_handlers.get("log_status_handler"))
        else:
            _data_diff(cli_options)
    except Exception as e:
        logging.error(e)
        raise


def _get_dbs(cli_options: CliOptions) -> Tuple[Database, Database]:
    db1 = connect(cli_options.database1, cli_options.threads1 or cli_options.threads)
    if cli_options.database1 == cli_options.database2:
        db2 = db1
    else:
        db2 = connect(cli_options.database2, cli_options.threads2 or cli_options.threads)

    if cli_options.interactive:
        db1.enable_interactive()
        db2.enable_interactive()

    return db1, db2


def _set_age(options: dict, cli_options: CliOptions, db: Database) -> None:
    if cli_options.min_age or cli_options.max_age:
        now: datetime = db.query(current_timestamp(), datetime).replace(tzinfo=None)
        try:
            if cli_options.max_age:
                options["min_update"] = parse_time_before(now, cli_options.max_age)
            if cli_options.min_age:
                options["max_update"] = parse_time_before(now, cli_options.min_age)
        except ParseError as e:
            logging.error(f"Error while parsing age expression: {e}")


def _get_table_differ(cli_options: CliOptions, db1: Database, db2: Database) -> TableDiffer:
    algorithm = Algorithm(cli_options.algorithm)
    if algorithm == Algorithm.AUTO:
        algorithm = Algorithm.JOINDIFF if db1 == db2 else Algorithm.HASHDIFF

    logging.info(f"Using algorithm '{algorithm.name.lower()}'.")

    if algorithm == Algorithm.JOINDIFF:
        return JoinDiffer(
            threaded=cli_options.threaded,
            max_threadpool_size=cli_options.threads and cli_options.threads * 2,
            validate_unique_key=not cli_options.assume_unique_key,
            sample_exclusive_rows=cli_options.sample_exclusive_rows,
            materialize_all_rows=cli_options.materialize_all_rows,
            table_write_limit=cli_options.table_write_limit,
            materialize_to_table=(
                cli_options.materialize_to_table
                and db1.dialect.parse_table_name(eval_name_template(cli_options.materialize_to_table))
            ),
        )

    assert algorithm == Algorithm.HASHDIFF
    return HashDiffer(
        bisection_factor=cli_options.bisection_factor,
        bisection_threshold=cli_options.bisection_threshold,
        threaded=cli_options.threaded,
        max_threadpool_size=cli_options.threads and cli_options.threads * 2,
    )


def _print_result(cli_options: CliOptions, diff_iter) -> None:
    if cli_options.stats:
        if cli_options.json_output:
            rich.print(json.dumps(diff_iter.get_stats_dict()))
        else:
            rich.print(diff_iter.get_stats_string())

    else:
        for op, values in diff_iter:
            color = COLOR_SCHEME.get(op, "grey62")

            if cli_options.json_output:
                jsonl = json.dumps([op, list(values)])
                rich.print(f"[{color}]{jsonl}[/{color}]")
            else:
                text = f"{op} {', '.join(map(str, values))}"
                rich.print(f"[{color}]{text}[/{color}]")

            sys.stdout.flush()


def _get_expanded_columns(
    columns: List[str],
    case_sensitive: bool,
    mutual: Set[str],
    db1: Database,
    schema1: dict,
    table1: str,
    db2: Database,
    schema2: dict,
    table2: str,
) -> Set[str]:
    expanded_columns: Set[str] = set()
    for c in columns:
        cc = c if case_sensitive else c.lower()
        match = set(match_like(cc, mutual))
        if not match:
            m1 = None if any(match_like(cc, schema1.keys())) else f"{db1}/{table1}"
            m2 = None if any(match_like(cc, schema2.keys())) else f"{db2}/{table2}"
            not_matched = ", ".join(m for m in [m1, m2] if m)
            raise ValueError(f"Column '{c}' not found in: {not_matched}")

        expanded_columns |= match
    return expanded_columns


def _set_threads(cli_options: CliOptions) -> None:
    cli_options.threaded = True
    if isinstance(cli_options.threads, str):
        if cli_options.threads.lower() != "serial":
            message = "Error: threads must be a number, or 'serial'."
            logging.error(message)
            raise ValueError(message)

        assert not (cli_options.threads1 or cli_options.threads2)
        cli_options.threaded = False
        cli_options.threads = 1
    else:
        assert isinstance(cli_options.threads, int)
        if cli_options.threads < 1:
            message = "Error: threads must be >= 1"
            logging.error(message)
            raise ValueError(message)


def _data_diff(cli_options: CliOptions) -> None:
    if cli_options.limit and cli_options.stats:
        logging.error("Cannot specify a limit when using the -s/--stats switch")
        return

    key_columns = cli_options.key_columns or ("id",)
    _set_threads(cli_options)
    start = time.monotonic()

    if cli_options.database1 is None or cli_options.database2 is None:
        logging.error(
            (
                f"Error: Databases not specified. Got {cli_options.database1} and {cli_options.database2}. "
                f"Use --help for more information."
            )
        )
        return

    db1: Database
    db2: Database
    db1, db2 = _get_dbs(cli_options)
    with db1, db2:
        options = {
            "case_sensitive": cli_options.case_sensitive,
            "where": cli_options.where,
        }

        _set_age(options, cli_options, db1)
        dbs: Tuple[Database, Database] = db1, db2
        differ = _get_table_differ(cli_options, db1, db2)
        table_names = cli_options.table1, cli_options.table2
        table_paths = [db.dialect.parse_table_name(t) for db, t in safezip(dbs, table_names)]

        schemas = list(differ._thread_map(_get_schema, safezip(dbs, table_paths)))
        schema1, schema2 = schemas = [
            create_schema(db.name, table_path, schema, cli_options.case_sensitive)
            for db, table_path, schema in safezip(dbs, table_paths, schemas)
        ]

        mutual = schema1.keys() & schema2.keys()  # Case-aware, according to case_sensitive
        logging.debug(f"Available mutual columns: {mutual}")

        expanded_columns = _get_expanded_columns(
            list(cli_options.columns),
            cli_options.case_sensitive,
            mutual,
            db1,
            schema1,
            cli_options.table1,
            db2,
            schema2,
            cli_options.table2,
        )
        columns = tuple(expanded_columns - {*key_columns, cli_options.update_column})

        if db1 == db2:
            diff_schemas(*table_names, *schemas, (*key_columns, cli_options.update_column, *columns))

        logging.info(f"Diffing using columns: key={key_columns} update={cli_options.update_column} extra={columns}.")
        segments = [
            TableSegment(db, table_path, key_columns, cli_options.update_column, columns, **options)._with_raw_schema(
                raw_schema
            )
            for db, table_path, raw_schema in safezip(dbs, table_paths, schemas)
        ]

        diff_iter = differ.diff_tables(*segments)
        if cli_options.limit:
            assert not cli_options.stats
            diff_iter = islice(diff_iter, int(cli_options.limit))

        _print_result(cli_options, diff_iter)

    end = time.monotonic()
    logging.info(f"Duration: {end-start:.2f} seconds.")


if __name__ == "__main__":
    main()
