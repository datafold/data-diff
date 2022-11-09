from copy import deepcopy
import sys
import time
import json
import logging
from itertools import islice
from typing import Optional
from datetime import datetime, timedelta

import rich
import click

from .utils import eval_name_template, remove_password_from_url, safezip, match_like
from .diff_tables import Algorithm
from .hashdiff_tables import HashDiffer, DEFAULT_BISECTION_THRESHOLD, DEFAULT_BISECTION_FACTOR
from .joindiff_tables import TABLE_WRITE_LIMIT, JoinDiffer
from .table_segment import TableSegment
from .databases.database_types import create_schema
from .databases.connect import connect
from .parse_time import parse_database_time_before_now, UNITS_STR, ParseError
from .config import apply_config_from_file
from .tracking import disable_tracking


LOG_FORMAT = "[%(asctime)s] %(levelname)s - %(message)s"
DATE_FORMAT = "%H:%M:%S"

COLOR_SCHEME = {
    "+": "green",
    "-": "red",
}


def _remove_passwords_in_dict(d: dict):
    for k, v in d.items():
        if k == "password":
            d[k] = "*" * len(v)
        elif isinstance(v, dict):
            _remove_passwords_in_dict(v)
        elif k.startswith("database"):
            d[k] = remove_password_from_url(v)


def _get_schema(pair):
    db, table_path = pair
    return db.query_table_schema(table_path)


def diff_schemas(schema1, schema2, columns):
    logging.info("Diffing schemas...")
    attrs = "name", "type", "datetime_precision", "numeric_precision", "numeric_scale"
    for c in columns:
        if c is None:  # Skip for convenience
            continue
        diffs = []
        for attr, v1, v2 in safezip(attrs, schema1[c], schema2[c]):
            if v1 != v2:
                diffs.append(f"{attr}:({v1} != {v2})")
        if diffs:
            logging.warning(f"Schema mismatch in column '{c}': {', '.join(diffs)}")


class MyHelpFormatter(click.HelpFormatter):
    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)
        self.indent_increment = 6

    def write_usage(self, prog: str, args: str = "", prefix: Optional[str] = None) -> None:
        self.write("data-diff - efficiently diff rows across database tables.\n\n")
        self.write("Usage:\n")
        self.write(f"  * In-db diff:    {prog} <database1> <table1> <table2> [OPTIONS]\n")
        self.write(f"  * Cross-db diff: {prog} <database1> <table1> <database2> <table2> [OPTIONS]\n")
        self.write(f"  * Using config:  {prog} --conf PATH [--run NAME] [OPTIONS]\n")


click.Context.formatter_class = MyHelpFormatter


@click.command(no_args_is_help=True)
@click.argument("database1", required=False)
@click.argument("table1", required=False)
@click.argument("database2", required=False)
@click.argument("table2", required=False)
@click.option(
    "-k", "--key-columns", default=[], multiple=True, help="Names of primary key columns. Default='id'.", metavar="NAME"
)
@click.option("-t", "--update-column", default=None, help="Name of updated_at/last_updated column", metavar="NAME")
@click.option(
    "-c",
    "--columns",
    default=[],
    multiple=True,
    help="Names of extra columns to compare."
    "Can be used more than once in the same command. "
    "Accepts a name or a pattern like in SQL. Example: -c col% -c another_col",
    metavar="NAME",
)
@click.option("-l", "--limit", default=None, help="Maximum number of differences to find", metavar="NUM")
@click.option(
    "--bisection-factor",
    default=None,
    help=f"Segments per iteration. Default={DEFAULT_BISECTION_FACTOR}.",
    metavar="NUM",
)
@click.option(
    "--bisection-threshold",
    default=None,
    help=f"Minimal bisection threshold. Below it, data-diff will download the data and compare it locally. Default={DEFAULT_BISECTION_THRESHOLD}.",
    metavar="NUM",
)
@click.option(
    "-m",
    "--materialize",
    default=None,
    metavar="TABLE_NAME",
    help="(joindiff only) Materialize the diff results into a new table in the database. If a table exists by that name, it will be replaced.",
)
@click.option(
    "--min-age",
    default=None,
    help="Considers only rows older than specified. Useful for specifying replication lag."
    "Example: --min-age=5min ignores rows from the last 5 minutes. "
    f"\nValid units: {UNITS_STR}",
    metavar="AGE",
)
@click.option(
    "--max-age", default=None, help="Considers only rows younger than specified. See --min-age.", metavar="AGE"
)
@click.option("-s", "--stats", is_flag=True, help="Print stats instead of a detailed diff")
@click.option("-d", "--debug", is_flag=True, help="Print debug info")
@click.option("--json", "json_output", is_flag=True, help="Print JSONL output for machine readability")
@click.option("-v", "--verbose", is_flag=True, help="Print extra info")
@click.option("-i", "--interactive", is_flag=True, help="Confirm queries, implies --debug")
@click.option("--no-tracking", is_flag=True, help="data-diff sends home anonymous usage data. Use this to disable it.")
@click.option(
    "--case-sensitive",
    is_flag=True,
    help="Column names are treated as case-sensitive. Otherwise, data-diff corrects their case according to schema.",
)
@click.option(
    "--assume-unique-key",
    is_flag=True,
    help="Skip validating the uniqueness of the key column during joindiff, which is costly in non-cloud dbs.",
)
@click.option(
    "--sample-exclusive-rows",
    is_flag=True,
    help="Sample several rows that only appear in one of the tables, but not the other. (joindiff only)",
)
@click.option(
    "--materialize-all-rows",
    is_flag=True,
    help="Materialize every row, even if they are the same, instead of just the differing rows. (joindiff only)",
)
@click.option(
    "--table-write-limit",
    default=TABLE_WRITE_LIMIT,
    help=f"Maximum number of rows to write when creating materialized or sample tables, per thread. Default={TABLE_WRITE_LIMIT}",
    metavar="COUNT",
)
@click.option(
    "-j",
    "--threads",
    default=None,
    help="Number of worker threads to use per database. Default=1. "
    "A higher number will increase performance, but take more capacity from your database. "
    "'serial' guarantees a single-threaded execution of the algorithm (useful for debugging).",
    metavar="COUNT",
)
@click.option(
    "-w", "--where", default=None, help="An additional 'where' expression to restrict the search space.", metavar="EXPR"
)
@click.option("-a", "--algorithm", default=Algorithm.AUTO.value, type=click.Choice([i.value for i in Algorithm]))
@click.option(
    "--conf",
    default=None,
    help="Path to a configuration.toml file, to provide a default configuration, and a list of possible runs.",
    metavar="PATH",
)
@click.option(
    "--run",
    default=None,
    help="Name of run-configuration to run. If used, CLI arguments for database and table must be omitted.",
    metavar="NAME",
)
def main(conf, run, **kw):
    indb_syntax = False
    if kw["table2"] is None and kw["database2"]:
        # Use the "database table table" form
        kw["table2"] = kw["database2"]
        kw["database2"] = kw["database1"]
        indb_syntax = True

    if conf:
        kw = apply_config_from_file(conf, run, kw)

    kw["algorithm"] = Algorithm(kw["algorithm"])
    if kw["algorithm"] == Algorithm.AUTO:
        kw["algorithm"] = Algorithm.JOINDIFF if indb_syntax else Algorithm.HASHDIFF

    return _main(**kw)


def _main(
    database1,
    table1,
    database2,
    table2,
    key_columns,
    update_column,
    columns,
    limit,
    algorithm,
    bisection_factor,
    bisection_threshold,
    min_age,
    max_age,
    stats,
    debug,
    verbose,
    interactive,
    no_tracking,
    threads,
    case_sensitive,
    json_output,
    where,
    assume_unique_key,
    sample_exclusive_rows,
    materialize_all_rows,
    table_write_limit,
    materialize,
    threads1=None,
    threads2=None,
    __conf__=None,
):

    if no_tracking:
        disable_tracking()

    if interactive:
        debug = True

    if debug:
        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)
        if __conf__:
            __conf__ = deepcopy(__conf__)
            _remove_passwords_in_dict(__conf__)
            logging.debug(f"Applied run configuration: {__conf__}")
    elif verbose:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
    else:
        logging.basicConfig(level=logging.WARNING, format=LOG_FORMAT, datefmt=DATE_FORMAT)

    if limit and stats:
        logging.error("Cannot specify a limit when using the -s/--stats switch")
        return

    key_columns = key_columns or ("id",)
    bisection_factor = DEFAULT_BISECTION_FACTOR if bisection_factor is None else int(bisection_factor)
    bisection_threshold = DEFAULT_BISECTION_THRESHOLD if bisection_threshold is None else int(bisection_threshold)

    threaded = True
    if threads is None:
        threads = 1
    elif isinstance(threads, str) and threads.lower() == "serial":
        assert not (threads1 or threads2)
        threaded = False
        threads = 1
    else:
        try:
            threads = int(threads)
        except ValueError:
            logging.error("Error: threads must be a number, or 'serial'.")
            return
        if threads < 1:
            logging.error("Error: threads must be >= 1")
            return

    start = time.monotonic()

    if database1 is None or database2 is None:
        logging.error(
            f"Error: Databases not specified. Got {database1} and {database2}. Use --help for more information."
        )
        return

    try:
        db1 = connect(database1, threads1 or threads)
        if database1 == database2:
            db2 = db1
        else:
            db2 = connect(database2, threads2 or threads)
    except Exception as e:
        logging.error(e)
        return

    dbs = db1, db2

    try:
        db1_time = db1.query_database_current_timestamp()
    except Exception as e:
        logging.error(e)
        return

    try:
        db2_time = db2.query_database_current_timestamp()
    except Exception as e:
        logging.error(e)
        return

    if abs(db1_time - db2_time) >= timedelta(hours=1):
        logging.error(
            "Error: Databases have difference > 1 hour in current_timestamp values. Likely that databases use different time zones."
        )
        return

    try:
        options = dict(
            min_update=max_age and parse_database_time_before_now(max_age, db1_time),
            max_update=min_age and parse_database_time_before_now(min_age, db1_time),
            case_sensitive=case_sensitive,
            where=where,
        )
    except ParseError as e:
        logging.error(f"Error while parsing age expression: {e}")
        return

    if interactive:
        for db in dbs:
            db.enable_interactive()

    if algorithm == Algorithm.JOINDIFF:
        differ = JoinDiffer(
            threaded=threaded,
            max_threadpool_size=threads and threads * 2,
            validate_unique_key=not assume_unique_key,
            sample_exclusive_rows=sample_exclusive_rows,
            materialize_all_rows=materialize_all_rows,
            table_write_limit=table_write_limit,
            materialize_to_table=materialize and db1.parse_table_name(eval_name_template(materialize)),
        )
    else:
        assert algorithm == Algorithm.HASHDIFF
        differ = HashDiffer(
            bisection_factor=bisection_factor,
            bisection_threshold=bisection_threshold,
            threaded=threaded,
            max_threadpool_size=threads and threads * 2,
        )

    table_names = table1, table2
    table_paths = [db.parse_table_name(t) for db, t in safezip(dbs, table_names)]

    schemas = list(differ._thread_map(_get_schema, safezip(dbs, table_paths)))
    schema1, schema2 = schemas = [
        create_schema(db, table_path, schema, case_sensitive)
        for db, table_path, schema in safezip(dbs, table_paths, schemas)
    ]

    mutual = schema1.keys() & schema2.keys()  # Case-aware, according to case_sensitive
    logging.debug(f"Available mutual columns: {mutual}")

    expanded_columns = set()
    for c in columns:
        match = set(match_like(c, mutual))
        if not match:
            m1 = None if any(match_like(c, schema1.keys())) else f"{db1}/{table1}"
            m2 = None if any(match_like(c, schema2.keys())) else f"{db2}/{table2}"
            not_matched = ", ".join(m for m in [m1, m2] if m)
            raise ValueError(f"Column '{c}' not found in: {not_matched}")

        expanded_columns |= match

    columns = tuple(expanded_columns - {*key_columns, update_column})

    if db1 is db2:
        diff_schemas(
            schema1,
            schema2,
            (
                *key_columns,
                update_column,
                *columns,
            ),
        )

    logging.info(f"Diffing using columns: key={key_columns} update={update_column} extra={columns}")

    segments = [
        TableSegment(db, table_path, key_columns, update_column, columns, **options)._with_raw_schema(raw_schema)
        for db, table_path, raw_schema in safezip(dbs, table_paths, schemas)
    ]

    diff_iter = differ.diff_tables(*segments)

    if limit:
        diff_iter = islice(diff_iter, int(limit))

    if stats:
        diff = list(diff_iter)
        unique_diff_count = len({i[0] for _, i in diff})
        max_table_count = max(differ.stats["table1_count"], differ.stats["table2_count"])
        percent = 100 * unique_diff_count / (max_table_count or 1)
        plus = len([1 for op, _ in diff if op == "+"])
        minus = len([1 for op, _ in diff if op == "-"])

        if json_output:
            json_output = {
                "different_rows": len(diff),
                "different_percent": percent,
                "different_+": plus,
                "different_-": minus,
                "total": max_table_count,
                "stats": differ.stats,
            }
            print(json.dumps(json_output))
        else:
            print(f"Diff-Total: {len(diff)} changed rows out of {max_table_count}")
            print(f"Diff-Percent: {percent:.14f}%")
            print(f"Diff-Split: +{plus}  -{minus}")
            if differ.stats:
                print("Extra-Info:")
                for k, v in differ.stats.items():
                    print(f"  {k} = {v}")
    else:
        for op, values in diff_iter:
            color = COLOR_SCHEME[op]

            if json_output:
                jsonl = json.dumps([op, list(values)])
                rich.print(f"[{color}]{jsonl}[/{color}]")
            else:
                text = f"{op} {', '.join(map(str, values))}"
                rich.print(f"[{color}]{text}[/{color}]")

            sys.stdout.flush()

    end = time.monotonic()

    logging.info(f"Duration: {end-start:.2f} seconds.")


if __name__ == "__main__":
    main()
