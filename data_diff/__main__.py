from copy import deepcopy
import sys
import time
import json
import logging
from itertools import islice

import rich
import click


from .utils import remove_password_from_url, safezip, match_like
from .diff_tables import TableDiffer, DEFAULT_BISECTION_THRESHOLD, DEFAULT_BISECTION_FACTOR
from .table_segment import TableSegment
from .databases.database_types import create_schema
from .databases.connect import connect
from .parse_time import parse_time_before_now, UNITS_STR, ParseError
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


@click.command()
@click.argument("database1", required=False)
@click.argument("table1", required=False)
@click.argument("database2", required=False)
@click.argument("table2", required=False)
@click.option("-k", "--key-column", default=None, help="Name of primary key column. Default='id'.")
@click.option("-t", "--update-column", default=None, help="Name of updated_at/last_updated column")
@click.option(
    "-c",
    "--columns",
    default=[],
    multiple=True,
    help="Names of extra columns to compare."
    "Can be used more than once in the same command. "
    "Accepts a name or a pattern like in SQL. Example: -c col% -c another_col",
)
@click.option("-l", "--limit", default=None, help="Maximum number of differences to find")
@click.option("--bisection-factor", default=None, help=f"Segments per iteration. Default={DEFAULT_BISECTION_FACTOR}.")
@click.option(
    "--bisection-threshold",
    default=None,
    help=f"Minimal bisection threshold. Below it, data-diff will download the data and compare it locally. Default={DEFAULT_BISECTION_THRESHOLD}.",
)
@click.option(
    "--min-age",
    default=None,
    help="Considers only rows older than specified. Useful for specifying replication lag."
    "Example: --min-age=5min ignores rows from the last 5 minutes. "
    f"\nValid units: {UNITS_STR}",
)
@click.option("--max-age", default=None, help="Considers only rows younger than specified. See --min-age.")
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
    "-j",
    "--threads",
    default=None,
    help="Number of worker threads to use per database. Default=1. "
    "A higher number will increase performance, but take more capacity from your database. "
    "'serial' guarantees a single-threaded execution of the algorithm (useful for debugging).",
)
@click.option("-w", "--where", default=None, help="An additional 'where' expression to restrict the search space.")
@click.option(
    "--conf",
    default=None,
    help="Path to a configuration.toml file, to provide a default configuration, and a list of possible runs.",
)
@click.option(
    "--run",
    default=None,
    help="Name of run-configuration to run. If used, CLI arguments for database and table must be omitted.",
)
def main(conf, run, **kw):
    if conf:
        kw = apply_config_from_file(conf, run, kw)
    return _main(**kw)


def _main(
    database1,
    table1,
    database2,
    table2,
    key_column,
    update_column,
    columns,
    limit,
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

    key_column = key_column or "id"
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

    try:
        options = dict(
            min_update=max_age and parse_time_before_now(max_age),
            max_update=min_age and parse_time_before_now(min_age),
            case_sensitive=case_sensitive,
            where=where,
        )
    except ParseError as e:
        logging.error(f"Error while parsing age expression: {e}")
        return

    differ = TableDiffer(
        bisection_factor=bisection_factor,
        bisection_threshold=bisection_threshold,
        threaded=threaded,
        max_threadpool_size=threads and threads * 2,
        debug=debug,
    )

    if database1 is None or database2 is None:
        logging.error(
            f"Error: Databases not specified. Got {database1} and {database2}. Use --help for more information."
        )
        return

    try:
        db1 = connect(database1, threads1 or threads)
        db2 = connect(database2, threads2 or threads)
    except Exception as e:
        logging.error(e)
        return

    dbs = db1, db2

    if interactive:
        for db in dbs:
            db.enable_interactive()

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
            raise ValueError(f"Column {c} not found in: {not_matched}")

        expanded_columns |= match

    columns = tuple(expanded_columns - {key_column, update_column})

    logging.info(f"Diffing columns: key={key_column} update={update_column} extra={columns}")

    segments = [
        TableSegment(db, table_path, key_column, update_column, columns, **options)._with_raw_schema(raw_schema)
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
            }
            print(json.dumps(json_output))
        else:
            print(f"Diff-Total: {len(diff)} changed rows out of {max_table_count}")
            print(f"Diff-Percent: {percent:.14f}%")
            print(f"Diff-Split: +{plus}  -{minus}")
    else:
        for op, values in diff_iter:
            color = COLOR_SCHEME[op]

            if json_output:
                jsonl = json.dumps([op, list(values)])
                rich.print(f"[{color}]{jsonl}[/{color}]")
            else:
                text = f"{op} {', '.join(values)}"
                rich.print(f"[{color}]{text}[/{color}]")

            sys.stdout.flush()

    end = time.monotonic()

    logging.info(f"Duration: {end-start:.2f} seconds.")


if __name__ == "__main__":
    main()
