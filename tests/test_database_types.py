import sys
import unittest
import time
import json
import re
import uuid
from datetime import datetime, timedelta, timezone
import logging
from decimal import Decimal
from itertools import islice, repeat, chain
from typing import Iterator

from parameterized import parameterized

from data_diff.databases.base import Row
from data_diff.utils import number_to_human
from data_diff.queries.api import table, commit, this, Code
from data_diff.queries.api import insert_rows_in_batches

from data_diff import databases as db
from data_diff.query_utils import drop_table
from data_diff.utils import accumulate
from data_diff.hashdiff_tables import HashDiffer, DEFAULT_BISECTION_THRESHOLD
from data_diff.table_segment import TableSegment
from tests.common import (
    CONN_STRINGS,
    N_SAMPLES,
    N_THREADS,
    BENCHMARK,
    GIT_REVISION,
    TEST_ACROSS_ALL_DBS,
    get_conn,
    random_table_suffix,
)

CONNS = None


def init_conns():
    global CONNS
    if CONNS is not None:
        return

    CONNS = {cls: get_conn(cls) for cls in CONN_STRINGS}


DATABASE_TYPES = {
    db.PostgreSQL: {
        # https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-INT
        "int": [
            # "smallint",  # 2 bytes
            "int",  # 4 bytes
            "bigint",  # 8 bytes
        ],
        # https://www.postgresql.org/docs/current/datatype-datetime.html
        "datetime": [
            "timestamp(6) without time zone",
            "timestamp(3) without time zone",
            "timestamp(0) without time zone",
            "timestamp with time zone",
        ],
        # https://www.postgresql.org/docs/current/datatype-numeric.html
        "float": [
            "real",
            "float",
            "double precision",
            "numeric(6,3)",
        ],
        "uuid": [
            "text",
            "varchar(100)",
            "char(100)",
        ],
        "boolean": [
            "boolean",
        ],
        "json": ["json", "jsonb"],
    },
    db.MySQL: {
        # https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
        "int": [
            # "tinyint", # 1 byte
            # "smallint", # 2 bytes
            # "mediumint", # 3 bytes
            "int",  # 4 bytes
            "bigint",  # 8 bytes
        ],
        # https://dev.mysql.com/doc/refman/8.0/en/datetime.html
        "datetime": [
            "timestamp(6)",
            "timestamp(3)",
            "timestamp(0)",
            "timestamp",
            "datetime(6)",
        ],
        # https://dev.mysql.com/doc/refman/8.0/en/numeric-types.html
        "float": [
            "float",
            "double",
            "numeric",
            "numeric(65, 10)",
        ],
        "uuid": [
            "varchar(100)",
            "char(100)",
            "varbinary(100)",
        ],
        "boolean": [
            "boolean",
        ],
    },
    db.DuckDB: {
        "int": [
            "INTEGER",  # 4 bytes
            "BIGINT",  # 8 bytes
        ],
        "datetime": ["TIMESTAMP", "TIMESTAMPTZ"],
        #  DDB truncates instead of rounding on Prec loss. Currently
        "float": [
            # "FLOAT",
            # "DOUBLE",
            # 'DECIMAL'
        ],
        "uuid": [
            "VARCHAR(100)",
        ],
        "boolean": [
            "BOOLEAN",
        ],
    },
    db.BigQuery: {
        "int": ["int"],
        "datetime": [
            "timestamp",
            "datetime",
        ],
        "float": [
            "numeric",
            "float64",
            "bignumeric",
        ],
        "uuid": [
            "STRING",
        ],
        "boolean": [
            "boolean",
        ],
    },
    db.Snowflake: {
        # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#int-integer-bigint-smallint-tinyint-byteint
        "int": [
            # all 38 digits with 0 precision, don't need to test all
            "int",
            "bigint",
            # "smallint",
            # "tinyint",
            # "byteint"
        ],
        # https://docs.snowflake.com/en/sql-reference/data-types-datetime.html
        "datetime": [
            "timestamp(0)",
            "timestamp(3)",
            "timestamp(6)",
            "timestamp(9)",
            "timestamp_tz(9)",
            "timestamp_ntz(9)",
        ],
        # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#decimal-numeric
        "float": [
            "float",
            "numeric",
        ],
        "uuid": [
            "varchar",
            "varchar(100)",
        ],
        "boolean": [
            "boolean",
        ],
    },
    db.Redshift: {
        "int": [
            "int",
        ],
        "datetime": [
            "TIMESTAMP",
            "timestamp with time zone",
        ],
        # https://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html#r_Numeric_types201-floating-point-types
        "float": [
            "float4",
            "float8",
            "numeric",
        ],
        "uuid": [
            "text",
            "varchar(100)",
            "char(100)",
        ],
        "boolean": [
            "boolean",
        ],
        "json": [
            "super",
        ],
    },
    db.Oracle: {
        "int": [
            "int",
        ],
        "datetime": [
            "timestamp with local time zone",
            "timestamp(6) with local time zone",
            "timestamp(9) with local time zone",
        ],
        "float": [
            "float",
            "numeric",
            "real",
            "double precision",
            "Number(5, 2)",
        ],
        "uuid": [
            "CHAR(100)",
            "VARCHAR(100)",
            "NCHAR(100)",
            "NVARCHAR2(100)",
        ],
        "boolean": [],  # Oracle has no boolean type
    },
    db.Presto: {
        "int": [
            # "tinyint", # 1 byte
            # "smallint", # 2 bytes
            # "mediumint", # 3 bytes
            "int",  # 4 bytes
            "bigint",  # 8 bytes
        ],
        "datetime": [
            "timestamp",
            "timestamp with time zone",
        ],
        "float": [
            "real",
            "double",
            "decimal(10,2)",
            "decimal(30,6)",
        ],
        "uuid": [
            "varchar",
            "char(100)",
        ],
        "boolean": [
            "boolean",
        ],
    },
    db.Databricks: {
        # https://docs.databricks.com/spark/latest/spark-sql/language-manual/data-types/int-type.html
        # https://docs.databricks.com/spark/latest/spark-sql/language-manual/data-types/bigint-type.html
        "int": [
            "INT",
            "BIGINT",
        ],
        # https://docs.databricks.com/spark/latest/spark-sql/language-manual/data-types/timestamp-type.html
        "datetime": [
            "TIMESTAMP",
        ],
        # https://docs.databricks.com/spark/latest/spark-sql/language-manual/data-types/float-type.html
        # https://docs.databricks.com/spark/latest/spark-sql/language-manual/data-types/double-type.html
        # https://docs.databricks.com/spark/latest/spark-sql/language-manual/data-types/decimal-type.html
        "float": [
            "FLOAT",
            "DOUBLE",
            "DECIMAL(6, 2)",
        ],
        "uuid": [
            "STRING",
        ],
        "boolean": [
            "boolean",
        ],
    },
    db.Trino: {
        "int": [
            "int",
            "bigint",
        ],
        "datetime": [
            "timestamp",
            "timestamp with time zone",
        ],
        "float": [
            "real",
            "double",
            "decimal(10,2)",
            "decimal(30,6)",
        ],
        "uuid": [
            "varchar",
            "char(100)",
        ],
        "boolean": [
            "boolean",
        ],
    },
    db.Clickhouse: {
        "int": [
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "Int128",
            "Int256",
        ],
        "datetime": [
            "DateTime64(6)",
            "DateTime64(3)",
            "DateTime64(0)",
            "DateTime",
        ],
        "float": [
            "Decimal(6, 2)",
            "Float32",
            "Float64",
        ],
        "uuid": [
            "String",
        ],
        "boolean": [
            "boolean",
        ],
    },
    db.Vertica: {
        "int": ["int"],
        "datetime": [
            "timestamp(6) without time zone",
            "timestamp(3) without time zone",
            "timestamp(0) without time zone",
            "timestamp with time zone",
        ],
        "float": [
            "numeric(6, 2)",
            "float",
            "float8",
        ],
        "uuid": [
            "varchar(100)",
            "char(100)",
        ],
        "boolean": [
            "boolean",
        ],
    },
    db.MsSQL: {
        "int": ["INT", "BIGINT"],
        "datetime": ["datetime2(6)"],
        "float": ["DECIMAL(6, 2)", "FLOAT", "REAL"],
        "uuid": ["VARCHAR(100)", "CHAR(100)", "UNIQUEIDENTIFIER"],
        "boolean": [
            "BIT",
        ],
    },
}


class PaginatedTable:
    # We can't query all the rows at once for large tables. It'll occupy too
    # much memory.
    RECORDS_PER_BATCH = 1000000

    def __init__(self, table_path, conn) -> None:
        super().__init__()
        self.table_path = table_path
        self.conn = conn

    def __iter__(self) -> Iterator[Row]:
        last_id = 0
        while True:
            query = (
                table(self.table_path)
                .select(this.id, this.col)
                .where(this.id > last_id)
                .order_by(this.id)
                .limit(self.RECORDS_PER_BATCH)
            )
            rows = self.conn.query(query, list)
            if not rows:
                break
            last_id = rows[-1][0]
            yield from rows


class DateTimeFaker:
    MANUAL_FAKES = [
        datetime.fromisoformat("2020-01-01 15:10:10"),
        datetime.fromisoformat("2020-02-01 09:09:09"),
        datetime.fromisoformat("2022-03-01 15:10:01.139"),
        datetime.fromisoformat("2022-04-01 15:10:02.020409"),
        datetime.fromisoformat("2022-05-01 15:10:03.003030"),
        datetime.fromisoformat("2022-06-01 15:10:05.009900"),
    ]

    def __init__(self, max) -> None:
        super().__init__()
        self.max = max

    def __iter__(self) -> Iterator[datetime]:
        initial = datetime(2000, 1, 1, 0, 0, 0, 0)
        step = timedelta(seconds=3, microseconds=571)
        return islice(chain(self.MANUAL_FAKES, accumulate(repeat(step), initial=initial)), self.max)

    def __len__(self) -> int:
        return self.max


class IntFaker:
    MANUAL_FAKES = [127, -3, -9, 37, 15, 0]

    def __init__(self, max) -> None:
        super().__init__()
        self.max = max

    def __iter__(self) -> Iterator[int]:
        initial = -128
        step = 1
        return islice(chain(self.MANUAL_FAKES, accumulate(repeat(step), initial=initial)), self.max)

    def __len__(self) -> int:
        return self.max


class BooleanFaker:
    MANUAL_FAKES = [False, True, True, False]

    def __init__(self, max) -> None:
        super().__init__()
        self.max = max

    def __iter__(self) -> Iterator[bool]:
        return iter(self.MANUAL_FAKES[: self.max])

    def __len__(self) -> int:
        return min(self.max, len(self.MANUAL_FAKES))


class FloatFaker:
    MANUAL_FAKES = [
        0.0,
        0.1,
        0.00188,
        0.99999,
        0.091919,
        0.10,
        10.0,
        100.98,
        0.001201923076923077,
        1 / 3,
        1 / 5,
        1 / 109,
        1 / 109489,
        1 / 1094893892389,
        1 / 10948938923893289,
        3.141592653589793,
    ]

    def __init__(self, max) -> None:
        super().__init__()
        self.max = max

    def __iter__(self) -> Iterator[float]:
        initial = -10.0001
        step = 0.00571
        return islice(chain(self.MANUAL_FAKES, accumulate(repeat(step), initial=initial)), self.max)

    def __len__(self) -> int:
        return self.max


class UUID_Faker:
    def __init__(self, max) -> None:
        super().__init__()
        self.max = max

    def __len__(self) -> int:
        return self.max

    def __iter__(self) -> Iterator[uuid.UUID]:
        return (uuid.uuid1(i) for i in range(self.max))


class JsonFaker:
    MANUAL_FAKES = [
        '{"keyText": "text", "keyInt": 3, "keyFloat": 5.4445, "keyBoolean": true}',
    ]

    def __init__(self, max) -> None:
        super().__init__()
        self.max = max

    def __iter__(self) -> Iterator[str]:
        return iter(self.MANUAL_FAKES[: self.max])

    def __len__(self) -> int:
        return min(self.max, len(self.MANUAL_FAKES))


TYPE_SAMPLES = {
    "int": IntFaker(N_SAMPLES),
    "datetime": DateTimeFaker(N_SAMPLES),
    "float": FloatFaker(N_SAMPLES),
    "uuid": UUID_Faker(N_SAMPLES),
    "boolean": BooleanFaker(N_SAMPLES),
    "json": JsonFaker(N_SAMPLES),
}


def _get_test_db_pairs():
    if str(TEST_ACROSS_ALL_DBS).lower() == "full":
        for source_db in DATABASE_TYPES:
            for target_db in DATABASE_TYPES:
                yield source_db, target_db
    elif int(TEST_ACROSS_ALL_DBS):
        for db_cls in DATABASE_TYPES:
            yield db_cls, db.PostgreSQL
            yield db.PostgreSQL, db_cls
            yield db_cls, db.Snowflake
            yield db.Snowflake, db_cls
    else:
        yield db.PostgreSQL, db.PostgreSQL


def get_test_db_pairs():
    active_pairs = {(db1, db2) for db1, db2 in _get_test_db_pairs() if db1 in CONN_STRINGS and db2 in CONN_STRINGS}
    for db1, db2 in active_pairs:
        yield db1, DATABASE_TYPES[db1], db2, DATABASE_TYPES[db2]


type_pairs = []
for source_db, source_type_categories, target_db, target_type_categories in get_test_db_pairs():
    for type_category, source_types in source_type_categories.items():  # int, datetime, ..
        for source_type in source_types:
            if type_category in target_type_categories:  # only cross-compatible types
                for target_type in target_type_categories[type_category]:
                    type_pairs.append(
                        (
                            source_db,
                            target_db,
                            source_type,
                            target_type,
                            type_category,
                        )
                    )


def sanitize(name):
    name = name.lower()
    name = re.sub(r"[\(\)]", "", name)  #  timestamp(9) -> timestamp9
    # Try to shorten long fields, due to length limitations in some DBs
    name = name.replace(r"without time zone", "n_tz")
    name = name.replace(r"with time zone", "y_tz")
    name = name.replace(r"with local time zone", "y_tz")
    name = name.replace(r"timestamp", "ts")
    name = name.replace(r"double precision", "double")
    name = name.replace(r"numeric", "num")
    return parameterized.to_safe_name(name)


# Pass --verbose to test run to get a nice output.
def expand_params(testcase_func, param_num, param):
    source_db, target_db, source_type, target_type, type_category = param.args
    source_db_type = source_db.__name__
    target_db_type = target_db.__name__

    name = "%s_%s_%s_%s_%s_%s" % (
        testcase_func.__name__,
        sanitize(source_db_type),
        sanitize(source_type),
        sanitize(target_db_type),
        sanitize(target_type),
        number_to_human(N_SAMPLES),
    )

    return name


def _insert_to_table(conn, table_path, values, coltype):
    tbl = table(table_path)

    current_n_rows = conn.query(tbl.count(), int)
    if current_n_rows == N_SAMPLES:
        assert BENCHMARK, "Table should've been deleted, or we should be in BENCHMARK mode"
        return
    elif current_n_rows > 0:
        conn.query(drop_table(table_name))
        _create_table_with_indexes(conn, table_path, coltype)

    # if BENCHMARK and N_SAMPLES > 10_000:
    #     description = f"{conn.name}: {table}"
    #     values = rich.progress.track(values, total=N_SAMPLES, description=description)

    if coltype == "boolean":
        values = [(i, bool(sample)) for i, sample in values]
    elif re.search(r"(time zone|tz)", coltype):
        values = [(i, sample.replace(tzinfo=timezone.utc)) for i, sample in values]

    if isinstance(conn, db.Clickhouse):
        if coltype.startswith("DateTime64"):
            values = [(i, f"{sample.replace(tzinfo=None)}") for i, sample in values]

        elif coltype == "DateTime":
            # Clickhouse's DateTime does not allow to store micro/milli/nano seconds
            values = [(i, str(sample)[:19]) for i, sample in values]

        elif coltype.startswith("Decimal("):
            precision = int(coltype[8:].rstrip(")").split(",")[1])
            values = [(i, round(sample, precision)) for i, sample in values]
    elif isinstance(conn, db.BigQuery) and coltype == "datetime":
        values = [(i, Code(f"cast(timestamp '{sample}' as datetime)")) for i, sample in values]

    elif isinstance(conn, db.Redshift) and coltype in ("json", "jsonb"):
        values = [(i, Code(f"JSON_PARSE({sample})")) for i, sample in values]
    elif isinstance(conn, db.PostgreSQL) and coltype in ("json", "jsonb"):
        values = [
            (
                i,
                Code(
                    "'{}'".format(
                        (json.dumps(sample) if isinstance(sample, (dict, list)) else sample).replace("'", "''")
                    )
                ),
            )
            for i, sample in values
        ]
    # mssql represents with int
    elif isinstance(conn, db.MsSQL) and coltype in ("BIT"):
        values = [(i, int(sample)) for i, sample in values]

    insert_rows_in_batches(conn, tbl, values, columns=["id", "col"])
    conn.query(commit)


def _create_table_with_indexes(conn, table_path, type_):
    quote = conn.dialect.quote
    table_name = ".".join(map(quote, table_path))

    tbl = table(
        table_path,
        schema={
            "id": int,
            "col": type_,
        },
    )

    if isinstance(conn, db.Clickhouse):
        conn.query(f"CREATE TABLE {table_name}(id int, col {type_}) engine = Memory;", None)
    else:
        conn.query(tbl.create())

    (index_id,) = table_path
    if conn.dialect.SUPPORTS_INDEXES and type_ not in ("json", "jsonb", "array", "struct"):
        conn.query(f"CREATE INDEX xa_{index_id} ON {table_name} ({quote('id')}, {quote('col')})")
    if conn.dialect.SUPPORTS_INDEXES:
        conn.query(f"CREATE INDEX xb_{index_id} ON {table_name} ({quote('id')})")

    conn.query(commit)


class TestDiffCrossDatabaseTables(unittest.TestCase):
    maxDiff = 10000

    def setUp(self) -> None:
        init_conns()

    def tearDown(self) -> None:
        if not BENCHMARK:
            drop_table(self.src_conn, self.src_table_path)
            drop_table(self.dst_conn, self.dst_table_path)

        return super().tearDown()

    @parameterized.expand(type_pairs, name_func=expand_params)
    def test_types(self, source_db, target_db, source_type, target_type, type_category):
        start = time.monotonic()

        self.src_conn = src_conn = CONNS[source_db]
        self.dst_conn = dst_conn = CONNS[target_db]

        self.connections = [self.src_conn, self.dst_conn]
        sample_values = TYPE_SAMPLES[type_category]

        table_suffix = ""
        # Benchmarks we re-use tables for performance. For tests, we create
        # unique tables to ensure isolation.
        if not BENCHMARK:
            table_suffix = random_table_suffix()

        # Limit in MySQL is 64, Presto seems to be 63
        src_table_name = f"src_{self._testMethodName[11:]}{table_suffix}"
        dst_table_name = f"dst_{self._testMethodName[11:]}{table_suffix}"

        self.src_table_path = src_table_path = src_conn.dialect.parse_table_name(src_table_name)
        self.dst_table_path = dst_table_path = dst_conn.dialect.parse_table_name(dst_table_name)

        start = time.monotonic()
        if not BENCHMARK:
            drop_table(src_conn, src_table_path)
        _create_table_with_indexes(src_conn, src_table_path, source_type)
        _insert_to_table(src_conn, src_table_path, enumerate(sample_values, 1), source_type)
        insertion_source_duration = time.monotonic() - start

        values_in_source = PaginatedTable(src_table_path, src_conn)
        if source_db is db.Presto or source_db is db.Trino:
            if source_type.startswith("decimal"):
                values_in_source = ((a, Decimal(b)) for a, b in values_in_source)
            elif source_type.startswith("timestamp"):
                values_in_source = ((a, datetime.fromisoformat(b.rstrip(" UTC"))) for a, b in values_in_source)

        start = time.monotonic()
        if not BENCHMARK:
            drop_table(dst_conn, dst_table_path)
        _create_table_with_indexes(dst_conn, dst_table_path, target_type)
        _insert_to_table(dst_conn, dst_table_path, values_in_source, target_type)
        insertion_target_duration = time.monotonic() - start

        if type_category == "uuid":
            self.table = TableSegment(self.src_conn, src_table_path, ("col",), None, ("id",), case_sensitive=False)
            self.table2 = TableSegment(self.dst_conn, dst_table_path, ("col",), None, ("id",), case_sensitive=False)
        else:
            self.table = TableSegment(self.src_conn, src_table_path, ("id",), None, ("col",), case_sensitive=False)
            self.table2 = TableSegment(self.dst_conn, dst_table_path, ("id",), None, ("col",), case_sensitive=False)

        start = time.monotonic()
        self.assertEqual(len(sample_values), self.table.count())
        count_source_duration = time.monotonic() - start

        start = time.monotonic()
        self.assertEqual(len(sample_values), self.table2.count())
        count_target_duration = time.monotonic() - start

        # When testing, we configure these to their lowest possible values for
        # the DEFAULT_N_SAMPLES.
        # When benchmarking, we try to dynamically create some more optima
        # configuration with each segment being ~250k rows.
        ch_factor = min(max(int(N_SAMPLES / 250_000), 2), 128) if BENCHMARK else 2
        ch_threshold = min(DEFAULT_BISECTION_THRESHOLD, int(N_SAMPLES / ch_factor)) if BENCHMARK else 3
        ch_threads = N_THREADS
        differ = HashDiffer(
            bisection_threshold=ch_threshold,
            bisection_factor=ch_factor,
            max_threadpool_size=ch_threads,
        )
        start = time.monotonic()
        diff = list(differ.diff_tables(self.table, self.table2))
        checksum_duration = time.monotonic() - start
        expected = []
        self.assertEqual(expected, diff)

        # For fuzzily diffed types, some rows can be downloaded for local comparison. This happens
        # when hashes are diferent but the essential payload is not; e.g. due to json serialization.
        if not {source_type, target_type} & {"json", "jsonb", "array", "struct"}:
            self.assertEqual(0, differ.stats.get("rows_downloaded", 0))

        # This section downloads all rows to ensure that Python agrees with the
        # database, in terms of comparison.
        #
        # For benchmarking, to make it fair, we split into segments of a
        # reasonable amount of rows each. These will then be downloaded in
        # parallel, using the existing implementation.
        dl_factor = max(int(N_SAMPLES / 100_000), 2) if BENCHMARK else 2
        dl_threshold = int(N_SAMPLES / dl_factor) + 1 if BENCHMARK else sys.maxsize
        dl_threads = N_THREADS
        differ = HashDiffer(
            bisection_factor=dl_factor,
            bisection_threshold=dl_threshold,
            bisection_disabled=True,
            max_threadpool_size=dl_threads,
        )
        start = time.monotonic()
        diff = list(differ.diff_tables(self.table, self.table2))
        download_duration = time.monotonic() - start
        expected = []
        self.assertEqual(expected, diff)
        self.assertEqual(len(sample_values), differ.stats.get("rows_downloaded", 0))

        result = {
            "test": self._testMethodName,
            "source_db": source_db.__name__,
            "target_db": target_db.__name__,
            "date": str(datetime.today()),
            "git_revision": GIT_REVISION,
            "rows": N_SAMPLES,
            "rows_human": number_to_human(N_SAMPLES),
            "name_human": f"{source_db.__name__}/{sanitize(source_type)} <-> {target_db.__name__}/{sanitize(target_type)}",
            "src_table": src_table_path,
            "target_table": dst_table_path,
            "source_type": source_type,
            "target_type": target_type,
            "insertion_source_sec": round(insertion_source_duration, 3),
            "insertion_target_sec": round(insertion_target_duration, 3),
            "count_source_sec": round(count_source_duration, 3),
            "count_target_sec": round(count_target_duration, 3),
            "count_max_sec": max(round(count_target_duration, 3), round(count_source_duration, 3)),
            "checksum_sec": round(checksum_duration, 3),
            "download_sec": round(download_duration, 3),
            "download_bisection_factor": dl_factor,
            "download_bisection_threshold": dl_threshold,
            "download_threads": dl_threads,
            "checksum_bisection_factor": ch_factor,
            "checksum_bisection_threshold": ch_threshold,
            "checksum_threads": ch_threads,
        }

        if BENCHMARK:
            print(json.dumps(result, indent=2))
            file_name = f"benchmark_{GIT_REVISION}.jsonl"
            with open(file_name, "a", encoding="utf-8") as file:
                file.write(json.dumps(result) + "\n")
                file.flush()
            print(f"Written to {file_name}")
        else:
            logging.debug(json.dumps(result, indent=2))
