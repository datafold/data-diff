import unittest
import time
import csv
import math
from google.cloud import bigquery
import re
import time
import datetime
from parameterized import parameterized
import rich.progress
from data_diff import databases as db
from data_diff.diff_tables import TableDiffer, TableSegment
from .common import (
    CONN_STRINGS,
    BENCHMARK,
    DEFAULT_N_SAMPLES,
    N_SAMPLES,
    str_to_checksum,
)
import logging
from decimal import Decimal


class Faker:
    pass


class PaginatedTable:
    # We can't query all the rows at once for large tables. It'll occupy too
    # much memory.
    RECORDS_PER_BATCH = 1000000

    def __init__(self, table, conn):
        self.table = table
        self.conn = conn

    def __iter__(self):
        self.last_id = 0
        self.values = []
        self.value_index = 0
        return self

    def __next__(self) -> str:
        if self.value_index == len(self.values):  #  end of current batch
            query = f"SELECT id, col FROM {self.table} WHERE id > {self.last_id} ORDER BY id ASC LIMIT {self.RECORDS_PER_BATCH}"
            if isinstance(self.conn, db.Oracle):
                query = f"SELECT id, col FROM {self.table} WHERE id > {self.last_id} ORDER BY id ASC OFFSET 0 ROWS FETCH NEXT {self.RECORDS_PER_BATCH} ROWS ONLY"

            self.values = self.conn.query(query, list)
            if len(self.values) == 0:  #  we must be done!
                raise StopIteration
            self.last_id = self.values[-1][0]
            self.value_index = 0

        this_value = self.values[self.value_index]
        self.value_index += 1
        return this_value


class DateTimeFaker:
    MANUAL_FAKES = [
        "2020-01-01 15:10:10",
        "2020-02-01 9:9:9",
        "2022-03-01 15:10:01.139",
        "2022-04-01 15:10:02.020409",
        "2022-05-01 15:10:03.003030",
        "2022-06-01 15:10:05.009900",
    ]

    def __init__(self, max):
        self.max = max

    def __iter__(self):
        self.prev = datetime.datetime(2000, 1, 1, 0, 0, 0, 0)
        self.i = 0
        return self

    def __len__(self):
        return self.max

    def __next__(self) -> str:
        if self.i < len(self.MANUAL_FAKES):
            fake = self.MANUAL_FAKES[self.i]
            self.i += 1
            return fake
        elif self.i < self.max:
            self.prev = self.prev + datetime.timedelta(seconds=3, microseconds=571)
            self.i += 1
            return str(self.prev)
        else:
            raise StopIteration


class IntFaker:
    MANUAL_FAKES = [127, -3, -9, 37, 15, 127]

    def __init__(self, max):
        self.max = max

    def __iter__(self):
        self.prev = -128
        self.i = 0
        return self

    def __len__(self):
        return self.max

    def __next__(self) -> int:
        if self.i < len(self.MANUAL_FAKES):
            fake = self.MANUAL_FAKES[self.i]
            self.i += 1
            return fake
        elif self.i < self.max:
            self.prev += 1
            self.i += 1
            return self.prev
        else:
            raise StopIteration


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

    def __init__(self, max):
        self.max = max

    def __iter__(self):
        self.prev = -10.0001
        self.i = 0
        return self

    def __len__(self):
        return self.max

    def __next__(self) -> float:
        if self.i < len(self.MANUAL_FAKES):
            fake = self.MANUAL_FAKES[self.i]
            self.i += 1
            return fake
        elif self.i < self.max:
            self.prev += 0.00571
            self.i += 1
            return self.prev
        else:
            raise StopIteration


TYPE_SAMPLES = {
    "int": IntFaker(N_SAMPLES),
    "datetime_no_timezone": DateTimeFaker(N_SAMPLES),
    "float": FloatFaker(N_SAMPLES),
}

# This adds _benchmark after the test name so we can easily run them with -k
# benchmark.
BENCHMARK_TESTS = [
    # "test_types_postgres_int_to_postgres_int",
    # "test_types_mysql_int_to_mysql_int",
    # "test_types_postgres_int_to_mysql_int",
    # "test_types_postgres_timestamp6_no_tz_to_mysql_timestamp",
    # "test_types_postgres_timestamp6_no_tz_to_snowflake_timestamp9",
    # "test_types_postgres_int_to_presto_int",
    # "test_types_postgres_int_to_redshift_int",
    # "test_types_postgres_int_to_snowflake_int",
    # "test_types_postgres_int_to_bigquery_int",
    "test_types_snowflake_int_to_snowflake_int",
]

DATABASE_TYPES = {
    db.PostgreSQL: {
        # https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-INT
        "int": [
            # "smallint",  # 2 bytes
            # "int",  # 4 bytes
            # "bigint",  # 8 bytes
        ],
        # https://www.postgresql.org/docs/current/datatype-datetime.html
        "datetime_no_timezone": [
            "timestamp(6) without time zone",
            # "timestamp(3) without time zone",
            # "timestamp(0) without time zone",
        ],
        # https://www.postgresql.org/docs/current/datatype-numeric.html
        "float": [
            # "real",
            # "float",
            # "double precision",
            # "numeric(6,3)",
        ],
    },
    db.MySQL: {
        # https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
        "int": [
            # "tinyint",  # 1 byte
            # "smallint",  # 2 bytes
            # "mediumint",  # 3 bytes
            # "int",  # 4 bytes
            # "bigint",  # 8 bytes
        ],
        # https://dev.mysql.com/doc/refman/8.0/en/datetime.html
        "datetime_no_timezone": [
            # "timestamp(6)",
            # "timestamp(3)",
            # "timestamp(0)",
            "timestamp",
            # "datetime(6)",
        ],
        # https://dev.mysql.com/doc/refman/8.0/en/numeric-types.html
        "float": [
            # "float",
            # "double",
            # "numeric",
            # "numeric(65, 10)",
        ],
    },
    db.BigQuery: {
        "int": [
            # "int",
        ],
        "datetime_no_timezone": [
            "timestamp",
            # "datetime",
        ],
        "float": [
            # "numeric",
            # "float64",
            # "bignumeric",
        ],
    },
    db.Snowflake: {
        # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#int-integer-bigint-smallint-tinyint-byteint
        "int": [
            # all 38 digits with 0 precision, don't need to test all
            # "int",
            # "integer",
            # "bigint",
            # "smallint",
            # "tinyint",
            # "byteint"
        ],
        # https://docs.snowflake.com/en/sql-reference/data-types-datetime.html
        "datetime_no_timezone": [
            # "timestamp(0)",
            # "timestamp(3)",
            # "timestamp(6)",
            "timestamp(9)",
        ],
        # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#decimal-numeric
        "float": [
            # "float",
            # "numeric",
        ],
    },
    db.Redshift: {
        "int": [
            # "int",
        ],
        "datetime_no_timezone": [
            "TIMESTAMP",
        ],
        # https://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html#r_Numeric_types201-floating-point-types
        "float": [
            # "float4",
            # "float8",
            # "numeric",
        ],
    },
    db.Oracle: {
        "int": [
            # "int",
        ],
        "datetime_no_timezone": [
            "timestamp with local time zone",
            # "timestamp(6) with local time zone",
            # "timestamp(9) with local time zone",
        ],
        "float": [
            # "float",
            # "numeric",
        ],
    },
    db.Presto: {
        "int": [
            # "tinyint", # 1 byte
            # "smallint", # 2 bytes
            # "mediumint", # 3 bytes
            # "int",  # 4 bytes
            # "bigint",  # 8 bytes
        ],
        "datetime_no_timezone": ["timestamp"],
        "float": [
            # "real",
            # "double",
            # "decimal(10,2)",
            # "decimal(30,6)",
        ],
    },
}


def human_format(n):
    millnames = ["", "K", "M", "B"]
    n = float(n)
    millidx = max(
        0,
        min(
            len(millnames) - 1, int(math.floor(0 if n == 0 else math.log10(abs(n)) / 3))
        ),
    )

    return "{:.0f}{}".format(n / 10 ** (3 * millidx), millnames[millidx])


type_pairs = []
for source_db, source_type_categories in DATABASE_TYPES.items():
    for target_db, target_type_categories in DATABASE_TYPES.items():
        for (
            type_category,
            source_types,
        ) in source_type_categories.items():  # int, datetime, ..
            for source_type in source_types:
                for target_type in target_type_categories[type_category]:
                    if CONN_STRINGS.get(source_db, False) and CONN_STRINGS.get(
                        target_db, False
                    ):
                        type_pairs.append(
                            (
                                source_db,
                                target_db,
                                source_type,
                                target_type,
                                type_category,
                            )
                        )

# timestamp(9)
def sanitize(name):
    name = name.lower()
    name = re.sub(r"[\(\)]", "", name)  #  timestamp(9) -> timestamp9
    name = name.replace(r"without time zone", "no_tz")  #  too long for some DBs
    return parameterized.to_safe_name(name)


# Pass --verbose to test run to get a nice output.
def expand_params(testcase_func, param_num, param):
    source_db, target_db, source_type, target_type, type_category = param.args
    source_db_type = source_db.__name__
    target_db_type = target_db.__name__
    name = "%s_%s_%s_to_%s_%s" % (
        testcase_func.__name__,
        sanitize(source_db_type),
        sanitize(source_type),
        sanitize(target_db_type),
        sanitize(target_type),
    )

    if name in BENCHMARK_TESTS:
        name += "_benchmark"

    return name


def _drop_table_if_exists(conn, table):
    try:
        if isinstance(conn, db.Oracle):
            conn.query(f"DROP TABLE {table}", None)
        else:
            conn.query(f"DROP TABLE IF EXISTS {table}", None)
    except BaseException as err:
        # Oracle's error for not existing
        if str(err).startswith("ORA-00942"):
            pass
        else:
            raise (err)


class TestDiffCrossDatabaseTables(unittest.TestCase):
    # https://docs.python.org/2/library/unittest.html#unittest.TestCase.maxDiff
    # For showing assertEqual differences under a certain length.
    maxDiff = 10000

    def tearDown(self):
        self.src_conn.close()
        self.dst_conn.close()
        self.src_thread_pool.close()
        self.dst_thread_pool.close()

    @parameterized.expand(type_pairs, name_func=expand_params)
    def test_types(self, source_db, target_db, source_type, target_type, type_category):
        # TODO: Rename to DDL conn. We use a single thread here to avoid the
        # thread pool giving us a new thread for some reason, where the changes
        # might not be applied to.
        self.src_conn = src_conn = db.connect_to_uri(CONN_STRINGS[source_db], 1)
        self.dst_conn = dst_conn = db.connect_to_uri(CONN_STRINGS[target_db], 1)

        if source_db == db.MySQL:
            src_conn.query("SET @@session.time_zone='+00:00'", None)
        if target_db == db.MySQL:
            dst_conn.query("SET @@session.time_zone='+00:00'", None)

        sample_values = TYPE_SAMPLES[type_category]

        # Limit in MySQL is 64
        # src_table_name = f"src_{self._testMethodName[:60]}"
        # dst_table_name = f"dst_{self._testMethodName[:60]}"

        # We need to include the database name because of e.g. Presto which has
        # Postgres as a backing catalog and shouldn't re-use those.
        src_table_name = f"src_{source_db.__name__.lower()}_{sanitize(source_type)}_{human_format(N_SAMPLES)}"
        src_table_path = src_conn.parse_table_name(src_table_name)
        src_table = src_conn.quote(".".join(src_table_path))

        dst_table_name = (
            f"dst_{target_db.__name__.lower()}_{sanitize(target_type)}_{src_table_name}"
        )

        if len(dst_table_name) > 64:  #  length limits, cut from start
            dst_table_name = dst_table_name[len(dst_table_name) - 60 :]
        # Since we insert `src` to `dst`, the `dst` can be different for the
        # same type. 😭
        dst_table_path = dst_conn.parse_table_name(dst_table_name)
        dst_table = dst_conn.quote(".".join(dst_table_path))

        # For Benchmark we might be working with millions of rows; let's not
        # recreate them.
        if not BENCHMARK:
            _drop_table_if_exists(src_conn, src_table)
            _drop_table_if_exists(dst_conn, dst_table)

        # 990785
        start = time.time()
        already_seeded = self._create_table(src_conn, src_table, source_type)
        if not already_seeded:
            self._insert_to_table(
                src_conn, src_table, source_type, enumerate(sample_values, 1)
            )
        insertion_source_duration = time.time() - start

        start = time.time()
        already_seeded = self._create_table(dst_conn, dst_table, target_type)
        if not already_seeded:
            values_in_source = PaginatedTable(src_table, src_conn)
            self._insert_to_table(dst_conn, dst_table, target_type, values_in_source)
        insertion_target_duration = time.time() - start

        self.src_thread_pool = src_thread_pool = db.connect_to_uri(
            CONN_STRINGS[source_db], 8
        )
        self.dst_thread_pool = dst_thread_pool = db.connect_to_uri(
            CONN_STRINGS[target_db], 8
        )

        self.table = TableSegment(
            src_thread_pool, src_table_path, "id", None, ("col",), case_sensitive=False
        )
        self.table2 = TableSegment(
            dst_thread_pool, dst_table_path, "id", None, ("col",), case_sensitive=False
        )

        start = time.time()
        self.assertEqual(N_SAMPLES, self.table.count())
        count1_duration = time.time() - start

        start = time.time()
        self.assertEqual(N_SAMPLES, self.table2.count())
        count2_duration = time.time() - start

        # For large sample sizes (e.g. for benchmarks) we set the batch to ~10k,
        # and try to keep the checksummed batches to a minimum of 250k records
        # each, to minimize round-trips while also trying to pump some
        # concurrency.
        #
        # For unit tests, we keep the values low to ensure we actually use
        # checksums even for small tests.
        ch_threshold = 10_000 if N_SAMPLES > DEFAULT_N_SAMPLES else 3
        ch_factor = (
            min(max(int(N_SAMPLES / 250_000), 2), 128)
            if N_SAMPLES > DEFAULT_N_SAMPLES
            else 2
        )
        ch_threads = 16

        differ = TableDiffer(
            bisection_threshold=ch_threshold,
            bisection_factor=ch_factor,
            max_threadpool_size=ch_threads,
        )
        start = time.time()
        diff = list(differ.diff_tables(self.table, self.table2))
        checksum_duration = time.time() - start
        expected = []
        self.assertEqual(expected, diff)
        self.assertEqual(0, differ.stats.get("rows_downloaded", 0))

        start = time.time()

        # Here, we force-download.
        #
        # By default, we set the threshold above the samples. If we have more
        # samples than the default, we'll split it into a bunch of segments of
        # ~100K records to pull at a time. This ensure it's threaded in a
        # benchmarking context where BENCHMARK is set.
        dl_factor = (
            max(int(N_SAMPLES / 100_000), 2) if N_SAMPLES > DEFAULT_N_SAMPLES else 2
        )
        dl_threshold = (
            int(N_SAMPLES / dl_factor) + 1
            if N_SAMPLES > DEFAULT_N_SAMPLES
            else N_SAMPLES + 1
        )
        dl_threads = 16

        differ = TableDiffer(
            bisection_threshold=dl_threshold,
            bisection_factor=dl_factor,
            max_threadpool_size=dl_threads,
        )
        diff = list(differ.diff_tables(self.table, self.table2))
        expected = []
        self.assertEqual(expected, diff)
        self.assertEqual(len(sample_values), differ.stats.get("rows_downloaded", 0))

        download_duration = time.time() - start

        logging.getLogger("benchmark").debug(
            f"""
            test={self._testMethodName}
            source_db={source_db.__name__}
            target_db={target_db.__name__}
            rows={N_SAMPLES}
            rows_human={human_format(N_SAMPLES)}

            src_table={src_table}
            target_table={dst_table}
            source_type={repr(source_type)}
            target_type={repr(target_type)}

            insertion_source={insertion_source_duration:.2f}s
            insertion_target={insertion_target_duration:.2f}s

            count_source={count1_duration:.3f}s
            count_target={count2_duration:.3f}s
            checksum={checksum_duration:.3f}s
            download={download_duration:.3f}s

            checksum_threads={ch_threads}
            checksum_bisection_factor={ch_factor}
            checksum_bisection_threshold={ch_threshold}

            download_threads={dl_threads}
            download_bisection_factor={dl_factor}
            download_bisection_threshold={dl_threshold}
            """
        )

        if BENCHMARK:
            with open("benchmark.csv", "a") as file:
                file.write(
                    f"{self._testMethodName}, {source_db.__name__} -> {target_db.__name__}, {N_SAMPLES}, {max(count1_duration, count2_duration):.3f}, {checksum_duration:.3f}, {download_duration:.3f}, {source_type}, {target_type}\n"
                )

    def _create_table(self, conn, table, type) -> bool:
        if isinstance(conn, db.Oracle):
            already_exists = (
                conn.query(
                    f"select count(*) from tab where tname='{table.upper()}'", int
                )
                > 0
            )
            if not already_exists:
                conn.query(f"CREATE TABLE {table}(id int, col {type})", None)
        else:
            conn.query(f"CREATE TABLE IF NOT EXISTS {table}(id int, col {type})", None)

        idx_name = f"idx_{table[1:-1]}"

        if isinstance(conn, db.MySQL) or isinstance(conn, db.Oracle):
            max_suffix = len("_id_col")
            if len(idx_name) + max_suffix > 64:  #  length limits, cut from start
                idx_name = idx_name[65 - max_suffix - len(idx_name)]

            try:
                conn.query(
                    f"CREATE UNIQUE INDEX {idx_name}_id_col ON {table}(id, col)",
                    None,
                )
                conn.query(
                    f"CREATE UNIQUE INDEX {idx_name}_id ON {table}(id)",
                    None,
                )
            except Exception as err:
                if "Duplicate key name" in str(err):  #  mysql
                    pass
                elif "such column list already indexed" in str(err):  #  oracle
                    pass
                elif "name is already used" in str(err):  #  oracle
                    pass
                else:
                    raise (err)
        elif (
            not isinstance(conn, db.Snowflake)
            and not isinstance(conn, db.Presto)
            and not isinstance(conn, db.Redshift)
            and not isinstance(conn, db.BigQuery)
        ):
            conn.query(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {idx_name}_id_col ON {table} (id, col)",
                None,
            )
            conn.query(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {idx_name}_id ON {table} (id)",
                None,
            )

        # print(conn.query("SHOW TABLES FROM public", list))
        # print(conn.query("SELECT * FROM INFORMATION_SCHEMA.TABLES", list))
        existing_count = conn.query(f"SELECT COUNT(*) FROM {table}", int)

        # Ensure it's clean if it was partially instantiated.
        # This should only be relevant for BENCHMARK.
        if existing_count != N_SAMPLES and existing_count != 0:
            _drop_table_if_exists(conn, table)
            return self._create_table(conn, table, type)

        if not isinstance(conn, db.BigQuery):
            conn.query("COMMIT", None)

        return existing_count == N_SAMPLES

    def _insert_to_table(self, conn, table, col_type, values):
        default_insertion_query = f"INSERT INTO {table} (id, col) VALUES "
        if isinstance(conn, db.Oracle):
            default_insertion_query = f"INSERT INTO {table} (id, col)"

        insertion_query = default_insertion_query

        if BENCHMARK:
            description = f"{type(conn).__name__}: {table}"
            values = rich.progress.track(
                values, total=N_SAMPLES, description=description
            )

        selects = []
        with open("_tmp.csv", "w+", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["id", "col"])
            for id, sample in values:
                if (
                    isinstance(conn, db.Presto) or isinstance(conn, db.Oracle)
                ) and col_type.startswith("timestamp"):
                    sample = f"timestamp '{sample}'"  #  must be cast...
                elif isinstance(sample, int) or isinstance(sample, float):
                    pass  #  don't make string, some dbs need them to be raw
                elif col_type == "timestamp" and isinstance(conn, db.BigQuery):
                    pass
                else:
                    sample = f"'{sample}'"

                # TODO: create one per test; so we can parallelize
                if isinstance(conn, db.Oracle):
                    selects.append(f"SELECT {id}, {sample} FROM dual")
                else:
                    insertion_query += f"({id}, {sample}),"

                #  snowflake has some annoying limitations here
                if id % 8000 == 0 and not isinstance(conn, db.BigQuery):
                    if isinstance(conn, db.Oracle):
                        insertion_query += " UNION ALL ".join(selects)
                        conn.query(insertion_query, None)
                        selects = []
                    else:
                        conn.query(insertion_query[0:-1], None)

                    insertion_query = default_insertion_query
                else:
                    writer.writerow([id, sample])

        if not isinstance(conn, db.BigQuery):
            if (
                insertion_query != default_insertion_query
            ):  #   didn't end at a clean divisor
                conn.query(insertion_query[0:-1], None)
                conn.query("COMMIT", None)
            elif isinstance(conn, db.Oracle) and len(selects) > 0:
                insertion_query += " UNION ALL ".join(selects)
                conn.query(insertion_query, None)
                conn.query("COMMIT", None)
        else:
            client = conn._client
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
            )
            with open("_tmp.csv", "rb") as source_file:
                job = client.load_table_from_file(
                    source_file, table[1:-1], job_config=job_config
                )
            job.result()
