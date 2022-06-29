from contextlib import suppress
import unittest
import time
import re
import math
from datetime import datetime, timedelta
from decimal import Decimal
from parameterized import parameterized

from data_diff import databases as db
from data_diff.diff_tables import TableDiffer, TableSegment
from .common import CONN_STRINGS, N_SAMPLES


CONNS = {k: db.connect_to_uri(v, 1) for k, v in CONN_STRINGS.items()}

CONNS[db.MySQL].query("SET @@session.time_zone='+00:00'", None)


class PaginatedTable:
    # We can't query all the rows at once for large tables. It'll occupy too
    # much memory.
    RECORDS_PER_BATCH = 1000000

    def __init__(self, table, conn):
        self.table = table
        self.conn = conn

    def __iter__(self):
        iter = PaginatedTable(self.table, self.conn)
        iter.last_id = 0
        iter.values = []
        iter.value_index = 0
        return iter

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
        datetime.fromisoformat("2020-01-01 15:10:10"),
        datetime.fromisoformat("2020-02-01 09:09:09"),
        datetime.fromisoformat("2022-03-01 15:10:01.139"),
        datetime.fromisoformat("2022-04-01 15:10:02.020409"),
        datetime.fromisoformat("2022-05-01 15:10:03.003030"),
        datetime.fromisoformat("2022-06-01 15:10:05.009900"),
    ]

    def __init__(self, max):
        self.max = max

    def __iter__(self):
        iter = DateTimeFaker(self.max)
        iter.prev = datetime(2000, 1, 1, 0, 0, 0, 0)
        iter.i = 0
        return iter

    def __len__(self):
        return self.max

    def __next__(self) -> datetime:
        if self.i < len(self.MANUAL_FAKES):
            fake = self.MANUAL_FAKES[self.i]
            self.i += 1
            return fake
        elif self.i < self.max:
            self.prev = self.prev + timedelta(seconds=3, microseconds=571)
            self.i += 1
            return self.prev
        else:
            raise StopIteration


class IntFaker:
    MANUAL_FAKES = [127, -3, -9, 37, 15, 127]

    def __init__(self, max):
        self.max = max

    def __iter__(self):
        iter = IntFaker(self.max)
        iter.prev = -128
        iter.i = 0
        return iter

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
        iter = FloatFaker(self.max)
        iter.prev = -10.0001
        iter.i = 0
        return iter

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

DATABASE_TYPES = {
    db.PostgreSQL: {
        # https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-INT
        "int": [
            # "smallint",  # 2 bytes
            "int",  # 4 bytes
            "bigint", # 8 bytes
        ],
        # https://www.postgresql.org/docs/current/datatype-datetime.html
        "datetime_no_timezone": [
            "timestamp(6) without time zone",
            "timestamp(3) without time zone",
            "timestamp(0) without time zone",
        ],
        # https://www.postgresql.org/docs/current/datatype-numeric.html
        "float": [
            "real",
            "float",
            "double precision",
            "numeric(6,3)",
        ],
    },
    db.MySQL: {
        # https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
        "int": [
            # "tinyint", # 1 byte
            # "smallint", # 2 bytes
            # "mediumint", # 3 bytes
            "int",  # 4 bytes
            "bigint", # 8 bytes
        ],
        # https://dev.mysql.com/doc/refman/8.0/en/datetime.html
        "datetime_no_timezone": [
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
    },
    db.BigQuery: {
        "int": ["int"],
        "datetime_no_timezone": [
            "timestamp",
            # "datetime",
        ],
        "float": [
            "numeric",
            "float64",
            "bignumeric",
        ],
    },
    db.Snowflake: {
        # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#int-integer-bigint-smallint-tinyint-byteint
        "int": [
            # all 38 digits with 0 precision, don't need to test all
            "int",
            "integer",
            "bigint",
            # "smallint",
            # "tinyint",
            # "byteint"
        ],
        # https://docs.snowflake.com/en/sql-reference/data-types-datetime.html
        "datetime_no_timezone": [
            "timestamp(0)",
            "timestamp(3)",
            "timestamp(6)",
            "timestamp(9)",
        ],
        # https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#decimal-numeric
        "float": [
            "float",
            "numeric",
        ],
    },
    db.Redshift: {
        "int": [
            "int",
        ],
        "datetime_no_timezone": [
            "TIMESTAMP",
        ],
        # https://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html#r_Numeric_types201-floating-point-types
        "float": [
            "float4",
            "float8",
            "numeric",
        ],
    },
    db.Oracle: {
        "int": [
            "int",
        ],
        "datetime_no_timezone": [
            "timestamp with local time zone",
            "timestamp(6) with local time zone",
            "timestamp(9) with local time zone",
        ],
        "float": [
            "float",
            "numeric",
        ],
    },
    db.Presto: {
        "int": [
            # "tinyint", # 1 byte
            # "smallint", # 2 bytes
            # "mediumint", # 3 bytes
            "int",  # 4 bytes
            "bigint", # 8 bytes
        ],
        "datetime_no_timezone": [
            "timestamp",
            "timestamp with time zone",
        ],
        "float": [
            "real",
            "double",
            "decimal(10,2)",
            "decimal(30,6)",
        ],
    },
}


type_pairs = []
for source_db, source_type_categories in DATABASE_TYPES.items():
    for target_db, target_type_categories in DATABASE_TYPES.items():
        for (
            type_category,
            source_types,
        ) in source_type_categories.items():  # int, datetime, ..
            for source_type in source_types:
                for target_type in target_type_categories[type_category]:
                    if CONNS.get(source_db, False) and CONNS.get(target_db, False):
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
    return parameterized.to_safe_name(name)


def number_to_human(n):
    millnames = ["", "k", "m", "b"]
    n = float(n)
    millidx = max(
        0,
        min(len(millnames) - 1, int(math.floor(0 if n == 0 else math.log10(abs(n)) / 3))),
    )

    return "{:.0f}{}".format(n / 10 ** (3 * millidx), millnames[millidx])


# Pass --verbose to test run to get a nice output.
def expand_params(testcase_func, param_num, param):
    source_db, target_db, source_type, target_type, type_category = param.args
    source_db_type = source_db.__name__
    target_db_type = target_db.__name__
    name = "%s_%s_%s_to_%s_%s_%s" % (
        testcase_func.__name__,
        sanitize(source_db_type),
        sanitize(source_type),
        sanitize(target_db_type),
        sanitize(target_type),
        number_to_human(N_SAMPLES),
    )
    return name


def _insert_to_table(conn, table, values):
    insertion_query = f"INSERT INTO {table} (id, col) "

    if isinstance(conn, db.Oracle):
        selects = []
        for j, sample in values:
            if isinstance(sample, (float, Decimal, int)):
                value = str(sample)
            else:
                value = f"timestamp '{sample}'"
            selects.append(f"SELECT {j}, {value} FROM dual")
        insertion_query += " UNION ALL ".join(selects)
    else:
        insertion_query += " VALUES "
        for j, sample in values:
            if isinstance(sample, (float, Decimal, int)):
                value = str(sample)
            elif isinstance(sample, datetime) and isinstance(conn, db.Presto):
                value = f"timestamp '{sample}'"
            else:
                value = f"'{sample}'"
            insertion_query += f"({j}, {value}),"

        insertion_query = insertion_query[0:-1]

    conn.query(insertion_query, None)

    if not isinstance(conn, db.BigQuery):
        conn.query("COMMIT", None)


def _drop_table_if_exists(conn, table):
    with suppress(db.QueryError):
        if isinstance(conn, db.Oracle):
            conn.query(f"DROP TABLE {table}", None)
            conn.query(f"DROP TABLE {table}", None)
        else:
            conn.query(f"DROP TABLE IF EXISTS {table}", None)
            conn.query("COMMIT", None)


class TestDiffCrossDatabaseTables(unittest.TestCase):
    @parameterized.expand(type_pairs, name_func=expand_params)
    def test_types(self, source_db, target_db, source_type, target_type, type_category):
        start = time.time()

        self.src_conn = src_conn = CONNS[source_db]
        self.dst_conn = dst_conn = CONNS[target_db]

        self.connections = [self.src_conn, self.dst_conn]
        sample_values = TYPE_SAMPLES[type_category]

        # Limit in MySQL is 64, Presto seems to be 63
        src_table_name = f"src_{self._testMethodName[11:]}"
        dst_table_name = f"dst_{self._testMethodName[11:]}"

        src_table_path = src_conn.parse_table_name(src_table_name)
        dst_table_path = dst_conn.parse_table_name(dst_table_name)
        src_table = src_conn.quote(".".join(src_table_path))
        dst_table = dst_conn.quote(".".join(dst_table_path))

        _drop_table_if_exists(src_conn, src_table)
        src_conn.query(f"CREATE TABLE {src_table}(id int, col {source_type})", None)
        _insert_to_table(src_conn, src_table, enumerate(sample_values, 1))

        values_in_source = PaginatedTable(src_table, src_conn)
        if source_db is db.Presto:
            if source_type.startswith("decimal"):
                values_in_source = [(a, Decimal(b)) for a, b in values_in_source]
            elif source_type.startswith("timestamp"):
                values_in_source = [(a, datetime.fromisoformat(b.rstrip(" UTC"))) for a, b in values_in_source]

        _drop_table_if_exists(dst_conn, dst_table)
        dst_conn.query(f"CREATE TABLE {dst_table}(id int, col {target_type})", None)
        _insert_to_table(dst_conn, dst_table, values_in_source)

        self.table = TableSegment(self.src_conn, src_table_path, "id", None, ("col",), case_sensitive=False)
        self.table2 = TableSegment(self.dst_conn, dst_table_path, "id", None, ("col",), case_sensitive=False)

        self.assertEqual(len(sample_values), self.table.count())
        self.assertEqual(len(sample_values), self.table2.count())

        differ = TableDiffer(bisection_threshold=3, bisection_factor=2)  # ensure we actually checksum
        diff = list(differ.diff_tables(self.table, self.table2))
        expected = []
        self.assertEqual(expected, diff)
        self.assertEqual(0, differ.stats.get("rows_downloaded", 0))

        # Ensure that Python agrees with the checksum!
        differ = TableDiffer(bisection_threshold=1000000000)
        diff = list(differ.diff_tables(self.table, self.table2))
        expected = []
        self.assertEqual(expected, diff)
        self.assertEqual(len(sample_values), differ.stats.get("rows_downloaded", 0))

        duration = time.time() - start
        # print(f"source_db={source_db.__name__} target_db={target_db.__name__} source_type={source_type} target_type={target_type} duration={round(duration * 1000, 2)}ms")
