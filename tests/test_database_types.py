from contextlib import suppress
import unittest
import time
import logging
from decimal import Decimal

from parameterized import parameterized, parameterized_class
import preql

from data_diff import database as db
from data_diff.diff_tables import TableDiffer, TableSegment
from parameterized import parameterized, parameterized_class
from .common import CONN_STRINGS
import logging


logging.getLogger("diff_tables").setLevel(logging.ERROR)
logging.getLogger("database").setLevel(logging.WARN)

CONNS = {k: db.connect_to_uri(v) for k, v in CONN_STRINGS.items()}

CONNS[db.MySQL].query("SET @@session.time_zone='+00:00'", None)

TYPE_SAMPLES = {
    "int": [127, -3, -9, 37, 15, 127],
    "datetime_no_timezone": [
        "2020-01-01 15:10:10",
        "2020-02-01 9:9:9",
        "2022-03-01 15:10:01.139",
        "2022-04-01 15:10:02.020409",
        "2022-05-01 15:10:03.003030",
        "2022-06-01 15:10:05.009900",
    ],
    "float": [
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
    ],
}

DATABASE_TYPES = {
    db.PostgreSQL: {
        # https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-INT
        "int": [
            # "smallint",  # 2 bytes
            # "int", # 4 bytes
            # "bigint", # 8 bytes
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
            # "int", # 4 bytes
            # "bigint", # 8 bytes
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
            # "int",
            # "integer",
            # "bigint",
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
            # "int",
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
            # "int",
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
            # "int", # 4 bytes
            # "bigint", # 8 bytes
        ],
        "datetime_no_timezone": [
            "timestamp(6)",
            "timestamp(3)",
            "timestamp(0)",
            "timestamp",
            "datetime(6)",
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
# =>
# { source: (preql, connection)
# target: (preql, connection)
# source_type: (int, tinyint),
# target_type: (int, bigint) }
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

# Pass --verbose to test run to get a nice output.
def expand_params(testcase_func, param_num, param):
    source_db, target_db, source_type, target_type, type_category = param.args
    source_db_type = source_db.__name__
    target_db_type = target_db.__name__
    return "%s_%s_%s_to_%s_%s" % (
        testcase_func.__name__,
        source_db_type,
        parameterized.to_safe_name(source_type),
        target_db_type,
        parameterized.to_safe_name(target_type),
    )


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
            if isinstance(sample, (float, Decimal)):
                value = str(sample)
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


class TestDiffCrossDatabaseTables(unittest.TestCase):
    @parameterized.expand(type_pairs, name_func=expand_params)
    def test_types(self, source_db, target_db, source_type, target_type, type_category):
        start = time.time()

        self.src_conn = src_conn = CONNS[source_db]
        self.dst_conn = dst_conn = CONNS[target_db]

        self.connections = [self.src_conn, self.dst_conn]
        sample_values = TYPE_SAMPLES[type_category]

        # Limit in MySQL is 64
        src_table_name = f"src_{self._testMethodName[:60]}"
        dst_table_name = f"dst_{self._testMethodName[:60]}"

        src_table_path = src_conn.parse_table_name(src_table_name)
        dst_table_path = dst_conn.parse_table_name(dst_table_name)
        src_table = src_conn.quote(".".join(src_table_path))
        dst_table = dst_conn.quote(".".join(dst_table_path))

        _drop_table_if_exists(src_conn, src_table)
        src_conn.query(f"CREATE TABLE {src_table}(id int, col {source_type})", None)
        _insert_to_table(src_conn, src_table, enumerate(sample_values, 1))

        values_in_source = src_conn.query(f"SELECT id, col FROM {src_table}", list)

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
