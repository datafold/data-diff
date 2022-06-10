import unittest
import preql
import time
from data_diff import database as db
from data_diff.diff_tables import TableDiffer, TableSegment, split_space
from parameterized import parameterized, parameterized_class
from .common import CONN_STRINGS, str_to_checksum
import logging

logging.getLogger("diff_tables").setLevel(logging.WARN)
logging.getLogger("database").setLevel(logging.WARN)

CONNS = {k: db.connect_to_uri(v) for k, v in CONN_STRINGS.items()}

TYPE_SAMPLES = {
    "int": [127, -3, -9, 37, 15, 127],
    "datetime_no_timezone": [
        "2020-01-01 15:10:10",
        "2020-01-01 9:9:9",
        "2022-01-01 15:10:01.139",
        "2022-01-01 15:10:02.020409",
        "2022-01-01 15:10:03.003030",
        "2022-01-01 15:10:05.009900",
    ],
}

DATABASE_TYPES = {
    db.Postgres: {
        # https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-INT
        "int": [
            # "smallint",  # 2 bytes
            # "int", # 4 bytes
            # "bigint", # 8 bytes
        ],
        # https://www.postgresql.org/docs/current/datatype-datetime.html
        "datetime_no_timezone": [
            "timestamp(6) without time zone",
            "timestamp(0) without time zone",
        ]
    },
    db.MySQL: {
         # https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
        "int": [
            "tinyint", # 1 byte
            "smallint", # 2 bytes
            "mediumint", # 3 bytes
            "int", # 4 bytes
            "bigint", # 8 bytes
        ],
        # https://dev.mysql.com/doc/refman/8.0/en/datetime.html
        "datetime_no_timezone": [
            "timestamp(6)",
            "timestamp(0)",
            "timestamp",
            "datetime(6)"
        ]
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
            "timestamp(6)",
            "timestamp(9)",
        ]
    },
    db.Redshift: {
        "int": [
            # "int",
        ],
        "datetime_no_timezone": [
            "TIMESTAMP",
        ]
    },
    db.Oracle: {
        "int": [
            # "int",
        ],
        "datetime_no_timezone": [
            "timestamp",
            "timestamp(6)",
            "timestamp(9)",
        ]
    }
}


type_pairs = []
# =>
# { source: (preql, connection)
# target: (preql, connection)
# source_type: (int, tinyint),
# target_type: (int, bigint) }
for source_db, source_type_categories in DATABASE_TYPES.items():
    for target_db, target_type_categories in DATABASE_TYPES.items():
        for type_category, source_types in source_type_categories.items(): # int, datetime, ..
            for source_type in source_types:
                for target_type in target_type_categories[type_category]:
                    if CONNS.get(source_db, False) and CONNS.get(target_db, False):
                        type_pairs.append((
                          source_db,
                          target_db,
                          source_type,
                          target_type,
                          type_category,
                        ))

# Pass --verbose to test run to get a nice output.
def expand_params(testcase_func, param_num, param):
    source_db, target_db, source_type, target_type, type_category = param.args
    source_db_type = source_db.__name__
    target_db_type = target_db.__name__
    return "%s_%s_%s_to_%s_%s" %(
        testcase_func.__name__,
        source_db_type,
        parameterized.to_safe_name(source_type),
        target_db_type,
        parameterized.to_safe_name(target_type),
    )

class TestDiffCrossDatabaseTables(unittest.TestCase):
    @parameterized.expand(type_pairs, name_func=expand_params)
    def test_types(self, source_db, target_db, source_type, target_type, type_category):
        start = time.time()

        self.connection1 = CONNS[source_db]
        self.connection2 = CONNS[target_db]

        self.connections = [self.connection1, self.connection2]
        sample_values = TYPE_SAMPLES[type_category]

        for i, connection in enumerate(self.connections):
            db_type = type(connection)
            table = "a" if i == 0 else "b"
            col_type = source_type if i == 0 else target_type

            connection.query(f"DROP TABLE IF EXISTS {table}", None)
            connection.query(f"CREATE TABLE {table}(id int, col {col_type});", None)

            insertion_query = f"INSERT INTO {table} (id, col) VALUES "
            for j, sample in enumerate(sample_values):
                insertion_query += f"({j+1}, '{sample}'),"

            connection.query(insertion_query[0:-1], None)
            connection.query("COMMIT", None)

        self.table = TableSegment(self.connection1, ("a",), "id", None, ("col", ))
        self.table2 = TableSegment(self.connection2, ("b",), "id", None, ("col", ))

        self.assertEqual(len(sample_values), self.table.count())
        self.assertEqual(len(sample_values), self.table2.count())

        differ = TableDiffer(bisection_threshold=3, bisection_factor=2) # ensure we actually checksum
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
