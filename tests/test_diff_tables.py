import datetime
import unittest
import uuid

from parameterized import parameterized_class
import preql
import arrow  # comes with preql

from data_diff.databases import connect
from data_diff.diff_tables import TableDiffer, TableSegment, split_space
from data_diff import databases as db
from data_diff.utils import ArithAlphanumeric

from .common import (
    TEST_MYSQL_CONN_STRING,
    str_to_checksum,
    random_table_suffix,
    _drop_table_if_exists,
    CONN_STRINGS,
    N_THREADS,
)

DATABASE_URIS = {k.__name__: v for k, v in CONN_STRINGS.items()}
DATABASE_INSTANCES = {k.__name__: connect(v, N_THREADS) for k, v in CONN_STRINGS.items()}

TEST_DATABASES = {x.__name__ for x in (db.MySQL, db.PostgreSQL, db.Oracle, db.Redshift, db.Snowflake, db.BigQuery)}


_class_per_db_dec = parameterized_class(
    ("name", "db_name"), [(name, name) for name in DATABASE_URIS if name in TEST_DATABASES]
)


def test_per_database(cls):
    return _class_per_db_dec(cls)


def _insert_row(conn, table, fields, values):
    fields = ", ".join(map(str, fields))
    values = ", ".join(map(str, values))
    conn.query(f"INSERT INTO {table}({fields}) VALUES ({values})", None)


def _insert_rows(conn, table, fields, tuple_list):
    for t in tuple_list:
        _insert_row(conn, table, fields, t)


def _commit(conn):
    if not isinstance(conn, db.BigQuery):
        conn.query("COMMIT", None)


def _get_text_type(conn):
    if isinstance(conn, db.BigQuery):
        return "STRING"
    return "varchar(100)"


class TestUtils(unittest.TestCase):
    def test_split_space(self):
        for i in range(0, 10):
            for j in range(1, 16328, 17):
                for n in range(1, 32):
                    r = split_space(i, j + i + n, n)
                    assert len(r) == n, f"split_space({i}, {j+n}, {n}) = {(r)}"


class TestPerDatabase(unittest.TestCase):
    db_name = None
    with_preql = False

    preql = None

    def setUp(self):
        assert self.db_name

        self.connection = DATABASE_INSTANCES[self.db_name]
        if self.with_preql:
            self.preql = preql.Preql(DATABASE_URIS[self.db_name])

        table_suffix = random_table_suffix()
        self.table_src_name = f"src{table_suffix}"
        self.table_dst_name = f"dst{table_suffix}"

        self.table_src_path = self.connection.parse_table_name(self.table_src_name)
        self.table_dst_path = self.connection.parse_table_name(self.table_dst_name)

        self.table_src = ".".join(map(self.connection.quote, self.table_src_path))
        self.table_dst = ".".join(map(self.connection.quote, self.table_dst_path))

        _drop_table_if_exists(self.connection, self.table_src)
        _drop_table_if_exists(self.connection, self.table_dst)

        return super().setUp()

    def tearDown(self):
        if self.preql:
            self.preql._interp.state.db.rollback()
            self.preql.close()

        _drop_table_if_exists(self.connection, self.table_src)
        _drop_table_if_exists(self.connection, self.table_dst)


@test_per_database
class TestDates(TestPerDatabase):
    with_preql = True

    def setUp(self):
        super().setUp()
        self.preql(
            f"""
            table {self.table_src_name} {{
                datetime: timestamp
                text_comment: string
            }}
            commit()

            func add(date, text_comment) {{
                new {self.table_src_name}(date, text_comment)
            }}
        """
        )
        self.now = now = arrow.get(self.preql.now())
        self.preql.add(now.shift(days=-50), "50 days ago")
        self.preql.add(now.shift(hours=-3), "3 hours ago")
        self.preql.add(now.shift(minutes=-10), "10 mins ago")
        self.preql.add(now.shift(seconds=-1), "1 second ago")
        self.preql.add(now, "now")

        self.preql(
            f"""
            const table {self.table_dst_name} = {self.table_src_name}
            commit()
        """
        )

        self.preql.add(self.now.shift(seconds=-3), "2 seconds ago")
        self.preql.commit()

    def test_init(self):
        a = TableSegment(
            self.connection, self.table_src_path, "id", "datetime", max_update=self.now.datetime, case_sensitive=False
        )
        self.assertRaises(
            ValueError, TableSegment, self.connection, self.table_src_path, "id", max_update=self.now.datetime
        )

    def test_basic(self):
        differ = TableDiffer(10, 100)
        a = TableSegment(self.connection, self.table_src_path, "id", "datetime", case_sensitive=False)
        b = TableSegment(self.connection, self.table_dst_path, "id", "datetime", case_sensitive=False)
        assert a.count() == 6
        assert b.count() == 5

        assert not list(differ.diff_tables(a, a))
        self.assertEqual(len(list(differ.diff_tables(a, b))), 1)

    def test_offset(self):
        differ = TableDiffer(2, 10)
        sec1 = self.now.shift(seconds=-1).datetime
        a = TableSegment(self.connection, self.table_src_path, "id", "datetime", max_update=sec1, case_sensitive=False)
        b = TableSegment(self.connection, self.table_dst_path, "id", "datetime", max_update=sec1, case_sensitive=False)
        assert a.count() == 4
        assert b.count() == 3

        assert not list(differ.diff_tables(a, a))
        self.assertEqual(len(list(differ.diff_tables(a, b))), 1)

        a = TableSegment(self.connection, self.table_src_path, "id", "datetime", min_update=sec1, case_sensitive=False)
        b = TableSegment(self.connection, self.table_dst_path, "id", "datetime", min_update=sec1, case_sensitive=False)
        assert a.count() == 2
        assert b.count() == 2
        assert not list(differ.diff_tables(a, b))

        day1 = self.now.shift(days=-1).datetime

        a = TableSegment(
            self.connection,
            self.table_src_path,
            "id",
            "datetime",
            min_update=day1,
            max_update=sec1,
            case_sensitive=False,
        )
        b = TableSegment(
            self.connection,
            self.table_dst_path,
            "id",
            "datetime",
            min_update=day1,
            max_update=sec1,
            case_sensitive=False,
        )
        assert a.count() == 3
        assert b.count() == 2
        assert not list(differ.diff_tables(a, a))
        self.assertEqual(len(list(differ.diff_tables(a, b))), 1)


@test_per_database
class TestDiffTables(TestPerDatabase):
    with_preql = True

    def setUp(self):
        super().setUp()

        self.connection.query(
            f"create table {self.table_src}(id int, userid int, movieid int, rating float, timestamp timestamp)", None
        )
        self.connection.query(
            f"create table {self.table_dst}(id int, userid int, movieid int, rating float, timestamp timestamp)", None
        )
        # self.preql(
        #     f"""
        #     table {self.table_src_name} {{
        #         userid: int
        #         movieid: int
        #         rating: float
        #         timestamp: timestamp
        #     }}

        #     table {self.table_dst_name} {{
        #         userid: int
        #         movieid: int
        #         rating: float
        #         timestamp: timestamp
        #     }}
        #     commit()
        # """
        # )
        self.preql.commit()

        self.table = TableSegment(self.connection, self.table_src_path, "id", "timestamp", case_sensitive=False)
        self.table2 = TableSegment(self.connection, self.table_dst_path, "id", "timestamp", case_sensitive=False)

        self.differ = TableDiffer(3, 4)

    def test_properties_on_empty_table(self):
        table = self.table.with_schema()
        self.assertEqual(0, table.count())
        self.assertEqual(None, table.count_and_checksum()[1])

    def test_get_values(self):
        time = "2022-01-01 00:00:00.000000"
        time_str = f"timestamp '{time}'"

        cols = "id userid movieid rating timestamp".split()
        _insert_row(self.connection, self.table_src, cols, [1, 1, 1, 9, time_str])
        _commit(self.connection)
        id_ = self.connection.query(f"select id from {self.table_src}", int)

        table = self.table.with_schema()

        self.assertEqual(1, table.count())
        concatted = str(id_) + "|" + time
        self.assertEqual(str_to_checksum(concatted), table.count_and_checksum()[1])

    def test_diff_small_tables(self):
        time = "2022-01-01 00:00:00"
        time_str = f"timestamp '{time}'"

        cols = "id userid movieid rating timestamp".split()
        _insert_rows(self.connection, self.table_src, cols, [[1, 1, 1, 9, time_str], [2, 2, 2, 9, time_str]])
        _insert_rows(self.connection, self.table_dst, cols, [[1, 1, 1, 9, time_str]])
        _commit(self.connection)
        diff = list(self.differ.diff_tables(self.table, self.table2))
        expected = [("-", ("2", time + ".000000"))]
        self.assertEqual(expected, diff)
        self.assertEqual(2, self.differ.stats["table1_count"])
        self.assertEqual(1, self.differ.stats["table2_count"])

    def test_diff_table_above_bisection_threshold(self):
        time = "2022-01-01 00:00:00"
        time_str = f"timestamp '{time}'"

        cols = "id userid movieid rating timestamp".split()
        _insert_rows(
            self.connection,
            self.table_src,
            cols,
            [
                [1, 1, 1, 9, time_str],
                [2, 2, 2, 9, time_str],
                [3, 3, 3, 9, time_str],
                [4, 4, 4, 9, time_str],
                [5, 5, 5, 9, time_str],
            ],
        )

        _insert_rows(
            self.connection,
            self.table_dst,
            cols,
            [
                [1, 1, 1, 9, time_str],
                [2, 2, 2, 9, time_str],
                [3, 3, 3, 9, time_str],
                [4, 4, 4, 9, time_str],
            ],
        )
        _commit(self.connection)

        diff = list(self.differ.diff_tables(self.table, self.table2))
        expected = [("-", ("5", time + ".000000"))]
        self.assertEqual(expected, diff)
        self.assertEqual(5, self.differ.stats["table1_count"])
        self.assertEqual(4, self.differ.stats["table2_count"])

    def test_return_empty_array_when_same(self):
        time = "2022-01-01 00:00:00"
        time_str = f"timestamp '{time}'"

        cols = "id userid movieid rating timestamp".split()

        _insert_row(self.connection, self.table_src, cols, [1, 1, 1, 9, time_str])
        _insert_row(self.connection, self.table_dst, cols, [1, 1, 1, 9, time_str])

        self.preql.commit()
        diff = list(self.differ.diff_tables(self.table, self.table2))
        self.assertEqual([], diff)

    def test_diff_sorted_by_key(self):
        time = "2022-01-01 00:00:00"
        time2 = "2021-01-01 00:00:00"

        time_str = f"timestamp '{time}'"
        time_str2 = f"timestamp '{time2}'"

        cols = "id userid movieid rating timestamp".split()

        _insert_rows(
            self.connection,
            self.table_src,
            cols,
            [
                [1, 1, 1, 9, time_str],
                [2, 2, 2, 9, time_str2],
                [3, 3, 3, 9, time_str],
                [4, 4, 4, 9, time_str2],
                [5, 5, 5, 9, time_str],
            ],
        )

        _insert_rows(
            self.connection,
            self.table_dst,
            cols,
            [
                [1, 1, 1, 9, time_str],
                [2, 2, 2, 9, time_str],
                [3, 3, 3, 9, time_str],
                [4, 4, 4, 9, time_str],
                [5, 5, 5, 9, time_str],
            ],
        )
        _commit(self.connection)

        differ = TableDiffer()
        diff = list(differ.diff_tables(self.table, self.table2))
        expected = [
            ("-", ("2", time2 + ".000000")),
            ("+", ("2", time + ".000000")),
            ("-", ("4", time2 + ".000000")),
            ("+", ("4", time + ".000000")),
        ]
        self.assertEqual(expected, diff)


@test_per_database
class TestUUIDs(TestPerDatabase):
    def setUp(self):
        super().setUp()

        queries = [
            f"CREATE TABLE {self.table_src}(id varchar(100), text_comment varchar(1000))",
        ]
        for i in range(100):
            queries.append(f"INSERT INTO {self.table_src} VALUES ('{uuid.uuid1(i)}', '{i}')")

        queries += [
            f"CREATE TABLE {self.table_dst} AS SELECT * FROM {self.table_src}",
        ]

        self.new_uuid = uuid.uuid1(32132131)
        queries.append(f"INSERT INTO {self.table_src} VALUES ('{self.new_uuid}', 'This one is different')")

        # TODO test unexpected values?

        for query in queries:
            self.connection.query(query, None)

        _commit(self.connection)

        self.a = TableSegment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = TableSegment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

    def test_string_keys(self):
        differ = TableDiffer()
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.new_uuid), "This one is different"))])

        self.connection.query(
            f"INSERT INTO {self.table_src} VALUES ('unexpected', '<-- this bad value should not break us')", None
        )

        self.assertRaises(ValueError, list, differ.diff_tables(self.a, self.b))


@test_per_database
class TestAlphanumericKeys(TestPerDatabase):
    def setUp(self):
        super().setUp()

        queries = [
            f"CREATE TABLE {self.table_src}(id varchar(100), text_comment varchar(1000))",
        ]
        for i in range(0, 10000, 1000):
            queries.append(f"INSERT INTO {self.table_src} VALUES ('{ArithAlphanumeric(int=i, max_len=10)}', '{i}')")

        queries += [
            f"CREATE TABLE {self.table_dst} AS SELECT * FROM {self.table_src}",
        ]

        self.new_alphanum = "aBcDeFgHiJ"
        queries.append(f"INSERT INTO {self.table_src} VALUES ('{self.new_alphanum}', 'This one is different')")

        # TODO test unexpected values?

        for query in queries:
            self.connection.query(query, None)

        _commit(self.connection)

        self.a = TableSegment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = TableSegment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

    def test_alphanum_keys(self):
        # Test the class itself
        assert str(ArithAlphanumeric(int=0, max_len=1)) == "0"
        assert str(ArithAlphanumeric(int=0, max_len=10)) == "0" * 10
        assert str(ArithAlphanumeric(int=1, max_len=10)) == "0" * 9 + "1"

        # Test in the differ

        differ = TableDiffer()
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.new_alphanum), "This one is different"))])

        self.connection.query(
            f"INSERT INTO {self.table_src} VALUES ('@@@', '<-- this bad value should not break us')", None
        )
        _commit(self.connection)

        self.a = TableSegment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = TableSegment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

        self.assertRaises(NotImplementedError, list, differ.diff_tables(self.a, self.b))


@test_per_database
class TestTableSegment(TestPerDatabase):
    def setUp(self) -> None:
        super().setUp()
        self.table = TableSegment(self.connection, self.table_src_path, "id", "timestamp", case_sensitive=False)
        self.table2 = TableSegment(self.connection, self.table_dst_path, "id", "timestamp", case_sensitive=False)

    def test_table_segment(self):
        early = datetime.datetime(2021, 1, 1, 0, 0)
        late = datetime.datetime(2022, 1, 1, 0, 0)
        self.assertRaises(ValueError, self.table.replace, min_update=late, max_update=early)

        self.assertRaises(ValueError, self.table.replace, min_key=10, max_key=0)


@test_per_database
class TestTableUUID(TestPerDatabase):
    def setUp(self):
        super().setUp()

        queries = [
            f"CREATE TABLE {self.table_src}(id varchar(100), text_comment varchar(1000))",
        ]
        for i in range(10):
            uuid_value = uuid.uuid1(i)
            queries.append(f"INSERT INTO {self.table_src} VALUES ('{uuid_value}', '{uuid_value}')")

        self.null_uuid = uuid.uuid1(32132131)
        queries += [
            f"CREATE TABLE {self.table_dst} AS SELECT * FROM {self.table_src}",
            f"INSERT INTO {self.table_src} VALUES ('{self.null_uuid}', NULL)",
        ]

        for query in queries:
            self.connection.query(query, None)

        _commit(self.connection)

        self.a = TableSegment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = TableSegment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

    def test_uuid_column_with_nulls(self):
        differ = TableDiffer()
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.null_uuid), None))])


@test_per_database
class TestTableNullRowChecksum(TestPerDatabase):
    def setUp(self):
        super().setUp()

        self.null_uuid = uuid.uuid1(1)
        queries = [
            f"CREATE TABLE {self.table_src}(id varchar(100), text_comment varchar(1000))",
            f"INSERT INTO {self.table_src} VALUES ('{uuid.uuid1(1)}', '1')",
            f"CREATE TABLE {self.table_dst} AS SELECT * FROM {self.table_src}",
            # Add a row where a column has NULL value
            f"INSERT INTO {self.table_src} VALUES ('{self.null_uuid}', NULL)",
        ]

        for query in queries:
            self.connection.query(query, None)

        _commit(self.connection)

        self.a = TableSegment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = TableSegment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

    def test_uuid_columns_with_nulls(self):
        """
        Here we test a case when in one segment one or more columns has only null values. For example,
        Table A:
        | id   |   value   |
        |------|-----------|
        | pk_1 | 'value_1' |
        | pk_2 |    NULL   |

        Table B:
        | id   |   value   |
        |------|-----------|
        | pk_1 | 'value_1' |

        We can choose some bisection factor and bisection threshold (2 and 3 for our example, respectively)
        that one segment will look like ('pk_2', NULL). Some databases, when we do a cast these values to string and
        try to concatenate, some databases return NULL when concatenating (for example, MySQL). As the result, all next
        operations like substring, sum etc return nulls that leads incorrect diff results: ('pk_2', null) should be in
        diff results, but it's not. This test helps to detect such cases.
        """

        differ = TableDiffer(bisection_factor=2, bisection_threshold=3)
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, [("-", (str(self.null_uuid), None))])


@test_per_database
class TestConcatMultipleColumnWithNulls(TestPerDatabase):
    def setUp(self):
        super().setUp()

        text_type = _get_text_type(self.connection)

        queries = [
            f"CREATE TABLE {self.table_src}(id {text_type}, c1 {text_type}, c2 {text_type})",
            f"CREATE TABLE {self.table_dst}(id {text_type}, c1 {text_type}, c2 {text_type})",
        ]

        self.diffs = []
        for i in range(0, 8):
            pk = uuid.uuid1(i)
            table_src_c1_val = str(i)
            table_dst_c1_val = str(i) + "-different"

            queries.append(f"INSERT INTO {self.table_src} VALUES ('{pk}', '{table_src_c1_val}', NULL)")
            queries.append(f"INSERT INTO {self.table_dst} VALUES ('{pk}', '{table_dst_c1_val}', NULL)")

            self.diffs.append(("-", (str(pk), table_src_c1_val, None)))
            self.diffs.append(("+", (str(pk), table_dst_c1_val, None)))

        for query in queries:
            self.connection.query(query, None)

        _commit(self.connection)

        self.a = TableSegment(
            self.connection, self.table_src_path, "id", extra_columns=("c1", "c2"), case_sensitive=False
        )
        self.b = TableSegment(
            self.connection, self.table_dst_path, "id", extra_columns=("c1", "c2"), case_sensitive=False
        )

    def test_tables_are_different(self):
        """
        Here we test a case when in one segment one or more columns has only null values. For example,
        Table A:
        | id   | c1 |  c2  |
        |------|----|------|
        | pk_1 | 1  | NULL |
        | pk_2 | 2  | NULL |
                ...
        | pk_n | n | NULL |

        Table B:
        | id   |   c1   |  c2  |
        |------|--------|------|
        | pk_1 | 1-diff | NULL |
        | pk_2 | 2-diff | NULL |
                  ...
        | pk_n | n-diff | NULL |

        To calculate a checksum, we need to concatenate string values by rows. If both tables have columns with NULL
        value, it may lead that concat(pk_i, i, NULL) == concat(pk_i, i-diff, NULL). This test handle such cases.
        """

        differ = TableDiffer(bisection_factor=2, bisection_threshold=4)
        diff = list(differ.diff_tables(self.a, self.b))
        self.assertEqual(diff, self.diffs)


@test_per_database
class TestTableTableEmpty(TestPerDatabase):
    def setUp(self):
        super().setUp()

        self.null_uuid = uuid.uuid1(1)
        queries = [
            f"CREATE TABLE {self.table_src}(id varchar(100), text_comment varchar(1000))",
            f"CREATE TABLE {self.table_dst}(id varchar(100), text_comment varchar(1000))",
        ]

        self.diffs = [(uuid.uuid1(i), i) for i in range(100)]
        for pk, value in self.diffs:
            queries.append(f"INSERT INTO {self.table_src} VALUES ('{pk}', '{value}')")

        for query in queries:
            self.connection.query(query, None)

        _commit(self.connection)

        self.a = TableSegment(self.connection, self.table_src_path, "id", "text_comment", case_sensitive=False)
        self.b = TableSegment(self.connection, self.table_dst_path, "id", "text_comment", case_sensitive=False)

    def test_right_table_empty(self):
        differ = TableDiffer()
        self.assertRaises(ValueError, list, differ.diff_tables(self.a, self.b))

    def test_left_table_empty(self):
        queries = [
            f"INSERT INTO {self.table_dst} SELECT id, text_comment FROM {self.table_src}",
            f"TRUNCATE TABLE {self.table_src}",
        ]
        for query in queries:
            self.connection.query(query, None)

        _commit(self.connection)

        differ = TableDiffer()
        self.assertRaises(ValueError, list, differ.diff_tables(self.a, self.b))
