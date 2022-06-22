import math
from functools import lru_cache
from itertools import zip_longest
import re
from abc import ABC, abstractmethod
from runtype import dataclass
import logging
from typing import Sequence, Tuple, Optional, List
from concurrent.futures import ThreadPoolExecutor
import threading
from typing import Dict

import dsnparse
import sys

from .sql import DbPath, SqlOrStr, Compiler, Explain, Select


logger = logging.getLogger("database")


def parse_table_name(t):
    return tuple(t.split("."))


def import_postgres():
    import psycopg2
    import psycopg2.extras

    psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)
    return psycopg2


def import_mysql():
    import mysql.connector

    return mysql.connector


def import_snowflake():
    import snowflake.connector

    return snowflake


def import_mssql():
    import pymssql

    return pymssql


def import_oracle():
    import cx_Oracle

    return cx_Oracle


def import_presto():
    import prestodb

    return prestodb


class ConnectError(Exception):
    pass


class QueryError(Exception):
    pass


def _one(seq):
    (x,) = seq
    return x


def _query_conn(conn, sql_code: str) -> list:
    c = conn.cursor()
    c.execute(sql_code)
    if sql_code.lower().startswith("select"):
        return c.fetchall()


class ColType:
    pass


@dataclass
class PrecisionType(ColType):
    precision: Optional[int]
    rounds: bool


class TemporalType(PrecisionType):
    pass


class Timestamp(TemporalType):
    pass


class TimestampTZ(TemporalType):
    pass


class Datetime(TemporalType):
    pass


@dataclass
class NumericType(ColType):
    # 'precision' signifies how many fractional digits (after the dot) we want to compare
    precision: int


class Float(NumericType):
    pass


class Decimal(NumericType):
    pass


@dataclass
class Integer(Decimal):
    def __post_init__(self):
        assert self.precision == 0


@dataclass
class UnknownColType(ColType):
    text: str


class AbstractDatabase(ABC):
    @abstractmethod
    def quote(self, s: str):
        "Quote SQL name (implementation specific)"
        ...

    @abstractmethod
    def to_string(self, s: str) -> str:
        "Provide SQL for casting a column to string"
        ...

    @abstractmethod
    def md5_to_int(self, s: str) -> str:
        "Provide SQL for computing md5 and returning an int"
        ...

    @abstractmethod
    def _query(self, sql_code: str) -> list:
        "Send query to database and return result"
        ...

    @abstractmethod
    def select_table_schema(self, path: DbPath) -> str:
        "Provide SQL for selecting the table schema as (name, type, date_prec, num_prec)"
        ...

    @abstractmethod
    def query_table_schema(self, path: DbPath, filter_columns: Optional[Sequence[str]] = None) -> Dict[str, ColType]:
        "Query the table for its schema for table in 'path', and return {column: type}"
        ...

    @abstractmethod
    def parse_table_name(self, name: str) -> DbPath:
        "Parse the given table name into a DbPath"
        ...

    @abstractmethod
    def close(self):
        "Close connection(s) to the database instance. Querying will stop functioning."
        ...


    @abstractmethod
    def normalize_timestamp(self, value: str, coltype: ColType) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized timestamp.

        The returned expression must accept any SQL datetime/timestamp, and return a string.

        Date format: "YYYY-MM-DD HH:mm:SS.FFFFFF"

        Precision of dates should be rounded up/down according to coltype.rounds
        """
        ...

    @abstractmethod
    def normalize_number(self, value: str, coltype: ColType) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized number.

        The returned expression must accept any SQL int/numeric/float, and return a string.

        - Floats/Decimals are expected in the format
            "I.P"

            Where I is the integer part of the number (as many digits as necessary),
            and must be at least one digit (0).
            P is the fractional digits, the amount of which is specified with
            coltype.precision. Trailing zeroes may be necessary.
            If P is 0, the dot is omitted.

            Note: This precision is different than the one used by databases. For decimals,
            it's the same as ``numeric_scale``, and for floats, who use binary precision,
            it can be calculated as ``log10(2**numeric_precision)``.
        """
        ...

    def normalize_value_by_type(self, value: str, coltype: ColType) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized representation.

        The returned expression must accept any SQL value, and return a string.

        The default implementation dispatches to a method according to ``coltype``:

            TemporalType -> normalize_timestamp()
            NumericType  -> normalize_number()
            -else-       -> to_string()

        """
        if isinstance(coltype, TemporalType):
            return self.normalize_timestamp(value, coltype)
        elif isinstance(coltype, NumericType):
            return self.normalize_number(value, coltype)
        return self.to_string(f"{value}")


class Database(AbstractDatabase):
    """Base abstract class for databases.

    Used for providing connection code and implementation specific SQL utilities.

    Instanciated using :meth:`~data_diff.connect_to_uri`
    """

    DATETIME_TYPES = {}
    default_schema = None

    @property
    def name(self):
        return type(self).__name__

    def query(self, sql_ast: SqlOrStr, res_type: type):
        "Query the given SQL code/AST, and attempt to convert the result to type 'res_type'"

        compiler = Compiler(self)
        sql_code = compiler.compile(sql_ast)
        logger.debug("Running SQL (%s): %s", type(self).__name__, sql_code)
        if getattr(self, "_interactive", False) and isinstance(sql_ast, Select):
            explained_sql = compiler.compile(Explain(sql_ast))
            logger.info(f"EXPLAIN for SQL SELECT")
            logger.info(self._query(explained_sql))
            answer = input("Continue? [y/n] ")
            if not answer.lower() in ["y", "yes"]:
                sys.exit(1)

        res = self._query(sql_code)
        if res_type is int:
            res = _one(_one(res))
            if res is None:  # May happen due to sum() of 0 items
                return None
            return int(res)
        elif res_type is tuple:
            assert len(res) == 1, (sql_code, res)
            return res[0]
        elif getattr(res_type, "__origin__", None) is list and len(res_type.__args__) == 1:
            if res_type.__args__ == (int,):
                return [_one(row) for row in res]
            elif res_type.__args__ == (Tuple,):
                return [tuple(row) for row in res]
            else:
                raise ValueError(res_type)
        return res

    def enable_interactive(self):
        self._interactive = True

    def _convert_db_precision_to_digits(self, p: int) -> int:
        """Convert from binary precision, used by floats, to decimal precision."""
        # See: https://en.wikipedia.org/wiki/Single-precision_floating-point_format
        return math.floor(math.log(2**p, 10))

    def _parse_type(
        self, type_repr: str, datetime_precision: int = None, numeric_precision: int = None, numeric_scale: int = None
    ) -> ColType:
        """ """

        cls = self.DATETIME_TYPES.get(type_repr)
        if cls:
            return cls(
                precision=datetime_precision if datetime_precision is not None else DEFAULT_DATETIME_PRECISION,
                rounds=self.ROUNDS_ON_PREC_LOSS,
            )

        cls = self.NUMERIC_TYPES.get(type_repr)
        if cls:
            if issubclass(cls, Integer):
                # Some DBs have a constant numeric_scale, so they don't report it.
                # We fill in the constant, so we need to ignore it for integers.
                return cls(precision=0)

            elif issubclass(cls, Decimal):
                return cls(precision=numeric_scale)

            assert issubclass(cls, Float)
            # assert numeric_scale is None
            return cls(
                precision=self._convert_db_precision_to_digits(
                    numeric_precision if numeric_precision is not None else DEFAULT_NUMERIC_PRECISION
                )
            )

        return UnknownColType(type_repr)

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)

        return (
            "SELECT column_name, data_type, datetime_precision, numeric_precision, numeric_scale FROM information_schema.columns "
            f"WHERE table_name = '{table}' AND table_schema = '{schema}'"
        )

    def query_table_schema(self, path: DbPath, filter_columns: Optional[Sequence[str]] = None) -> Dict[str, ColType]:
        rows = self.query(self.select_table_schema(path), list)
        if not rows:
            raise RuntimeError(f"{self.name}: Table '{'.'.join(path)}' does not exist, or has no columns")

        if filter_columns is not None:
            accept = {i.lower() for i in filter_columns}
            rows = [r for r in rows if r[0].lower() in accept]

        # Return a dict of form {name: type} after normalization
        return {row[0]: self._parse_type(*row[1:]) for row in rows}

    # @lru_cache()
    # def get_table_schema(self, path: DbPath) -> Dict[str, ColType]:
    #     return self.query_table_schema(path)

    def _normalize_table_path(self, path: DbPath) -> DbPath:
        if len(path) == 1:
            if self.default_schema:
                return self.default_schema, path[0]
        elif len(path) != 2:
            raise ValueError(
                f"{self.name}: Bad table path for {self}: '{'.'.join(path)}'. Expected form: schema.table"
            )

        return path


    def parse_table_name(self, name: str) -> DbPath:
        return parse_table_name(name)


class ThreadedDatabase(Database):
    """Access the database through singleton threads.

    Used for database connectors that do not support sharing their connection between different threads.
    """

    def __init__(self, thread_count=1):
        self._queue = ThreadPoolExecutor(thread_count, initializer=self.set_conn)
        self.thread_local = threading.local()

    def set_conn(self):
        assert not hasattr(self.thread_local, "conn")
        self.thread_local.conn = self.create_connection()

    def _query(self, sql_code: str):
        r = self._queue.submit(self._query_in_worker, sql_code)
        return r.result()

    def _query_in_worker(self, sql_code: str):
        "This method runs in a worker thread"
        return _query_conn(self.thread_local.conn, sql_code)

    def close(self):
        self._queue.shutdown(True)

    @abstractmethod
    def create_connection(self):
        ...

    def close(self):
        self._queue.shutdown()


CHECKSUM_HEXDIGITS = 15  # Must be 15 or lower
MD5_HEXDIGITS = 32

_CHECKSUM_BITSIZE = CHECKSUM_HEXDIGITS << 2
CHECKSUM_MASK = (2**_CHECKSUM_BITSIZE) - 1

DEFAULT_DATETIME_PRECISION = 6
DEFAULT_NUMERIC_PRECISION = 24

TIMESTAMP_PRECISION_POS = 20  # len("2022-06-03 12:24:35.") == 20


class Postgres(ThreadedDatabase):
    DATETIME_TYPES = {
        "timestamp with time zone": TimestampTZ,
        "timestamp without time zone": Timestamp,
        "timestamp": Timestamp,
        # "datetime": Datetime,
    }
    NUMERIC_TYPES = {
        "double precision": Float,
        "real": Float,
        "decimal": Decimal,
        "integer": Integer,
        "numeric": Decimal,
        "bigint": Integer,
    }
    ROUNDS_ON_PREC_LOSS = True

    default_schema = "public"

    def __init__(self, host, port, user, password, *, database, thread_count, **kw):
        self.args = dict(host=host, port=port, database=database, user=user, password=password, **kw)

        super().__init__(thread_count=thread_count)

    def _convert_db_precision_to_digits(self, p: int) -> int:
        # Subtracting 2 due to wierd precision issues in Postgres
        return super()._convert_db_precision_to_digits(p) - 2

    def create_connection(self):
        postgres = import_postgres()
        try:
            c = postgres.connect(**self.args)
            # c.cursor().execute("SET TIME ZONE 'UTC'")
            return c
        except postgres.OperationalError as e:
            raise ConnectError(*e.args) from e

    def quote(self, s: str):
        return f'"{s}"'

    def md5_to_int(self, s: str) -> str:
        return f"('x' || substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}))::bit({_CHECKSUM_BITSIZE})::bigint"

    def to_string(self, s: str):
        return f"{s}::varchar"


    def normalize_timestamp(self, value: str, coltype: ColType) -> str:
        if coltype.rounds:
            return f"to_char({value}::timestamp({coltype.precision}), 'YYYY-mm-dd HH24:MI:SS.US')"

        timestamp6 = f"to_char({value}::timestamp(6), 'YYYY-mm-dd HH24:MI:SS.US')"
        return f"RPAD(LEFT({timestamp6}, {TIMESTAMP_PRECISION_POS+coltype.precision}), {TIMESTAMP_PRECISION_POS+6}, '0')"

    def normalize_number(self, value: str, coltype: ColType) -> str:
        return self.to_string(f"{value}::decimal(38, {coltype.precision})")


class Presto(Database):
    default_schema = "public"
    DATETIME_TYPES = {
        "timestamp with time zone": TimestampTZ,
        "timestamp without time zone": Timestamp,
        "timestamp": Timestamp,
        # "datetime": Datetime,
    }
    NUMERIC_TYPES = {
        "integer": Integer,
        "real": Float,
        "double": Float,
    }
    ROUNDS_ON_PREC_LOSS = True

    def __init__(self, host, port, user, password, *, catalog, schema=None, **kw):
        prestodb = import_presto()
        self.args = dict(host=host, user=user, catalog=catalog, schema=schema, **kw)

        self._conn = prestodb.dbapi.connect(**self.args)

    def quote(self, s: str):
        return f'"{s}"'

    def md5_to_int(self, s: str) -> str:
        return f"cast(from_base(substr(to_hex(md5(to_utf8({s}))), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16) as decimal(38, 0))"

    def to_string(self, s: str):
        return f"cast({s} as varchar)"

    def _query(self, sql_code: str) -> list:
        "Uses the standard SQL cursor interface"
        return _query_conn(self._conn, sql_code)

    def close(self):
        self._conn.close()

    def normalize_timestamp(self, value: str, coltype: ColType) -> str:
        # TODO
        if coltype.rounds:
            s = f"date_format(cast({value} as timestamp(6)), '%Y-%m-%d %H:%i:%S.%f')"
        else:
            s = f"date_format(cast({value} as timestamp(6)), '%Y-%m-%d %H:%i:%S.%f')"

        return (
            f"RPAD(RPAD({s}, {TIMESTAMP_PRECISION_POS+coltype.precision}, '.'), {TIMESTAMP_PRECISION_POS+6}, '0')"
        )

    def normalize_number(self, value: str, coltype: ColType) -> str:
        return self.to_string(f"cast({value} as decimal(38,{coltype.precision}))")

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)

        return (
            f"SELECT column_name, data_type, 3 as datetime_precision, 3 as numeric_precision FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = '{table}' AND table_schema = '{schema}'"
        )

    def _parse_type(self, type_repr: str, datetime_precision: int = None, numeric_precision: int = None) -> ColType:
        regexps = {
            r"timestamp\((\d)\)": Timestamp,
            r"timestamp\((\d)\) with time zone": TimestampTZ,
        }
        for regexp, cls in regexps.items():
            m = re.match(regexp + "$", type_repr)
            if m:
                datetime_precision = int(m.group(1))
                return cls(
                    precision=datetime_precision if datetime_precision is not None else DEFAULT_DATETIME_PRECISION,
                    rounds=False,
                )

        regexps = {r"decimal\((\d+),(\d+)\)": Decimal}
        for regexp, cls in regexps.items():
            m = re.match(regexp + "$", type_repr)
            if m:
                prec, scale = map(int, m.groups())
                return cls(scale)

        cls = self.NUMERIC_TYPES.get(type_repr)
        if cls:
            if issubclass(cls, Integer):
                assert numeric_precision is not None
                return cls(0)

            assert issubclass(cls, Float)
            return cls(
                precision=self._convert_db_precision_to_digits(
                    numeric_precision if numeric_precision is not None else DEFAULT_NUMERIC_PRECISION
                )
            )

        return UnknownColType(type_repr)


class MySQL(ThreadedDatabase):
    DATETIME_TYPES = {
        "datetime": Datetime,
        "timestamp": Timestamp,
    }
    NUMERIC_TYPES = {
        "double": Float,
        "float": Float,
        "decimal": Decimal,
        "int": Integer,
    }
    ROUNDS_ON_PREC_LOSS = True

    def __init__(self, host, port, user, password, *, database, thread_count, **kw):
        args = dict(host=host, port=port, database=database, user=user, password=password, **kw)
        self._args = {k: v for k, v in args.items() if v is not None}

        super().__init__(thread_count=thread_count)

        self.default_schema = user

    def create_connection(self):
        mysql = import_mysql()
        try:
            return mysql.connect(charset="utf8", use_unicode=True, **self._args)
        except mysql.Error as e:
            if e.errno == mysql.errorcode.ER_ACCESS_DENIED_ERROR:
                raise ConnectError("Bad user name or password") from e
            elif e.errno == mysql.errorcode.ER_BAD_DB_ERROR:
                raise ConnectError("Database does not exist") from e
            else:
                raise ConnectError(*e.args) from e

    def quote(self, s: str):
        return f"`{s}`"

    def md5_to_int(self, s: str) -> str:
        return f"cast(conv(substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16, 10) as unsigned)"

    def to_string(self, s: str):
        return f"cast({s} as char)"

    def normalize_timestamp(self, value: str, coltype: ColType) -> str:
        if coltype.rounds:
            return self.to_string(f"cast( cast({value} as datetime({coltype.precision})) as datetime(6))")

        s = self.to_string(f"cast({value} as datetime(6))")
        return f"RPAD(RPAD({s}, {TIMESTAMP_PRECISION_POS+coltype.precision}, '.'), {TIMESTAMP_PRECISION_POS+6}, '0')"

    def normalize_number(self, value: str, coltype: ColType) -> str:
        return self.to_string(f"cast({value} as decimal(38, {coltype.precision}))")



class Oracle(ThreadedDatabase):
    ROUNDS_ON_PREC_LOSS = True

    def __init__(self, host, port, user, password, *, database, thread_count, **kw):
        assert not port
        self.kwargs = dict(user=user, password=password, dsn="%s/%s" % (host, database), **kw)
        super().__init__(thread_count=thread_count)

    def create_connection(self):
        self._oracle = import_oracle()
        try:
            return self._oracle.connect(**self.kwargs)
        except Exception as e:
            raise ConnectError(*e.args) from e

    def _query(self, sql_code: str):
        try:
            return super()._query(sql_code)
        except self._oracle.DatabaseError as e:
            raise QueryError(e)

    def md5_to_int(self, s: str) -> str:
        # standard_hash is faster than DBMS_CRYPTO.Hash
        # TODO: Find a way to use UTL_RAW.CAST_TO_BINARY_INTEGER ?
        return f"to_number(substr(standard_hash({s}, 'MD5'), 18), 'xxxxxxxxxxxxxxx')"

    def quote(self, s: str):
        return f"{s}"

    def to_string(self, s: str):
        return f"cast({s} as varchar(1024))"

    def select_table_schema(self, path: DbPath) -> str:
        if len(path) > 1:
            raise ValueError("Unexpected table path for oracle")
        (table,) = path

        return (
            f"SELECT column_name, data_type, 6 as datetime_precision, data_precision as numeric_precision, data_scale as numeric_scale"
            f" FROM USER_TAB_COLUMNS WHERE table_name = '{table.upper()}'"
        )

    def normalize_timestamp(self, value: str, coltype: ColType) -> str:
        return f"to_char(cast({value} as timestamp({coltype.precision})), 'YYYY-MM-DD HH24:MI:SS.FF6')"

    def normalize_number(self, value: str, coltype: ColType) -> str:
        # FM999.9990
        format_str = "FM" + "9" * (38 - coltype.precision)
        if coltype.precision:
            format_str += "0." + "9" * (coltype.precision - 1) + "0"
        return f"to_char({value}, '{format_str}')"

    def _parse_type(
        self, type_repr: str, datetime_precision: int = None, numeric_precision: int = None, numeric_scale: int = None
    ) -> ColType:
        """ """
        regexps = {
            r"TIMESTAMP\((\d)\) WITH LOCAL TIME ZONE": Timestamp,
            r"TIMESTAMP\((\d)\) WITH TIME ZONE": TimestampTZ,
        }
        for regexp, cls in regexps.items():
            m = re.match(regexp + "$", type_repr)
            if m:
                datetime_precision = int(m.group(1))
                return cls(
                    precision=datetime_precision if datetime_precision is not None else DEFAULT_DATETIME_PRECISION,
                    rounds=self.ROUNDS_ON_PREC_LOSS,
                )

        cls = {
            "NUMBER": Decimal,
            "FLOAT": Float,
        }.get(type_repr, None)
        if cls:
            if issubclass(cls, Decimal):
                assert numeric_scale is not None, (type_repr, numeric_precision, numeric_scale)
                return cls(precision=numeric_scale)

            assert issubclass(cls, Float)
            return cls(
                precision=self._convert_db_precision_to_digits(
                    numeric_precision if numeric_precision is not None else DEFAULT_NUMERIC_PRECISION
                )
            )

        return UnknownColType(type_repr)


class Redshift(Postgres):
    NUMERIC_TYPES = {
        **Postgres.NUMERIC_TYPES,
        "double": Float,
        "real": Float,
    }

    # def _convert_db_precision_to_digits(self, p: int) -> int:
    #     return super()._convert_db_precision_to_digits(p // 2)

    def md5_to_int(self, s: str) -> str:
        return f"strtol(substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16)::decimal(38)"

    def normalize_timestamp(self, value: str, coltype: ColType) -> str:
        if coltype.rounds:
            timestamp = f"{value}::timestamp(6)"
            # Get seconds since epoch. Redshift doesn't support milli- or micro-seconds.
            secs = f"timestamp 'epoch' + round(extract(epoch from {timestamp})::decimal(38)"
            # Get the milliseconds from timestamp.
            ms = f"extract(ms from {timestamp})"
            # Get the microseconds from timestamp, without the milliseconds!
            us = f"extract(us from {timestamp})"
            # epoch = Total time since epoch in microseconds.
            epoch = f"{secs}*1000000 + {ms}*1000 + {us}"
            timestamp6 = f"to_char({epoch}, -6+{coltype.precision}) * interval '0.000001 seconds', 'YYYY-mm-dd HH24:MI:SS.US')"
        else:
            timestamp6 = f"to_char({value}::timestamp(6), 'YYYY-mm-dd HH24:MI:SS.US')"
        return f"RPAD(LEFT({timestamp6}, {TIMESTAMP_PRECISION_POS+coltype.precision}), {TIMESTAMP_PRECISION_POS+6}, '0')"

    def normalize_number(self, value: str, coltype: ColType) -> str:
        return self.to_string(f"{value}::decimal(38,{coltype.precision})")


    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)

        return (
            "SELECT column_name, data_type, datetime_precision, numeric_precision, numeric_scale FROM information_schema.columns "
            f"WHERE table_name = '{table.lower()}' AND table_schema = '{schema.lower()}'"
        )


class MsSQL(ThreadedDatabase):
    "AKA sql-server"

    def __init__(self, host, port, user, password, *, database, thread_count, **kw):
        args = dict(server=host, port=port, database=database, user=user, password=password, **kw)
        self._args = {k: v for k, v in args.items() if v is not None}

        super().__init__(thread_count=thread_count)

    def create_connection(self):
        mssql = import_mssql()
        try:
            return mssql.connect(**self._args)
        except mssql.Error as e:
            raise ConnectError(*e.args) from e

    def quote(self, s: str):
        return f"[{s}]"

    def md5_to_int(self, s: str) -> str:
        return f"CONVERT(decimal(38,0), CONVERT(bigint, HashBytes('MD5', {s}), 2))"
        # return f"CONVERT(bigint, (CHECKSUM({s})))"

    def to_string(self, s: str):
        return f"CONVERT(varchar, {s})"


class BigQuery(Database):
    DATETIME_TYPES = {
        "TIMESTAMP": Timestamp,
        "DATETIME": Datetime,
    }
    NUMERIC_TYPES = {
        "INT64": Integer,
        "INT32": Integer,
        "NUMERIC": Decimal,
        "BIGNUMERIC": Decimal,
        "FLOAT64": Float,
        "FLOAT32": Float,
    }
    ROUNDS_ON_PREC_LOSS = False  # Technically BigQuery doesn't allow implicit rounding or truncation

    def __init__(self, project, *, dataset, **kw):
        from google.cloud import bigquery

        self._client = bigquery.Client(project, **kw)
        self.project = project
        self.dataset = dataset

        self.default_schema = dataset

    def quote(self, s: str):
        return f"`{s}`"

    def md5_to_int(self, s: str) -> str:
        return f"cast(cast( ('0x' || substr(TO_HEX(md5({s})), 18)) as int64) as numeric)"

    def _normalize_returned_value(self, value):
        if isinstance(value, bytes):
            return value.decode()
        return value

    def _query(self, sql_code: str):
        from google.cloud import bigquery

        try:
            res = list(self._client.query(sql_code))
        except Exception as e:
            msg = "Exception when trying to execute SQL code:\n    %s\n\nGot error: %s"
            raise ConnectError(msg % (sql_code, e))

        if res and isinstance(res[0], bigquery.table.Row):
            res = [tuple(self._normalize_returned_value(v) for v in row.values()) for row in res]
        return res

    def to_string(self, s: str):
        return f"cast({s} as string)"

    def close(self):
        self._client.close()

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)

        return (
            f"SELECT column_name, data_type, 6 as datetime_precision, 38 as numeric_precision, 9 as numeric_scale FROM {schema}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = '{table}' AND table_schema = '{schema}'"
        )

    def normalize_timestamp(self, value: str, coltype: ColType) -> str:
        if coltype.rounds:
            timestamp = f"timestamp_micros(cast(round(unix_micros(cast({value} as timestamp))/1000000, {coltype.precision})*1000000 as int))"
            return f"FORMAT_TIMESTAMP('%F %H:%M:%E6S', {timestamp})"

        if coltype.precision == 0:
            return f"FORMAT_TIMESTAMP('%F %H:%M:%S.000000, {value})"
        elif coltype.precision == 6:
            return f"FORMAT_TIMESTAMP('%F %H:%M:%E6S', {value})"

        timestamp6 = f"FORMAT_TIMESTAMP('%F %H:%M:%E6S', {value})"
        return f"RPAD(LEFT({timestamp6}, {TIMESTAMP_PRECISION_POS+coltype.precision}), {TIMESTAMP_PRECISION_POS+6}, '0')"

    def normalize_number(self, value: str, coltype: ColType) -> str:
        if isinstance(coltype, Integer):
            return self.to_string(value)
        return f"format('%.{coltype.precision}f', {value})"

    def parse_table_name(self, name: str) -> DbPath:
        path = parse_table_name(name)
        return self._normalize_table_path(path)


class Snowflake(Database):
    DATETIME_TYPES = {
        "TIMESTAMP_NTZ": Timestamp,
        "TIMESTAMP_LTZ": Timestamp,
        "TIMESTAMP_TZ": TimestampTZ,
    }
    NUMERIC_TYPES = {
        "NUMBER": Decimal,
        "FLOAT": Float,
    }
    ROUNDS_ON_PREC_LOSS = False

    def __init__(
        self,
        account: str,
        _port: int,
        user: str,
        password: str,
        *,
        warehouse: str,
        schema: str,
        database: str,
        role: str = None,
        **kw,
    ):
        snowflake = import_snowflake()
        logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

        # Got an error: snowflake.connector.network.RetryRequest: could not find io module state (interpreter shutdown?)
        # It's a known issue: https://github.com/snowflakedb/snowflake-connector-python/issues/145
        # Found a quick solution in comments
        logging.getLogger("snowflake.connector.network").disabled = True

        assert '"' not in schema, "Schema name should not contain quotes!"
        self._conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            role=role,
            database=database,
            warehouse=warehouse,
            schema=f'"{schema}"',
            **kw,
        )

        self.default_schema = schema

    def close(self):
        self._conn.close()

    def _query(self, sql_code: str) -> list:
        "Uses the standard SQL cursor interface"
        return _query_conn(self._conn, sql_code)

    def quote(self, s: str):
        return f'"{s}"'

    def md5_to_int(self, s: str) -> str:
        return f"BITAND(md5_number_lower64({s}), {CHECKSUM_MASK})"

    def to_string(self, s: str):
        return f"cast({s} as string)"

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)
        return super().select_table_schema((schema, table))

    def normalize_timestamp(self, value: str, coltype: ColType) -> str:
        if coltype.rounds:
            timestamp = f"to_timestamp(round(date_part(epoch_nanosecond, {value}::timestamp(9))/1000000000, {coltype.precision}))"
        else:
            timestamp = f"cast({value} as timestamp({coltype.precision}))"

        return f"to_char({timestamp}, 'YYYY-MM-DD HH24:MI:SS.FF6')"

    def normalize_number(self, value: str, coltype: ColType) -> str:
        return self.to_string(f"cast({value} as decimal(38, {coltype.precision}))")


@dataclass
class MatchUriPath:
    database_cls: type
    params: List[str]
    kwparams: List[str] = []
    help_str: str

    def match_path(self, dsn):
        dsn_dict = dict(dsn.query)
        matches = {}
        for param, arg in zip_longest(self.params, dsn.paths):
            if param is None:
                raise ValueError(f"Too many parts to path. Expected format: {self.help_str}")

            optional = param.endswith("?")
            param = param.rstrip("?")

            if arg is None:
                try:
                    arg = dsn_dict.pop(param)
                except KeyError:
                    if not optional:
                        raise ValueError(f"URI must specify '{param}'. Expected format: {self.help_str}")

                    arg = None

            assert param and param not in matches
            matches[param] = arg

        for param in self.kwparams:
            try:
                arg = dsn_dict.pop(param)
            except KeyError:
                raise ValueError(f"URI must specify '{param}'. Expected format: {self.help_str}")

            assert param and arg and param not in matches, (param, arg, matches.keys())
            matches[param] = arg

        for param, value in dsn_dict.items():
            if param in matches:
                raise ValueError(
                    f"Parameter '{param}' already provided as positional argument. Expected format: {self.help_str}"
                )

            matches[param] = value

        return matches


MATCH_URI_PATH = {
    "postgres": MatchUriPath(Postgres, ["database?"], help_str="postgres://<user>:<pass>@<host>/<database>"),
    "mysql": MatchUriPath(MySQL, ["database?"], help_str="mysql://<user>:<pass>@<host>/<database>"),
    "oracle": MatchUriPath(Oracle, ["database?"], help_str="oracle://<user>:<pass>@<host>/<database>"),
    "mssql": MatchUriPath(MsSQL, ["database?"], help_str="mssql://<user>:<pass>@<host>/<database>"),
    "redshift": MatchUriPath(Redshift, ["database?"], help_str="redshift://<user>:<pass>@<host>/<database>"),
    "snowflake": MatchUriPath(
        Snowflake,
        ["database", "schema"],
        ["warehouse"],
        help_str="snowflake://<user>:<pass>@<account>/<database>/<SCHEMA>?warehouse=<WAREHOUSE>",
    ),
    "presto": MatchUriPath(Presto, ["catalog", "schema"], help_str="presto://<user>@<host>/<catalog>/<schema>"),
    "bigquery": MatchUriPath(BigQuery, ["dataset"], help_str="bigquery://<project>/<dataset>"),
}


def connect_to_uri(db_uri: str, thread_count: Optional[int] = 1) -> Database:
    """Connect to the given database uri

    thread_count determines the max number of worker threads per database,
    if relevant. None means no limit.

    Parameters:
        db_uri (str): The URI for the database to connect
        thread_count (int, optional): Size of the threadpool. Ignored by cloud databases. (default: 1)

    Note: For non-cloud databases, a low thread-pool size may be a performance bottleneck.

    Supported schemes:
    - postgres
    - mysql
    - mssql
    - oracle
    - snowflake
    - bigquery
    - redshift
    - presto
    """

    dsn = dsnparse.parse(db_uri)
    if len(dsn.schemes) > 1:
        raise NotImplementedError("No support for multiple schemes")
    (scheme,) = dsn.schemes

    try:
        matcher = MATCH_URI_PATH[scheme]
    except KeyError:
        raise NotImplementedError(f"Scheme {scheme} currently not supported")

    cls = matcher.database_cls
    kw = matcher.match_path(dsn)

    if scheme == "bigquery":
        return cls(dsn.host, **kw)

    if issubclass(cls, ThreadedDatabase):
        return cls(dsn.host, dsn.port, dsn.user, dsn.password, thread_count=thread_count, **kw)

    return cls(dsn.host, dsn.port, dsn.user, dsn.password, **kw)
