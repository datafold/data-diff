from functools import lru_cache
import re
from abc import ABC, abstractmethod
from runtype import dataclass
import logging
from typing import Tuple, Optional
from concurrent.futures import ThreadPoolExecutor
import threading
from typing import Dict

import dsnparse
import sys

from .sql import DbPath, SqlOrStr, Compiler, Explain, Select


logger = logging.getLogger("database")


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


def _one(seq):
    (x,) = seq
    return x


def _query_conn(conn, sql_code: str) -> list:
    c = conn.cursor()
    c.execute(sql_code)
    return c.fetchall()


class ColType:
    pass


@dataclass
class PrecisionType(ColType):
    precision: Optional[int]


class TemporalType(PrecisionType):
    pass


class Timestamp(TemporalType):
    pass


class TimestampTZ(TemporalType):
    pass


class Datetime(TemporalType):
    pass


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
    def close(self):
        "Close connection(s) to the database instance. Querying will stop functioning."
        ...

    @abstractmethod
    def normalize_value_by_type(value: str, coltype: ColType) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized representation.

        The returned expression must accept any SQL value, and return a string.

        - Dates are expected in the format:
            YYYY-MM-DD HH:mm:SS.FFFFFF
            (number of F depends on coltype.precision)

        """
        ...


class Database(AbstractDatabase):
    """Base abstract class for databases.

    Used for providing connection code and implementation specific SQL utilities.
    """

    DATETIME_TYPES = NotImplemented
    default_schema = NotImplemented

    def query(self, sql_ast: SqlOrStr, res_type: type):
        "Query the given SQL AST, and attempt to convert the result to type 'res_type'"
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
            assert len(res) == 1
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

    def _parse_type(self, type_repr: str, datetime_precision: int = None, numeric_precision: int = None) -> ColType:
        """ """

        cls = self.DATETIME_TYPES.get(type_repr)
        if cls:
            return cls(precision=datetime_precision or DEFAULT_PRECISION)

        return UnknownColType(type_repr)

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_path(path)

        return (
            "SELECT column_name, data_type, datetime_precision, numeric_precision FROM information_schema.columns "
            f"WHERE table_name = '{table}' AND table_schema = '{schema}'"
        )

    def query_table_schema(self, path: DbPath) -> Dict[str, ColType]:
        rows = self.query(self.select_table_schema(path), list)

        # Return a dict of form {name: type} after canonizaation
        return {row[0].lower(): self._parse_type(*row[1:]) for row in rows}

    @lru_cache()
    def get_table_schema(self, path: DbPath) -> Dict[str, ColType]:
        return self.query_table_schema(path)

    def _normalize_path(self, path: DbPath) -> DbPath:
        if len(path) == 1:
            return self.default_schema, path[0]
        elif len(path) == 2:
            return path

        raise ValueError(f"Bad table path for {self}: '{'.'.join(path)}'. Expected form: schema.table")


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

DEFAULT_PRECISION = 6

TIMESTAMP_PRECISION_POS = 20  # len("2022-06-03 12:24:35.") == 20


class Postgres(ThreadedDatabase):
    DATETIME_TYPES = {
        "timestamp with time zone": TimestampTZ,
        "timestamp without time zone": Timestamp,
        "timestamp": Timestamp,
        # "datetime": Datetime,
    }
    default_schema = "public"

    def __init__(self, host, port, database, user, password, *, thread_count):
        self.args = dict(host=host, port=port, database=database, user=user, password=password)

        super().__init__(thread_count=thread_count)

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

    def normalize_value_by_type(self, value: str, coltype: ColType) -> str:
        if isinstance(coltype, TemporalType):
            timestamp = self.to_string(f"{value}::timestamp({coltype.precision})")
            return f"RPAD({timestamp}, {TIMESTAMP_PRECISION_POS + coltype.precision}, '0')"
        return self.to_string(f"{value}")

    def _query_in_worker(self, sql_code: str):
        postgres = import_postgres()
        try:
            return _query_conn(self.thread_local.conn, sql_code)
        except postgres.ProgrammingError:
            return []


class Presto(Database):
    def __init__(self, host, port, database, user, password):
        prestodb = import_presto()
        self.args = dict(host=host, user=user)

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


class MySQL(ThreadedDatabase):
    DATETIME_TYPES = {
        "datetime": Datetime,
        "timestamp": Timestamp,
    }

    def __init__(self, host, port, database, user, password, *, thread_count):
        args = dict(host=host, port=port, database=database, user=user, password=password)
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

    def normalize_value_by_type(self, value: str, coltype: ColType) -> str:
        if isinstance(coltype, TemporalType):
            return self.to_string(f"cast({value} as datetime({coltype.precision}))")
        return self.to_string(f"{value}")


class Oracle(ThreadedDatabase):
    def __init__(self, host, port, database, user, password, *, thread_count):
        assert not port
        self.kwargs = dict(user=user, password=password, dsn="%s/%s" % (host, database))
        super().__init__(thread_count=thread_count)

    def create_connection(self):
        oracle = import_oracle()
        try:
            return oracle.connect(**self.kwargs)
        except Exception as e:
            raise ConnectError(*e.args) from e

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
            f"SELECT column_name, data_type, 9 as datetime_precision, data_precision as numeric_precision"
            f" FROM USER_TAB_COLUMNS WHERE table_name = '{table.upper()}'"
        )

    def normalize_value_by_type(self, value: str, coltype: ColType) -> str:
        if isinstance(coltype, PrecisionType):
            return f"to_char(cast({value} as timestamp), 'YYYY-MM-DD HH24:MI:SS.FF{coltype.precision or ''}')"
        return self.to_string(f"{value}")

    def _parse_type(self, type_repr: str, datetime_precision: int = None, numeric_precision: int = None) -> ColType:
        """ """
        regexps = {
            r"TIMESTAMP\((\d)\) WITH LOCAL TIME ZONE": Timestamp,
            r"TIMESTAMP\((\d)\) WITH TIME ZONE": TimestampTZ,
        }
        for regexp, cls in regexps.items():
            m = re.match(regexp + "$", type_repr)
            if m:
                datetime_precision = int(m.group(1))
                return cls(precision=datetime_precision or DEFAULT_PRECISION)

        return UnknownColType(type_repr)


class Redshift(Postgres):
    def md5_to_int(self, s: str) -> str:
        return f"strtol(substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16)::decimal(38)"

    def normalize_value_by_type(self, value: str, coltype: ColType) -> str:
        if isinstance(coltype, PrecisionType):
            if coltype.precision == 3:
                return f"to_char({value}, 'YYYY-mm-dd HH24:MI:SS.MS')"
            if coltype.precision != 6:
                # Redshift doesn't support arbitrary precision
                raise TypeError(f"Bad precision for Redshift: {coltype})")
        return super().normalize_value_by_type(value, coltype)


class MsSQL(ThreadedDatabase):
    "AKA sql-server"

    def __init__(self, host, port, database, user, password, *, thread_count):
        args = dict(server=host, port=port, database=database, user=user, password=password)
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

    def __init__(self, project, dataset):
        from google.cloud import bigquery

        self._client = bigquery.Client(project)
        self.project = project
        self.dataset = dataset

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
        schema, table = self._normalize_path(path)

        return (
            f"SELECT column_name, data_type, 6 as datetime_precision, 6 as numeric_precision FROM {self.dataset}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = '{table}' AND table_schema = '{schema}'"
        )

    def normalize_value_by_type(self, value: str, coltype: ColType) -> str:
        if isinstance(coltype, PrecisionType):
            return self.to_string(f'FORMAT_TIMESTAMP("%F %H:%M:%E6S", {value})')
        return self.to_string(f"{value}")


class Snowflake(Database):
    DATETIME_TYPES = {
        "TIMESTAMP_NTZ": Timestamp,
        "TIMESTAMP_LTZ": Timestamp,
        "TIMESTAMP_TZ": TimestampTZ,
    }

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: str,
        schema: str,
        database: str,
        role: str = None,
        print_sql: bool = False,
    ):
        snowflake = import_snowflake()
        logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

        # Got an error: snowflake.connector.network.RetryRequest: could not find io module state (interpreter shutdown?)
        # It's a known issue: https://github.com/snowflakedb/snowflake-connector-python/issues/145
        # Found a quick solution in comments
        logging.getLogger("snowflake.connector.network").disabled = True

        self._conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            role=role,
            database=database,
            warehouse=warehouse,
            schema=schema,
        )

        self.default_schema = schema

    def close(self):
        self._conn.close()

    def _query(self, sql_code: str) -> list:
        "Uses the standard SQL cursor interface"
        return _query_conn(self._conn, sql_code)

    def quote(self, s: str):
        return s

    def md5_to_int(self, s: str) -> str:
        return f"BITAND(md5_number_lower64({s}), {CHECKSUM_MASK})"

    def to_string(self, s: str):
        return f"cast({s} as string)"

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_path(path)
        return super().select_table_schema((schema.upper(), table.upper()))

    def normalize_value_by_type(self, value: str, coltype: ColType) -> str:
        if isinstance(coltype, TemporalType):
            return f"{value}::timestamp({coltype.precision})::text"
        return f"{value}::text"


def connect_to_uri(db_uri: str, thread_count: Optional[int] = 1) -> Database:
    """Connect to the given database uri

    thread_count determines the max number of worker threads per database,
    if relevant. None means no limit.

    Supported databases:
    - postgres
    - mysql
    - mssql
    - oracle
    - snowflake
    - bigquery
    - redshift
    """

    dsn = dsnparse.parse(db_uri)
    if len(dsn.schemes) > 1:
        raise NotImplementedError("No support for multiple schemes")
    (scheme,) = dsn.schemes

    if scheme == "snowflake":
        database, schema = dsn.paths
        try:
            warehouse = dsn.query["warehouse"]
        except KeyError:
            raise ValueError(
                "Must provide warehouse. Format: 'snowflake://<user>:<pass>@<account>/<database>/<schema>?warehouse=<warehouse>'"
            )
        return Snowflake(dsn.host, dsn.user, dsn.password, warehouse=warehouse, database=database, schema=schema)

    if len(dsn.paths) == 0:
        path = ""
    elif len(dsn.paths) == 1:
        (path,) = dsn.paths
    else:
        raise ValueError("Bad value for uri, too many paths: %s" % db_uri)

    if scheme == "postgres":
        return Postgres(dsn.host, dsn.port, path, dsn.user, dsn.password, thread_count=thread_count)
    elif scheme == "mysql":
        return MySQL(dsn.host, dsn.port, path, dsn.user, dsn.password, thread_count=thread_count)
    elif scheme == "mssql":
        return MsSQL(dsn.host, dsn.port, path, dsn.user, dsn.password, thread_count=thread_count)
    elif scheme == "bigquery":
        return BigQuery(dsn.host, path)
    elif scheme == "redshift":
        return Redshift(dsn.host, dsn.port, path, dsn.user, dsn.password, thread_count=thread_count)
    elif scheme == "oracle":
        return Oracle(dsn.host, dsn.port, path, dsn.user, dsn.password, thread_count=thread_count)
    elif scheme == "presto":
        return Presto(dsn.host, dsn.port, path, dsn.user, dsn.password)

    raise NotImplementedError(f"Scheme {dsn.scheme} currently not supported")
