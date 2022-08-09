import math
import sys
import logging
from typing import Dict, Tuple, Optional, Sequence, Type, List
from functools import lru_cache, wraps
from concurrent.futures import ThreadPoolExecutor
import threading
from abc import abstractmethod

from data_diff.utils import CaseAwareMapping, is_uuid, safezip
from .database_types import (
    AbstractDatabase,
    ColType,
    Integer,
    Decimal,
    Float,
    ColType_UUID,
    Native_UUID,
    String_Alphanum,
    String_UUID,
    TemporalType,
    UnknownColType,
    Text,
    DbTime,
)
from data_diff.sql import DbPath, SqlOrStr, Compiler, Explain, Select, TableName

logger = logging.getLogger("database")


def parse_table_name(t):
    return tuple(t.split("."))


def import_helper(package: str = None, text=""):
    def dec(f):
        @wraps(f)
        def _inner():
            try:
                return f()
            except ModuleNotFoundError as e:
                s = text
                if package:
                    s += f"You can install it using 'pip install data-diff[{package}]'."
                raise ModuleNotFoundError(f"{e}\n\n{s}\n")

        return _inner

    return dec


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


class Database(AbstractDatabase):
    """Base abstract class for databases.

    Used for providing connection code and implementation specific SQL utilities.

    Instanciated using :meth:`~data_diff.connect_to_uri`
    """

    TYPE_CLASSES: Dict[str, type] = {}
    default_schema: str = None

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
            if res_type.__args__ == (int,) or res_type.__args__ == (str,):
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

    def _parse_type_repr(self, type_repr: str) -> Optional[Type[ColType]]:
        return self.TYPE_CLASSES.get(type_repr)

    def _parse_type(
        self,
        table_path: DbPath,
        col_name: str,
        type_repr: str,
        datetime_precision: int = None,
        numeric_precision: int = None,
        numeric_scale: int = None,
    ) -> ColType:
        """ """

        cls = self._parse_type_repr(type_repr)
        if not cls:
            return UnknownColType(type_repr)

        if issubclass(cls, TemporalType):
            return cls(
                precision=datetime_precision if datetime_precision is not None else DEFAULT_DATETIME_PRECISION,
                rounds=self.ROUNDS_ON_PREC_LOSS,
            )

        elif issubclass(cls, Integer):
            return cls()

        elif issubclass(cls, Decimal):
            if numeric_scale is None:
                numeric_scale = 0  # Needed for Oracle.
                # raise ValueError(
                #     f"{self.name}: Unexpected numeric_scale is NULL, for column {'.'.join(table_path)}.{col_name} of type {type_repr}."
                # )
            return cls(precision=numeric_scale)

        elif issubclass(cls, Float):
            # assert numeric_scale is None
            return cls(
                precision=self._convert_db_precision_to_digits(
                    numeric_precision if numeric_precision is not None else DEFAULT_NUMERIC_PRECISION
                )
            )

        elif issubclass(cls, (Text, Native_UUID)):
            return cls()

        raise TypeError(f"Parsing {type_repr} returned an unknown type '{cls}'.")

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)

        return (
            "SELECT column_name, data_type, datetime_precision, numeric_precision, numeric_scale FROM information_schema.columns "
            f"WHERE table_name = '{table}' AND table_schema = '{schema}'"
        )

    def query_table_schema(self, path: DbPath) -> Dict[str, tuple]:
        rows = self.query(self.select_table_schema(path), list)
        if not rows:
            raise RuntimeError(f"{self.name}: Table '{'.'.join(path)}' does not exist, or has no columns")

        d = {r[0]: r for r in rows}
        assert len(d) == len(rows)
        return d

    def _process_table_schema(self, path: DbPath, raw_schema: Dict[str, tuple], filter_columns: Sequence[str]):
        accept = {i.lower() for i in filter_columns}

        col_dict = {row[0]: self._parse_type(path, *row) for name, row in raw_schema.items() if name.lower() in accept}

        self._refine_coltypes(path, col_dict)

        # Return a dict of form {name: type} after normalization
        return col_dict

    def _refine_coltypes(self, table_path: DbPath, col_dict: Dict[str, ColType]):
        "Refine the types in the column dict, by querying the database for a sample of their values"

        text_columns = [k for k, v in col_dict.items() if isinstance(v, Text)]
        if not text_columns:
            return

        fields = [self.normalize_uuid(c, String_UUID()) for c in text_columns]
        samples_by_row = self.query(Select(fields, TableName(table_path), limit=16), list)
        if not samples_by_row:
            raise ValueError(f"Table {table_path} is empty.")

        samples_by_col = list(zip(*samples_by_row))

        for col_name, samples in safezip(text_columns, samples_by_col):
            uuid_samples = [s for s in samples if s and is_uuid(s)]

            if uuid_samples:
                if len(uuid_samples) != len(samples):
                    logger.warning(
                        f"Mixed UUID/Non-UUID values detected in column {'.'.join(table_path)}.{col_name}, disabling UUID support."
                    )
                else:
                    assert col_name in col_dict
                    col_dict[col_name] = String_UUID()
                    continue

            alphanum_samples = [s for s in samples if s and String_Alphanum.test_value(s)]
            if alphanum_samples:
                if len(alphanum_samples) != len(samples):
                    logger.warning(
                        f"Mixed Alphanum/Non-Alphanum values detected in column {'.'.join(table_path)}.{col_name}, disabling Alphanum support."
                    )
                else:
                    assert col_name in col_dict
                    lens = set(map(len, alphanum_samples))
                    if len(lens) > 1:
                        logger.warning(
                            f"Mixed Alphanum lengths detected in column {'.'.join(table_path)}.{col_name}, disabling Alphanum support."
                        )
                    else:
                        (length,) = lens
                        col_dict[col_name] = String_Alphanum(length=length)
                        continue

    # @lru_cache()
    # def get_table_schema(self, path: DbPath) -> Dict[str, ColType]:
    #     return self.query_table_schema(path)

    def _normalize_table_path(self, path: DbPath) -> DbPath:
        if len(path) == 1:
            if self.default_schema:
                return self.default_schema, path[0]
        elif len(path) != 2:
            raise ValueError(f"{self.name}: Bad table path for {self}: '{'.'.join(path)}'. Expected form: schema.table")

        return path

    def parse_table_name(self, name: str) -> DbPath:
        return parse_table_name(name)

    def offset_limit(self, offset: Optional[int] = None, limit: Optional[int] = None):
        if offset:
            raise NotImplementedError("No support for OFFSET in query")

        return f"LIMIT {limit}"

    def concat(self, l: List[str]) -> str:
        joined_exprs = ", ".join(l)
        return f"concat({joined_exprs})"

    def timestamp_value(self, t: DbTime) -> str:
        return "'%s'" % t.isoformat()

    def normalize_uuid(self, value: str, coltype: ColType_UUID) -> str:
        if isinstance(coltype, String_UUID):
            return f"TRIM({value})"
        return self.to_string(value)


class ThreadedDatabase(Database):
    """Access the database through singleton threads.

    Used for database connectors that do not support sharing their connection between different threads.
    """

    def __init__(self, thread_count=1):
        self._init_error = None
        self._queue = ThreadPoolExecutor(thread_count, initializer=self.set_conn)
        self.thread_local = threading.local()
        logger.info(f"[{self.name}] Starting a threadpool, size={thread_count}.")

    def set_conn(self):
        assert not hasattr(self.thread_local, "conn")
        try:
            self.thread_local.conn = self.create_connection()
        except ModuleNotFoundError as e:
            self._init_error = e

    def _query(self, sql_code: str):
        r = self._queue.submit(self._query_in_worker, sql_code)
        return r.result()

    def _query_in_worker(self, sql_code: str):
        "This method runs in a worker thread"
        if self._init_error:
            raise self._init_error
        return _query_conn(self.thread_local.conn, sql_code)

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
