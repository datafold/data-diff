from datetime import datetime
import math
import sys
import logging
from typing import Any, Callable, Dict, Generator, Tuple, Optional, Sequence, Type, List, Union, TypeVar, TYPE_CHECKING
from functools import partial, wraps
from concurrent.futures import ThreadPoolExecutor
import threading
from abc import abstractmethod
from uuid import UUID
import decimal

from runtype import dataclass

from ..utils import is_uuid, safezip, Self
from ..queries import Expr, Compiler, table, Select, SKIP, Explain, Code, this
from ..queries.ast_classes import Random
from ..abcs.database_types import (
    AbstractDatabase,
    Array,
    Struct,
    AbstractDialect,
    AbstractTable,
    ColType,
    Integer,
    Decimal,
    Float,
    Native_UUID,
    String_UUID,
    String_Alphanum,
    String_VaryingAlphanum,
    TemporalType,
    UnknownColType,
    TimestampTZ,
    Text,
    DbTime,
    DbPath,
    Boolean,
    JSON,
)
from ..abcs.mixins import Compilable
from ..abcs.mixins import (
    AbstractMixin_Schema,
    AbstractMixin_RandomSample,
    AbstractMixin_NormalizeValue,
    AbstractMixin_OptimizerHints,
)
from ..bound_exprs import bound_table

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
                    s += f"Please complete setup by running: pip install 'data_diff[{package}]'."
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


class ThreadLocalInterpreter:
    """An interpeter used to execute a sequence of queries within the same thread and cursor.

    Useful for cursor-sensitive operations, such as creating a temporary table.
    """

    def __init__(self, compiler: Compiler, gen: Generator):
        self.gen = gen
        self.compiler = compiler

    def apply_queries(self, callback: Callable[[str], Any]):
        q: Expr = next(self.gen)
        while True:
            sql = self.compiler.compile(q)
            logger.debug("Running SQL (%s-TL): %s", self.compiler.database.name, sql)
            try:
                try:
                    res = callback(sql) if sql is not SKIP else SKIP
                except Exception as e:
                    q = self.gen.throw(type(e), e)
                else:
                    q = self.gen.send(res)
            except StopIteration:
                break


def apply_query(callback: Callable[[str], Any], sql_code: Union[str, ThreadLocalInterpreter]) -> list:
    if isinstance(sql_code, ThreadLocalInterpreter):
        return sql_code.apply_queries(callback)
    else:
        return callback(sql_code)


class Mixin_Schema(AbstractMixin_Schema):
    def table_information(self) -> Compilable:
        return table("information_schema", "tables")

    def list_tables(self, table_schema: str, like: Compilable = None) -> Compilable:
        return (
            self.table_information()
            .where(
                this.table_schema == table_schema,
                this.table_name.like(like) if like is not None else SKIP,
                this.table_type == "BASE TABLE",
            )
            .select(this.table_name)
        )


class Mixin_RandomSample(AbstractMixin_RandomSample):
    def random_sample_n(self, tbl: AbstractTable, size: int) -> AbstractTable:
        # TODO use a more efficient algorithm, when the table count is known
        return tbl.order_by(Random()).limit(size)

    def random_sample_ratio_approx(self, tbl: AbstractTable, ratio: float) -> AbstractTable:
        return tbl.where(Random() < ratio)


class Mixin_OptimizerHints(AbstractMixin_OptimizerHints):
    def optimizer_hints(self, hints: str) -> str:
        return f"/*+ {hints} */ "


class BaseDialect(AbstractDialect):
    SUPPORTS_PRIMARY_KEY = False
    SUPPORTS_INDEXES = False
    TYPE_CLASSES: Dict[str, type] = {}
    MIXINS = frozenset()

    PLACEHOLDER_TABLE = None  # Used for Oracle

    def offset_limit(self, offset: Optional[int] = None, limit: Optional[int] = None):
        if offset:
            raise NotImplementedError("No support for OFFSET in query")

        return f"LIMIT {limit}"

    def concat(self, items: List[str]) -> str:
        assert len(items) > 1
        joined_exprs = ", ".join(items)
        return f"concat({joined_exprs})"

    def to_comparable(self, value: str, coltype: ColType) -> str:
        """Ensure that the expression is comparable in ``IS DISTINCT FROM``."""
        return value

    def is_distinct_from(self, a: str, b: str) -> str:
        return f"{a} is distinct from {b}"

    def timestamp_value(self, t: DbTime) -> str:
        return f"'{t.isoformat()}'"

    def random(self) -> str:
        return "random()"

    def current_timestamp(self) -> str:
        return "current_timestamp()"

    def explain_as_text(self, query: str) -> str:
        return f"EXPLAIN {query}"

    def _constant_value(self, v):
        if v is None:
            return "NULL"
        elif isinstance(v, str):
            return f"'{v}'"
        elif isinstance(v, datetime):
            return self.timestamp_value(v)
        elif isinstance(v, UUID):
            return f"'{v}'"
        elif isinstance(v, decimal.Decimal):
            return str(v)
        elif isinstance(v, bytearray):
            return f"'{v.decode()}'"
        elif isinstance(v, Code):
            return v.code
        return repr(v)

    def constant_values(self, rows) -> str:
        values = ", ".join("(%s)" % ", ".join(self._constant_value(v) for v in row) for row in rows)
        return f"VALUES {values}"

    def type_repr(self, t) -> str:
        if isinstance(t, str):
            return t
        elif isinstance(t, TimestampTZ):
            return f"TIMESTAMP({min(t.precision, DEFAULT_DATETIME_PRECISION)})"
        return {
            int: "INT",
            str: "VARCHAR",
            bool: "BOOLEAN",
            float: "FLOAT",
            datetime: "TIMESTAMP",
        }[t]

    def _parse_type_repr(self, type_repr: str) -> Optional[Type[ColType]]:
        return self.TYPE_CLASSES.get(type_repr)

    def parse_type(
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
        if cls is None:
            return UnknownColType(type_repr)

        if issubclass(cls, TemporalType):
            return cls(
                precision=datetime_precision if datetime_precision is not None else DEFAULT_DATETIME_PRECISION,
                rounds=self.ROUNDS_ON_PREC_LOSS,
            )

        elif issubclass(cls, Integer):
            return cls()

        elif issubclass(cls, Boolean):
            return cls()

        elif issubclass(cls, Decimal):
            if numeric_scale is None:
                numeric_scale = 0  # Needed for Oracle.
            return cls(precision=numeric_scale)

        elif issubclass(cls, Float):
            # assert numeric_scale is None
            return cls(
                precision=self._convert_db_precision_to_digits(
                    numeric_precision if numeric_precision is not None else DEFAULT_NUMERIC_PRECISION
                )
            )

        elif issubclass(cls, (JSON, Array, Struct, Text, Native_UUID)):
            return cls()

        raise TypeError(f"Parsing {type_repr} returned an unknown type '{cls}'.")

    def _convert_db_precision_to_digits(self, p: int) -> int:
        """Convert from binary precision, used by floats, to decimal precision."""
        # See: https://en.wikipedia.org/wiki/Single-precision_floating-point_format
        return math.floor(math.log(2**p, 10))

    @classmethod
    def load_mixins(cls, *abstract_mixins) -> "Self":
        mixins = {m for m in cls.MIXINS if issubclass(m, abstract_mixins)}

        class _DialectWithMixins(cls, *mixins, *abstract_mixins):
            pass

        _DialectWithMixins.__name__ = cls.__name__
        return _DialectWithMixins()


T = TypeVar("T", bound=BaseDialect)


@dataclass
class QueryResult:
    rows: list
    columns: list = None

    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, i):
        return self.rows[i]


class Database(AbstractDatabase[T]):
    """Base abstract class for databases.

    Used for providing connection code and implementation specific SQL utilities.

    Instanciated using :meth:`~data_diff.sqeleton.connect`
    """

    default_schema: str = None
    SUPPORTS_ALPHANUMS = True
    SUPPORTS_UNIQUE_CONSTAINT = False

    CONNECT_URI_KWPARAMS = []

    _interactive = False
    is_closed = False

    @property
    def name(self):
        return type(self).__name__

    def compile(self, sql_ast):
        compiler = Compiler(self)
        return compiler.compile(sql_ast)

    def query(self, sql_ast: Union[Expr, Generator], res_type: type = None):
        """Query the given SQL code/AST, and attempt to convert the result to type 'res_type'

        If given a generator, it will execute all the yielded sql queries with the same thread and cursor.
        The results of the queries a returned by the `yield` stmt (using the .send() mechanism).
        It's a cleaner approach than exposing cursors, but may not be enough in all cases.
        """

        compiler = Compiler(self)
        if isinstance(sql_ast, Generator):
            sql_code = ThreadLocalInterpreter(compiler, sql_ast)
        elif isinstance(sql_ast, list):
            for i in sql_ast[:-1]:
                self.query(i)
            return self.query(sql_ast[-1], res_type)
        else:
            if isinstance(sql_ast, str):
                sql_code = sql_ast
            else:
                if res_type is None:
                    res_type = sql_ast.type
                sql_code = compiler.compile(sql_ast)
                if sql_code is SKIP:
                    return SKIP

            logger.debug("Running SQL (%s): %s", self.name, sql_code)

        if self._interactive and isinstance(sql_ast, Select):
            explained_sql = compiler.compile(Explain(sql_ast))
            explain = self._query(explained_sql)
            for row in explain:
                # Most returned a 1-tuple. Presto returns a string
                if isinstance(row, tuple):
                    (row,) = row
                logger.debug("EXPLAIN: %s", row)
            answer = input("Continue? [y/n] ")
            if answer.lower() not in ["y", "yes"]:
                sys.exit(1)

        res = self._query(sql_code)
        if res_type is list:
            return list(res)
        elif res_type is int:
            if not res:
                raise ValueError("Query returned 0 rows, expected 1")
            row = _one(res)
            if not row:
                raise ValueError("Row is empty, expected 1 column")
            res = _one(row)
            if res is None:  # May happen due to sum() of 0 items
                return None
            return int(res)
        elif res_type is datetime:
            res = _one(_one(res))
            if isinstance(res, str):
                res = datetime.fromisoformat(res[:23])  # TODO use a better parsing method
            return res
        elif res_type is tuple:
            assert len(res) == 1, (sql_code, res)
            return res[0]
        elif getattr(res_type, "__origin__", None) is list and len(res_type.__args__) == 1:
            if res_type.__args__ in ((int,), (str,)):
                return [_one(row) for row in res]
            elif res_type.__args__ in [(Tuple,), (tuple,)]:
                return [tuple(row) for row in res]
            elif res_type.__args__ == (dict,):
                return [dict(safezip(res.columns, row)) for row in res]
            else:
                raise ValueError(res_type)
        return res

    def enable_interactive(self):
        self._interactive = True

    def select_table_schema(self, path: DbPath) -> str:
        """Provide SQL for selecting the table schema as (name, type, date_prec, num_prec)"""
        schema, name = self._normalize_table_path(path)

        return (
            "SELECT column_name, data_type, datetime_precision, numeric_precision, numeric_scale "
            "FROM information_schema.columns "
            f"WHERE table_name = '{name}' AND table_schema = '{schema}'"
        )

    def query_table_schema(self, path: DbPath) -> Dict[str, tuple]:
        rows = self.query(self.select_table_schema(path), list)
        if not rows:
            raise RuntimeError(f"{self.name}: Table '{'.'.join(path)}' does not exist, or has no columns")

        d = {r[0]: r for r in rows}
        assert len(d) == len(rows)
        return d

    def select_table_unique_columns(self, path: DbPath) -> str:
        schema, name = self._normalize_table_path(path)

        return (
            "SELECT column_name "
            "FROM information_schema.key_column_usage "
            f"WHERE table_name = '{name}' AND table_schema = '{schema}'"
        )

    def query_table_unique_columns(self, path: DbPath) -> List[str]:
        if not self.SUPPORTS_UNIQUE_CONSTAINT:
            raise NotImplementedError("This database doesn't support 'unique' constraints")
        res = self.query(self.select_table_unique_columns(path), List[str])
        return list(res)

    def _process_table_schema(
        self, path: DbPath, raw_schema: Dict[str, tuple], filter_columns: Sequence[str] = None, where: str = None
    ):
        if filter_columns is None:
            filtered_schema = raw_schema
        else:
            accept = {i.lower() for i in filter_columns}
            filtered_schema = {name: row for name, row in raw_schema.items() if name.lower() in accept}

        col_dict = {row[0]: self.dialect.parse_type(path, *row) for _name, row in filtered_schema.items()}

        self._refine_coltypes(path, col_dict, where)

        # Return a dict of form {name: type} after normalization
        return col_dict

    def _refine_coltypes(self, table_path: DbPath, col_dict: Dict[str, ColType], where: str = None, sample_size=64):
        """Refine the types in the column dict, by querying the database for a sample of their values

        'where' restricts the rows to be sampled.
        """

        text_columns = [k for k, v in col_dict.items() if isinstance(v, Text)]
        if not text_columns:
            return

        if isinstance(self.dialect, AbstractMixin_NormalizeValue):
            fields = [Code(self.dialect.normalize_uuid(self.dialect.quote(c), String_UUID())) for c in text_columns]
        else:
            fields = this[text_columns]

        samples_by_row = self.query(
            table(*table_path).select(*fields).where(Code(where) if where else SKIP).limit(sample_size), list
        )
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

            if self.SUPPORTS_ALPHANUMS:  # Anything but MySQL (so far)
                alphanum_samples = [s for s in samples if String_Alphanum.test_value(s)]
                if alphanum_samples:
                    if len(alphanum_samples) != len(samples):
                        logger.debug(
                            f"Mixed Alphanum/Non-Alphanum values detected in column {'.'.join(table_path)}.{col_name}. It cannot be used as a key."
                        )
                    else:
                        assert col_name in col_dict
                        col_dict[col_name] = String_VaryingAlphanum()

    # @lru_cache()
    # def get_table_schema(self, path: DbPath) -> Dict[str, ColType]:
    #     return self.query_table_schema(path)

    def _normalize_table_path(self, path: DbPath) -> DbPath:
        if len(path) == 1:
            return self.default_schema, path[0]
        elif len(path) == 2:
            return path

        raise ValueError(f"{self.name}: Bad table path for {self}: '{'.'.join(path)}'. Expected form: schema.table")

    def parse_table_name(self, name: str) -> DbPath:
        return parse_table_name(name)

    def _query_cursor(self, c, sql_code: str) -> QueryResult:
        assert isinstance(sql_code, str), sql_code
        try:
            c.execute(sql_code)
            if sql_code.lower().startswith(("select", "explain", "show")):
                columns = [col[0] for col in c.description]
                return QueryResult(c.fetchall(), columns)
        except Exception as _e:
            # logger.exception(e)
            # logger.error(f'Caused by SQL: {sql_code}')
            raise

    def _query_conn(self, conn, sql_code: Union[str, ThreadLocalInterpreter]) -> QueryResult:
        c = conn.cursor()
        callback = partial(self._query_cursor, c)
        return apply_query(callback, sql_code)

    def close(self):
        self.is_closed = True
        return super().close()

    def list_tables(self, tables_like, schema=None):
        return self.query(self.dialect.list_tables(schema or self.default_schema, tables_like))

    def table(self, *path, **kw):
        return bound_table(self, path, **kw)

    @classmethod
    def load_mixins(cls, *abstract_mixins) -> type:
        class _DatabaseWithMixins(cls):
            dialect = cls.dialect.load_mixins(*abstract_mixins)

        _DatabaseWithMixins.__name__ = cls.__name__
        return _DatabaseWithMixins


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
        except Exception as e:
            self._init_error = e

    def _query(self, sql_code: Union[str, ThreadLocalInterpreter]) -> QueryResult:
        r = self._queue.submit(self._query_in_worker, sql_code)
        return r.result()

    def _query_in_worker(self, sql_code: Union[str, ThreadLocalInterpreter]):
        "This method runs in a worker thread"
        if self._init_error:
            raise self._init_error
        return self._query_conn(self.thread_local.conn, sql_code)

    @abstractmethod
    def create_connection(self):
        "Return a connection instance, that supports the .cursor() method."

    def close(self):
        super().close()
        self._queue.shutdown()

    @property
    def is_autocommit(self) -> bool:
        return False


CHECKSUM_HEXDIGITS = 15  # Must be 15 or lower, otherwise SUM() overflows
MD5_HEXDIGITS = 32

_CHECKSUM_BITSIZE = CHECKSUM_HEXDIGITS << 2
CHECKSUM_MASK = (2**_CHECKSUM_BITSIZE) - 1

DEFAULT_DATETIME_PRECISION = 6
DEFAULT_NUMERIC_PRECISION = 24

TIMESTAMP_PRECISION_POS = 20  # len("2022-06-03 12:24:35.") == 20
