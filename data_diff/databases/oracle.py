from typing import Any, Dict, List, Optional

import attrs

from data_diff.utils import match_regexps
from data_diff.abcs.database_types import (
    Decimal,
    Float,
    Text,
    DbPath,
    TemporalType,
    ColType,
    DbTime,
    ColType_UUID,
    Timestamp,
    TimestampTZ,
    FractionalType,
)
from data_diff.abcs.mixins import AbstractMixin_MD5, AbstractMixin_NormalizeValue, AbstractMixin_Schema
from data_diff.abcs.compiler import Compilable
from data_diff.queries.api import this, table, SKIP
from data_diff.databases.base import (
    BaseDialect,
    Mixin_OptimizerHints,
    ThreadedDatabase,
    import_helper,
    ConnectError,
    QueryError,
    Mixin_RandomSample,
)
from data_diff.databases.base import TIMESTAMP_PRECISION_POS

SESSION_TIME_ZONE = None  # Changed by the tests


@import_helper("oracle")
def import_oracle():
    import oracledb

    return oracledb


@attrs.define(frozen=False)
class Mixin_MD5(AbstractMixin_MD5):
    def md5_as_int(self, s: str) -> str:
        # standard_hash is faster than DBMS_CRYPTO.Hash
        # TODO: Find a way to use UTL_RAW.CAST_TO_BINARY_INTEGER ?
        return f"to_number(substr(standard_hash({s}, 'MD5'), 18), 'xxxxxxxxxxxxxxx')"


@attrs.define(frozen=False)
class Mixin_NormalizeValue(AbstractMixin_NormalizeValue):
    def normalize_uuid(self, value: str, coltype: ColType_UUID) -> str:
        # Cast is necessary for correct MD5 (trimming not enough)
        return f"CAST(TRIM({value}) AS VARCHAR(36))"

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            return f"to_char(cast({value} as timestamp({coltype.precision})), 'YYYY-MM-DD HH24:MI:SS.FF6')"

        if coltype.precision > 0:
            truncated = f"to_char({value}, 'YYYY-MM-DD HH24:MI:SS.FF{coltype.precision}')"
        else:
            truncated = f"to_char({value}, 'YYYY-MM-DD HH24:MI:SS.')"
        return f"RPAD({truncated}, {TIMESTAMP_PRECISION_POS+6}, '0')"

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        # FM999.9990
        format_str = "FM" + "9" * (38 - coltype.precision)
        if coltype.precision:
            format_str += "0." + "9" * (coltype.precision - 1) + "0"
        return f"to_char({value}, '{format_str}')"


@attrs.define(frozen=False)
class Mixin_Schema(AbstractMixin_Schema):
    def list_tables(self, table_schema: str, like: Compilable = None) -> Compilable:
        return (
            table("ALL_TABLES")
            .where(
                this.OWNER == table_schema,
                this.TABLE_NAME.like(like) if like is not None else SKIP,
            )
            .select(table_name=this.TABLE_NAME)
        )


@attrs.define(frozen=False)
class Dialect(
    BaseDialect,
    Mixin_Schema,
    Mixin_OptimizerHints,
    Mixin_MD5,
    Mixin_NormalizeValue,
    AbstractMixin_MD5,
    AbstractMixin_NormalizeValue,
):
    name = "Oracle"
    SUPPORTS_PRIMARY_KEY = True
    SUPPORTS_INDEXES = True
    TYPE_CLASSES: Dict[str, type] = {
        "NUMBER": Decimal,
        "FLOAT": Float,
        # Text
        "CHAR": Text,
        "NCHAR": Text,
        "NVARCHAR2": Text,
        "VARCHAR2": Text,
        "DATE": Timestamp,
    }
    ROUNDS_ON_PREC_LOSS = True
    PLACEHOLDER_TABLE = "DUAL"

    def quote(self, s: str):
        return f'"{s}"'

    def to_string(self, s: str):
        return f"cast({s} as varchar(1024))"

    def offset_limit(
        self, offset: Optional[int] = None, limit: Optional[int] = None, has_order_by: Optional[bool] = None
    ) -> str:
        if offset:
            raise NotImplementedError("No support for OFFSET in query")

        return f"FETCH NEXT {limit} ROWS ONLY"

    def concat(self, items: List[str]) -> str:
        joined_exprs = " || ".join(items)
        return f"({joined_exprs})"

    def timestamp_value(self, t: DbTime) -> str:
        return "timestamp '%s'" % t.isoformat(" ")

    def random(self) -> str:
        return "dbms_random.value"

    def is_distinct_from(self, a: str, b: str) -> str:
        return f"DECODE({a}, {b}, 1, 0) = 0"

    def type_repr(self, t) -> str:
        try:
            return {
                str: "VARCHAR(1024)",
            }[t]
        except KeyError:
            return super().type_repr(t)

    def constant_values(self, rows) -> str:
        return " UNION ALL ".join(
            "SELECT %s FROM DUAL" % ", ".join(self._constant_value(v) for v in row) for row in rows
        )

    def explain_as_text(self, query: str) -> str:
        raise NotImplementedError("Explain not yet implemented in Oracle")

    def parse_type(
        self,
        table_path: DbPath,
        col_name: str,
        type_repr: str,
        datetime_precision: int = None,
        numeric_precision: int = None,
        numeric_scale: int = None,
    ) -> ColType:
        regexps = {
            r"TIMESTAMP\((\d)\) WITH LOCAL TIME ZONE": Timestamp,
            r"TIMESTAMP\((\d)\) WITH TIME ZONE": TimestampTZ,
            r"TIMESTAMP\((\d)\)": Timestamp,
        }

        for m, t_cls in match_regexps(regexps, type_repr):
            precision = int(m.group(1))
            return t_cls(precision=precision, rounds=self.ROUNDS_ON_PREC_LOSS)

        return super().parse_type(table_path, col_name, type_repr, datetime_precision, numeric_precision, numeric_scale)

    def set_timezone_to_utc(self) -> str:
        return "ALTER SESSION SET TIME_ZONE = 'UTC'"

    def current_timestamp(self) -> str:
        return "LOCALTIMESTAMP"


@attrs.define(frozen=False, init=False, kw_only=True)
class Oracle(ThreadedDatabase):
    dialect = Dialect()
    CONNECT_URI_HELP = "oracle://<user>:<password>@<host>/<database>"
    CONNECT_URI_PARAMS = ["database?"]

    kwargs: Dict[str, Any]
    _oracle: Any

    def __init__(self, *, host, database, thread_count, **kw):
        super().__init__(thread_count=thread_count)
        self.kwargs = dict(dsn=f"{host}/{database}" if database else host, **kw)
        self.default_schema = kw.get("user").upper()
        self._oracle = None

    def create_connection(self):
        self._oracle = import_oracle()
        try:
            c = self._oracle.connect(**self.kwargs)
            if SESSION_TIME_ZONE:
                c.cursor().execute(f"ALTER SESSION SET TIME_ZONE = '{SESSION_TIME_ZONE}'")
            return c
        except Exception as e:
            raise ConnectError(*e.args) from e

    def _query_cursor(self, c, sql_code: str):
        try:
            return super()._query_cursor(c, sql_code)
        except self._oracle.DatabaseError as e:
            raise QueryError(e)

    def select_table_schema(self, path: DbPath) -> str:
        schema, name = self._normalize_table_path(path)

        return (
            f"SELECT column_name, data_type, 6 as datetime_precision, data_precision as numeric_precision, data_scale as numeric_scale"
            f" FROM ALL_TAB_COLUMNS WHERE table_name = '{name}' AND owner = '{schema}'"
        )
