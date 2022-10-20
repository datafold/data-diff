from typing import Dict, List, Optional

from ..utils import match_regexps
from .database_types import (
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
from .base import ThreadedDatabase, import_helper, ConnectError, QueryError
from .base import TIMESTAMP_PRECISION_POS

SESSION_TIME_ZONE = None  # Changed by the tests


@import_helper("oracle")
def import_oracle():
    import cx_Oracle

    return cx_Oracle


class Oracle(ThreadedDatabase):
    TYPE_CLASSES: Dict[str, type] = {
        "NUMBER": Decimal,
        "FLOAT": Float,
        # Text
        "CHAR": Text,
        "NCHAR": Text,
        "NVARCHAR2": Text,
        "VARCHAR2": Text,
    }
    ROUNDS_ON_PREC_LOSS = True

    def __init__(self, *, host, database, thread_count, **kw):
        self.kwargs = dict(dsn=f"{host}/{database}" if database else host, **kw)

        self.default_schema = kw.get("user")

        super().__init__(thread_count=thread_count)

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

    def md5_to_int(self, s: str) -> str:
        # standard_hash is faster than DBMS_CRYPTO.Hash
        # TODO: Find a way to use UTL_RAW.CAST_TO_BINARY_INTEGER ?
        return f"to_number(substr(standard_hash({s}, 'MD5'), 18), 'xxxxxxxxxxxxxxx')"

    def quote(self, s: str):
        return f"{s}"

    def to_string(self, s: str):
        return f"cast({s} as varchar(1024))"

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)

        return (
            f"SELECT column_name, data_type, 6 as datetime_precision, data_precision as numeric_precision, data_scale as numeric_scale"
            f" FROM ALL_TAB_COLUMNS WHERE table_name = '{table.upper()}' AND owner = '{schema.upper()}'"
        )

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

    def _parse_type(
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

        return super()._parse_type(
            table_path, col_name, type_repr, datetime_precision, numeric_precision, numeric_scale
        )

    def offset_limit(self, offset: Optional[int] = None, limit: Optional[int] = None):
        if offset:
            raise NotImplementedError("No support for OFFSET in query")

        return f"FETCH NEXT {limit} ROWS ONLY"

    def concat(self, l: List[str]) -> str:
        joined_exprs = " || ".join(l)
        return f"({joined_exprs})"

    def timestamp_value(self, t: DbTime) -> str:
        return "timestamp '%s'" % t.isoformat(" ")

    def normalize_uuid(self, value: str, coltype: ColType_UUID) -> str:
        # Cast is necessary for correct MD5 (trimming not enough)
        return f"CAST(TRIM({value}) AS VARCHAR(36))"

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
