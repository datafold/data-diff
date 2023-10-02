from functools import partial
import re
from typing import Any

import attrs

from data_diff.utils import match_regexps

from data_diff.abcs.database_types import (
    Timestamp,
    TimestampTZ,
    Integer,
    Float,
    Text,
    FractionalType,
    DbPath,
    DbTime,
    Decimal,
    ColType,
    ColType_UUID,
    TemporalType,
    Boolean,
)
from data_diff.abcs.mixins import AbstractMixin_MD5, AbstractMixin_NormalizeValue
from data_diff.databases.base import (
    BaseDialect,
    Database,
    import_helper,
    ThreadLocalInterpreter,
    Mixin_Schema,
    Mixin_RandomSample,
)
from data_diff.databases.base import (
    MD5_HEXDIGITS,
    CHECKSUM_HEXDIGITS,
    TIMESTAMP_PRECISION_POS,
)


def query_cursor(c, sql_code):
    c.execute(sql_code)
    if sql_code.lower().startswith("select"):
        return c.fetchall()
    # Required for the query to actually run 🤯
    if re.match(r"(insert|create|truncate|drop|explain)", sql_code, re.IGNORECASE):
        return c.fetchone()


@import_helper("presto")
def import_presto():
    import prestodb

    return prestodb


@attrs.define(frozen=False)
class Mixin_MD5(AbstractMixin_MD5):
    def md5_as_int(self, s: str) -> str:
        return f"cast(from_base(substr(to_hex(md5(to_utf8({s}))), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16) as decimal(38, 0))"


@attrs.define(frozen=False)
class Mixin_NormalizeValue(AbstractMixin_NormalizeValue):
    def normalize_uuid(self, value: str, coltype: ColType_UUID) -> str:
        # Trim doesn't work on CHAR type
        return f"TRIM(CAST({value} AS VARCHAR))"

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        # TODO rounds
        if coltype.rounds:
            s = f"date_format(cast({value} as timestamp(6)), '%Y-%m-%d %H:%i:%S.%f')"
        else:
            s = f"date_format(cast({value} as timestamp(6)), '%Y-%m-%d %H:%i:%S.%f')"

        return f"RPAD(RPAD({s}, {TIMESTAMP_PRECISION_POS+coltype.precision}, '.'), {TIMESTAMP_PRECISION_POS+6}, '0')"

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"cast({value} as decimal(38,{coltype.precision}))")

    def normalize_boolean(self, value: str, _coltype: Boolean) -> str:
        return self.to_string(f"cast ({value} as int)")


class Dialect(
    BaseDialect, Mixin_Schema, Mixin_MD5, Mixin_NormalizeValue, AbstractMixin_MD5, AbstractMixin_NormalizeValue
):
    name = "Presto"
    ROUNDS_ON_PREC_LOSS = True
    TYPE_CLASSES = {
        # Timestamps
        "timestamp with time zone": TimestampTZ,
        "timestamp without time zone": Timestamp,
        "timestamp": Timestamp,
        # Numbers
        "integer": Integer,
        "bigint": Integer,
        "real": Float,
        "double": Float,
        # Text
        "varchar": Text,
        # Boolean
        "boolean": Boolean,
    }

    def explain_as_text(self, query: str) -> str:
        return f"EXPLAIN (FORMAT TEXT) {query}"

    def type_repr(self, t) -> str:
        if isinstance(t, TimestampTZ):
            return f"timestamp with time zone"

        try:
            return {float: "REAL"}[t]
        except KeyError:
            return super().type_repr(t)

    def timestamp_value(self, t: DbTime) -> str:
        return f"timestamp '{t.isoformat(' ')}'"

    def quote(self, s: str):
        return f'"{s}"'

    def to_string(self, s: str):
        return f"cast({s} as varchar)"

    def parse_type(
        self,
        table_path: DbPath,
        col_name: str,
        type_repr: str,
        datetime_precision: int = None,
        numeric_precision: int = None,
        _numeric_scale: int = None,
    ) -> ColType:
        timestamp_regexps = {
            r"timestamp\((\d)\)": Timestamp,
            r"timestamp\((\d)\) with time zone": TimestampTZ,
        }
        for m, t_cls in match_regexps(timestamp_regexps, type_repr):
            precision = int(m.group(1))
            return t_cls(precision=precision, rounds=self.ROUNDS_ON_PREC_LOSS)

        number_regexps = {r"decimal\((\d+),(\d+)\)": Decimal}
        for m, n_cls in match_regexps(number_regexps, type_repr):
            _prec, scale = map(int, m.groups())
            return n_cls(scale)

        string_regexps = {r"varchar\((\d+)\)": Text, r"char\((\d+)\)": Text}
        for m, n_cls in match_regexps(string_regexps, type_repr):
            return n_cls()

        return super().parse_type(table_path, col_name, type_repr, datetime_precision, numeric_precision)

    def set_timezone_to_utc(self) -> str:
        return "SET TIME ZONE '+00:00'"

    def current_timestamp(self) -> str:
        return "current_timestamp"


@attrs.define(frozen=False, init=False, kw_only=True)
class Presto(Database):
    dialect = Dialect()
    CONNECT_URI_HELP = "presto://<user>@<host>/<catalog>/<schema>"
    CONNECT_URI_PARAMS = ["catalog", "schema"]

    _conn: Any

    def __init__(self, **kw):
        super().__init__()
        self.default_schema = "public"
        prestodb = import_presto()

        if kw.get("schema"):
            self.default_schema = kw.get("schema")

        if kw.get("auth") == "basic":  # if auth=basic, add basic authenticator for Presto
            kw["auth"] = prestodb.auth.BasicAuthentication(kw.pop("user"), kw.pop("password"))

        if "cert" in kw:  # if a certificate was specified in URI, verify session with cert
            cert = kw.pop("cert")
            self._conn = prestodb.dbapi.connect(**kw)
            self._conn._http_session.verify = cert
        else:
            self._conn = prestodb.dbapi.connect(**kw)

    def _query(self, sql_code: str) -> list:
        "Uses the standard SQL cursor interface"
        c = self._conn.cursor()

        if isinstance(sql_code, ThreadLocalInterpreter):
            return sql_code.apply_queries(partial(query_cursor, c))

        return query_cursor(c, sql_code)

    def close(self):
        super().close()
        self._conn.close()

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)

        return (
            "SELECT column_name, data_type, 3 as datetime_precision, 3 as numeric_precision, NULL as numeric_scale "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = '{table}' AND table_schema = '{schema}'"
        )

    @property
    def is_autocommit(self) -> bool:
        return False
