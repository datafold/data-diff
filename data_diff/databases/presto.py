import re

from data_diff.utils import match_regexps
from data_diff.queries import ThreadLocalInterpreter

from .database_types import *
from .base import Database, import_helper
from .base import (
    MD5_HEXDIGITS,
    CHECKSUM_HEXDIGITS,
    TIMESTAMP_PRECISION_POS,
)


def query_cursor(c, sql_code):
    c.execute(sql_code)
    if sql_code.lower().startswith("select"):
        return c.fetchall()
    # Required for the query to actually run 🤯
    if re.match(r"(insert|create|truncate|drop)", sql_code, re.IGNORECASE):
        return c.fetchone()


@import_helper("presto")
def import_presto():
    import prestodb

    return prestodb


class Presto(Database):
    default_schema = "public"
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
    }
    ROUNDS_ON_PREC_LOSS = True

    def __init__(self, **kw):
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

    def quote(self, s: str):
        return f'"{s}"'

    def md5_to_int(self, s: str) -> str:
        return f"cast(from_base(substr(to_hex(md5(to_utf8({s}))), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16) as decimal(38, 0))"

    def to_string(self, s: str):
        return f"cast({s} as varchar)"

    def _query(self, sql_code: str) -> list:
        "Uses the standard SQL cursor interface"
        c = self._conn.cursor()

        if isinstance(sql_code, ThreadLocalInterpreter):
            # TODO reuse code from base.py
            g = sql_code.interpret()
            q = next(g)
            while True:
                res = query_cursor(c, q)
                try:
                    q = g.send(res)
                except StopIteration:
                    break
            return

        return query_cursor(c, sql_code)

    def close(self):
        self._conn.close()

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        # TODO
        if coltype.rounds:
            s = f"date_format(cast({value} as timestamp(6)), '%Y-%m-%d %H:%i:%S.%f')"
        else:
            s = f"date_format(cast({value} as timestamp(6)), '%Y-%m-%d %H:%i:%S.%f')"

        return f"RPAD(RPAD({s}, {TIMESTAMP_PRECISION_POS+coltype.precision}, '.'), {TIMESTAMP_PRECISION_POS+6}, '0')"

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"cast({value} as decimal(38,{coltype.precision}))")

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)

        return (
            "SELECT column_name, data_type, 3 as datetime_precision, 3 as numeric_precision "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = '{table}' AND table_schema = '{schema}'"
        )

    def _parse_type(
        self,
        table_path: DbPath,
        col_name: str,
        type_repr: str,
        datetime_precision: int = None,
        numeric_precision: int = None,
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

        return super()._parse_type(table_path, col_name, type_repr, datetime_precision, numeric_precision)

    def normalize_uuid(self, value: str, coltype: ColType_UUID) -> str:
        # Trim doesn't work on CHAR type
        return f"TRIM(CAST({value} AS VARCHAR))"
