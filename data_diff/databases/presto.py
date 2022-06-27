import re

from .database_types import *
from .base import Database, import_helper, _query_conn
from .base import (
    MD5_HEXDIGITS,
    CHECKSUM_HEXDIGITS,
    TIMESTAMP_PRECISION_POS,
    DEFAULT_DATETIME_PRECISION,
    DEFAULT_NUMERIC_PRECISION,
)


@import_helper("presto")
def import_presto():
    import prestodb

    return prestodb


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
        c = self._conn.cursor()
        c.execute(sql_code)
        if sql_code.lower().startswith("select"):
            return c.fetchall()
        # Required for the query to actually run 🤯
        if re.match(r"(insert|create|truncate|drop)", sql_code, re.IGNORECASE):
            return c.fetchone()

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
            f"SELECT column_name, data_type, 3 as datetime_precision, 3 as numeric_precision FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = '{table}' AND table_schema = '{schema}'"
        )

    def _parse_type(
        self, col_name: str, type_repr: str, datetime_precision: int = None, numeric_precision: int = None
    ) -> ColType:
        timestamp_regexps = {
            r"timestamp\((\d)\)": Timestamp,
            r"timestamp\((\d)\) with time zone": TimestampTZ,
        }
        for regexp, t_cls in timestamp_regexps.items():
            m = re.match(regexp + "$", type_repr)
            if m:
                datetime_precision = int(m.group(1))
                return t_cls(
                    precision=datetime_precision if datetime_precision is not None else DEFAULT_DATETIME_PRECISION,
                    rounds=self.ROUNDS_ON_PREC_LOSS,
                )

        number_regexps = {r"decimal\((\d+),(\d+)\)": Decimal}
        for regexp, n_cls in number_regexps.items():
            m = re.match(regexp + "$", type_repr)
            if m:
                prec, scale = map(int, m.groups())
                return n_cls(scale)

        n_cls = self.NUMERIC_TYPES.get(type_repr)
        if n_cls:
            if issubclass(n_cls, Integer):
                assert numeric_precision is not None
                return n_cls(0)

            assert issubclass(n_cls, Float)
            return n_cls(
                precision=self._convert_db_precision_to_digits(
                    numeric_precision if numeric_precision is not None else DEFAULT_NUMERIC_PRECISION
                )
            )

        return UnknownColType(type_repr)
