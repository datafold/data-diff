import re

from .database_types import *
from .base import ThreadedDatabase, import_helper, ConnectError, QueryError
from .base import DEFAULT_DATETIME_PRECISION, DEFAULT_NUMERIC_PRECISION


@import_helper("oracle")
def import_oracle():
    import cx_Oracle

    return cx_Oracle


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

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        return f"to_char(cast({value} as timestamp({coltype.precision})), 'YYYY-MM-DD HH24:MI:SS.FF6')"

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        # FM999.9990
        format_str = "FM" + "9" * (38 - coltype.precision)
        if coltype.precision:
            format_str += "0." + "9" * (coltype.precision - 1) + "0"
        return f"to_char({value}, '{format_str}')"

    def _parse_type(
        self,
        col_name: str,
        type_repr: str,
        datetime_precision: int = None,
        numeric_precision: int = None,
        numeric_scale: int = None,
    ) -> ColType:
        """ """
        regexps = {
            r"TIMESTAMP\((\d)\) WITH LOCAL TIME ZONE": Timestamp,
            r"TIMESTAMP\((\d)\) WITH TIME ZONE": TimestampTZ,
        }
        for regexp, t_cls in regexps.items():
            m = re.match(regexp + "$", type_repr)
            if m:
                datetime_precision = int(m.group(1))
                return t_cls(
                    precision=datetime_precision if datetime_precision is not None else DEFAULT_DATETIME_PRECISION,
                    rounds=self.ROUNDS_ON_PREC_LOSS,
                )

        n_cls = {
            "NUMBER": Decimal,
            "FLOAT": Float,
        }.get(type_repr, None)
        if n_cls:
            if issubclass(n_cls, Decimal):
                assert numeric_scale is not None, (type_repr, numeric_precision, numeric_scale)
                return n_cls(precision=numeric_scale)

            assert issubclass(n_cls, Float)
            return n_cls(
                precision=self._convert_db_precision_to_digits(
                    numeric_precision if numeric_precision is not None else DEFAULT_NUMERIC_PRECISION
                )
            )

        return UnknownColType(type_repr)
