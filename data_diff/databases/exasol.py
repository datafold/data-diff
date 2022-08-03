import re

from .database_types import *
from .base import ThreadedDatabase, import_helper, ConnectError, QueryError
from .base import (
    MD5_HEXDIGITS,
    CHECKSUM_HEXDIGITS
)

@import_helper("exasol")
def import_exasol():
    import pyexasol

    return pyexasol


def _query_conn(conn, sql_code: str) -> list:
    c = conn.execute(sql_code)
    if sql_code.lower().startswith("select"):
        return c.fetchall()


class Exasol(ThreadedDatabase):
    TYPE_CLASSES: Dict[str, type] = {
        "DECIMAL": Decimal,
        "DOUBLE PRECISSION": Float,
        # Text
        "CHAR": Text,
    }
    ROUNDS_ON_PREC_LOSS = False

    def __init__(self, *, host, thread_count, **kw):
        self.kwargs = dict(dsn="%s" % host, **kw)

        self.default_schema = kw.get("schema")

        super().__init__(thread_count=thread_count)

    def create_connection(self):
        self._exasol = import_exasol()
        try:
            c = self._exasol.connect(**self.kwargs)
            return c
        except Exception as e:
            raise ConnectError(*e.args) from e

    def _query(self, sql_code: str):
        try:
            r = self._queue.submit(self._query_in_worker, sql_code)
            return r.result()
        except Exception as e:
            raise QueryError(e)

    def _query_in_worker(self, sql_code: str):
        "This method runs in a worker thread"
        if self._init_error:
            raise self._init_error
        return _query_conn(self.thread_local.conn, sql_code)

    def md5_to_int(self, s: str) -> str:
        return f"CAST(TO_NUMBER(SUBSTR(HASH_MD5({s}) ,{1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}, 16),'xxxxxxxxxxxxxxx') AS DECIMAL(36,0))"

    def quote(self, s: str):
        return f"\"{s}\""

    def to_string(self, s: str):
        return f"cast({s} as varchar(1024))"

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            if coltype.precision > 0:
                return f"TO_TIMESTAMP(TO_CHAR({value}, 'YYYY-MM-DD HH24:MI:SS.FF{coltype.precision}'), 'YYYY-MM-DD HH24:MI:SS.FF6')"
            else:
                return f"TO_CHAR(TO_TIMESTAMP(TO_CHAR({value}, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS.FF6'))"
        return f"TO_CHAR(TO_TIMESTAMP({value}, 'YYYY-MM-DD HH24:MI:SS.FF6'))"

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        if coltype.precision > 0:
            precision_format = "9" * (coltype.precision - 1)
            format_str = f"FM999999999999999990.{precision_format}0"
        else:
            format_str = "FM" + "9" * 18
        return self.to_string(f"TO_CHAR({value}, '{format_str}')")


    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)

        return (
            f'SELECT "COLUMN_NAME", "COLUMN_TYPE", 3 AS \'datetime_precision\', "COLUMN_NUM_PREC", "COLUMN_NUM_SCALE" FROM SYS.EXA_ALL_COLUMNS WHERE "COLUMN_TABLE" = \'{ table }\' AND "COLUMN_SCHEMA" = \'{ schema }\''
        )

    def _process_table_schema(self, path: DbPath, raw_schema: Dict[str, tuple], filter_columns: Sequence[str]):
        accept = {i.lower() for i in filter_columns}
        
        col_dict = {name: self._parse_type(row[1],row[2], row[3]) for name, row in raw_schema.items() if name.lower() in accept}

        self._refine_coltypes(path, col_dict)

        # Return a dict of form {name: type} after normalization
        return col_dict

    def _parse_type(
        self,
        type_repr: str,
        datetime_precision: int = None,
        numeric_precision: int = None,
    ) -> ColType:
        timestamp_regexps = {
            r"DATE": Datetime,
            r"TIMESTAMP": Timestamp,
            r"TIMESTAMP WITH LOCAL TIME ZONE": TimestampTZ,
        }
        for regexp, t_cls in timestamp_regexps.items():
            m = re.match(regexp + "$", type_repr)
            if m:
                return t_cls(
                    precision=3,
                    rounds=self.ROUNDS_ON_PREC_LOSS,
                )

        number_regexps = {
            r"DECIMAL\((\d+),(\d+)\)": Decimal,
        }
        for regexp, n_cls in number_regexps.items():
            m = re.match(regexp + "$", type_repr)
            if m:
                prec, scale = map(int, m.groups())
                return n_cls(scale)

        double_regexps = {
            r"DOUBLE PRECISSION": Float,
            r"DOUBLE": Float,
        }
        for regexp, d_cls in double_regexps.items():
            m = re.match(regexp + "$", type_repr)
            if m:
                return d_cls(12)

        string_regexps = {
            r"VARCHAR\((\d+)\) UTF8": Text,
            r"VARCHAR\((\d+)\)": Text,
            r"CHAR\((\d+)\) UTF8": Text,
            r"CHAR\((\d+)\)": Text,
        }
        for regexp, n_cls in string_regexps.items():
            m = re.match(regexp + "$", type_repr)
            if m:
                return n_cls()

        return super()._parse_type(type_repr, datetime_precision, numeric_precision)
