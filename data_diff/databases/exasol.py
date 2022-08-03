import re

from .database_types import *
from .base import ThreadedDatabase, import_helper, ConnectError, QueryError
from .base import (
    MD5_HEXDIGITS,
    CHECKSUM_HEXDIGITS,
    TIMESTAMP_PRECISION_POS,
    DEFAULT_DATETIME_PRECISION,
)

@import_helper("exasol")
def import_exasol():
    import pyexasol

    return pyexasol

SESSION_TIME_ZONE = None  # Changed by the tests

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
    ROUNDS_ON_PREC_LOSS = True

    def __init__(self, *, host, thread_count, **kw):
        self.kwargs = dict(dsn="%s" % host, **kw)

        self.default_schema = kw.get("schema")

        super().__init__(thread_count=thread_count)

    def create_connection(self):
        self._exasol = import_exasol()
        try:
            c = self._exasol.connect(**self.kwargs)
            if SESSION_TIME_ZONE:
                c.execute(f"ALTER SESSION SET TIME_ZONE = '{SESSION_TIME_ZONE}'")
            return c
        except Exception as e:
            raise ConnectError(*e.args) from e

    def _query(self, sql_code: str):
        try:
            return super()._query(sql_code)
        except Exception as e:
            raise QueryError(e)

    def md5_to_int(self, s: str) -> str:
        return f"CAST(TO_NUMBER(SUBSTR(HASH_MD5({s}) ,{1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}, 16),'xxxxxxxxxxxxxxx') AS DECIMAL(36,0))"

    def quote(self, s: str):
        return f"\"{s}\""

    def to_string(self, s: str):
        return f"cast({s} as varchar(1024))"

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            return f"to_char(cast({value} as timestamp({coltype.precision})), 'YYYY-MM-DD HH24:MI:SS.FF6')"
        else:
            if coltype.precision > 0:
                truncated = f"to_char({value}, 'YYYY-MM-DD HH24:MI:SS.FF{coltype.precision}')"
            else:
                truncated = f"to_char({value}, 'YYYY-MM-DD HH24:MI:SS.')"
            return f"RPAD({truncated}, {TIMESTAMP_PRECISION_POS+6}, '0')"

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"cast({value} as DECIMAL(18, {coltype.precision}))")

    def _query(self, sql_code: str):
        r = self._queue.submit(self._query_in_worker, sql_code)
        return r.result()

    def _query_in_worker(self, sql_code: str):
        "This method runs in a worker thread"
        if self._init_error:
            raise self._init_error
        return _query_conn(self.thread_local.conn, sql_code)

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)

        return (
            f'SELECT "COLUMN_NAME", "COLUMN_TYPE", 3 AS \'datetime_precision\', "COLUMN_NUM_PREC", "COLUMN_NUM_SCALE" FROM SYS.EXA_ALL_COLUMNS WHERE "COLUMN_TABLE" = \'{ table }\' AND "COLUMN_SCHEMA" = \'{ schema }\''
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
        print('##DEBUG##')
        for name, row in raw_schema.items():
            if name.lower() in accept:
                print(*row)

        col_dict = {name: self._parse_type( row[1],row[2], row[3],) for name, row in raw_schema.items() if name.lower() in accept}

        self._refine_coltypes(path, col_dict)

        # Return a dict of form {name: type} after normalization
        return col_dict

    def _parse_type(
        self,
        #table_path: DbPath,
        #col_name: str,
        type_repr: str,
        datetime_precision: int = None,
        numeric_precision: int = None,
    ) -> ColType:
        timestamp_regexps = {
            r"DATE\((\d)\)": Datetime,
            r"TIMESTAMP\((\d)\)": Timestamp,
            r"TIMESTAMP WITH LOCAL TIME ZONE\((\d)\)": TimestampTZ,
        }
        for regexp, t_cls in timestamp_regexps.items():
            m = re.match(regexp + "$", type_repr)
            if m:
                datetime_precision = int(m.group(1))
                return t_cls(
                    precision=DEFAULT_DATETIME_PRECISION,
                    rounds=self.ROUNDS_ON_PREC_LOSS,
                )

        number_regexps = {
            r"DECIMAL\((\d+),(\d+)\)": Decimal,
            r"DOUBLE PRECISION": Float,
        }
        for regexp, n_cls in number_regexps.items():
            m = re.match(regexp + "$", type_repr)
            if m:
                prec, scale = map(int, m.groups())
                return n_cls(scale)

        string_regexps = {
            r"VARCHAR\((\d+)\)": Text,
            r"CHAR\((\d+)\)": Text,
        }
        for regexp, n_cls in string_regexps.items():
            m = re.match(regexp + "$", type_repr)
            if m:
                return n_cls()

        return super()._parse_type(type_repr, datetime_precision, numeric_precision)
