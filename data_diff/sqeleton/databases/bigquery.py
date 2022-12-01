from typing import List, Union
from ..abcs.database_types import (
    Timestamp,
    Datetime,
    Integer,
    Decimal,
    Float,
    Text,
    DbPath,
    FractionalType,
    TemporalType,
    Boolean,
)
from ..abcs.mixins import AbstractMixin_MD5, AbstractMixin_NormalizeValue, AbstractMixin_Schema
from ..abcs import Compilable
from ..queries import this, table, SKIP
from .base import BaseDialect, Database, import_helper, parse_table_name, ConnectError, apply_query
from .base import TIMESTAMP_PRECISION_POS, ThreadLocalInterpreter


@import_helper(text="Please install BigQuery and configure your google-cloud access.")
def import_bigquery():
    from google.cloud import bigquery

    return bigquery


class Mixin_MD5(AbstractMixin_MD5):
    def md5_as_int(self, s: str) -> str:
        return f"cast(cast( ('0x' || substr(TO_HEX(md5({s})), 18)) as int64) as numeric)"


class Mixin_NormalizeValue(AbstractMixin_NormalizeValue):
    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            timestamp = f"timestamp_micros(cast(round(unix_micros(cast({value} as timestamp))/1000000, {coltype.precision})*1000000 as int))"
            return f"FORMAT_TIMESTAMP('%F %H:%M:%E6S', {timestamp})"

        if coltype.precision == 0:
            return f"FORMAT_TIMESTAMP('%F %H:%M:%S.000000, {value})"
        elif coltype.precision == 6:
            return f"FORMAT_TIMESTAMP('%F %H:%M:%E6S', {value})"

        timestamp6 = f"FORMAT_TIMESTAMP('%F %H:%M:%E6S', {value})"
        return (
            f"RPAD(LEFT({timestamp6}, {TIMESTAMP_PRECISION_POS+coltype.precision}), {TIMESTAMP_PRECISION_POS+6}, '0')"
        )

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return f"format('%.{coltype.precision}f', {value})"

    def normalize_boolean(self, value: str, coltype: Boolean) -> str:
        return self.to_string(f"cast({value} as int)")


class Mixin_Schema(AbstractMixin_Schema):
    def list_tables(self, table_schema: str, like: Compilable = None) -> Compilable:
        return (
            table(table_schema, "INFORMATION_SCHEMA", "TABLES")
            .where(
                this.table_schema == table_schema,
                this.table_name.like(like) if like is not None else SKIP,
                this.table_type == "BASE TABLE",
            )
            .select(this.table_name)
        )


class Dialect(BaseDialect, Mixin_Schema):
    name = "BigQuery"
    ROUNDS_ON_PREC_LOSS = False  # Technically BigQuery doesn't allow implicit rounding or truncation
    TYPE_CLASSES = {
        # Dates
        "TIMESTAMP": Timestamp,
        "DATETIME": Datetime,
        # Numbers
        "INT64": Integer,
        "INT32": Integer,
        "NUMERIC": Decimal,
        "BIGNUMERIC": Decimal,
        "FLOAT64": Float,
        "FLOAT32": Float,
        # Text
        "STRING": Text,
        # Boolean
        "BOOL": Boolean,
    }

    def random(self) -> str:
        return "RAND()"

    def quote(self, s: str):
        return f"`{s}`"

    def to_string(self, s: str):
        return f"cast({s} as string)"

    def type_repr(self, t) -> str:
        try:
            return {str: "STRING", float: "FLOAT64"}[t]
        except KeyError:
            return super().type_repr(t)

    def set_timezone_to_utc(self) -> str:
        raise NotImplementedError()


class BigQuery(Database):
    CONNECT_URI_HELP = "bigquery://<project>/<dataset>"
    CONNECT_URI_PARAMS = ["dataset"]
    dialect = Dialect()

    def __init__(self, project, *, dataset, **kw):
        bigquery = import_bigquery()

        self._client = bigquery.Client(project, **kw)
        self.project = project
        self.dataset = dataset

        self.default_schema = dataset

    def _normalize_returned_value(self, value):
        if isinstance(value, bytes):
            return value.decode()
        return value

    def _query_atom(self, sql_code: str):
        from google.cloud import bigquery

        try:
            res = list(self._client.query(sql_code))
        except Exception as e:
            msg = "Exception when trying to execute SQL code:\n    %s\n\nGot error: %s"
            raise ConnectError(msg % (sql_code, e))

        if res and isinstance(res[0], bigquery.table.Row):
            res = [tuple(self._normalize_returned_value(v) for v in row.values()) for row in res]
        return res

    def _query(self, sql_code: Union[str, ThreadLocalInterpreter]):
        return apply_query(self._query_atom, sql_code)

    def close(self):
        super().close()
        self._client.close()

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)

        return (
            f"SELECT column_name, data_type, 6 as datetime_precision, 38 as numeric_precision, 9 as numeric_scale FROM {schema}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = '{table}' AND table_schema = '{schema}'"
        )

    def query_table_unique_columns(self, path: DbPath) -> List[str]:
        return []

    def parse_table_name(self, name: str) -> DbPath:
        path = parse_table_name(name)
        return self._normalize_table_path(path)

    @property
    def is_autocommit(self) -> bool:
        return True
