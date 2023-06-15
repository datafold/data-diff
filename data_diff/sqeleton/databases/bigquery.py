import re
from typing import Any, List, Union
from ..abcs.database_types import (
    ColType,
    Array,
    JSON,
    Struct,
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
    UnknownColType,
)
from ..abcs.mixins import (
    AbstractMixin_MD5,
    AbstractMixin_NormalizeValue,
    AbstractMixin_Schema,
    AbstractMixin_TimeTravel,
)
from ..abcs import Compilable
from ..queries import this, table, SKIP, code
from .base import BaseDialect, Database, import_helper, parse_table_name, ConnectError, apply_query, QueryResult
from .base import TIMESTAMP_PRECISION_POS, ThreadLocalInterpreter, Mixin_RandomSample


@import_helper(text="Please install BigQuery and configure your google-cloud access.")
def import_bigquery():
    from google.cloud import bigquery

    return bigquery


def import_bigquery_service_account():
    from google.oauth2 import service_account

    return service_account


class Mixin_MD5(AbstractMixin_MD5):
    def md5_as_int(self, s: str) -> str:
        return f"cast(cast( ('0x' || substr(TO_HEX(md5({s})), 18)) as int64) as numeric)"


class Mixin_NormalizeValue(AbstractMixin_NormalizeValue):
    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            timestamp = f"timestamp_micros(cast(round(unix_micros(cast({value} as timestamp))/1000000, {coltype.precision})*1000000 as int))"
            return f"FORMAT_TIMESTAMP('%F %H:%M:%E6S', {timestamp})"

        if coltype.precision == 0:
            return f"FORMAT_TIMESTAMP('%F %H:%M:%S.000000', {value})"
        elif coltype.precision == 6:
            return f"FORMAT_TIMESTAMP('%F %H:%M:%E6S', {value})"

        timestamp6 = f"FORMAT_TIMESTAMP('%F %H:%M:%E6S', {value})"
        return (
            f"RPAD(LEFT({timestamp6}, {TIMESTAMP_PRECISION_POS+coltype.precision}), {TIMESTAMP_PRECISION_POS+6}, '0')"
        )

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return f"format('%.{coltype.precision}f', {value})"

    def normalize_boolean(self, value: str, _coltype: Boolean) -> str:
        return self.to_string(f"cast({value} as int)")

    def normalize_json(self, value: str, _coltype: JSON) -> str:
        # BigQuery is unable to compare arrays & structs with ==/!=/distinct from, e.g.:
        #   Got error: 400 Grouping is not defined for arguments of type ARRAY<INT64> at …
        # So we do the best effort and compare it as strings, hoping that the JSON forms
        # match on both sides: i.e. have properly ordered keys, same spacing, same quotes, etc.
        return f"to_json_string({value})"

    def normalize_array(self, value: str, _coltype: Array) -> str:
        # BigQuery is unable to compare arrays & structs with ==/!=/distinct from, e.g.:
        #   Got error: 400 Grouping is not defined for arguments of type ARRAY<INT64> at …
        # So we do the best effort and compare it as strings, hoping that the JSON forms
        # match on both sides: i.e. have properly ordered keys, same spacing, same quotes, etc.
        return f"to_json_string({value})"

    def normalize_struct(self, value: str, _coltype: Struct) -> str:
        # BigQuery is unable to compare arrays & structs with ==/!=/distinct from, e.g.:
        #   Got error: 400 Grouping is not defined for arguments of type ARRAY<INT64> at …
        # So we do the best effort and compare it as strings, hoping that the JSON forms
        # match on both sides: i.e. have properly ordered keys, same spacing, same quotes, etc.
        return f"to_json_string({value})"


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


class Mixin_TimeTravel(AbstractMixin_TimeTravel):
    def time_travel(
        self,
        table: Compilable,
        before: bool = False,
        timestamp: Compilable = None,
        offset: Compilable = None,
        statement: Compilable = None,
    ) -> Compilable:
        if before:
            raise NotImplementedError("before=True not supported for BigQuery time-travel")

        if statement is not None:
            raise NotImplementedError("BigQuery time-travel doesn't support querying by statement id")

        if timestamp is not None:
            assert offset is None
            return code("{table} FOR SYSTEM_TIME AS OF {timestamp}", table=table, timestamp=timestamp)

        assert offset is not None
        return code(
            "{table} FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {offset} HOUR);",
            table=table,
            offset=offset,
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
        "STRING": Text,
        "BOOL": Boolean,
        "JSON": JSON,
    }
    TYPE_ARRAY_RE = re.compile(r"ARRAY<(.+)>")
    TYPE_STRUCT_RE = re.compile(r"STRUCT<(.+)>")
    MIXINS = {Mixin_Schema, Mixin_MD5, Mixin_NormalizeValue, Mixin_TimeTravel, Mixin_RandomSample}

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

    def parse_type(
        self,
        table_path: DbPath,
        col_name: str,
        type_repr: str,
        *args: Any,  # pass-through args
        **kwargs: Any,  # pass-through args
    ) -> ColType:
        col_type = super().parse_type(table_path, col_name, type_repr, *args, **kwargs)
        if isinstance(col_type, UnknownColType):
            m = self.TYPE_ARRAY_RE.fullmatch(type_repr)
            if m:
                item_type = self.parse_type(table_path, col_name, m.group(1), *args, **kwargs)
                col_type = Array(item_type=item_type)

            # We currently ignore structs' structure, but later can parse it too. Examples:
            # - STRUCT<INT64, STRING(10)> (unnamed)
            # - STRUCT<foo INT64, bar STRING(10)> (named)
            # - STRUCT<foo INT64, bar ARRAY<INT64>> (with complex fields)
            # - STRUCT<foo INT64, bar STRUCT<a INT64, b INT64>> (nested)
            m = self.TYPE_STRUCT_RE.fullmatch(type_repr)
            if m:
                col_type = Struct()

        return col_type

    def to_comparable(self, value: str, coltype: ColType) -> str:
        """Ensure that the expression is comparable in ``IS DISTINCT FROM``."""
        if isinstance(coltype, (JSON, Array, Struct)):
            return self.normalize_value_by_type(value, coltype)
        else:
            return super().to_comparable(value, coltype)

    def set_timezone_to_utc(self) -> str:
        raise NotImplementedError()


class BigQuery(Database):
    CONNECT_URI_HELP = "bigquery://<project>/<dataset>"
    CONNECT_URI_PARAMS = ["dataset"]
    dialect = Dialect()

    def __init__(self, project, *, dataset, **kw):
        credentials = None
        bigquery = import_bigquery()

        keyfile = kw.pop("keyfile", None)
        if keyfile:
            bigquery_service_account = import_bigquery_service_account()
            credentials = bigquery_service_account.Credentials.from_service_account_file(
                keyfile,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )

        self._client = bigquery.Client(project=project, credentials=credentials, **kw)
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
            result = self._client.query(sql_code).result()
            columns = [c.name for c in result.schema]
            rows = list(result)
        except Exception as e:
            msg = "Exception when trying to execute SQL code:\n    %s\n\nGot error: %s"
            raise ConnectError(msg % (sql_code, e))

        if rows and isinstance(rows[0], bigquery.table.Row):
            rows = [tuple(self._normalize_returned_value(v) for v in row.values()) for row in rows]
        return QueryResult(rows, columns)

    def _query(self, sql_code: Union[str, ThreadLocalInterpreter]) -> QueryResult:
        return apply_query(self._query_atom, sql_code)

    def close(self):
        super().close()
        self._client.close()

    def select_table_schema(self, path: DbPath) -> str:
        project, schema, name = self._normalize_table_path(path)
        return (
            "SELECT column_name, data_type, 6 as datetime_precision, 38 as numeric_precision, 9 as numeric_scale "
            f"FROM `{project}`.`{schema}`.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = '{name}' AND table_schema = '{schema}'"
        )

    def query_table_unique_columns(self, path: DbPath) -> List[str]:
        return []

    def _normalize_table_path(self, path: DbPath) -> DbPath:
        if len(path) == 0:
            raise ValueError(f"{self.name}: Bad table path for {self}: ()")
        elif len(path) == 1:
            return (self.project, self.default_schema, path[0])
        elif len(path) == 2:
            return (self.project,) + path
        elif len(path) == 3:
            return path
        else:
            raise ValueError(
                f"{self.name}: Bad table path for {self}: '{'.'.join(path)}'. Expected form: [project.]schema.table"
            )

    def parse_table_name(self, name: str) -> DbPath:
        path = parse_table_name(name)
        return tuple(i for i in self._normalize_table_path(path) if i is not None)

    @property
    def is_autocommit(self) -> bool:
        return True
