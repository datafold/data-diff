from typing import Any, Dict, Optional

import attrs

from data_diff.abcs.mixins import AbstractMixin_MD5, AbstractMixin_NormalizeValue
from data_diff.databases.base import (
    CHECKSUM_HEXDIGITS,
    Mixin_OptimizerHints,
    Mixin_RandomSample,
    QueryError,
    ThreadedDatabase,
    import_helper,
    ConnectError,
    BaseDialect,
)
from data_diff.databases.base import Mixin_Schema
from data_diff.abcs.database_types import (
    JSON,
    Timestamp,
    TimestampTZ,
    DbPath,
    Float,
    Decimal,
    Integer,
    TemporalType,
    Native_UUID,
    Text,
    FractionalType,
    Boolean,
)


@import_helper("mssql")
def import_mssql():
    import pyodbc

    return pyodbc


@attrs.define(frozen=False)
class Mixin_NormalizeValue(AbstractMixin_NormalizeValue):
    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.precision > 0:
            formatted_value = (
                f"FORMAT({value}, 'yyyy-MM-dd HH:mm:ss') + '.' + "
                f"SUBSTRING(FORMAT({value}, 'fffffff'), 1, {coltype.precision})"
            )
        else:
            formatted_value = f"FORMAT({value}, 'yyyy-MM-dd HH:mm:ss')"

        return formatted_value

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        if coltype.precision == 0:
            return f"CAST(FLOOR({value}) AS VARCHAR)"

        return f"FORMAT({value}, 'N{coltype.precision}')"


@attrs.define(frozen=False)
class Mixin_MD5(AbstractMixin_MD5):
    def md5_as_int(self, s: str) -> str:
        return f"convert(bigint, convert(varbinary, '0x' + RIGHT(CONVERT(NVARCHAR(32), HashBytes('MD5', {s}), 2), {CHECKSUM_HEXDIGITS}), 1))"


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
    name = "MsSQL"
    ROUNDS_ON_PREC_LOSS = True
    SUPPORTS_PRIMARY_KEY = True
    SUPPORTS_INDEXES = True
    TYPE_CLASSES = {
        # Timestamps
        "datetimeoffset": TimestampTZ,
        "datetime": Timestamp,
        "datetime2": Timestamp,
        "smalldatetime": Timestamp,
        "date": Timestamp,
        # Numbers
        "float": Float,
        "real": Float,
        "decimal": Decimal,
        "money": Decimal,
        "smallmoney": Decimal,
        # int
        "int": Integer,
        "bigint": Integer,
        "tinyint": Integer,
        "smallint": Integer,
        # Text
        "varchar": Text,
        "char": Text,
        "text": Text,
        "ntext": Text,
        "nvarchar": Text,
        "nchar": Text,
        "binary": Text,
        "varbinary": Text,
        "xml": Text,
        # UUID
        "uniqueidentifier": Native_UUID,
        # Bool
        "bit": Boolean,
        # JSON
        "json": JSON,
    }

    def quote(self, s: str):
        return f"[{s}]"

    def set_timezone_to_utc(self) -> str:
        raise NotImplementedError("MsSQL does not support a session timezone setting.")

    def current_timestamp(self) -> str:
        return "GETDATE()"

    def current_database(self) -> str:
        return "DB_NAME()"

    def current_schema(self) -> str:
        return """default_schema_name
        FROM sys.database_principals
        WHERE name = CURRENT_USER"""

    def to_string(self, s: str):
        return f"CONVERT(varchar, {s})"

    def type_repr(self, t) -> str:
        try:
            return {bool: "bit"}[t]
        except KeyError:
            return super().type_repr(t)

    def random(self) -> str:
        return "rand()"

    def is_distinct_from(self, a: str, b: str) -> str:
        # IS (NOT) DISTINCT FROM is available only since SQLServer 2022.
        # See: https://stackoverflow.com/a/18684859/857383
        return f"(({a}<>{b} OR {a} IS NULL OR {b} IS NULL) AND NOT({a} IS NULL AND {b} IS NULL))"

    def offset_limit(
        self, offset: Optional[int] = None, limit: Optional[int] = None, has_order_by: Optional[bool] = None
    ) -> str:
        if offset:
            raise NotImplementedError("No support for OFFSET in query")

        result = ""
        if not has_order_by:
            result += "ORDER BY 1"

        result += f" OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
        return result

    def constant_values(self, rows) -> str:
        values = ", ".join("(%s)" % ", ".join(self._constant_value(v) for v in row) for row in rows)
        return f"VALUES {values}"


@attrs.define(frozen=False, init=False, kw_only=True)
class MsSQL(ThreadedDatabase):
    dialect = Dialect()
    CONNECT_URI_HELP = "mssql://<user>:<password>@<host>/<database>/<schema>"
    CONNECT_URI_PARAMS = ["database", "schema"]

    default_database: str
    _args: Dict[str, Any]
    _mssql: Any

    def __init__(self, host, port, user, password, *, database, thread_count, **kw):
        super().__init__(thread_count=thread_count)

        args = dict(server=host, port=port, database=database, user=user, password=password, **kw)
        self._args = {k: v for k, v in args.items() if v is not None}
        self._args["driver"] = "{ODBC Driver 18 for SQL Server}"

        # TODO temp dev debug
        self._args["TrustServerCertificate"] = "yes"

        try:
            self.default_database = self._args["database"]
            self.default_schema = self._args["schema"]
        except KeyError:
            raise ValueError("Specify a default database and schema.")

        self._mssql = None

    def create_connection(self):
        self._mssql = import_mssql()
        try:
            connection = self._mssql.connect(**self._args)
            return connection
        except self._mssql.Error as error:
            raise ConnectError(*error.args) from error

    def select_table_schema(self, path: DbPath) -> str:
        """Provide SQL for selecting the table schema as (name, type, date_prec, num_prec)"""
        database, schema, name = self._normalize_table_path(path)
        info_schema_path = ["information_schema", "columns"]
        if database:
            info_schema_path.insert(0, self.dialect.quote(database))

        return (
            "SELECT column_name, data_type, datetime_precision, numeric_precision, numeric_scale "
            f"FROM {'.'.join(info_schema_path)} "
            f"WHERE table_name = '{name}' AND table_schema = '{schema}'"
        )

    def _normalize_table_path(self, path: DbPath) -> DbPath:
        if len(path) == 1:
            return self.default_database, self.default_schema, path[0]
        elif len(path) == 2:
            return self.default_database, path[0], path[1]
        elif len(path) == 3:
            return path

        raise ValueError(
            f"{self.name}: Bad table path for {self}: '{'.'.join(path)}'. Expected format: table, schema.table, or database.schema.table"
        )

    def _query_cursor(self, c, sql_code: str):
        try:
            return super()._query_cursor(c, sql_code)
        except self._mssql.DatabaseError as e:
            raise QueryError(e)
