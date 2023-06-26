from ..abcs.database_types import (
    DbPath,
    JSON,
    Timestamp,
    TimestampTZ,
    Float,
    Decimal,
    Integer,
    TemporalType,
    Native_UUID,
    Text,
    FractionalType,
    Boolean,
    Date,
)
from ..abcs.mixins import AbstractMixin_MD5, AbstractMixin_NormalizeValue
from .base import BaseDialect, ThreadedDatabase, import_helper, ConnectError, Mixin_Schema
from .base import MD5_HEXDIGITS, CHECKSUM_HEXDIGITS, _CHECKSUM_BITSIZE, TIMESTAMP_PRECISION_POS, Mixin_RandomSample

SESSION_TIME_ZONE = None  # Changed by the tests


@import_helper("postgresql")
def import_postgresql():
    import psycopg2
    import psycopg2.extras

    psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)
    return psycopg2


class Mixin_MD5(AbstractMixin_MD5):
    def md5_as_int(self, s: str) -> str:
        return f"('x' || substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}))::bit({_CHECKSUM_BITSIZE})::bigint"


class Mixin_NormalizeValue(AbstractMixin_NormalizeValue):
    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            return f"to_char({value}::timestamp({coltype.precision}), 'YYYY-mm-dd HH24:MI:SS.US')"

        timestamp6 = f"to_char({value}::timestamp(6), 'YYYY-mm-dd HH24:MI:SS.US')"
        return (
            f"RPAD(LEFT({timestamp6}, {TIMESTAMP_PRECISION_POS+coltype.precision}), {TIMESTAMP_PRECISION_POS+6}, '0')"
        )

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"{value}::decimal(38, {coltype.precision})")

    def normalize_boolean(self, value: str, _coltype: Boolean) -> str:
        return self.to_string(f"{value}::int")

    def normalize_json(self, value: str, _coltype: JSON) -> str:
        return f"{value}::text"


class PostgresqlDialect(BaseDialect, Mixin_Schema):
    name = "PostgreSQL"
    ROUNDS_ON_PREC_LOSS = True
    SUPPORTS_PRIMARY_KEY = True
    SUPPORTS_INDEXES = True
    MIXINS = {Mixin_Schema, Mixin_MD5, Mixin_NormalizeValue, Mixin_RandomSample}

    TYPE_CLASSES = {
        # Timestamps
        "timestamp with time zone": TimestampTZ,
        "timestamp without time zone": Timestamp,
        "timestamp": Timestamp,
        "date": Date,
        # Numbers
        "double precision": Float,
        "real": Float,
        "decimal": Decimal,
        "smallint": Integer,
        "integer": Integer,
        "numeric": Decimal,
        "bigint": Integer,
        # Text
        "character": Text,
        "character varying": Text,
        "varchar": Text,
        "text": Text,
        "json": JSON,
        "jsonb": JSON,
        "uuid": Native_UUID,
        "boolean": Boolean,
    }

    def quote(self, s: str):
        return f'"{s}"'

    def to_string(self, s: str):
        return f"{s}::varchar"

    def _convert_db_precision_to_digits(self, p: int) -> int:
        # Subtracting 2 due to wierd precision issues in PostgreSQL
        return super()._convert_db_precision_to_digits(p) - 2

    def set_timezone_to_utc(self) -> str:
        return "SET TIME ZONE 'UTC'"

    def current_timestamp(self) -> str:
        return "current_timestamp"

    def type_repr(self, t) -> str:
        if isinstance(t, TimestampTZ):
            return f"timestamp ({t.precision}) with time zone"
        return super().type_repr(t)


class PostgreSQL(ThreadedDatabase):
    dialect = PostgresqlDialect()
    SUPPORTS_UNIQUE_CONSTAINT = True
    CONNECT_URI_HELP = "postgresql://<user>:<password>@<host>/<database>"
    CONNECT_URI_PARAMS = ["database?"]

    default_schema = "public"

    def __init__(self, *, thread_count, **kw):
        self._args = kw

        super().__init__(thread_count=thread_count)

    def create_connection(self):
        if not self._args:
            self._args["host"] = None  # psycopg2 requires 1+ arguments

        pg = import_postgresql()
        try:
            c = pg.connect(**self._args)
            if SESSION_TIME_ZONE:
                c.cursor().execute(f"SET TIME ZONE '{SESSION_TIME_ZONE}'")
            return c
        except pg.OperationalError as e:
            raise ConnectError(*e.args) from e

    def select_table_schema(self, path: DbPath) -> str:
        database, schema, table = self._normalize_table_path(path)

        info_schema_path = ["information_schema", "columns"]
        if database:
            info_schema_path.insert(0, database)

        return (
            f"SELECT column_name, data_type, datetime_precision, numeric_precision, numeric_scale FROM {'.'.join(info_schema_path)} "
            f"WHERE table_name = '{table}' AND table_schema = '{schema}'"
        )

    def select_table_unique_columns(self, path: DbPath) -> str:
        database, schema, table = self._normalize_table_path(path)

        info_schema_path = ["information_schema", "key_column_usage"]
        if database:
            info_schema_path.insert(0, database)

        return (
            "SELECT column_name "
            f"FROM {'.'.join(info_schema_path)} "
            f"WHERE table_name = '{table}' AND table_schema = '{schema}'"
        )

    def _normalize_table_path(self, path: DbPath) -> DbPath:
        if len(path) == 1:
            return None, self.default_schema, path[0]
        elif len(path) == 2:
            return None, path[0], path[1]
        elif len(path) == 3:
            return path

        raise ValueError(
            f"{self.name}: Bad table path for {self}: '{'.'.join(path)}'. Expected format: table, schema.table, or database.schema.table"
        )
