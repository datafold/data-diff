from typing import Any, ClassVar, Dict, List, Type

import attrs

from data_diff.abcs.database_types import (
    ColType,
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
from data_diff.databases.base import BaseDialect, ThreadedDatabase, import_helper, ConnectError
from data_diff.databases.base import (
    MD5_HEXDIGITS,
    CHECKSUM_HEXDIGITS,
    _CHECKSUM_BITSIZE,
    TIMESTAMP_PRECISION_POS,
    CHECKSUM_OFFSET,
)

SESSION_TIME_ZONE = None  # Changed by the tests


@import_helper("postgresql")
def import_postgresql():
    import psycopg2.extras

    psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)
    return psycopg2


@attrs.define(frozen=False)
class PostgresqlDialect(BaseDialect):
    name = "PostgreSQL"
    ROUNDS_ON_PREC_LOSS = True
    SUPPORTS_PRIMARY_KEY: ClassVar[bool] = True
    SUPPORTS_INDEXES = True

    TYPE_CLASSES: ClassVar[Dict[str, Type[ColType]]] = {
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

    def concat(self, items: List[str]) -> str:
        joined_exprs = " || ".join(items)
        return f"({joined_exprs})"

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

    def md5_as_int(self, s: str) -> str:
        return f"('x' || substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}))::bit({_CHECKSUM_BITSIZE})::bigint - {CHECKSUM_OFFSET}"

    def md5_as_hex(self, s: str) -> str:
        return f"md5({s})"

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        def _add_padding(coltype: TemporalType, timestamp6: str):
            return f"RPAD(LEFT({timestamp6}, {TIMESTAMP_PRECISION_POS+coltype.precision}), {TIMESTAMP_PRECISION_POS+6}, '0')"

        if coltype.rounds:
            # NULL value expected to return NULL after normalization
            null_case_begin = f"CASE WHEN {value} IS NULL THEN NULL ELSE "
            null_case_end = "END"

            # 294277 or 4714 BC would be out of range, make sure we can't round to that
            # TODO test timezones for overflow?
            max_timestamp = "294276-12-31 23:59:59.0000"
            min_timestamp = "4713-01-01 00:00:00.00 BC"
            timestamp = f"least('{max_timestamp}'::timestamp(6), {value}::timestamp(6))"
            timestamp = f"greatest('{min_timestamp}'::timestamp(6), {timestamp})"

            interval = format((0.5 * (10 ** (-coltype.precision))), f".{coltype.precision+1}f")

            rounded_timestamp = (
                f"left(to_char(least('{max_timestamp}'::timestamp, {timestamp})"
                f"+ interval '{interval}', 'YYYY-mm-dd HH24:MI:SS.US'),"
                f"length(to_char(least('{max_timestamp}'::timestamp, {timestamp})"
                f"+ interval '{interval}', 'YYYY-mm-dd HH24:MI:SS.US')) - (6-{coltype.precision}))"
            )

            padded = _add_padding(coltype, rounded_timestamp)
            return f"{null_case_begin} {padded} {null_case_end}"

            # TODO years with > 4 digits not padded correctly
            # current w/ precision 6: 294276-12-31 23:59:59.0000
            # should be 294276-12-31 23:59:59.000000
        else:
            rounded_timestamp = f"to_char({value}::timestamp(6), 'YYYY-mm-dd HH24:MI:SS.US')"
            padded = _add_padding(coltype, rounded_timestamp)
            return padded

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"{value}::decimal(38, {coltype.precision})")

    def normalize_boolean(self, value: str, _coltype: Boolean) -> str:
        return self.to_string(f"{value}::int")

    def normalize_json(self, value: str, _coltype: JSON) -> str:
        return f"{value}::text"


@attrs.define(frozen=False, init=False, kw_only=True)
class PostgreSQL(ThreadedDatabase):
    DIALECT_CLASS: ClassVar[Type[BaseDialect]] = PostgresqlDialect
    SUPPORTS_UNIQUE_CONSTAINT = True
    CONNECT_URI_HELP = "postgresql://<user>:<password>@<host>/<database>"
    CONNECT_URI_PARAMS = ["database?"]

    _args: Dict[str, Any]
    _conn: Any

    def __init__(self, *, thread_count, **kw):
        super().__init__(thread_count=thread_count)
        self._args = kw
        self.default_schema = "public"

    def create_connection(self):
        if not self._args:
            self._args["host"] = None  # psycopg2 requires 1+ arguments

        pg = import_postgresql()
        try:
            self._conn = pg.connect(
                **self._args, keepalives=1, keepalives_idle=5, keepalives_interval=2, keepalives_count=2
            )
            if SESSION_TIME_ZONE:
                self._conn.cursor().execute(f"SET TIME ZONE '{SESSION_TIME_ZONE}'")
            return self._conn
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
