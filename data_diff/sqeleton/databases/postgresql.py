from .database_types import (
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
)
from .base import BaseDialect, ThreadedDatabase, import_helper, ConnectError
from .base import MD5_HEXDIGITS, CHECKSUM_HEXDIGITS, _CHECKSUM_BITSIZE, TIMESTAMP_PRECISION_POS

SESSION_TIME_ZONE = None  # Changed by the tests


@import_helper("postgresql")
def import_postgresql():
    import psycopg2
    import psycopg2.extras

    psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)
    return psycopg2


class PostgresqlDialect(BaseDialect):
    name = "PostgreSQL"
    ROUNDS_ON_PREC_LOSS = True
    SUPPORTS_PRIMARY_KEY = True

    TYPE_CLASSES = {
        # Timestamps
        "timestamp with time zone": TimestampTZ,
        "timestamp without time zone": Timestamp,
        "timestamp": Timestamp,
        # Numbers
        "double precision": Float,
        "real": Float,
        "decimal": Decimal,
        "integer": Integer,
        "numeric": Decimal,
        "bigint": Integer,
        # Text
        "character": Text,
        "character varying": Text,
        "varchar": Text,
        "text": Text,
        # UUID
        "uuid": Native_UUID,
        # Boolean
        "boolean": Boolean,
    }

    def quote(self, s: str):
        return f'"{s}"'

    def md5_as_int(self, s: str) -> str:
        return f"('x' || substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}))::bit({_CHECKSUM_BITSIZE})::bigint"

    def to_string(self, s: str):
        return f"{s}::varchar"

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            return f"to_char({value}::timestamp({coltype.precision}), 'YYYY-mm-dd HH24:MI:SS.US')"

        timestamp6 = f"to_char({value}::timestamp(6), 'YYYY-mm-dd HH24:MI:SS.US')"
        return (
            f"RPAD(LEFT({timestamp6}, {TIMESTAMP_PRECISION_POS+coltype.precision}), {TIMESTAMP_PRECISION_POS+6}, '0')"
        )

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"{value}::decimal(38, {coltype.precision})")

    def normalize_boolean(self, value: str, coltype: Boolean) -> str:
        return self.to_string(f"{value}::int")

    def _convert_db_precision_to_digits(self, p: int) -> int:
        # Subtracting 2 due to wierd precision issues in PostgreSQL
        return super()._convert_db_precision_to_digits(p) - 2


class PostgreSQL(ThreadedDatabase):
    dialect = PostgresqlDialect()
    SUPPORTS_UNIQUE_CONSTAINT = True
    CONNECT_URI_HELP = "postgresql://<user>:<pass>@<host>/<database>"
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
