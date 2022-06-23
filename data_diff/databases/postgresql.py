from .database_types import *
from .base import ThreadedDatabase, import_helper, ConnectError
from .base import MD5_HEXDIGITS, CHECKSUM_HEXDIGITS, _CHECKSUM_BITSIZE, TIMESTAMP_PRECISION_POS

@import_helper("postgresql")
def import_postgresql():
    import psycopg2
    import psycopg2.extras

    psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)
    return psycopg2



class PostgreSQL(ThreadedDatabase):
    DATETIME_TYPES = {
        "timestamp with time zone": TimestampTZ,
        "timestamp without time zone": Timestamp,
        "timestamp": Timestamp,
        # "datetime": Datetime,
    }
    NUMERIC_TYPES = {
        "double precision": Float,
        "real": Float,
        "decimal": Decimal,
        "integer": Integer,
        "numeric": Decimal,
        "bigint": Integer,
    }
    ROUNDS_ON_PREC_LOSS = True

    default_schema = "public"

    def __init__(self, host, port, user, password, *, database, thread_count, **kw):
        self.args = dict(host=host, port=port, database=database, user=user, password=password, **kw)

        super().__init__(thread_count=thread_count)

    def _convert_db_precision_to_digits(self, p: int) -> int:
        # Subtracting 2 due to wierd precision issues in PostgreSQL
        return super()._convert_db_precision_to_digits(p) - 2

    def create_connection(self):
        pg = import_postgresql()
        try:
            c = pg.connect(**self.args)
            # c.cursor().execute("SET TIME ZONE 'UTC'")
            return c
        except pg.OperationalError as e:
            raise ConnectError(*e.args) from e

    def quote(self, s: str):
        return f'"{s}"'

    def md5_to_int(self, s: str) -> str:
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

    def normalize_number(self, value: str, coltype: NumericType) -> str:
        return self.to_string(f"{value}::decimal(38, {coltype.precision})")
