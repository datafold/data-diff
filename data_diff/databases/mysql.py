from .database_types import *
from .base import ThreadedDatabase, import_helper, ConnectError
from .base import MD5_HEXDIGITS, CHECKSUM_HEXDIGITS, TIMESTAMP_PRECISION_POS


@import_helper("mysql")
def import_mysql():
    import mysql.connector

    return mysql.connector


class MySQL(ThreadedDatabase):
    TYPE_CLASSES = {
        # Dates
        "datetime": Datetime,
        "timestamp": Timestamp,
        # Numbers
        "double": Float,
        "float": Float,
        "decimal": Decimal,
        "int": Integer,
        "bigint": Integer,
        # Text
        "varchar": Text,
        "char": Text,
        "varbinary": Text,
        "binary": Text,
    }
    ROUNDS_ON_PREC_LOSS = True

    def __init__(self, *, thread_count, **kw):
        self._args = kw

        super().__init__(thread_count=thread_count)

        # In MySQL schema and database are synonymous
        self.default_schema = kw["database"]

    def create_connection(self):
        mysql = import_mysql()
        try:
            return mysql.connect(charset="utf8", use_unicode=True, **self._args)
        except mysql.Error as e:
            if e.errno == mysql.errorcode.ER_ACCESS_DENIED_ERROR:
                raise ConnectError("Bad user name or password") from e
            elif e.errno == mysql.errorcode.ER_BAD_DB_ERROR:
                raise ConnectError("Database does not exist") from e
            raise ConnectError(*e.args) from e

    def quote(self, s: str):
        return f"`{s}`"

    def md5_to_int(self, s: str) -> str:
        return f"cast(conv(substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16, 10) as unsigned)"

    def to_string(self, s: str):
        return f"cast({s} as char)"

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            return self.to_string(f"cast( cast({value} as datetime({coltype.precision})) as datetime(6))")

        s = self.to_string(f"cast({value} as datetime(6))")
        return f"RPAD(RPAD({s}, {TIMESTAMP_PRECISION_POS+coltype.precision}, '.'), {TIMESTAMP_PRECISION_POS+6}, '0')"

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"cast({value} as decimal(38, {coltype.precision}))")

    def normalize_uuid(self, value: str, coltype: ColType_UUID) -> str:
        return f"TRIM(CAST({value} AS char))"

    def is_distinct_from(self, a: str, b: str) -> str:
        return f"not ({a} <=> {b})"
