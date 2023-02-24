from ..abcs.database_types import (
    Datetime,
    Timestamp,
    Float,
    Decimal,
    Integer,
    Text,
    TemporalType,
    FractionalType,
    ColType_UUID,
    Boolean,
    Date,
)
from ..abcs.mixins import (
    AbstractMixin_MD5,
    AbstractMixin_NormalizeValue,
    AbstractMixin_Regex,
    AbstractMixin_RandomSample,
)
from .base import Mixin_OptimizerHints, ThreadedDatabase, import_helper, ConnectError, BaseDialect, Compilable
from .base import MD5_HEXDIGITS, CHECKSUM_HEXDIGITS, TIMESTAMP_PRECISION_POS, Mixin_Schema, Mixin_RandomSample
from ..queries.ast_classes import BinBoolOp


@import_helper("mysql")
def import_mysql():
    import mysql.connector

    return mysql.connector


class Mixin_MD5(AbstractMixin_MD5):
    def md5_as_int(self, s: str) -> str:
        return f"cast(conv(substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16, 10) as unsigned)"


class Mixin_NormalizeValue(AbstractMixin_NormalizeValue):
    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            return self.to_string(f"cast( cast({value} as datetime({coltype.precision})) as datetime(6))")

        s = self.to_string(f"cast({value} as datetime(6))")
        return f"RPAD(RPAD({s}, {TIMESTAMP_PRECISION_POS+coltype.precision}, '.'), {TIMESTAMP_PRECISION_POS+6}, '0')"

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"cast({value} as decimal(38, {coltype.precision}))")

    def normalize_uuid(self, value: str, coltype: ColType_UUID) -> str:
        return f"TRIM(CAST({value} AS char))"


class Mixin_Regex(AbstractMixin_Regex):
    def test_regex(self, string: Compilable, pattern: Compilable) -> Compilable:
        return BinBoolOp("REGEXP", [string, pattern])


class Dialect(BaseDialect, Mixin_Schema, Mixin_OptimizerHints):
    name = "MySQL"
    ROUNDS_ON_PREC_LOSS = True
    SUPPORTS_PRIMARY_KEY = True
    SUPPORTS_INDEXES = True
    TYPE_CLASSES = {
        # Dates
        "datetime": Datetime,
        "timestamp": Timestamp,
        "date": Date,
        # Numbers
        "double": Float,
        "float": Float,
        "decimal": Decimal,
        "int": Integer,
        "bigint": Integer,
        "smallint": Integer,
        "tinyint": Integer,
        # Text
        "varchar": Text,
        "char": Text,
        "varbinary": Text,
        "binary": Text,
        "text": Text,
        "mediumtext": Text,
        "longtext": Text,
        "tinytext": Text,
        # Boolean
        "boolean": Boolean,
    }
    MIXINS = {Mixin_Schema, Mixin_MD5, Mixin_NormalizeValue, Mixin_RandomSample}

    def quote(self, s: str):
        return f"`{s}`"

    def to_string(self, s: str):
        return f"cast({s} as char)"

    def is_distinct_from(self, a: str, b: str) -> str:
        return f"not ({a} <=> {b})"

    def random(self) -> str:
        return "RAND()"

    def type_repr(self, t) -> str:
        try:
            return {
                str: "VARCHAR(1024)",
            }[t]
        except KeyError:
            return super().type_repr(t)

    def explain_as_text(self, query: str) -> str:
        return f"EXPLAIN FORMAT=TREE {query}"

    def optimizer_hints(self, s: str):
        return f"/*+ {s} */ "

    def set_timezone_to_utc(self) -> str:
        return "SET @@session.time_zone='+00:00'"


class MySQL(ThreadedDatabase):
    dialect = Dialect()
    SUPPORTS_ALPHANUMS = False
    SUPPORTS_UNIQUE_CONSTAINT = True
    CONNECT_URI_HELP = "mysql://<user>:<password>@<host>/<database>"
    CONNECT_URI_PARAMS = ["database?"]

    def __init__(self, *, thread_count, **kw):
        self._args = kw

        super().__init__(thread_count=thread_count)

        # In MySQL schema and database are synonymous
        try:
            self.default_schema = kw["database"]
        except KeyError:
            raise ValueError("MySQL URL must specify a database")

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
