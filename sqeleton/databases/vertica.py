from typing import List

from ..utils import match_regexps
from .base import (
    CHECKSUM_HEXDIGITS,
    MD5_HEXDIGITS,
    TIMESTAMP_PRECISION_POS,
    BaseDialect,
    ConnectError,
    DbPath,
    ColType,
    ThreadedDatabase,
    import_helper,
    Mixin_RandomSample,
)
from ..abcs.database_types import (
    Decimal,
    Float,
    FractionalType,
    Integer,
    TemporalType,
    Text,
    Timestamp,
    TimestampTZ,
    Boolean,
    ColType_UUID,
)
from ..abcs.mixins import AbstractMixin_MD5, AbstractMixin_NormalizeValue, AbstractMixin_Schema
from ..abcs import Compilable
from ..queries import table, this, SKIP


@import_helper("vertica")
def import_vertica():
    import vertica_python

    return vertica_python


class Mixin_MD5(AbstractMixin_MD5):
    def md5_as_int(self, s: str) -> str:
        return f"CAST(HEX_TO_INTEGER(SUBSTRING(MD5({s}), {1 + MD5_HEXDIGITS - CHECKSUM_HEXDIGITS})) AS NUMERIC(38, 0))"


class Mixin_NormalizeValue(AbstractMixin_NormalizeValue):
    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            return f"TO_CHAR({value}::TIMESTAMP({coltype.precision}), 'YYYY-MM-DD HH24:MI:SS.US')"

        timestamp6 = f"TO_CHAR({value}::TIMESTAMP(6), 'YYYY-MM-DD HH24:MI:SS.US')"
        return (
            f"RPAD(LEFT({timestamp6}, {TIMESTAMP_PRECISION_POS+coltype.precision}), {TIMESTAMP_PRECISION_POS+6}, '0')"
        )

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"CAST({value} AS NUMERIC(38, {coltype.precision}))")

    def normalize_uuid(self, value: str, _coltype: ColType_UUID) -> str:
        # Trim doesn't work on CHAR type
        return f"TRIM(CAST({value} AS VARCHAR))"

    def normalize_boolean(self, value: str, _coltype: Boolean) -> str:
        return self.to_string(f"cast ({value} as int)")


class Mixin_Schema(AbstractMixin_Schema):
    def table_information(self) -> Compilable:
        return table("v_catalog", "tables")

    def list_tables(self, table_schema: str, like: Compilable = None) -> Compilable:
        return (
            self.table_information()
            .where(
                this.table_schema == table_schema,
                this.table_name.like(like) if like is not None else SKIP,
            )
            .select(this.table_name)
        )


class Dialect(BaseDialect, Mixin_Schema):
    name = "Vertica"
    ROUNDS_ON_PREC_LOSS = True

    TYPE_CLASSES = {
        # Timestamps
        "timestamp": Timestamp,
        "timestamptz": TimestampTZ,
        # Numbers
        "numeric": Decimal,
        "int": Integer,
        "float": Float,
        # Text
        "char": Text,
        "varchar": Text,
        # Boolean
        "boolean": Boolean,
    }
    MIXINS = {Mixin_Schema, Mixin_MD5, Mixin_NormalizeValue, Mixin_RandomSample}

    def quote(self, s: str):
        return f'"{s}"'

    def concat(self, items: List[str]) -> str:
        return " || ".join(items)

    def to_string(self, s: str) -> str:
        return f"CAST({s} AS VARCHAR)"

    def is_distinct_from(self, a: str, b: str) -> str:
        return f"not ({a} <=> {b})"

    def parse_type(
        self,
        table_path: DbPath,
        col_name: str,
        type_repr: str,
        datetime_precision: int = None,
        numeric_precision: int = None,
        numeric_scale: int = None,
    ) -> ColType:
        timestamp_regexps = {
            r"timestamp\(?(\d?)\)?": Timestamp,
            r"timestamptz\(?(\d?)\)?": TimestampTZ,
        }
        for m, t_cls in match_regexps(timestamp_regexps, type_repr):
            precision = int(m.group(1)) if m.group(1) else 6
            return t_cls(precision=precision, rounds=self.ROUNDS_ON_PREC_LOSS)

        number_regexps = {
            r"numeric\((\d+),(\d+)\)": Decimal,
        }
        for m, n_cls in match_regexps(number_regexps, type_repr):
            _prec, scale = map(int, m.groups())
            return n_cls(scale)

        string_regexps = {
            r"varchar\((\d+)\)": Text,
            r"char\((\d+)\)": Text,
        }
        for m, n_cls in match_regexps(string_regexps, type_repr):
            return n_cls()

        return super().parse_type(table_path, col_name, type_repr, datetime_precision, numeric_precision)

    def set_timezone_to_utc(self) -> str:
        return "SET TIME ZONE TO 'UTC'"

    def current_timestamp(self) -> str:
        return "current_timestamp(6)"


class Vertica(ThreadedDatabase):
    dialect = Dialect()
    CONNECT_URI_HELP = "vertica://<user>:<password>@<host>/<database>"
    CONNECT_URI_PARAMS = ["database?"]

    default_schema = "public"

    def __init__(self, *, thread_count, **kw):
        self._args = kw
        self._args["AUTOCOMMIT"] = False

        super().__init__(thread_count=thread_count)

    def create_connection(self):
        vertica = import_vertica()
        try:
            c = vertica.connect(**self._args)
            return c
        except vertica.errors.ConnectionError as e:
            raise ConnectError(*e.args) from e

    def select_table_schema(self, path: DbPath) -> str:
        schema, name = self._normalize_table_path(path)

        return (
            "SELECT column_name, data_type, datetime_precision, numeric_precision, numeric_scale "
            "FROM V_CATALOG.COLUMNS "
            f"WHERE table_name = '{name}' AND table_schema = '{schema}'"
        )
