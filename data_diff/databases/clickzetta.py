import math
from typing import Any, Dict, Sequence
import logging

import attrs

from data_diff.abcs.database_types import (
    Integer,
    Float,
    Decimal,
    Timestamp,
    Text,
    TemporalType,
    NumericType,
    DbPath,
    ColType,
    UnknownColType,
    Boolean,
    Date,
)
from data_diff.databases.base import (
    MD5_HEXDIGITS,
    CHECKSUM_HEXDIGITS,
    BaseDialect,
    ThreadedDatabase,
    import_helper,
    parse_table_name,
)
from data_diff.schema import RawColumnInfo


@import_helper(text="You can install it using 'pip install clickzetta-connector'")
def import_clickzetta():
    import clickzetta

    return clickzetta


@attrs.define(frozen=False)
class Dialect(BaseDialect):
    name = "Clickzetta"
    ROUNDS_ON_PREC_LOSS = True
    TYPE_CLASSES = {
        # Numbers
        "INT": Integer,
        "SMALLINT": Integer,
        "TINYINT": Integer,
        "BIGINT": Integer,
        "FLOAT": Float,
        "DOUBLE": Float,
        "DECIMAL": Decimal,
        # Date
        "DATE": Date,
        # Timestamps
        "TIMESTAMP": Timestamp,
        # Text
        "STRING": Text,
        "CHAR": Text,
        "VARCHAR": Text,
        # Boolean
        "BOOLEAN": Boolean,
    }

    def quote(self, s: str):
        return f"`{s}`"

    def to_string(self, s: str) -> str:
        return f"cast({s} as string)"

    def _convert_db_precision_to_digits(self, p: int) -> int:
        return max(super()._convert_db_precision_to_digits(p) - 2, 0)

    def set_timezone_to_utc(self) -> str:
        raise NotImplementedError("Clickzetta does not support timezones")

    def parse_table_name(self, name: str) -> DbPath:
        path = parse_table_name(name)
        return tuple(i for i in path if i is not None)

    def md5_as_int(self, s: str) -> str:
        return f"cast(conv(substr(md5({s}), {1 + MD5_HEXDIGITS - CHECKSUM_HEXDIGITS}), 16, 10) as decimal(38, 0))"

    def md5_as_hex(self, s: str) -> str:
        return f"md5({s})"

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        precision_format = "S" * coltype.precision + "0" * (6 - coltype.precision)
        return f"date_format({value}, 'yyyy-MM-dd HH:mm:ss.{precision_format}')"

    def normalize_number(self, value: str, coltype: NumericType) -> str:
        return self.to_string(f"cast({value} as decimal(38, {coltype.precision}))")

    def normalize_boolean(self, value: str, _coltype: Boolean) -> str:
        return self.to_string(f"cast ({value} as int)")

    def type_repr(self, t) -> str:
        try:
            return {str: "STRING"}[t]
        except KeyError:
            return super().type_repr(t)


@attrs.define(frozen=False, init=False, kw_only=True)
class Clickzetta(ThreadedDatabase):
    dialect = Dialect()
    CONNECT_URI_HELP = "clickzetta://<username>:<pwd>@<instance>.<service>/<workspace>"
    CONNECT_URI_PARAMS = ["virtualcluster", "schema"]

    _args: Dict[str, Any]
    workspace: str

    def __init__(self, *, thread_count, **kw):
        super().__init__(thread_count=thread_count)
        logging.getLogger("clickzetta").setLevel(logging.WARNING)

        self._args = kw
        self.default_schema = kw.get("schema", "public")
        self.workspace = kw.get("workspace", "default")

    def create_connection(self):
        clickzetta = import_clickzetta()

        try:
            return clickzetta.connect(
                username=self._args["username"],
                password=self._args["password"],
                instance=self._args["instance"],
                service=self._args["service"],
                workspace=self._args["workspace"],
                vcluster=self._args["virtualcluster"],
                schema=self.default_schema,
            )
        except Exception as e:
            raise ConnectionError(*e.args) from e

    def query_table_schema(self, path: DbPath) -> Dict[str, tuple]:
        conn = self.create_connection()

        workspace, schema, table = self._normalize_table_path(path)
        with conn.cursor() as cursor:
            cursor.execute(f"show columns in {schema}.{table}")
            try:
                rows = cursor.fetchall()
            finally:
                conn.close()
            if not rows:
                raise RuntimeError(f"{self.name}: Table '{'.'.join(path)}' does not exist, or has no columns")

            d = {r[2]: RawColumnInfo(column_name=r[2], data_type=r[3].upper(), numeric_precision=None) for r in rows}
            assert len(d) == len(rows)
            return d

    def _process_table_schema(
        self, path: DbPath, raw_schema: Dict[str, RawColumnInfo], filter_columns: Sequence[str], where: str = None
    ):
        accept = {i.lower() for i in filter_columns}
        col_infos = [row for name, row in raw_schema.items() if name.lower() in accept]

        resulted_rows = []
        for info in col_infos:
            row_type = "DECIMAL" if info.data_type.startswith("DECIMAL") else info.data_type
            info = attrs.evolve(info, data_type=row_type)
            type_cls = self.dialect.TYPE_CLASSES.get(row_type, UnknownColType)

            if issubclass(type_cls, Integer):
                info = attrs.evolve(info, numeric_scale=0)

            elif issubclass(type_cls, Decimal):
                items = info.data_type[8:].rstrip(")").split(",")
                numeric_precision, numeric_scale = int(items[0]), int(items[1])
                info = attrs.evolve(
                    info,
                    numeric_precision=numeric_precision,
                    numeric_scale=numeric_scale,
                )

            else:
                info = attrs.evolve(info, numeric_precision=None)

            resulted_rows.append(info)

        col_dict: Dict[str, ColType] = {info.column_name: self.dialect.parse_type(path, info) for info in resulted_rows}

        self._refine_coltypes(path, col_dict, where)
        return col_dict

    @property
    def is_autocommit(self) -> bool:
        return True

    def _normalize_table_path(self, path: DbPath) -> DbPath:
        if len(path) == 1:
            return self.workspace, self.default_schema, path[0]
        elif len(path) == 2:
            return self.workspace, path[0], path[1]
        elif len(path) == 3:
            return path

        raise ValueError(
            f"{self.name}: Bad table path for {self}: '{'.'.join(path)}'. Expected format: table, schema.table, or workspace.schema.table"
        )
