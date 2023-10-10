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
)
from data_diff.abcs.mixins import AbstractMixin_MD5, AbstractMixin_NormalizeValue
from data_diff.databases.base import (
    MD5_HEXDIGITS,
    CHECKSUM_HEXDIGITS,
    BaseDialect,
    ThreadedDatabase,
    import_helper,
    parse_table_name,
    Mixin_RandomSample,
)


@import_helper(text="You can install it using 'pip install databricks-sql-connector'")
def import_databricks():
    import databricks.sql

    return databricks


@attrs.define(frozen=False)
class Mixin_MD5(AbstractMixin_MD5):
    def md5_as_int(self, s: str) -> str:
        return f"cast(conv(substr(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16, 10) as decimal(38, 0))"


@attrs.define(frozen=False)
class Mixin_NormalizeValue(AbstractMixin_NormalizeValue):
    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        """Databricks timestamp contains no more than 6 digits in precision"""

        if coltype.rounds:
            timestamp = f"cast(round(unix_micros({value}) / 1000000, {coltype.precision}) * 1000000 as bigint)"
            return f"date_format(timestamp_micros({timestamp}), 'yyyy-MM-dd HH:mm:ss.SSSSSS')"

        precision_format = "S" * coltype.precision + "0" * (6 - coltype.precision)
        return f"date_format({value}, 'yyyy-MM-dd HH:mm:ss.{precision_format}')"

    def normalize_number(self, value: str, coltype: NumericType) -> str:
        value = f"cast({value} as decimal(38, {coltype.precision}))"
        if coltype.precision > 0:
            value = f"format_number({value}, {coltype.precision})"
        return f"replace({self.to_string(value)}, ',', '')"

    def normalize_boolean(self, value: str, _coltype: Boolean) -> str:
        return self.to_string(f"cast ({value} as int)")


@attrs.define(frozen=False)
class Dialect(BaseDialect, Mixin_MD5, Mixin_NormalizeValue, AbstractMixin_MD5, AbstractMixin_NormalizeValue):
    name = "Databricks"
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
        # Timestamps
        "TIMESTAMP": Timestamp,
        # Text
        "STRING": Text,
        # Boolean
        "BOOLEAN": Boolean,
    }

    def quote(self, s: str):
        return f"`{s}`"

    def to_string(self, s: str) -> str:
        return f"cast({s} as string)"

    def _convert_db_precision_to_digits(self, p: int) -> int:
        # Subtracting 2 due to wierd precision issues
        return max(super()._convert_db_precision_to_digits(p) - 2, 0)

    def set_timezone_to_utc(self) -> str:
        return "SET TIME ZONE 'UTC'"

    def parse_table_name(self, name: str) -> DbPath:
        path = parse_table_name(name)
        return tuple(i for i in path if i is not None)


@attrs.define(frozen=False, init=False, kw_only=True)
class Databricks(ThreadedDatabase):
    dialect = Dialect()
    CONNECT_URI_HELP = "databricks://:<access_token>@<server_hostname>/<http_path>"
    CONNECT_URI_PARAMS = ["catalog", "schema"]

    catalog: str
    _args: Dict[str, Any]

    def __init__(self, *, thread_count, **kw):
        super().__init__(thread_count=thread_count)
        logging.getLogger("databricks.sql").setLevel(logging.WARNING)

        self._args = kw
        self.default_schema = kw.get("schema", "default")
        self.catalog = kw.get("catalog", "hive_metastore")

    def create_connection(self):
        databricks = import_databricks()

        try:
            return databricks.sql.connect(
                server_hostname=self._args["server_hostname"],
                http_path=self._args["http_path"],
                access_token=self._args["access_token"],
                catalog=self.catalog,
            )
        except databricks.sql.exc.Error as e:
            raise ConnectionError(*e.args) from e

    def query_table_schema(self, path: DbPath) -> Dict[str, tuple]:
        # Databricks has INFORMATION_SCHEMA only for Databricks Runtime, not for Databricks SQL.
        # https://docs.databricks.com/spark/latest/spark-sql/language-manual/information-schema/columns.html
        # So, to obtain information about schema, we should use another approach.

        conn = self.create_connection()

        catalog, schema, table = self._normalize_table_path(path)
        with conn.cursor() as cursor:
            cursor.columns(catalog_name=catalog, schema_name=schema, table_name=table)
            try:
                rows = cursor.fetchall()
            finally:
                conn.close()
            if not rows:
                raise RuntimeError(f"{self.name}: Table '{'.'.join(path)}' does not exist, or has no columns")

            d = {r.COLUMN_NAME: (r.COLUMN_NAME, r.TYPE_NAME, r.DECIMAL_DIGITS, None, None) for r in rows}
            assert len(d) == len(rows)
            return d

    def _process_table_schema(
        self, path: DbPath, raw_schema: Dict[str, tuple], filter_columns: Sequence[str], where: str = None
    ):
        accept = {i.lower() for i in filter_columns}
        rows = [row for name, row in raw_schema.items() if name.lower() in accept]

        resulted_rows = []
        for row in rows:
            row_type = "DECIMAL" if row[1].startswith("DECIMAL") else row[1]
            type_cls = self.dialect.TYPE_CLASSES.get(row_type, UnknownColType)

            if issubclass(type_cls, Integer):
                row = (row[0], row_type, None, None, 0)

            elif issubclass(type_cls, Float):
                numeric_precision = math.ceil(row[2] / math.log(2, 10))
                row = (row[0], row_type, None, numeric_precision, None)

            elif issubclass(type_cls, Decimal):
                items = row[1][8:].rstrip(")").split(",")
                numeric_precision, numeric_scale = int(items[0]), int(items[1])
                row = (row[0], row_type, None, numeric_precision, numeric_scale)

            elif issubclass(type_cls, Timestamp):
                row = (row[0], row_type, row[2], None, None)

            else:
                row = (row[0], row_type, None, None, None)

            resulted_rows.append(row)

        col_dict: Dict[str, ColType] = {row[0]: self.dialect.parse_type(path, *row) for row in resulted_rows}

        self._refine_coltypes(path, col_dict, where)
        return col_dict

    @property
    def is_autocommit(self) -> bool:
        return True

    def _normalize_table_path(self, path: DbPath) -> DbPath:
        if len(path) == 1:
            return self.catalog, self.default_schema, path[0]
        elif len(path) == 2:
            return self.catalog, path[0], path[1]
        elif len(path) == 3:
            return path

        raise ValueError(
            f"{self.name}: Bad table path for {self}: '{'.'.join(path)}'. Expected format: table, schema.table, or catalog.schema.table"
        )
