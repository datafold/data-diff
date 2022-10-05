import logging

from .database_types import *
from .base import MD5_HEXDIGITS, CHECKSUM_HEXDIGITS, Database, import_helper, parse_table_name


@import_helper(text="You can install it using 'pip install databricks-sql-connector'")
def import_databricks():
    import databricks.sql

    return databricks


class Databricks(Database):
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
    }

    ROUNDS_ON_PREC_LOSS = True

    def __init__(
        self,
        http_path: str,
        access_token: str,
        server_hostname: str,
        catalog: str = "hive_metastore",
        schema: str = "default",
        **kwargs,
    ):
        databricks = import_databricks()

        self._conn = databricks.sql.connect(
            server_hostname=server_hostname, http_path=http_path, access_token=access_token
        )

        logging.getLogger("databricks.sql").setLevel(logging.WARNING)

        self.catalog = catalog
        self.default_schema = schema
        self.kwargs = kwargs

    def _query(self, sql_code: str) -> list:
        "Uses the standard SQL cursor interface"
        return self._query_conn(self._conn, sql_code)

    def quote(self, s: str):
        return f"`{s}`"

    def md5_to_int(self, s: str) -> str:
        return f"cast(conv(substr(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16, 10) as decimal(38, 0))"

    def to_string(self, s: str) -> str:
        return f"cast({s} as string)"

    def _convert_db_precision_to_digits(self, p: int) -> int:
        # Subtracting 1 due to wierd precision issues
        return max(super()._convert_db_precision_to_digits(p) - 1, 0)

    def query_table_schema(self, path: DbPath) -> Dict[str, tuple]:
        # Databricks has INFORMATION_SCHEMA only for Databricks Runtime, not for Databricks SQL.
        # https://docs.databricks.com/spark/latest/spark-sql/language-manual/information-schema/columns.html
        # So, to obtain information about schema, we should use another approach.

        schema, table = self._normalize_table_path(path)
        with self._conn.cursor() as cursor:
            cursor.columns(catalog_name=self.catalog, schema_name=schema, table_name=table)
            rows = cursor.fetchall()
            if not rows:
                raise RuntimeError(f"{self.name}: Table '{'.'.join(path)}' does not exist, or has no columns")

            d = {r.COLUMN_NAME: r for r in rows}
            assert len(d) == len(rows)
            return d

    def _process_table_schema(
        self, path: DbPath, raw_schema: Dict[str, tuple], filter_columns: Sequence[str], where: str = None
    ):
        accept = {i.lower() for i in filter_columns}
        rows = [row for name, row in raw_schema.items() if name.lower() in accept]

        resulted_rows = []
        for row in rows:
            row_type = "DECIMAL" if row.DATA_TYPE == 3 else row.TYPE_NAME
            type_cls = self.TYPE_CLASSES.get(row_type, UnknownColType)

            if issubclass(type_cls, Integer):
                row = (row.COLUMN_NAME, row_type, None, None, 0)

            elif issubclass(type_cls, Float):
                numeric_precision = self._convert_db_precision_to_digits(row.DECIMAL_DIGITS)
                row = (row.COLUMN_NAME, row_type, None, numeric_precision, None)

            elif issubclass(type_cls, Decimal):
                # TYPE_NAME has a format DECIMAL(x,y)
                items = row.TYPE_NAME[8:].rstrip(")").split(",")
                numeric_precision, numeric_scale = int(items[0]), int(items[1])
                row = (row.COLUMN_NAME, row_type, None, numeric_precision, numeric_scale)

            elif issubclass(type_cls, Timestamp):
                row = (row.COLUMN_NAME, row_type, row.DECIMAL_DIGITS, None, None)

            else:
                row = (row.COLUMN_NAME, row_type, None, None, None)

            resulted_rows.append(row)

        col_dict: Dict[str, ColType] = {row[0]: self._parse_type(path, *row) for row in resulted_rows}

        self._refine_coltypes(path, col_dict, where)
        return col_dict

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        """Databricks timestamp contains no more than 6 digits in precision"""

        if coltype.rounds:
            timestamp = f"cast(round(unix_micros({value}) / 1000000, {coltype.precision}) * 1000000 as bigint)"
            return f"date_format(timestamp_micros({timestamp}), 'yyyy-MM-dd HH:mm:ss.SSSSSS')"

        precision_format = "S" * coltype.precision + "0" * (6 - coltype.precision)
        return f"date_format({value}, 'yyyy-MM-dd HH:mm:ss.{precision_format}')"

    def normalize_number(self, value: str, coltype: NumericType) -> str:
        return self.to_string(f"cast({value} as decimal(38, {coltype.precision}))")

    def parse_table_name(self, name: str) -> DbPath:
        path = parse_table_name(name)
        return self._normalize_table_path(path)

    def close(self):
        self._conn.close()
