from typing import ClassVar, List, Dict, Type

import attrs

from data_diff.abcs.database_types import (
    ColType,
    Float,
    JSON,
    TemporalType,
    FractionalType,
    DbPath,
    TimestampTZ,
)
from data_diff.databases.postgresql import (
    BaseDialect,
    PostgreSQL,
    MD5_HEXDIGITS,
    CHECKSUM_HEXDIGITS,
    CHECKSUM_OFFSET,
    TIMESTAMP_PRECISION_POS,
    PostgresqlDialect,
)


@attrs.define(frozen=False)
class Dialect(PostgresqlDialect):
    name = "Redshift"
    TYPE_CLASSES: ClassVar[Dict[str, Type[ColType]]] = {
        **PostgresqlDialect.TYPE_CLASSES,
        "double": Float,
        "real": Float,
        "super": JSON,
    }
    SUPPORTS_INDEXES = False

    def concat(self, items: List[str]) -> str:
        joined_exprs = " || ".join(items)
        return f"({joined_exprs})"

    def is_distinct_from(self, a: str, b: str) -> str:
        return f"({a} IS NULL != {b} IS NULL) OR ({a}!={b})"

    def type_repr(self, t) -> str:
        if isinstance(t, TimestampTZ):
            return f"timestamptz"
        return super().type_repr(t)

    def md5_as_int(self, s: str) -> str:
        return f"strtol(substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16)::decimal(38) - {CHECKSUM_OFFSET}"

    def md5_as_hex(self, s: str) -> str:
        return f"md5({s})"

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"{value}::decimal(38,{coltype.precision})")

    def normalize_json(self, value: str, _coltype: JSON) -> str:
        return f"nvl2({value}, json_serialize({value}), NULL)"


@attrs.define(frozen=False, init=False, kw_only=True)
class Redshift(PostgreSQL):
    DIALECT_CLASS: ClassVar[Type[BaseDialect]] = Dialect
    CONNECT_URI_HELP = "redshift://<user>:<password>@<host>/<database>"
    CONNECT_URI_PARAMS = ["database?"]

    def select_table_schema(self, path: DbPath) -> str:
        database, schema, table = self._normalize_table_path(path)

        info_schema_path = ["information_schema", "columns"]
        if database:
            info_schema_path.insert(0, database)

        return (
            f"SELECT column_name, data_type, datetime_precision, numeric_precision, numeric_scale FROM {'.'.join(info_schema_path)} "
            f"WHERE table_name = '{table.lower()}' AND table_schema = '{schema.lower()}'"
        )

    def select_external_table_schema(self, path: DbPath) -> str:
        database, schema, table = self._normalize_table_path(path)

        db_clause = ""
        if database:
            db_clause = f" AND redshift_database_name = '{database.lower()}'"

        return (
            f"""SELECT
                columnname AS column_name
                , CASE WHEN external_type = 'string' THEN 'varchar' ELSE external_type END AS data_type
                , NULL AS datetime_precision
                , NULL AS numeric_precision
                , NULL AS numeric_scale
            FROM svv_external_columns
                WHERE tablename = '{table.lower()}' AND schemaname = '{schema.lower()}'
            """
            + db_clause
        )

    def query_external_table_schema(self, path: DbPath) -> Dict[str, tuple]:
        rows = self.query(self.select_external_table_schema(path), list)
        if not rows:
            raise RuntimeError(f"{self.name}: Table '{'.'.join(path)}' does not exist, or has no columns")

        schema_dict = self._normalize_schema_info(rows)

        return schema_dict

    def select_view_columns(self, path: DbPath) -> str:
        _, schema, table = self._normalize_table_path(path)

        return """select * from pg_get_cols('{}.{}')
                cols(col_name name, col_type varchar)
            """.format(schema, table)

    def query_pg_get_cols(self, path: DbPath) -> Dict[str, tuple]:
        rows = self.query(self.select_view_columns(path), list)

        if not rows:
            raise RuntimeError(f"{self.name}: View '{'.'.join(path)}' does not exist, or has no columns")

        schema_dict = self._normalize_schema_info(rows)

        return schema_dict

    # when using a non-information_schema source, strip (N) from type(N) etc. to match
    # typical information_schema output
    def _normalize_schema_info(self, rows) -> Dict[str, tuple]:
        schema_dict = {}
        for r in rows:
            col_name = r[0]
            type_info = r[1].split("(")
            base_type = type_info[0]
            precision = None
            scale = None

            if len(type_info) > 1:
                if base_type == "numeric":
                    precision, scale = type_info[1][:-1].split(",")
                    precision = int(precision)
                    scale = int(scale)

            out = [col_name, base_type, None, precision, scale]
            schema_dict[col_name] = tuple(out)
        return schema_dict

    def query_table_schema(self, path: DbPath) -> Dict[str, tuple]:
        try:
            return super().query_table_schema(path)
        except RuntimeError:
            try:
                return self.query_external_table_schema(path)
            except RuntimeError:
                return self.query_pg_get_cols(path)

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
