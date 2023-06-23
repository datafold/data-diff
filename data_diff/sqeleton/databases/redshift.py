from typing import List, Dict
from ..abcs.database_types import (
    Float,
    JSON,
    TemporalType,
    FractionalType,
    DbPath,
    TimestampTZ,
)
from ..abcs.mixins import AbstractMixin_MD5
from .postgresql import (
    PostgreSQL,
    MD5_HEXDIGITS,
    CHECKSUM_HEXDIGITS,
    TIMESTAMP_PRECISION_POS,
    PostgresqlDialect,
    Mixin_NormalizeValue,
)


class Mixin_MD5(AbstractMixin_MD5):
    def md5_as_int(self, s: str) -> str:
        return f"strtol(substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16)::decimal(38)"


class Mixin_NormalizeValue(Mixin_NormalizeValue):
    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            timestamp = f"{value}::timestamp(6)"
            # Get seconds since epoch. Redshift doesn't support milli- or micro-seconds.
            secs = f"timestamp 'epoch' + round(extract(epoch from {timestamp})::decimal(38)"
            # Get the milliseconds from timestamp.
            ms = f"extract(ms from {timestamp})"
            # Get the microseconds from timestamp, without the milliseconds!
            us = f"extract(us from {timestamp})"
            # epoch = Total time since epoch in microseconds.
            epoch = f"{secs}*1000000 + {ms}*1000 + {us}"
            timestamp6 = (
                f"to_char({epoch}, -6+{coltype.precision}) * interval '0.000001 seconds', 'YYYY-mm-dd HH24:MI:SS.US')"
            )
        else:
            timestamp6 = f"to_char({value}::timestamp(6), 'YYYY-mm-dd HH24:MI:SS.US')"
        return (
            f"RPAD(LEFT({timestamp6}, {TIMESTAMP_PRECISION_POS+coltype.precision}), {TIMESTAMP_PRECISION_POS+6}, '0')"
        )

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"{value}::decimal(38,{coltype.precision})")

    def normalize_json(self, value: str, _coltype: JSON) -> str:
        return f"nvl2({value}, json_serialize({value}), NULL)"


class Dialect(PostgresqlDialect):
    name = "Redshift"
    TYPE_CLASSES = {
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


class Redshift(PostgreSQL):
    dialect = Dialect()
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

        d = {r[0]: r for r in rows}
        assert len(d) == len(rows)
        return d

    def select_view_columns(self, path: DbPath) -> str:
        _, schema, table = self._normalize_table_path(path)

        return """select * from pg_get_cols('{}.{}')
                cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int)
            """.format(
            schema, table
        )

    def query_pg_get_cols(self, path: DbPath) -> Dict[str, tuple]:
        rows = self.query(self.select_view_columns(path), list)

        if not rows:
            raise RuntimeError(f"{self.name}: View '{'.'.join(path)}' does not exist, or has no columns")

        output = {}
        for r in rows:
            col_name = r[2]
            type_info = r[3].split("(")
            base_type = type_info[0]
            precision = None
            scale = None

            if len(type_info) > 1:
                if base_type == "numeric":
                    precision, scale = type_info[1][:-1].split(",")
                    precision = int(precision)
                    scale = int(scale)

            out = [col_name, base_type, None, precision, scale]
            output[col_name] = tuple(out)

        return output

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
