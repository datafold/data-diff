from typing import List
from .database_types import Float, TemporalType, FractionalType, DbPath
from .postgresql import PostgreSQL, MD5_HEXDIGITS, CHECKSUM_HEXDIGITS, TIMESTAMP_PRECISION_POS, PostgresqlDialect


class Dialect(PostgresqlDialect):
    name = "Redshift"
    TYPE_CLASSES = {
        **PostgresqlDialect.TYPE_CLASSES,
        "double": Float,
        "real": Float,
    }

    def md5_as_int(self, s: str) -> str:
        return f"strtol(substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16)::decimal(38)"

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

    def concat(self, items: List[str]) -> str:
        joined_exprs = " || ".join(items)
        return f"({joined_exprs})"

    def is_distinct_from(self, a: str, b: str) -> str:
        return f"{a} IS NULL AND NOT {b} IS NULL OR {b} IS NULL OR {a}!={b}"


class Redshift(PostgreSQL):
    dialect = Dialect()

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)

        return (
            "SELECT column_name, data_type, datetime_precision, numeric_precision, numeric_scale FROM information_schema.columns "
            f"WHERE table_name = '{table.lower()}' AND table_schema = '{schema.lower()}'"
        )
