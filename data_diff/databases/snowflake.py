import logging

from .database_types import *
from .base import Database, import_helper, _query_conn, CHECKSUM_MASK


@import_helper("snowflake")
def import_snowflake():
    import snowflake.connector

    return snowflake


class Snowflake(Database):
    DATETIME_TYPES = {
        "TIMESTAMP_NTZ": Timestamp,
        "TIMESTAMP_LTZ": Timestamp,
        "TIMESTAMP_TZ": TimestampTZ,
    }
    NUMERIC_TYPES = {
        "NUMBER": Decimal,
        "FLOAT": Float,
    }
    ROUNDS_ON_PREC_LOSS = False

    def __init__(
        self,
        account: str,
        _port: int,
        user: str,
        password: str,
        *,
        warehouse: str,
        schema: str,
        database: str,
        role: str = None,
        **kw,
    ):
        snowflake = import_snowflake()
        logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

        # Got an error: snowflake.connector.network.RetryRequest: could not find io module state (interpreter shutdown?)
        # It's a known issue: https://github.com/snowflakedb/snowflake-connector-python/issues/145
        # Found a quick solution in comments
        logging.getLogger("snowflake.connector.network").disabled = True

        assert '"' not in schema, "Schema name should not contain quotes!"
        self._conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            role=role,
            database=database,
            warehouse=warehouse,
            schema=f'"{schema}"',
            **kw,
        )

        self.default_schema = schema

    def close(self):
        self._conn.close()

    def _query(self, sql_code: str) -> list:
        "Uses the standard SQL cursor interface"
        return _query_conn(self._conn, sql_code)

    def quote(self, s: str):
        return f'"{s}"'

    def md5_to_int(self, s: str) -> str:
        return f"BITAND(md5_number_lower64({s}), {CHECKSUM_MASK})"

    def to_string(self, s: str):
        return f"cast({s} as string)"

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)
        return super().select_table_schema((schema, table))

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            timestamp = f"to_timestamp(round(date_part(epoch_nanosecond, {value}::timestamp(9))/1000000000, {coltype.precision}))"
        else:
            timestamp = f"cast({value} as timestamp({coltype.precision}))"

        return f"to_char({timestamp}, 'YYYY-MM-DD HH24:MI:SS.FF6')"

    def normalize_number(self, value: str, coltype: NumericType) -> str:
        return self.to_string(f"cast({value} as decimal(38, {coltype.precision}))")
