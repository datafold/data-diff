from typing import Union, List
import logging

from .database_types import Timestamp, TimestampTZ, Decimal, Float, Text, FractionalType, TemporalType, DbPath
from .base import BaseDialect, ConnectError, Database, import_helper, CHECKSUM_MASK, ThreadLocalInterpreter


@import_helper("snowflake")
def import_snowflake():
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend

    return snowflake, serialization, default_backend


class Dialect(BaseDialect):
    name = "Snowflake"
    ROUNDS_ON_PREC_LOSS = False
    TYPE_CLASSES = {
        # Timestamps
        "TIMESTAMP_NTZ": Timestamp,
        "TIMESTAMP_LTZ": Timestamp,
        "TIMESTAMP_TZ": TimestampTZ,
        # Numbers
        "NUMBER": Decimal,
        "FLOAT": Float,
        # Text
        "TEXT": Text,
    }

    def explain_as_text(self, query: str) -> str:
        return f"EXPLAIN USING TEXT {query}"

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            timestamp = f"to_timestamp(round(date_part(epoch_nanosecond, {value}::timestamp(9))/1000000000, {coltype.precision}))"
        else:
            timestamp = f"cast({value} as timestamp({coltype.precision}))"

        return f"to_char({timestamp}, 'YYYY-MM-DD HH24:MI:SS.FF6')"

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"cast({value} as decimal(38, {coltype.precision}))")

    def quote(self, s: str):
        return f'"{s}"'

    def md5_as_int(self, s: str) -> str:
        return f"BITAND(md5_number_lower64({s}), {CHECKSUM_MASK})"

    def to_string(self, s: str):
        return f"cast({s} as string)"


class Snowflake(Database):
    dialect = Dialect()

    def __init__(self, *, schema: str, **kw):
        snowflake, serialization, default_backend = import_snowflake()
        logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

        # Ignore the error: snowflake.connector.network.RetryRequest: could not find io module state
        # It's a known issue: https://github.com/snowflakedb/snowflake-connector-python/issues/145
        logging.getLogger("snowflake.connector.network").disabled = True

        assert '"' not in schema, "Schema name should not contain quotes!"
        # If a private key is used, read it from the specified path and pass it as "private_key" to the connector.
        if "key" in kw:
            with open(kw.get("key"), "rb") as key:
                if "password" in kw:
                    raise ConnectError("Cannot use password and key at the same time")
                p_key = serialization.load_pem_private_key(
                    key.read(),
                    password=None,
                    backend=default_backend(),
                )

            kw["private_key"] = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

        self._conn = snowflake.connector.connect(schema=f'"{schema}"', **kw)

        self.default_schema = schema

    def close(self):
        self._conn.close()

    def _query(self, sql_code: Union[str, ThreadLocalInterpreter]):
        "Uses the standard SQL cursor interface"
        return self._query_conn(self._conn, sql_code)

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)
        return super().select_table_schema((schema, table))

    @property
    def is_autocommit(self) -> bool:
        return True

    def query_table_unique_columns(self, path: DbPath) -> List[str]:
        return []
