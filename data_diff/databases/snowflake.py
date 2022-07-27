import logging

from .database_types import *
from .base import Database, import_helper, _query_conn, CHECKSUM_MASK


@import_helper("snowflake")
def import_snowflake():
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    
    return snowflake, serialization, default_backend


class Snowflake(Database):
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
    ROUNDS_ON_PREC_LOSS = False

    def __init__(self,
        account: str,
        user: str,
        *,
        warehouse: str,
        schema: str,
        database: str,
        role: str = None,
        _port: int = None,
        password: str = None,  # default to None incase ssh key is used
        **kw,
    ):
        snowflake, serialization, default_backend = import_snowflake()
        logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

        # Got an error: snowflake.connector.network.RetryRequest: could not find io module state (interpreter shutdown?)
        # It's a known issue: https://github.com/snowflakedb/snowflake-connector-python/issues/145
        # Found a quick solution in comments
        logging.getLogger("snowflake.connector.network").disabled = True

        assert '"' not in schema, "Schema name should not contain quotes!"
        if (
            not password and "key" in kw
        ):  # if private keys are used instead of password for Snowflake connection, read in key from path specified and pass as "private_key" to connector.
            with open(kw.get("key"), "rb") as key:
                p_key = serialization.load_pem_private_key(key.read(), password=None, backend=default_backend())

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            self._conn = snowflake.connector.connect(
                user=user,
                private_key=pkb,  # replaces password
                account=account,
                role=role,
                database=database,
                warehouse=warehouse,
                schema=f'"{schema}"',
                **kw,
            )
        else:  # otherwise use password for connection
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

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"cast({value} as decimal(38, {coltype.precision}))")
