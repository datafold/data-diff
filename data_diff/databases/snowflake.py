import base64
from typing import Any, ClassVar, Union, List, Type, Optional
import logging

import attrs

from data_diff.abcs.database_types import (
    Timestamp,
    TimestampTZ,
    Decimal,
    Float,
    Text,
    FractionalType,
    TemporalType,
    DbPath,
    Boolean,
    Date,
)
from data_diff.databases.base import (
    BaseDialect,
    ConnectError,
    Database,
    import_helper,
    CHECKSUM_MASK,
    ThreadLocalInterpreter,
    CHECKSUM_OFFSET,
)


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
        "DATE": Date,
        # Numbers
        "NUMBER": Decimal,
        "FLOAT": Float,
        # Text
        "TEXT": Text,
        # Boolean
        "BOOLEAN": Boolean,
    }

    def explain_as_text(self, query: str) -> str:
        return f"EXPLAIN USING TEXT {query}"

    def quote(self, s: str):
        return f'"{s}"'

    def to_string(self, s: str):
        return f"cast({s} as string)"

    def set_timezone_to_utc(self) -> str:
        return "ALTER SESSION SET TIMEZONE = 'UTC'"

    def optimizer_hints(self, hints: str) -> str:
        raise NotImplementedError("Optimizer hints not yet implemented in snowflake")

    def type_repr(self, t) -> str:
        if isinstance(t, TimestampTZ):
            return f"timestamp_tz({t.precision})"
        return super().type_repr(t)

    def md5_as_int(self, s: str) -> str:
        return f"BITAND(md5_number_lower64({s}), {CHECKSUM_MASK}) - {CHECKSUM_OFFSET}"

    def md5_as_hex(self, s: str) -> str:
        return f"md5({s})"

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            timestamp = f"to_timestamp(round(date_part(epoch_nanosecond, convert_timezone('UTC', {value})::timestamp(9))/1000000000, {coltype.precision}))"
        else:
            timestamp = f"cast(convert_timezone('UTC', {value}) as timestamp({coltype.precision}))"

        return f"to_char({timestamp}, 'YYYY-MM-DD HH24:MI:SS.FF6')"

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"cast({value} as decimal(38, {coltype.precision}))")

    def normalize_boolean(self, value: str, _coltype: Boolean) -> str:
        return self.to_string(f"{value}::int")


@attrs.define(frozen=False, init=False, kw_only=True)
class Snowflake(Database):
    DIALECT_CLASS: ClassVar[Type[BaseDialect]] = Dialect
    CONNECT_URI_HELP = "snowflake://<user>:<password>@<account>/<database>/<SCHEMA>?warehouse=<WAREHOUSE>"
    CONNECT_URI_PARAMS = ["database", "schema"]
    CONNECT_URI_KWPARAMS = ["warehouse"]

    _conn: Any

    def __init__(self, *, schema: str, key: Optional[str] = None, key_content: Optional[str] = None, **kw):
        super().__init__()
        snowflake, serialization, default_backend = import_snowflake()
        logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

        # Ignore the error: snowflake.connector.network.RetryRequest: could not find io module state
        # It's a known issue: https://github.com/snowflakedb/snowflake-connector-python/issues/145
        logging.getLogger("snowflake.connector.network").disabled = True

        assert '"' not in schema, "Schema name should not contain quotes!"
        if key_content and key:
            raise ConnectError("Only key value or key file path can be specified, not both")

        key_bytes = None
        if key:
            with open(key, "rb") as f:
                key_bytes = f.read()
        if key_content:
            key_bytes = base64.b64decode(key_content)

        # If a private key is used, read it from the specified path and pass it as "private_key" to the connector.
        if key_bytes:
            if "password" in kw:
                raise ConnectError("Cannot use password and key at the same time")
            if kw.get("private_key_passphrase"):
                encoded_passphrase = kw.get("private_key_passphrase").encode()
            else:
                encoded_passphrase = None
            p_key = serialization.load_pem_private_key(
                key_bytes,
                password=encoded_passphrase,
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
        super().close()
        self._conn.close()

    def _query(self, sql_code: Union[str, ThreadLocalInterpreter]):
        "Uses the standard SQL cursor interface"
        return self._query_conn(self._conn, sql_code)

    def select_table_schema(self, path: DbPath) -> str:
        """Provide SQL for selecting the table schema as (name, type, date_prec, num_prec)"""
        database, schema, name = self._normalize_table_path(path)
        info_schema_path = ["information_schema", "columns"]
        if database:
            info_schema_path.insert(0, database)

        return (
            "SELECT column_name, data_type, datetime_precision, numeric_precision, numeric_scale "
            f"FROM {'.'.join(info_schema_path)} "
            f"WHERE table_name = '{name}' AND table_schema = '{schema}'"
        )

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

    @property
    def is_autocommit(self) -> bool:
        return True

    def query_table_unique_columns(self, path: DbPath) -> List[str]:
        return []
