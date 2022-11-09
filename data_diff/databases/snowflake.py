from data_diff.sqeleton.databases import snowflake
from .base import BaseDialect


class Dialect(BaseDialect, snowflake.Dialect):
    pass


class Snowflake(snowflake.Snowflake):
    dialect = Dialect()
