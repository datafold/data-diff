from data_diff.sqeleton.databases import snowflake
from .base import DatadiffDialect


class Dialect(snowflake.Dialect, snowflake.Mixin_MD5, snowflake.Mixin_NormalizeValue, DatadiffDialect):
    pass


class Snowflake(snowflake.Snowflake):
    dialect = Dialect()
