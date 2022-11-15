from data_diff.sqeleton.databases import clickhouse
from .base import DatadiffDialect


class Dialect(clickhouse.Dialect, clickhouse.Mixin_MD5, clickhouse.Mixin_NormalizeValue, DatadiffDialect):
    pass


class Clickhouse(clickhouse.Clickhouse):
    dialect = Dialect()
