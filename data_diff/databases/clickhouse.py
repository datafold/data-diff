from data_diff.sqeleton.databases import clickhouse
from .base import BaseDialect


class Dialect(BaseDialect, clickhouse.Dialect):
    pass


class Clickhouse(clickhouse.Clickhouse):
    dialect = Dialect()
