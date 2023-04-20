from data_diff.sqeleton.databases import postgresql as pg
from .base import DatadiffDialect


class PostgresqlDialect(pg.PostgresqlDialect, pg.Mixin_MD5, pg.Mixin_NormalizeValue, DatadiffDialect):
    pass


class PostgreSQL(pg.PostgreSQL):
    dialect = PostgresqlDialect()
