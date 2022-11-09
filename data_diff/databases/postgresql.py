from data_diff.sqeleton.databases import postgresql
from .base import BaseDialect


class PostgresqlDialect(BaseDialect, postgresql.PostgresqlDialect):
    pass


class PostgreSQL(postgresql.PostgreSQL):
    dialect = PostgresqlDialect()
