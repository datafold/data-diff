from data_diff.sqeleton.databases.postgresql import PostgresqlDialect, PostgreSQL
from .base import BaseDialect

class PostgresqlDialect(BaseDialect, PostgresqlDialect):
    pass

class PostgreSQL(PostgreSQL):
    dialect = PostgresqlDialect()
