from data_diff.sqeleton.databases import databricks
from .base import BaseDialect


class Dialect(BaseDialect, databricks.Dialect):
    pass


class Databricks(databricks.Databricks):
    dialect = Dialect()
