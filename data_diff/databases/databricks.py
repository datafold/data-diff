from data_diff.sqeleton.databases import databricks
from data_diff.databases.base import DatadiffDialect


class Dialect(databricks.Dialect, databricks.Mixin_MD5, databricks.Mixin_NormalizeValue, DatadiffDialect):
    pass


class Databricks(databricks.Databricks):
    dialect = Dialect()
