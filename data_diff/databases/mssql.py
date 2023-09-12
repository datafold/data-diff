from data_diff.sqeleton.databases import mssql
from .base import DatadiffDialect


class Dialect(mssql.Dialect, mssql.Mixin_MD5, mssql.Mixin_NormalizeValue, DatadiffDialect):
    pass


class MsSql(mssql.MsSQL):
    dialect = Dialect()
