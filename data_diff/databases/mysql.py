from data_diff.sqeleton.databases import mysql
from .base import DatadiffDialect


class Dialect(mysql.Dialect, mysql.Mixin_MD5, mysql.Mixin_NormalizeValue, DatadiffDialect):
    pass


class MySQL(mysql.MySQL):
    dialect = Dialect()
