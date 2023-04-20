from data_diff.sqeleton.databases import trino
from .base import DatadiffDialect


class Dialect(trino.Dialect, trino.Mixin_MD5, trino.Mixin_NormalizeValue, DatadiffDialect):
    pass


class Trino(trino.Trino):
    dialect = Dialect()
