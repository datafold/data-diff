from data_diff.sqeleton.databases import presto
from .base import DatadiffDialect


class Dialect(presto.Dialect, presto.Mixin_MD5, presto.Mixin_NormalizeValue, DatadiffDialect):
    pass


class Presto(presto.Presto):
    dialect = Dialect()
