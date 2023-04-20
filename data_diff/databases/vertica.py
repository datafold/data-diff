from data_diff.sqeleton.databases import vertica
from .base import DatadiffDialect


class Dialect(vertica.Dialect, vertica.Mixin_MD5, vertica.Mixin_NormalizeValue, DatadiffDialect):
    pass


class Vertica(vertica.Vertica):
    dialect = Dialect()
