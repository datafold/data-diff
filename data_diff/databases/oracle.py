from data_diff.sqeleton.databases import oracle
from data_diff.databases.base import DatadiffDialect


class Dialect(oracle.Dialect, oracle.Mixin_MD5, oracle.Mixin_NormalizeValue, DatadiffDialect):
    pass


class Oracle(oracle.Oracle):
    dialect = Dialect()
