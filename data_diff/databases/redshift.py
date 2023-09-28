from data_diff.sqeleton.databases import redshift
from data_diff.databases.base import DatadiffDialect


class Dialect(redshift.Dialect, redshift.Mixin_MD5, redshift.Mixin_NormalizeValue, DatadiffDialect):
    pass


class Redshift(redshift.Redshift):
    dialect = Dialect()
