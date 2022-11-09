from data_diff.sqeleton.databases import redshift
from .base import BaseDialect


class Dialect(BaseDialect, redshift.Dialect):
    pass


class Redshift(redshift.Redshift):
    dialect = Dialect()
