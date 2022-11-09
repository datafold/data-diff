from data_diff.sqeleton.databases import presto
from .base import BaseDialect


class Dialect(BaseDialect, presto.Dialect):
    pass


class Presto(presto.Presto):
    dialect = Dialect()
