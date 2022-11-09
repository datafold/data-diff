from data_diff.sqeleton.databases import trino
from .base import BaseDialect


class Dialect(BaseDialect, trino.Dialect):
    pass


class Trino(trino.Trino):
    dialect = Dialect()
