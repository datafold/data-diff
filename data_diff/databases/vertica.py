from data_diff.sqeleton.databases import vertica
from .base import BaseDialect

class Dialect(BaseDialect, vertica.Dialect):
    pass

class Vertica(vertica.Vertica):
    dialect = Dialect()
