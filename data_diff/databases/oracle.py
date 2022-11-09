from data_diff.sqeleton.databases import oracle
from .base import BaseDialect


class Dialect(BaseDialect, oracle.Dialect):
    pass


class Oracle(oracle.Oracle):
    dialect = Dialect()
