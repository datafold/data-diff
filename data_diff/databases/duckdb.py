from data_diff.sqeleton.databases import duckdb
from .base import BaseDialect

class Dialect(BaseDialect, duckdb.DuckDBDialect):
    pass

class DuckDB(duckdb.DuckDB):
    dialect = Dialect()
