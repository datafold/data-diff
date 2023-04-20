from data_diff.sqeleton.databases import duckdb
from .base import DatadiffDialect


class Dialect(duckdb.Dialect, duckdb.Mixin_MD5, duckdb.Mixin_NormalizeValue, DatadiffDialect):
    pass


class DuckDB(duckdb.DuckDB):
    dialect = Dialect()
