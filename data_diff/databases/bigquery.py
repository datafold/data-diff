from data_diff.sqeleton.databases import bigquery
from .base import DatadiffDialect


class Dialect(bigquery.Dialect, bigquery.Mixin_MD5, bigquery.Mixin_NormalizeValue, DatadiffDialect):
    pass


class BigQuery(bigquery.BigQuery):
    dialect = Dialect()
