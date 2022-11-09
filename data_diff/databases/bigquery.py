from data_diff.sqeleton.databases import bigquery
from .base import BaseDialect


class Dialect(BaseDialect, bigquery.Dialect):
    pass


class BigQuery(bigquery.BigQuery):
    dialect = Dialect()
