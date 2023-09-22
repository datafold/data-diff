import logging

from data_diff.sqeleton.databases import Connect

from data_diff.databases.postgresql import PostgreSQL
from data_diff.databases.mysql import MySQL
from data_diff.databases.oracle import Oracle
from data_diff.databases.snowflake import Snowflake
from data_diff.databases.bigquery import BigQuery
from data_diff.databases.redshift import Redshift
from data_diff.databases.presto import Presto
from data_diff.databases.databricks import Databricks
from data_diff.databases.trino import Trino
from data_diff.databases.clickhouse import Clickhouse
from data_diff.databases.vertica import Vertica
from data_diff.databases.duckdb import DuckDB
from data_diff.databases.mssql import MsSql


DATABASE_BY_SCHEME = {
    "postgresql": PostgreSQL,
    "mysql": MySQL,
    "oracle": Oracle,
    "redshift": Redshift,
    "snowflake": Snowflake,
    "presto": Presto,
    "bigquery": BigQuery,
    "databricks": Databricks,
    "duckdb": DuckDB,
    "trino": Trino,
    "clickhouse": Clickhouse,
    "vertica": Vertica,
    "mssql": MsSql,
}


class Connect_SetUTC(Connect):
    """Provides methods for connecting to a supported database using a URL or connection dict.

    Ensures all sessions use UTC Timezone, if possible.
    """

    def _connection_created(self, db):
        db = super()._connection_created(db)
        try:
            db.query(db.dialect.set_timezone_to_utc())
        except NotImplementedError:
            logging.debug(
                f"Database '{db}' does not allow setting timezone. We recommend making sure it's set to 'UTC'."
            )
        return db


connect = Connect_SetUTC(DATABASE_BY_SCHEME)
