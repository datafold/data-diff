import logging

from data_diff.sqeleton.databases import Connect

from .postgresql import PostgreSQL
from .mysql import MySQL
from .oracle import Oracle
from .snowflake import Snowflake
from .bigquery import BigQuery
from .redshift import Redshift
from .presto import Presto
from .databricks import Databricks
from .trino import Trino
from .clickhouse import Clickhouse
from .vertica import Vertica
from .duckdb import DuckDB


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
