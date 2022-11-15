from data_diff.sqeleton.databases.connect import MatchUriPath, Connect

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


MATCH_URI_PATH = {
    "postgresql": MatchUriPath(PostgreSQL, ["database?"], help_str="postgresql://<user>:<pass>@<host>/<database>"),
    "mysql": MatchUriPath(MySQL, ["database?"], help_str="mysql://<user>:<pass>@<host>/<database>"),
    "oracle": MatchUriPath(Oracle, ["database?"], help_str="oracle://<user>:<pass>@<host>/<database>"),
    # "mssql": MatchUriPath(MsSQL, ["database?"], help_str="mssql://<user>:<pass>@<host>/<database>"),
    "redshift": MatchUriPath(Redshift, ["database?"], help_str="redshift://<user>:<pass>@<host>/<database>"),
    "snowflake": MatchUriPath(
        Snowflake,
        ["database", "schema"],
        ["warehouse"],
        help_str="snowflake://<user>:<pass>@<account>/<database>/<SCHEMA>?warehouse=<WAREHOUSE>",
    ),
    "presto": MatchUriPath(Presto, ["catalog", "schema"], help_str="presto://<user>@<host>/<catalog>/<schema>"),
    "bigquery": MatchUriPath(BigQuery, ["dataset"], help_str="bigquery://<project>/<dataset>"),
    "databricks": MatchUriPath(
        Databricks,
        ["catalog", "schema"],
        help_str="databricks://:<access_token>@<server_name>/<http_path>",
    ),
    "trino": MatchUriPath(Trino, ["catalog", "schema"], help_str="trino://<user>@<host>/<catalog>/<schema>"),
    "clickhouse": MatchUriPath(Clickhouse, ["database?"], help_str="clickhouse://<user>:<pass>@<host>/<database>"),
    "vertica": MatchUriPath(Vertica, ["database?"], help_str="vertica://<user>:<pass>@<host>/<database>"),
    "duckdb": MatchUriPath(DuckDB, ["database", "dbpath"], help_str="duckdb://<database>@<dbpath>"),
}

connect = Connect(MATCH_URI_PATH)
