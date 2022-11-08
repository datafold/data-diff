from typing import Type, List, Optional, Union, Dict
from itertools import zip_longest
import dsnparse

from runtype import dataclass

from .base import Database, ThreadedDatabase
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


@dataclass
class MatchUriPath:
    database_cls: Type[Database]
    params: List[str]
    kwparams: List[str] = []
    help_str: str

    def match_path(self, dsn):
        dsn_dict = dict(dsn.query)
        matches = {}
        for param, arg in zip_longest(self.params, dsn.paths):
            if param is None:
                raise ValueError(f"Too many parts to path. Expected format: {self.help_str}")

            optional = param.endswith("?")
            param = param.rstrip("?")

            if arg is None:
                try:
                    arg = dsn_dict.pop(param)
                except KeyError:
                    if not optional:
                        raise ValueError(f"URI must specify '{param}'. Expected format: {self.help_str}")

                    arg = None

            assert param and param not in matches
            matches[param] = arg

        for param in self.kwparams:
            try:
                arg = dsn_dict.pop(param)
            except KeyError:
                raise ValueError(f"URI must specify '{param}'. Expected format: {self.help_str}")

            assert param and arg and param not in matches, (param, arg, matches.keys())
            matches[param] = arg

        for param, value in dsn_dict.items():
            if param in matches:
                raise ValueError(
                    f"Parameter '{param}' already provided as positional argument. Expected format: {self.help_str}"
                )

            matches[param] = value

        return matches


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
        help_str="databricks://:access_token@server_name/http_path",
    ),
    "duckdb": MatchUriPath(DuckDB, ['database', 'dbpath'], help_str="duckdb://<database>@<dbpath>"),
    "trino": MatchUriPath(Trino, ["catalog", "schema"], help_str="trino://<user>@<host>/<catalog>/<schema>"),
    "clickhouse": MatchUriPath(Clickhouse, ["database?"], help_str="clickhouse://<user>:<pass>@<host>/<database>"),
    "vertica": MatchUriPath(Vertica, ["database?"], help_str="vertica://<user>:<pass>@<host>/<database>"),
}


@dataclass
class Connect:
    match_uri_path: Dict[str, MatchUriPath]

    def connect_to_uri(self, db_uri: str, thread_count: Optional[int] = 1) -> Database:
        """Connect to the given database uri

        thread_count determines the max number of worker threads per database,
        if relevant. None means no limit.

        Parameters:
            db_uri (str): The URI for the database to connect
            thread_count (int, optional): Size of the threadpool. Ignored by cloud databases. (default: 1)

        Note: For non-cloud databases, a low thread-pool size may be a performance bottleneck.

        Supported schemes:
        - postgresql
        - mysql
        - oracle
        - snowflake
        - bigquery
        - redshift
        - presto
        - databricks
        - trino
        - clickhouse
        - vertica
        """

        dsn = dsnparse.parse(db_uri)
        if len(dsn.schemes) > 1:
            raise NotImplementedError("No support for multiple schemes")
        (scheme,) = dsn.schemes

        try:
            matcher = self.match_uri_path[scheme]
        except KeyError:
            raise NotImplementedError(f"Scheme {scheme} currently not supported")

        cls = matcher.database_cls

        if scheme == "databricks":
            assert not dsn.user
            kw = {}
            kw["access_token"] = dsn.password
            kw["http_path"] = dsn.path
            kw["server_hostname"] = dsn.host
            kw.update(dsn.query)
        elif scheme == 'duckdb':
            kw = {}
            kw['filepath'] = dsn.dbname
            kw['dbname'] = dsn.user
        else:
            kw = matcher.match_path(dsn)

            if scheme == "bigquery":
                kw["project"] = dsn.host
                return cls(**kw)

            if scheme == "snowflake":
                kw["account"] = dsn.host
                assert not dsn.port
                kw["user"] = dsn.user
                kw["password"] = dsn.password
            else:
                kw["host"] = dsn.host
                kw["port"] = dsn.port
                kw["user"] = dsn.user
                if dsn.password:
                    kw["password"] = dsn.password

        kw = {k: v for k, v in kw.items() if v is not None}

        if issubclass(cls, ThreadedDatabase):
            return cls(thread_count=thread_count, **kw)

        return cls(**kw)


    def connect_with_dict(self, d, thread_count):
        d = dict(d)
        driver = d.pop("driver")
        try:
            matcher = self.match_uri_path[driver]
        except KeyError:
            raise NotImplementedError(f"Driver {driver} currently not supported")

        cls = matcher.database_cls
        if issubclass(cls, ThreadedDatabase):
            return cls(thread_count=thread_count, **d)

        return cls(**d)


    def __call__(self, db_conf: Union[str, dict], thread_count: Optional[int] = 1) -> Database:
        """Connect to a database using the given database configuration.

        Configuration can be given either as a URI string, or as a dict of {option: value}.

        The dictionary configuration uses the same keys as the TOML 'database' definition given with --conf.

        thread_count determines the max number of worker threads per database,
        if relevant. None means no limit.

        Parameters:
            db_conf (str | dict): The configuration for the database to connect. URI or dict.
            thread_count (int, optional): Size of the threadpool. Ignored by cloud databases. (default: 1)

        Note: For non-cloud databases, a low thread-pool size may be a performance bottleneck.

        Supported drivers:
        - postgresql
        - mysql
        - oracle
        - snowflake
        - bigquery
        - redshift
        - presto
        - databricks
        - trino
        - clickhouse
        - vertica

        Example:
            >>> connect("mysql://localhost/db")
            <data_diff.databases.mysql.MySQL object at 0x0000025DB45F4190>
            >>> connect({"driver": "mysql", "host": "localhost", "database": "db"})
            <data_diff.databases.mysql.MySQL object at 0x0000025DB3F94820>
        """
        if isinstance(db_conf, str):
            return self.connect_to_uri(db_conf, thread_count)
        elif isinstance(db_conf, dict):
            return self.connect_with_dict(db_conf, thread_count)
        raise TypeError(f"db configuration must be a URI string or a dictionary. Instead got '{db_conf}'.")
