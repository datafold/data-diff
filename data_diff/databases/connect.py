from typing import Type, List, Optional
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
    "trino": MatchUriPath(Trino, ["catalog", "schema"], help_str="trino://<user>@<host>/<catalog>/<schema>"),
}


def connect_to_uri(db_uri: str, thread_count: Optional[int] = 1) -> Database:
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
    """

    dsn = dsnparse.parse(db_uri)
    if len(dsn.schemes) > 1:
        raise NotImplementedError("No support for multiple schemes")
    (scheme,) = dsn.schemes

    try:
        matcher = MATCH_URI_PATH[scheme]
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


def connect_with_dict(d, thread_count):
    d = dict(d)
    driver = d.pop("driver")
    try:
        matcher = MATCH_URI_PATH[driver]
    except KeyError:
        raise NotImplementedError(f"Driver {driver} currently not supported")

    cls = matcher.database_cls
    if issubclass(cls, ThreadedDatabase):
        return cls(thread_count=thread_count, **d)

    return cls(**d)


def connect(x, thread_count):
    if isinstance(x, str):
        return connect_to_uri(x, thread_count)
    elif isinstance(x, dict):
        return connect_with_dict(x, thread_count)
    raise RuntimeError(x)
