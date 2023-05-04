from typing import Type, Optional, Union, Dict
from itertools import zip_longest
from contextlib import suppress
import dsnparse
import toml

from runtype import dataclass

from ..abcs.mixins import AbstractMixin
from ..utils import WeakCache, Self
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

    def match_path(self, dsn):
        help_str = self.database_cls.CONNECT_URI_HELP
        params = self.database_cls.CONNECT_URI_PARAMS
        kwparams = self.database_cls.CONNECT_URI_KWPARAMS

        dsn_dict = dict(dsn.query)
        matches = {}
        for param, arg in zip_longest(params, dsn.paths):
            if param is None:
                raise ValueError(f"Too many parts to path. Expected format: {help_str}")

            optional = param.endswith("?")
            param = param.rstrip("?")

            if arg is None:
                try:
                    arg = dsn_dict.pop(param)
                except KeyError:
                    if not optional:
                        raise ValueError(f"URI must specify '{param}'. Expected format: {help_str}")

                    arg = None

            assert param and param not in matches
            matches[param] = arg

        for param in kwparams:
            try:
                arg = dsn_dict.pop(param)
            except KeyError:
                raise ValueError(f"URI must specify '{param}'. Expected format: {help_str}")

            assert param and arg and param not in matches, (param, arg, matches.keys())
            matches[param] = arg

        for param, value in dsn_dict.items():
            if param in matches:
                raise ValueError(
                    f"Parameter '{param}' already provided as positional argument. Expected format: {help_str}"
                )

            matches[param] = value

        return matches


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


class Connect:
    """Provides methods for connecting to a supported database using a URL or connection dict."""

    def __init__(self, database_by_scheme: Dict[str, Database] = DATABASE_BY_SCHEME):
        self.database_by_scheme = database_by_scheme
        self.match_uri_path = {name: MatchUriPath(cls) for name, cls in database_by_scheme.items()}
        self.conn_cache = WeakCache()

    def for_databases(self, *dbs):
        database_by_scheme = {k: db for k, db in self.database_by_scheme.items() if k in dbs}
        return type(self)(database_by_scheme)

    def load_mixins(self, *abstract_mixins: AbstractMixin) -> Self:
        "Extend all the databases with a list of mixins that implement the given abstract mixins."
        database_by_scheme = {k: db.load_mixins(*abstract_mixins) for k, db in self.database_by_scheme.items()}
        return type(self)(database_by_scheme)

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
        - duckdb
        """

        dsn = dsnparse.parse(db_uri)
        if len(dsn.schemes) > 1:
            raise NotImplementedError("No support for multiple schemes")
        (scheme,) = dsn.schemes

        if scheme == "toml":
            toml_path = dsn.path or dsn.host
            database = dsn.fragment
            if not database:
                raise ValueError("Must specify a database name, e.g. 'toml://path#database'. ")
            with open(toml_path) as f:
                config = toml.load(f)
            try:
                conn_dict = config["database"][database]
            except KeyError:
                raise ValueError(f"Cannot find database config named '{database}'.")
            return self.connect_with_dict(conn_dict, thread_count)

        try:
            matcher = self.match_uri_path[scheme]
        except KeyError:
            raise NotImplementedError(f"Scheme '{scheme}' currently not supported")

        cls = matcher.database_cls

        if scheme == "databricks":
            assert not dsn.user
            kw = {}
            kw["access_token"] = dsn.password
            kw["http_path"] = dsn.path
            kw["server_hostname"] = dsn.host
            kw.update(dsn.query)
        elif scheme == "duckdb":
            kw = {}
            kw["filepath"] = dsn.dbname
            kw["dbname"] = dsn.user
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
                if scheme == "oracle":
                    kw["host"] = dsn.hostloc
                else:
                    kw["host"] = dsn.host
                    kw["port"] = dsn.port
                kw["user"] = dsn.user
                if dsn.password:
                    kw["password"] = dsn.password

        kw = {k: v for k, v in kw.items() if v is not None}

        if issubclass(cls, ThreadedDatabase):
            db = cls(thread_count=thread_count, **kw)
        else:
            db = cls(**kw)

        return self._connection_created(db)

    def connect_with_dict(self, d, thread_count):
        d = dict(d)
        driver = d.pop("driver")
        try:
            matcher = self.match_uri_path[driver]
        except KeyError:
            raise NotImplementedError(f"Driver '{driver}' currently not supported")

        cls = matcher.database_cls
        if issubclass(cls, ThreadedDatabase):
            db = cls(thread_count=thread_count, **d)
        else:
            db = cls(**d)

        return self._connection_created(db)

    def _connection_created(self, db):
        "Nop function to be overridden by subclasses."
        return db

    def __call__(self, db_conf: Union[str, dict], thread_count: Optional[int] = 1, shared: bool = True) -> Database:
        """Connect to a database using the given database configuration.

        Configuration can be given either as a URI string, or as a dict of {option: value}.

        The dictionary configuration uses the same keys as the TOML 'database' definition given with --conf.

        thread_count determines the max number of worker threads per database,
        if relevant. None means no limit.

        Parameters:
            db_conf (str | dict): The configuration for the database to connect. URI or dict.
            thread_count (int, optional): Size of the threadpool. Ignored by cloud databases. (default: 1)
            shared (bool): Whether to cache and return the same connection for the same db_conf. (default: True)

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
            <data_diff.sqeleton.databases.mysql.MySQL object at ...>
            >>> connect({"driver": "mysql", "host": "localhost", "database": "db"})
            <data_diff.sqeleton.databases.mysql.MySQL object at ...>
        """
        if shared:
            with suppress(KeyError):
                conn = self.conn_cache.get(db_conf)
                if not conn.is_closed:
                    return conn

        if isinstance(db_conf, str):
            conn = self.connect_to_uri(db_conf, thread_count)
        elif isinstance(db_conf, dict):
            conn = self.connect_with_dict(db_conf, thread_count)
        else:
            raise TypeError(f"db configuration must be a URI string or a dictionary. Instead got '{db_conf}'.")

        if shared:
            self.conn_cache.add(db_conf, conn)
        return conn
