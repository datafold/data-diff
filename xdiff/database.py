import logging
from typing import Tuple

from .sql import SqlOrStr, Compiler

import dsnparse

def import_postgres():
    import psycopg2
    import psycopg2.extras
    psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)
    return psycopg2

def import_mysql():
    import mysql.connector
    return mysql.connector

def import_snowflake():
    import snowflake.connector
    return snowflake

def import_mssql():
    import pymssql
    return pymssql

logger = logging.getLogger('database')

class ConnectError(Exception):
    pass



def _one(seq):
    x ,= seq
    return x

class Database:
    "An interface that uses the standard SQL cursor interface"

    def _query(self, sql_code: str):
        c = self._conn.cursor()
        c.execute(sql_code)
        return c.fetchall()

    def query(self, sql_ast: SqlOrStr, res_type: type):
        sql_code = Compiler(self).compile(sql_ast)
        logger.debug("Running SQL (%s): %s", type(self).__name__, sql_code)
        res = self._query(sql_code)
        if res_type is int:
            return int(_one(_one(res)))
        elif getattr(res_type, '__origin__', None) is list and len(res_type.__args__) == 1:
            if res_type.__args__ == (int,):
                return [_one(row) for row in res]
            elif res_type.__args__ == (Tuple,):
                return res
            else:
                breakpoint()
                assert False
        return res

    def to_string(self, s: str):
        return f'cast({s} as string)' 

CHECKSUM_HEXDIGITS = 15     # Must be 15 or lower
MD5_HEXDIGITS = 32

_CHECKSUM_BITSIZE = CHECKSUM_HEXDIGITS<<2
CHECKSUM_MASK = (2**_CHECKSUM_BITSIZE) - 1

class Postgres(Database):
    def __init__(self, host, port, database, user, password):
        postgres = import_postgres()
        self.args = dict(host=host, port=port, database=database, user=user, password=password)

        try:
            self._conn = postgres.connect(**self.args)
        except postgres.OperationalError as e:
            raise ConnectError(*e.args) from e

    def quote(self, s: str):
        return f'"{s}"'

    def md5_to_int(self, s: str) -> str:
        return f"('x' || substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}))::bit({_CHECKSUM_BITSIZE})::bigint"

    def to_string(self, s: str):
        return f'{s}::varchar'


class MySQL(Database):
    def __init__(self, host, port, database, user, password):
        mysql = import_mysql()

        args = dict(host=host, port=port, database=database, user=user, password=password)
        self._args = {k:v for k, v in args.items() if v is not None}

        try:
            self._conn = mysql.connect(charset='utf8', use_unicode=True, **self._args)
        except mysql.Error as e:
            if e.errno == mysql.errorcode.ER_ACCESS_DENIED_ERROR:
                raise ConnectError("Bad user name or password") from e
            elif e.errno == mysql.errorcode.ER_BAD_DB_ERROR:
                raise ConnectError("Database does not exist") from e
            else:
                raise ConnectError(*e.args) from e

    def quote(self, s: str):
        return f'`{s}`'

    def md5_to_int(self, s: str) -> str:
        return f"cast(conv(substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16, 10) as unsigned)"

    def to_string(self, s: str):
        return f'cast({s} as char)' 


class Redshift(Postgres):
    def md5_to_int(self, s: str) -> str:
        return f"strtol(substring(md5({s}), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16)::decimal(38)"

class MsSQL(Database):
    def __init__(self, host, port, database, user, password):
        mssql = import_mssql()

        args = dict(server=host, port=port, database=database, user=user, password=password)
        self._args = {k:v for k, v in args.items() if v is not None}

        try:
            self._conn = mssql.connect(**self._args)
        except mssql.Error as e:
            raise ConnectError(*e.args) from e

    def quote(self, s: str):
        return f'[{s}]'

    def md5_to_int(self, s: str) -> str:
        return f"CONVERT(decimal(38,0), CONVERT(bigint, HashBytes('MD5', {s}), 2))"
        # return f"CONVERT(bigint, (CHECKSUM({s})))"

    def to_string(self, s: str):
        return f"CONVERT(varchar, {s})"

class BigQuery(Database):
    def __init__(self, project, dataset):
        from google.cloud import bigquery
        self._client = bigquery.Client(project)

    def quote(self, s: str):
        return f'`{s}`'

    def md5_to_int(self, s: str) -> str:
        return f"cast(cast( ('0x' || substr(TO_HEX(md5({s})), 18)) as int64) as numeric)"

    def _query(self, sql_code: str):
        from google.cloud import bigquery
        try:
            res = list(self._client.query(sql_code))
        except Exception as e:
            msg = "Exception when trying to execute SQL code:\n    %s\n\nGot error: %s"
            raise ConnectError(msg%(sql_code, e))

        if res and isinstance(res[0], bigquery.table.Row):
            res = [row.values() for row in res]
        return res


class Snowflake(Database):
    def __init__(self, account, user, password, path, schema, database, print_sql=False):
        snowflake = import_snowflake()
        logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

        self._conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account
            )
        self._conn.cursor().execute(f"USE WAREHOUSE {path.lstrip('/')}")
        self._conn.cursor().execute(f"USE DATABASE {database}")
        self._conn.cursor().execute(f"USE SCHEMA {schema}")

    def quote(self, s: str):
        return s

    def md5_to_int(self, s: str) -> str:
        return f"BITAND(md5_number_lower64({s}), {CHECKSUM_MASK})"



def connect_to_uri(db_uri):
    dsn = dsnparse.parse(db_uri)
    if len(dsn.schemes) > 1:
        raise NotImplementedError("No support for multiple schemes")
    scheme ,= dsn.schemes

    if len(dsn.paths) == 0:
        path = ''
    elif len(dsn.paths) == 1:
        path ,= dsn.paths
    else:
        raise ValueError("Bad value for uri, too many paths: %s" % db_uri)

    if scheme == 'postgres':
        return Postgres(dsn.host, dsn.port, path, dsn.user, dsn.password)
    elif scheme == 'mysql':
        return MySQL(dsn.host, dsn.port, path, dsn.user, dsn.password)
    elif scheme == 'snowflake':
        return Snowflake(dsn.host, dsn.user, dsn.password, path, **dsn.query)
    elif scheme == 'mssql':
        return MsSQL(dsn.host, dsn.port, path, dsn.user, dsn.password)
    elif scheme == 'bigquery':
        return BigQuery(dsn.host, path)
    elif scheme == 'redshift':
        return Redshift(dsn.host, dsn.port, path, dsn.user, dsn.password)

    raise NotImplementedError(f"Scheme {dsn.scheme} currently not supported")
