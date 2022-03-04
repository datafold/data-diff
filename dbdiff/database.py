import logging

from .sql import SqlOrStr, Compiler

import dsnparse

import psycopg2
import psycopg2.extras
psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)

import mysql.connector
from mysql.connector import errorcode

logger = logging.getLogger('database')

class ConnectError(Exception):
    pass

class Database:
    def query(self, sql: SqlOrStr): ...



class Postgres(Database):
    pass


def _one(seq):
    x ,= seq
    return x

class Database:
    "An interface that uses the standard SQL cursor interface"

    def __init__(self):
        self._conn = self._create_connection()

    def query(self, sql_ast: SqlOrStr, res_type: type):
        sql_code = Compiler(self).compile(sql_ast)
        c = self._conn.cursor()
        # print("##", sql_code)
        # logger.debug("SQL: %s", sql_code)
        c.execute(sql_code)
        res = c.fetchall()
        if res_type is int:
            return int(_one(_one(res)))
        elif getattr(res_type, '__origin__', None) is list and len(res_type.__args__) == 1:
            return [_one(row) for row in res]
        return res


class Postgres(Database):
    def __init__(self, host, port, database, user, password):
        self.args = dict(host=host, port=port, database=database, user=user, password=password)

        try:
            self._conn = psycopg2.connect(**self.args)
        except psycopg2.OperationalError as e:
            raise ConnectError(*e.args) from e

    def quote(self, s: str):
        return f'"{s}"'


class MySQL(Database):
    def __init__(self, host, port, database, user, password):
        args = dict(host=host, port=port, database=database, user=user, password=password)
        self._args = {k:v for k, v in args.items() if v is not None}

        try:
            self._conn = mysql.connector.connect(charset='utf8', use_unicode=True, **self._args)
        except mysql.connector.Error as e:
            if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                raise ConnectError("Bad user name or password") from e
            elif e.errno == errorcode.ER_BAD_DB_ERROR:
                raise ConnectError("Database does not exist") from e
            else:
                raise ConnectError(*e.args) from e

    def quote(self, s: str):
        return f'`{s}`'


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

    raise NotImplementedError(f"Scheme {dsn.scheme} currently not supported")
