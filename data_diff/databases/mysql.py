from data_diff.sqeleton.databases import mysql
from .base import BaseDialect


class Dialect(BaseDialect, mysql.Dialect):
    pass


class MySQL(mysql.MySQL):
    dialect = Dialect()
