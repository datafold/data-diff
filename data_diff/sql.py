"""Provides classes for a pseudo-SQL AST that compiles to SQL code
"""

from typing import List, Sequence, Union, Tuple, Optional
from datetime import datetime

from runtype import dataclass

from .databases.database_types import AbstractDatabase, DbPath, DbKey, DbTime, ArithUUID


class Sql:
    pass


SqlOrStr = Union[Sql, str]


@dataclass
class Compiler:
    """Provides a set of utility methods for compiling SQL

    For internal use.
    """

    database: AbstractDatabase
    in_select: bool = False  # Compilation

    def quote(self, s: str):
        return self.database.quote(s)

    def compile(self, elem):
        if isinstance(elem, Sql):
            return elem.compile(self)
        elif isinstance(elem, str):
            return elem
        elif isinstance(elem, int):
            return str(elem)
        assert False


@dataclass
class TableName(Sql):
    name: DbPath

    def compile(self, c: Compiler):
        path = c.database._normalize_table_path(self.name)
        return ".".join(map(c.quote, path))


@dataclass
class ColumnName(Sql):
    name: str

    def compile(self, c: Compiler):
        return c.quote(self.name)


@dataclass
class Value(Sql):
    value: object  # Primitive

    def compile(self, c: Compiler):
        if isinstance(self.value, bytes):
            return "b'%s'" % self.value.decode()
        elif isinstance(self.value, str):
            return "'%s'" % self.value
        elif isinstance(self.value, ArithUUID):
            return "'%s'" % self.value
        return str(self.value)


@dataclass
class Select(Sql):
    columns: Sequence[SqlOrStr]
    table: SqlOrStr = None
    where: Sequence[SqlOrStr] = None
    order_by: Sequence[SqlOrStr] = None
    group_by: Sequence[SqlOrStr] = None
    limit: int = None

    def compile(self, parent_c: Compiler):
        c = parent_c.replace(in_select=True)
        columns = ", ".join(map(c.compile, self.columns))
        select = f"SELECT {columns}"

        if self.table:
            select += " FROM " + c.compile(self.table)

        if self.where:
            select += " WHERE " + " AND ".join(map(c.compile, self.where))

        if self.group_by:
            select += " GROUP BY " + ", ".join(map(c.compile, self.group_by))

        if self.order_by:
            select += " ORDER BY " + ", ".join(map(c.compile, self.order_by))

        if self.limit is not None:
            select += " " + c.database.offset_limit(0, self.limit)

        if parent_c.in_select:
            select = "(%s)" % select
        return select


@dataclass
class Enum(Sql):
    table: DbPath
    order_by: SqlOrStr

    def compile(self, c: Compiler):
        table = ".".join(map(c.quote, self.table))
        order = c.compile(self.order_by)
        return f"(SELECT *, (row_number() over (ORDER BY {order})) as idx FROM {table} ORDER BY {order}) tmp"


@dataclass
class Checksum(Sql):
    exprs: Sequence[SqlOrStr]

    def compile(self, c: Compiler):
        if len(self.exprs) > 1:
            compiled_exprs = [f"coalesce({c.compile(expr)}, '<null>')" for expr in self.exprs]
            expr = c.database.concat(compiled_exprs)
        else:
            # No need to coalesce - safe to assume that key cannot be null
            (expr,) = self.exprs
            expr = c.compile(expr)
        md5 = c.database.md5_to_int(expr)
        return f"sum({md5})"


@dataclass
class Compare(Sql):
    op: str
    a: SqlOrStr
    b: SqlOrStr

    def compile(self, c: Compiler):
        return f"({c.compile(self.a)} {self.op} {c.compile(self.b)})"


@dataclass
class In(Sql):
    expr: SqlOrStr
    list: Sequence  # List[SqlOrStr]

    def compile(self, c: Compiler):
        elems = ", ".join(map(c.compile, self.list))
        return f"({c.compile(self.expr)} IN ({elems}))"


@dataclass
class Count(Sql):
    column: Optional[SqlOrStr] = None

    def compile(self, c: Compiler):
        if self.column:
            return f"count({c.compile(self.column)})"
        return "count(*)"


@dataclass
class Min(Sql):
    column: SqlOrStr

    def compile(self, c: Compiler):
        return f"min({c.compile(self.column)})"


@dataclass
class Max(Sql):
    column: SqlOrStr

    def compile(self, c: Compiler):
        return f"max({c.compile(self.column)})"


@dataclass
class Time(Sql):
    time: datetime

    def compile(self, c: Compiler):
        return c.database.timestamp_value(self.time)


@dataclass
class Explain(Sql):
    sql: Select

    def compile(self, c: Compiler):
        return f"EXPLAIN {c.compile(self.sql)}"
