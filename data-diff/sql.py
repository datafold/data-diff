"""Provides classes for a pseudo-SQL AST that compiles to SQL code
"""

from typing import List, Union, Tuple, Optional

from runtype import dataclass

DbPath = Tuple[str, ...]
DbKey = Union[int, str, bytes]

class Sql:
    pass

SqlOrStr = Union[Sql, str]

@dataclass
class Compiler:
    """Provides a set of utility methods for compiling SQL

    For internal use.
    """

    database: object #Database
    in_select: bool = False     # Compilation

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
        return c.quote('.'.join(self.name))

@dataclass
class Value(Sql):
    value: object   # Primitive

    def compile(self, c: Compiler):
        if isinstance(self.value, bytes):
            return "b'%s'" % self.value.decode()
        elif isinstance(self.value, str):
            return "'%s'" % self.value
        return str(self.value)

@dataclass
class Select(Sql):
    columns: List[SqlOrStr]
    table: SqlOrStr = None
    where: List[SqlOrStr] = None
    order_by: List[SqlOrStr] = None
    group_by: List[SqlOrStr] = None

    def compile(self, parent_c: Compiler):
        c = parent_c.replace(in_select=True)
        columns = ', '.join(map(c.compile, self.columns))
        select = f'SELECT {columns}'

        if self.table:
            select += ' FROM ' + c.compile(self.table)
        
        if self.where:
            select += ' WHERE ' + ' AND '.join(map(c.compile, self.where))

        if self.group_by:
            select += ' GROUP BY ' + ', '.join(map(c.compile, self.group_by))

        if self.order_by:
            select += ' ORDER BY ' + ', '.join(map(c.compile, self.order_by))

        if parent_c.in_select:
            select = '(%s)' % select
        return select

@dataclass
class Enum(Sql):
    table: DbPath
    order_by: SqlOrStr

    def compile(self, c: Compiler):
        table = c.quote('.'.join(self.table))
        order = c.compile(self.order_by)
        return f'(SELECT *, (row_number() over (ORDER BY {order})) as idx FROM {table} ORDER BY {order}) tmp'



@dataclass
class Checksum(Sql):
    exprs: List[SqlOrStr]

    def compile(self, c: Compiler):
        compiled_exprs = ', '.join(c.database.to_string(s) for s in map(c.compile, self.exprs))
        expr =  f'concat({compiled_exprs})'
        md5 = c.database.md5_to_int(expr)
        return f'sum({md5})'

@dataclass
class Compare(Sql):
    op: str
    a: SqlOrStr
    b: SqlOrStr

    def compile(self, c: Compiler):
        return f'({c.compile(self.a)} {self.op} {c.compile(self.b)})'

@dataclass
class In(Sql):
    expr: SqlOrStr
    list: List #List[SqlOrStr]

    def compile(self, c: Compiler):
        elems = ', '.join(map(c.compile, self.list))
        return f'({c.compile(self.expr)} IN ({elems}))'

@dataclass
class Count(Sql):
    column: Optional[SqlOrStr] = None

    def compile(self, c: Compiler):
        if self.column:
            return f"count({c.compile(self.column)})"
        return 'count(*)'
