from typing import Optional
from .ast_classes import *
from .base import args_as_tuple


this = This()


def join(*tables: ITable):
    "Joins each table into a 'struct'"
    return Join(tables)


def leftjoin(*tables: ITable):
    "Left-joins each table into a 'struct'"
    return Join(tables, "LEFT")

def rightjoin(*tables: ITable):
    "Right-joins each table into a 'struct'"
    return Join(tables, "RIGHT")

def outerjoin(*tables: ITable):
    "Outer-joins each table into a 'struct'"
    return Join(tables, "FULL OUTER")


def cte(expr: Expr, *, name: Optional[str] = None, params: Sequence[str] = None):
    return Cte(expr, name, params)


def table(*path: str, schema: Schema = None) -> ITable:
    assert all(isinstance(i, str) for i in path), path
    return TablePath(path, schema)


def or_(*exprs: Expr):
    exprs = args_as_tuple(exprs)
    if len(exprs) == 1:
        return exprs[0]
    return BinOp("OR", exprs)


def and_(*exprs: Expr):
    exprs = args_as_tuple(exprs)
    if len(exprs) == 1:
        return exprs[0]
    return BinOp("AND", exprs)


def sum_(expr: Expr):
    return Func("sum", [expr])


def avg(expr: Expr):
    return Func("avg", [expr])


def min_(expr: Expr):
    return Func("min", [expr])


def max_(expr: Expr):
    return Func("max", [expr])


def if_(cond: Expr, then: Expr, else_: Optional[Expr] = None):
    return CaseWhen([(cond, then)], else_=else_)
