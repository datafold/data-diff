"Module for query utilities that didn't make it into the query-builder (yet)"

from contextlib import suppress

from data_diff.abcs.database_types import DbPath
from data_diff.databases.base import QueryError
from data_diff.databases.oracle import Oracle
from data_diff.queries.api import table, commit, Expr


def _drop_table_oracle(name: DbPath):
    t = table(name)
    # Experience shows double drop is necessary
    with suppress(QueryError):
        yield t.drop()
        yield t.drop()
    yield commit


def _drop_table(name: DbPath):
    t = table(name)
    yield t.drop(if_exists=True)
    yield commit


def drop_table(db, tbl):
    if isinstance(db, Oracle):
        db.query(_drop_table_oracle(tbl))
    else:
        db.query(_drop_table(tbl))


def _append_to_table_oracle(path: DbPath, expr: Expr):
    """See append_to_table"""
    assert expr.schema, expr
    t = table(path, schema=expr.schema)
    with suppress(QueryError):
        yield t.create()  # uses expr.schema
        yield commit
    yield t.insert_expr(expr)
    yield commit


def _append_to_table(path: DbPath, expr: Expr):
    """Append to table"""
    assert expr.schema, expr
    t = table(path, schema=expr.schema)
    yield t.create(if_not_exists=True)  # uses expr.schema
    yield commit
    yield t.insert_expr(expr)
    yield commit


def append_to_table(db, path, expr):
    f = _append_to_table_oracle if isinstance(db, Oracle) else _append_to_table
    db.query(f(path, expr))
