from typing import Optional

from ..utils import CaseAwareMapping, CaseSensitiveDict
from .ast_classes import *
from .base import args_as_tuple


this = This()


def join(*tables: ITable) -> Join:
    """Inner-join a sequence of table expressions"

    When joining, it's recommended to use explicit tables names, instead of `this`, in order to avoid potential name collisions.

    Example:
        ::

            person = table('person')
            city = table('city')

            name_and_city = (
                join(person, city)
                .on(person['city_id'] == city['id'])
                .select(person['id'], city['name'])
            )
    """
    return Join(tables)


def leftjoin(*tables: ITable):
    """Left-joins a sequence of table expressions.

    See Also: ``join()``
    """
    return Join(tables, "LEFT")


def rightjoin(*tables: ITable):
    """Right-joins a sequence of table expressions.

    See Also: ``join()``
    """
    return Join(tables, "RIGHT")


def outerjoin(*tables: ITable):
    """Outer-joins a sequence of table expressions.

    See Also: ``join()``
    """
    return Join(tables, "FULL OUTER")


def cte(expr: Expr, *, name: Optional[str] = None, params: Sequence[str] = None):
    """Define a CTE"""
    return Cte(expr, name, params)


def table(*path: str, schema: Union[dict, CaseAwareMapping] = None) -> TablePath:
    """Defines a table with a path (dotted name), and optionally a schema.

    Parameters:
        path: A list of names that make up the path to the table.
        schema: a dictionary of {name: type}
    """
    if len(path) == 1 and isinstance(path[0], tuple):
        (path,) = path
    if not all(isinstance(i, str) for i in path):
        raise TypeError(f"All elements of table path must be of type 'str'. Got: {path}")
    if schema and not isinstance(schema, CaseAwareMapping):
        assert isinstance(schema, dict)
        schema = CaseSensitiveDict(schema)
    return TablePath(path, schema)


def or_(*exprs: Expr):
    """Apply OR between a sequence of boolean expressions"""
    exprs = args_as_tuple(exprs)
    if len(exprs) == 1:
        return exprs[0]
    return BinBoolOp("OR", exprs)


def and_(*exprs: Expr):
    """Apply AND between a sequence of boolean expressions"""
    exprs = args_as_tuple(exprs)
    if len(exprs) == 1:
        return exprs[0]
    return BinBoolOp("AND", exprs)


def sum_(expr: Expr):
    """Call SUM(expr)"""
    return Func("sum", [expr])


def avg(expr: Expr):
    """Call AVG(expr)"""
    return Func("avg", [expr])


def min_(expr: Expr):
    """Call MIN(expr)"""
    return Func("min", [expr])


def max_(expr: Expr):
    """Call MAX(expr)"""
    return Func("max", [expr])


def exists(expr: Expr):
    """Call EXISTS(expr)"""
    return Func("exists", [expr])


def if_(cond: Expr, then: Expr, else_: Optional[Expr] = None):
    """Conditional expression, shortcut to when-then-else.

    Example:
        ::

            # SELECT CASE WHEN b THEN c ELSE d END FROM foo
            table('foo').select(if_(b, c, d))
    """
    return when(cond).then(then).else_(else_)


def when(*when_exprs: Expr):
    """Start a when-then expression

    Example:
        ::

            # SELECT CASE
            #   WHEN (type = 'text') THEN text
            #   WHEN (type = 'number') THEN number
            #   ELSE 'unknown type' END
            # FROM foo
            rows = table('foo').select(
                    when(this.type == 'text').then(this.text)
                    .when(this.type == 'number').then(this.number)
                    .else_('unknown type')
                )
    """
    return CaseWhen([]).when(*when_exprs)


def coalesce(*exprs):
    "Returns a call to COALESCE"
    exprs = args_as_tuple(exprs)
    return Func("COALESCE", exprs)


def insert_rows_in_batches(db, tbl: TablePath, rows, *, columns=None, batch_size=1024 * 8):
    assert batch_size > 0
    rows = list(rows)

    while rows:
        batch, rows = rows[:batch_size], rows[batch_size:]
        db.query(tbl.insert_rows(batch, columns=columns))


def current_timestamp():
    """Returns CURRENT_TIMESTAMP() or NOW()"""
    return CurrentTimestamp()


def code(code: str, **kw: Dict[str, Expr]) -> Code:
    """Inline raw SQL code.

    It allows users to use features and syntax that Sqeleton doesn't yet support.

    It's the user's responsibility to make sure the contents of the string given to `code()` are correct and safe for execution.

    Strings given to `code()` are actually templates, and can embed query expressions given as arguments:

    Parameters:
        code: template string of SQL code. Templated variables are signified with '{var}'.
        kw: optional parameters for SQL template.

    Examples:
        ::

            # SELECT b, <x> FROM tmp WHERE <y>
            table('tmp').select(this.b, code("<x>")).where(code("<y>"))

        ::

            def tablesample(tbl, size):
                return code("SELECT * FROM {tbl} TABLESAMPLE BERNOULLI ({size})", tbl=tbl, size=size)

            nonzero = table('points').where(this.x > 0, this.y > 0)

            # SELECT * FROM points WHERE (x > 0) AND (y > 0) TABLESAMPLE BERNOULLI (10)
            sample_expr = tablesample(nonzero)
    """
    return Code(code, kw)


commit = Commit()
