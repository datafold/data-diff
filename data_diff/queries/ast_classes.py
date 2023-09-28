from dataclasses import field
from datetime import datetime
from typing import Any, Generator, List, Optional, Sequence, Union, Dict

from runtype import dataclass
from typing_extensions import Self

from data_diff.utils import ArithString
from data_diff.abcs.compiler import Compilable
from data_diff.schema import Schema

from data_diff.queries.base import SKIP, args_as_tuple, SqeletonError
from data_diff.abcs.database_types import DbPath


class QueryBuilderError(SqeletonError):
    pass


class QB_TypeError(QueryBuilderError):
    pass


class Root:
    "Nodes inheriting from Root can be used as root statements in SQL (e.g. SELECT yes, RANDOM() no)"


class ExprNode(Compilable):
    "Base class for query expression nodes"

    type: Any = None

    def _dfs_values(self):
        yield self
        for k, vs in dict(self).items():  # __dict__ provided by runtype.dataclass
            if k == "source_table":
                # Skip data-sources, we're only interested in data-parameters
                continue
            if not isinstance(vs, (list, tuple)):
                vs = [vs]
            for v in vs:
                if isinstance(v, ExprNode):
                    yield from v._dfs_values()

    def cast_to(self, to):
        return Cast(self, to)


# Query expressions can only interact with objects that are an instance of 'Expr'
Expr = Union[ExprNode, str, bool, int, float, datetime, ArithString, None]


@dataclass
class Code(ExprNode, Root):
    code: str
    args: Dict[str, Expr] = None


def _expr_type(e: Expr) -> type:
    if isinstance(e, ExprNode):
        return e.type
    return type(e)


@dataclass
class Alias(ExprNode):
    expr: Expr
    name: str

    @property
    def type(self):
        return _expr_type(self.expr)


def _drop_skips(exprs):
    return [e for e in exprs if e is not SKIP]


def _drop_skips_dict(exprs_dict):
    return {k: v for k, v in exprs_dict.items() if v is not SKIP}


class ITable:
    source_table: Any
    schema: Schema = None

    def select(self, *exprs, distinct=SKIP, optimizer_hints=SKIP, **named_exprs) -> "ITable":
        """Choose new columns, based on the old ones. (aka Projection)

        Parameters:
            exprs: List of expressions to constitute the columns of the new table.
                    If not provided, returns all columns in source table (i.e. ``select *``)
            distinct: 'select' or 'select distinct'
            named_exprs: More expressions to constitute the columns of the new table, aliased to keyword name.

        """
        exprs = args_as_tuple(exprs)
        exprs = _drop_skips(exprs)
        named_exprs = _drop_skips_dict(named_exprs)
        exprs += _named_exprs_as_aliases(named_exprs)
        resolve_names(self.source_table, exprs)
        return Select.make(self, columns=exprs, distinct=distinct, optimizer_hints=optimizer_hints)

    def where(self, *exprs):
        """Filter the rows, based on the given predicates. (aka Selection)"""
        exprs = args_as_tuple(exprs)
        exprs = _drop_skips(exprs)
        if not exprs:
            return self

        resolve_names(self.source_table, exprs)
        return Select.make(self, where_exprs=exprs)

    def order_by(self, *exprs):
        """Order the rows lexicographically, according to the given expressions."""
        exprs = _drop_skips(exprs)
        if not exprs:
            return self

        resolve_names(self.source_table, exprs)
        return Select.make(self, order_by_exprs=exprs)

    def limit(self, limit: int):
        """Stop yielding rows after the given limit. i.e. take the first 'n=limit' rows"""
        if limit is SKIP:
            return self

        return Select.make(self, limit_expr=limit)

    def join(self, target: "ITable"):
        """Join the current table with the target table, returning a new table containing both side-by-side.

        When joining, it's recommended to use explicit tables names, instead of `this`, in order to avoid potential name collisions.

        Example:
            ::

                person = table('person')
                city = table('city')

                name_and_city = (
                    person
                    .join(city)
                    .on(person['city_id'] == city['id'])
                    .select(person['id'], city['name'])
                )
        """
        return Join([self, target])

    def group_by(self, *keys) -> "GroupBy":
        """Behaves like in SQL, except for a small change in syntax:

        A call to `.agg()` must follow every call to `.group_by()`.

        Example:
            ::

                # SELECT a, sum(b) FROM tmp GROUP BY 1
                table('tmp').group_by(this.a).agg(this.b.sum())

                # SELECT a, sum(b) FROM a GROUP BY 1 HAVING (b > 10)
                (table('tmp')
                    .group_by(this.a)
                    .agg(this.b.sum())
                    .having(this.b > 10)
                )

        """
        keys = _drop_skips(keys)
        resolve_names(self.source_table, keys)

        return GroupBy(self, keys)

    def _get_column(self, name: str):
        if self.schema:
            name = self.schema.get_key(name)  # Get the actual name. Might be case-insensitive.
        return Column(self, name)

    # def __getattr__(self, column):
    #     return self._get_column(column)

    def __getitem__(self, column):
        if not isinstance(column, str):
            raise TypeError()
        return self._get_column(column)

    def count(self):
        """SELECT count() FROM self"""
        return Select(self, [Count()])

    def union(self, other: "ITable"):
        """SELECT * FROM self UNION other"""
        return TableOp("UNION", self, other)

    def union_all(self, other: "ITable"):
        """SELECT * FROM self UNION ALL other"""
        return TableOp("UNION ALL", self, other)

    def minus(self, other: "ITable"):
        """SELECT * FROM self EXCEPT other"""
        # aka
        return TableOp("EXCEPT", self, other)

    def intersect(self, other: "ITable"):
        """SELECT * FROM self INTERSECT other"""
        return TableOp("INTERSECT", self, other)


@dataclass
class Concat(ExprNode):
    exprs: list
    sep: str = None


@dataclass
class Count(ExprNode):
    expr: Expr = None
    distinct: bool = False
    type = int


class LazyOps:
    def __add__(self, other):
        return BinOp("+", [self, other])

    def __sub__(self, other):
        return BinOp("-", [self, other])

    def __neg__(self):
        return UnaryOp("-", self)

    def __gt__(self, other):
        return BinBoolOp(">", [self, other])

    def __ge__(self, other):
        return BinBoolOp(">=", [self, other])

    def __eq__(self, other):
        if other is None:
            return BinBoolOp("IS", [self, None])
        return BinBoolOp("=", [self, other])

    def __lt__(self, other):
        return BinBoolOp("<", [self, other])

    def __le__(self, other):
        return BinBoolOp("<=", [self, other])

    def __or__(self, other):
        return BinBoolOp("OR", [self, other])

    def __and__(self, other):
        return BinBoolOp("AND", [self, other])

    def is_distinct_from(self, other):
        return IsDistinctFrom(self, other)

    def like(self, other):
        return BinBoolOp("LIKE", [self, other])

    def sum(self):
        return Func("SUM", [self])

    def max(self):
        return Func("MAX", [self])

    def min(self):
        return Func("MIN", [self])


@dataclass(eq=False)
class Func(ExprNode, LazyOps):
    name: str
    args: Sequence[Expr]


@dataclass
class WhenThen(ExprNode):
    when: Expr
    then: Expr


@dataclass
class CaseWhen(ExprNode):
    cases: Sequence[WhenThen]
    else_expr: Expr = None

    @property
    def type(self):
        then_types = {_expr_type(case.then) for case in self.cases}
        if self.else_expr:
            then_types |= _expr_type(self.else_expr)
        if len(then_types) > 1:
            raise QB_TypeError(f"Non-matching types in when: {then_types}")
        (t,) = then_types
        return t

    def when(self, *whens: Expr) -> "QB_When":
        """Add a new 'when' clause to the case expression

        Must be followed by a call to `.then()`
        """
        whens = args_as_tuple(whens)
        whens = _drop_skips(whens)
        if not whens:
            raise QueryBuilderError("Expected valid whens")

        # XXX reimplementing api.and_()
        if len(whens) == 1:
            return QB_When(self, whens[0])
        return QB_When(self, BinBoolOp("AND", whens))

    def else_(self, then: Expr) -> Self:
        """Add an 'else' clause to the case expression.

        Can only be called once!
        """
        if self.else_expr is not None:
            raise QueryBuilderError(f"Else clause already specified in {self}")

        return self.replace(else_expr=then)


@dataclass
class QB_When:
    "Partial case-when, used for query-building"
    casewhen: CaseWhen
    when: Expr

    def then(self, then: Expr) -> CaseWhen:
        """Add a 'then' clause after a 'when' was added."""
        case = WhenThen(self.when, then)
        return self.casewhen.replace(cases=self.casewhen.cases + [case])


@dataclass(eq=False, order=False)
class IsDistinctFrom(ExprNode, LazyOps):
    a: Expr
    b: Expr
    type = bool


@dataclass(eq=False, order=False)
class BinOp(ExprNode, LazyOps):
    op: str
    args: Sequence[Expr]

    @property
    def type(self):
        types = {_expr_type(i) for i in self.args}
        if len(types) > 1:
            raise TypeError(f"Expected all args to have the same type, got {types}")
        (t,) = types
        return t


@dataclass
class UnaryOp(ExprNode, LazyOps):
    op: str
    expr: Expr


class BinBoolOp(BinOp):
    type = bool


@dataclass(eq=False, order=False)
class Column(ExprNode, LazyOps):
    source_table: ITable
    name: str

    @property
    def type(self):
        if self.source_table.schema is None:
            raise QueryBuilderError(f"Schema required for table {self.source_table}")
        return self.source_table.schema[self.name]


@dataclass
class TablePath(ExprNode, ITable):
    path: DbPath
    schema: Optional[Schema] = field(default=None, repr=False)

    @property
    def source_table(self) -> Self:
        return self

    # Statement shorthands
    def create(self, source_table: ITable = None, *, if_not_exists: bool = False, primary_keys: List[str] = None):
        """Returns a query expression to create a new table.

        Parameters:
            source_table: a table expression to use for initializing the table.
                          If not provided, the table must have a schema specified.
            if_not_exists: Add a 'if not exists' clause or not. (note: not all dbs support it!)
            primary_keys: List of column names which define the primary key
        """

        if source_table is None and not self.schema:
            raise ValueError("Either schema or source table needed to create table")
        if isinstance(source_table, TablePath):
            source_table = source_table.select()
        return CreateTable(self, source_table, if_not_exists=if_not_exists, primary_keys=primary_keys)

    def drop(self, if_exists=False):
        """Returns a query expression to delete the table.

        Parameters:
            if_not_exists: Add a 'if not exists' clause or not. (note: not all dbs support it!)
        """
        return DropTable(self, if_exists=if_exists)

    def truncate(self):
        """Returns a query expression to truncate the table. (remove all rows)"""
        return TruncateTable(self)

    def insert_rows(self, rows: Sequence, *, columns: List[str] = None):
        """Returns a query expression to insert rows to the table, given as Python values.

        Parameters:
            rows: A list of tuples. Must all have the same width.
            columns: Names of columns being populated. If specified, must have the same length as the tuples.
        """
        rows = list(rows)
        return InsertToTable(self, ConstantTable(rows), columns=columns)

    def insert_row(self, *values, columns: List[str] = None):
        """Returns a query expression to insert a single row to the table, given as Python values.

        Parameters:
            columns: Names of columns being populated. If specified, must have the same length as 'values'
        """
        return InsertToTable(self, ConstantTable([values]), columns=columns)

    def insert_expr(self, expr: Expr):
        """Returns a query expression to insert rows to the table, given as a query expression.

        Parameters:
            expr: query expression to from which to read the rows
        """
        if isinstance(expr, TablePath):
            expr = expr.select()
        return InsertToTable(self, expr)

    def time_travel(
        self, *, before: bool = False, timestamp: datetime = None, offset: int = None, statement: str = None
    ) -> Compilable:
        """Selects historical data from the table

        Parameters:
            before: If false, inclusive of the specified point in time.
                     If True, only return the time before it. (at/before)
            timestamp: A constant timestamp
            offset: the time 'offset' seconds before now
            statement: identifier for statement, e.g. query ID

        Must specify exactly one of `timestamp`, `offset` or `statement`.
        """
        if sum(int(i is not None) for i in (timestamp, offset, statement)) != 1:
            raise ValueError("Must specify exactly one of `timestamp`, `offset` or `statement`.")

        if timestamp is not None:
            assert offset is None and statement is None


@dataclass
class TableAlias(ExprNode, ITable):
    source_table: ITable
    name: str


@dataclass
class Join(ExprNode, ITable, Root):
    source_tables: Sequence[ITable]
    op: str = None
    on_exprs: Sequence[Expr] = None
    columns: Sequence[Expr] = None

    @property
    def source_table(self) -> Self:
        return self

    @property
    def schema(self):
        assert self.columns  # TODO Implement SELECT *
        s = self.source_tables[0].schema  # TODO validate types match between both tables
        return type(s)({c.name: c.type for c in self.columns})

    def on(self, *exprs) -> Self:
        """Add an ON clause, for filtering the result of the cartesian product (i.e. the JOIN)"""
        if len(exprs) == 1:
            (e,) = exprs
            if isinstance(e, Generator):
                exprs = tuple(e)

        exprs = _drop_skips(exprs)
        if not exprs:
            return self

        return self.replace(on_exprs=(self.on_exprs or []) + exprs)

    def select(self, *exprs, **named_exprs) -> Union[Self, ITable]:
        """Select fields to return from the JOIN operation

        See Also: ``ITable.select()``
        """
        if self.columns is not None:
            # join-select already applied
            return super().select(*exprs, **named_exprs)

        exprs = _drop_skips(exprs)
        named_exprs = _drop_skips_dict(named_exprs)
        exprs += _named_exprs_as_aliases(named_exprs)
        resolve_names(self.source_table, exprs)
        # TODO Ensure exprs <= self.columns ?
        return self.replace(columns=exprs)


@dataclass
class GroupBy(ExprNode, ITable, Root):
    table: ITable
    keys: Sequence[Expr] = None  # IKey?
    values: Sequence[Expr] = None
    having_exprs: Sequence[Expr] = None

    @property
    def source_table(self):
        return self

    def __post_init__(self):
        assert self.keys or self.values

    def having(self, *exprs) -> Self:
        """Add a 'HAVING' clause to the group-by"""
        exprs = args_as_tuple(exprs)
        exprs = _drop_skips(exprs)
        if not exprs:
            return self

        resolve_names(self.table, exprs)
        return self.replace(having_exprs=(self.having_exprs or []) + exprs)

    def agg(self, *exprs) -> Self:
        """Select aggregated fields for the group-by."""
        exprs = args_as_tuple(exprs)
        exprs = _drop_skips(exprs)
        resolve_names(self.table, exprs)
        return self.replace(values=(self.values or []) + exprs)


@dataclass
class TableOp(ExprNode, ITable, Root):
    op: str
    table1: ITable
    table2: ITable

    @property
    def source_table(self):
        return self

    @property
    def type(self):
        # TODO ensure types of both tables are compatible
        return self.table1.type

    @property
    def schema(self):
        s1 = self.table1.schema
        s2 = self.table2.schema
        assert len(s1) == len(s2)
        return s1


@dataclass
class Select(ExprNode, ITable, Root):
    table: Expr = None
    columns: Sequence[Expr] = None
    where_exprs: Sequence[Expr] = None
    order_by_exprs: Sequence[Expr] = None
    group_by_exprs: Sequence[Expr] = None
    having_exprs: Sequence[Expr] = None
    limit_expr: int = None
    distinct: bool = False
    optimizer_hints: Sequence[Expr] = None

    @property
    def schema(self):
        s = self.table.schema
        if s is None or self.columns is None:
            return s
        return type(s)({c.name: c.type for c in self.columns})

    @property
    def source_table(self):
        return self

    @classmethod
    def make(cls, table: ITable, distinct: bool = SKIP, optimizer_hints: str = SKIP, **kwargs):
        assert "table" not in kwargs

        if not isinstance(table, cls):  # If not Select
            if distinct is not SKIP:
                kwargs["distinct"] = distinct
            if optimizer_hints is not SKIP:
                kwargs["optimizer_hints"] = optimizer_hints
            return cls(table, **kwargs)

        # We can safely assume isinstance(table, Select)
        if optimizer_hints is not SKIP:
            kwargs["optimizer_hints"] = optimizer_hints

        if distinct is not SKIP:
            if distinct == False and table.distinct:
                return cls(table, **kwargs)
            kwargs["distinct"] = distinct

        if table.limit_expr or table.group_by_exprs:
            return cls(table, **kwargs)

        # Fill in missing attributes, instead of nesting instances
        for k, v in kwargs.items():
            if getattr(table, k) is not None:
                if k == "where_exprs":  # Additive attribute
                    kwargs[k] = getattr(table, k) + v
                elif k in ["distinct", "optimizer_hints"]:
                    pass
                else:
                    raise ValueError(k)

        return table.replace(**kwargs)


@dataclass
class Cte(ExprNode, ITable):
    source_table: Expr
    name: str = None
    params: Sequence[str] = None

    @property
    def schema(self):
        # TODO add cte to schema
        return self.source_table.schema


def _named_exprs_as_aliases(named_exprs):
    return [Alias(expr, name) for name, expr in named_exprs.items()]


def resolve_names(source_table, exprs):
    i = 0
    for expr in exprs:
        # Iterate recursively and update _ResolveColumn instances with the right expression
        if isinstance(expr, ExprNode):
            for v in expr._dfs_values():
                if isinstance(v, _ResolveColumn):
                    v.resolve(source_table._get_column(v.resolve_name))
                    i += 1


@dataclass(frozen=False, eq=False, order=False)
class _ResolveColumn(ExprNode, LazyOps):
    resolve_name: str
    resolved: Expr = None

    def resolve(self, expr: Expr):
        if self.resolved is not None:
            raise QueryBuilderError("Already resolved!")
        self.resolved = expr

    def _get_resolved(self) -> Expr:
        if self.resolved is None:
            raise QueryBuilderError(f"Column not resolved: {self.resolve_name}")
        return self.resolved

    @property
    def type(self):
        return self._get_resolved().type

    @property
    def name(self):
        return self._get_resolved().name


class This:
    """Builder object for accessing table attributes.

    Automatically evaluates to the the 'top-most' table during compilation.
    """

    def __getattr__(self, name):
        return _ResolveColumn(name)

    def __getitem__(self, name):
        if isinstance(name, (list, tuple)):
            return [_ResolveColumn(n) for n in name]
        return _ResolveColumn(name)


@dataclass
class In(ExprNode):
    expr: Expr
    list: Sequence[Expr]
    type = bool


@dataclass
class Cast(ExprNode):
    expr: Expr
    target_type: Expr


@dataclass
class Random(ExprNode, LazyOps):
    type = float


@dataclass
class ConstantTable(ExprNode):
    rows: Sequence[Sequence]


@dataclass
class Explain(ExprNode, Root):
    select: Select
    type = str


class CurrentTimestamp(ExprNode):
    type = datetime


@dataclass
class TimeTravel(ITable):
    table: TablePath
    before: bool = False
    timestamp: datetime = None
    offset: int = None
    statement: str = None


# DDL


class Statement(Compilable, Root):
    type = None


@dataclass
class CreateTable(Statement):
    path: TablePath
    source_table: Expr = None
    if_not_exists: bool = False
    primary_keys: List[str] = None


@dataclass
class DropTable(Statement):
    path: TablePath
    if_exists: bool = False


@dataclass
class TruncateTable(Statement):
    path: TablePath


@dataclass
class InsertToTable(Statement):
    path: TablePath
    expr: Expr
    columns: List[str] = None
    returning_exprs: List[str] = None

    def returning(self, *exprs) -> Self:
        """Add a 'RETURNING' clause to the insert expression.

        Note: Not all databases support this feature!
        """
        if self.returning_exprs:
            raise ValueError("A returning clause is already specified")

        exprs = args_as_tuple(exprs)
        exprs = _drop_skips(exprs)
        if not exprs:
            return self

        resolve_names(self.path, exprs)
        return self.replace(returning_exprs=exprs)


@dataclass
class Commit(Statement):
    """Generate a COMMIT statement, if we're in the middle of a transaction, or in auto-commit. Otherwise SKIP."""


@dataclass
class Param(ExprNode, ITable):
    """A value placeholder, to be specified at compilation time using the `cv_params` context variable."""
    name: str

    @property
    def source_table(self):
        return self
