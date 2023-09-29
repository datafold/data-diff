import abc
import functools
import random
from datetime import datetime
import math
import sys
import logging
from typing import Any, Callable, ClassVar, Dict, Generator, Tuple, Optional, Sequence, Type, List, Union, TypeVar
from functools import partial, wraps
from concurrent.futures import ThreadPoolExecutor
import threading
from abc import abstractmethod
from uuid import UUID
import decimal
import contextvars

import attrs
from typing_extensions import Self

from data_diff.abcs.compiler import AbstractCompiler
from data_diff.queries.extras import ApplyFuncAndNormalizeAsString, Checksum, NormalizeAsString
from data_diff.utils import ArithString, is_uuid, join_iter, safezip
from data_diff.queries.api import Expr, table, Select, SKIP, Explain, Code, this
from data_diff.queries.ast_classes import (
    Alias,
    BinOp,
    CaseWhen,
    Cast,
    Column,
    Commit,
    Concat,
    ConstantTable,
    Count,
    CreateTable,
    Cte,
    CurrentTimestamp,
    DropTable,
    Func,
    GroupBy,
    ITable,
    In,
    InsertToTable,
    IsDistinctFrom,
    Join,
    Param,
    Random,
    Root,
    TableAlias,
    TableOp,
    TablePath,
    TimeTravel,
    TruncateTable,
    UnaryOp,
    WhenThen,
    _ResolveColumn,
)
from data_diff.abcs.database_types import (
    Array,
    Struct,
    ColType,
    Integer,
    Decimal,
    Float,
    Native_UUID,
    String_UUID,
    String_Alphanum,
    String_VaryingAlphanum,
    TemporalType,
    UnknownColType,
    TimestampTZ,
    Text,
    DbTime,
    DbPath,
    Boolean,
    JSON,
)
from data_diff.abcs.mixins import AbstractMixin_TimeTravel, Compilable
from data_diff.abcs.mixins import (
    AbstractMixin_Schema,
    AbstractMixin_RandomSample,
    AbstractMixin_NormalizeValue,
    AbstractMixin_OptimizerHints,
)

logger = logging.getLogger("database")
cv_params = contextvars.ContextVar("params")


class CompileError(Exception):
    pass


@attrs.define(frozen=True)
class Compiler(AbstractCompiler):
    """
    Compiler bears the context for a single compilation.

    There can be multiple compilation per app run.
    There can be multiple compilers in one compilation (with varying contexts).
    """

    # Database is needed to normalize tables. Dialect is needed for recursive compilations.
    # In theory, it is many-to-many relations: e.g. a generic ODBC driver with multiple dialects.
    # In practice, we currently bind the dialects to the specific database classes.
    database: "Database"

    in_select: bool = False  # Compilation runtime flag
    in_join: bool = False  # Compilation runtime flag

    _table_context: List = attrs.field(factory=list)  # List[ITable]
    _subqueries: Dict[str, Any] = attrs.field(factory=dict)  # XXX not thread-safe
    root: bool = True

    _counter: List = attrs.field(factory=lambda: [0])

    @property
    def dialect(self) -> "BaseDialect":
        return self.database.dialect

    # TODO: DEPRECATED: Remove once the dialect is used directly in all places.
    def compile(self, elem, params=None) -> str:
        return self.dialect.compile(self, elem, params)

    def new_unique_name(self, prefix="tmp"):
        self._counter[0] += 1
        return f"{prefix}{self._counter[0]}"

    def new_unique_table_name(self, prefix="tmp") -> DbPath:
        self._counter[0] += 1
        table_name = f"{prefix}{self._counter[0]}_{'%x'%random.randrange(2**32)}"
        return self.database.dialect.parse_table_name(table_name)

    def add_table_context(self, *tables: Sequence, **kw) -> Self:
        return attrs.evolve(self, table_context=self._table_context + list(tables), **kw)


def parse_table_name(t):
    return tuple(t.split("."))


def import_helper(package: str = None, text=""):
    def dec(f):
        @wraps(f)
        def _inner():
            try:
                return f()
            except ModuleNotFoundError as e:
                s = text
                if package:
                    s += f"Please complete setup by running: pip install 'data_diff[{package}]'."
                raise ModuleNotFoundError(f"{e}\n\n{s}\n")

        return _inner

    return dec


class ConnectError(Exception):
    pass


class QueryError(Exception):
    pass


def _one(seq):
    (x,) = seq
    return x


@attrs.define(frozen=False)
class ThreadLocalInterpreter:
    """An interpeter used to execute a sequence of queries within the same thread and cursor.

    Useful for cursor-sensitive operations, such as creating a temporary table.
    """

    compiler: Compiler
    gen: Generator

    def apply_queries(self, callback: Callable[[str], Any]):
        q: Expr = next(self.gen)
        while True:
            sql = self.compiler.database.dialect.compile(self.compiler, q)
            logger.debug("Running SQL (%s-TL): %s", self.compiler.database.name, sql)
            try:
                try:
                    res = callback(sql) if sql is not SKIP else SKIP
                except Exception as e:
                    q = self.gen.throw(type(e), e)
                else:
                    q = self.gen.send(res)
            except StopIteration:
                break


def apply_query(callback: Callable[[str], Any], sql_code: Union[str, ThreadLocalInterpreter]) -> list:
    if isinstance(sql_code, ThreadLocalInterpreter):
        return sql_code.apply_queries(callback)
    else:
        return callback(sql_code)


@attrs.define(frozen=False)
class Mixin_Schema(AbstractMixin_Schema):
    def table_information(self) -> Compilable:
        return table("information_schema", "tables")

    def list_tables(self, table_schema: str, like: Compilable = None) -> Compilable:
        return (
            self.table_information()
            .where(
                this.table_schema == table_schema,
                this.table_name.like(like) if like is not None else SKIP,
                this.table_type == "BASE TABLE",
            )
            .select(this.table_name)
        )


@attrs.define(frozen=False)
class Mixin_RandomSample(AbstractMixin_RandomSample):
    def random_sample_n(self, tbl: ITable, size: int) -> ITable:
        # TODO use a more efficient algorithm, when the table count is known
        return tbl.order_by(Random()).limit(size)

    def random_sample_ratio_approx(self, tbl: ITable, ratio: float) -> ITable:
        return tbl.where(Random() < ratio)


@attrs.define(frozen=False)
class Mixin_OptimizerHints(AbstractMixin_OptimizerHints):
    def optimizer_hints(self, hints: str) -> str:
        return f"/*+ {hints} */ "


@attrs.define(frozen=False)
class BaseDialect(abc.ABC):
    SUPPORTS_PRIMARY_KEY: ClassVar[bool] = False
    SUPPORTS_INDEXES: ClassVar[bool] = False
    TYPE_CLASSES: ClassVar[Dict[str, Type[ColType]]] = {}

    PLACEHOLDER_TABLE = None  # Used for Oracle

    def parse_table_name(self, name: str) -> DbPath:
        "Parse the given table name into a DbPath"
        return parse_table_name(name)

    def compile(self, compiler: Compiler, elem, params=None) -> str:
        if params:
            cv_params.set(params)

        if compiler.root and isinstance(elem, Compilable) and not isinstance(elem, Root):
            from data_diff.queries.ast_classes import Select

            elem = Select(columns=[elem])

        res = self._compile(compiler, elem)
        if compiler.root and compiler._subqueries:
            subq = ", ".join(f"\n  {k} AS ({v})" for k, v in compiler._subqueries.items())
            compiler._subqueries.clear()
            return f"WITH {subq}\n{res}"
        return res

    def _compile(self, compiler: Compiler, elem) -> str:
        if elem is None:
            return "NULL"
        elif isinstance(elem, Compilable):
            return self.render_compilable(attrs.evolve(compiler, root=False), elem)
        elif isinstance(elem, str):
            return f"'{elem}'"
        elif isinstance(elem, (int, float)):
            return str(elem)
        elif isinstance(elem, datetime):
            return self.timestamp_value(elem)
        elif isinstance(elem, bytes):
            return f"b'{elem.decode()}'"
        elif isinstance(elem, ArithString):
            return f"'{elem}'"
        assert False, elem

    def render_compilable(self, c: Compiler, elem: Compilable) -> str:
        # All ifs are only for better code navigation, IDE usage detection, and type checking.
        # The last catch-all would render them anyway — it is a typical "visitor" pattern.
        if isinstance(elem, Column):
            return self.render_column(c, elem)
        elif isinstance(elem, Cte):
            return self.render_cte(c, elem)
        elif isinstance(elem, Commit):
            return self.render_commit(c, elem)
        elif isinstance(elem, Param):
            return self.render_param(c, elem)
        elif isinstance(elem, NormalizeAsString):
            return self.render_normalizeasstring(c, elem)
        elif isinstance(elem, ApplyFuncAndNormalizeAsString):
            return self.render_applyfuncandnormalizeasstring(c, elem)
        elif isinstance(elem, Checksum):
            return self.render_checksum(c, elem)
        elif isinstance(elem, Concat):
            return self.render_concat(c, elem)
        elif isinstance(elem, Func):
            return self.render_func(c, elem)
        elif isinstance(elem, WhenThen):
            return self.render_whenthen(c, elem)
        elif isinstance(elem, CaseWhen):
            return self.render_casewhen(c, elem)
        elif isinstance(elem, IsDistinctFrom):
            return self.render_isdistinctfrom(c, elem)
        elif isinstance(elem, UnaryOp):
            return self.render_unaryop(c, elem)
        elif isinstance(elem, BinOp):
            return self.render_binop(c, elem)
        elif isinstance(elem, TablePath):
            return self.render_tablepath(c, elem)
        elif isinstance(elem, TableAlias):
            return self.render_tablealias(c, elem)
        elif isinstance(elem, TableOp):
            return self.render_tableop(c, elem)
        elif isinstance(elem, Select):
            return self.render_select(c, elem)
        elif isinstance(elem, Join):
            return self.render_join(c, elem)
        elif isinstance(elem, GroupBy):
            return self.render_groupby(c, elem)
        elif isinstance(elem, Count):
            return self.render_count(c, elem)
        elif isinstance(elem, Alias):
            return self.render_alias(c, elem)
        elif isinstance(elem, In):
            return self.render_in(c, elem)
        elif isinstance(elem, Cast):
            return self.render_cast(c, elem)
        elif isinstance(elem, Random):
            return self.render_random(c, elem)
        elif isinstance(elem, Explain):
            return self.render_explain(c, elem)
        elif isinstance(elem, CurrentTimestamp):
            return self.render_currenttimestamp(c, elem)
        elif isinstance(elem, TimeTravel):
            return self.render_timetravel(c, elem)
        elif isinstance(elem, CreateTable):
            return self.render_createtable(c, elem)
        elif isinstance(elem, DropTable):
            return self.render_droptable(c, elem)
        elif isinstance(elem, TruncateTable):
            return self.render_truncatetable(c, elem)
        elif isinstance(elem, InsertToTable):
            return self.render_inserttotable(c, elem)
        elif isinstance(elem, Code):
            return self.render_code(c, elem)
        elif isinstance(elem, _ResolveColumn):
            return self.render__resolvecolumn(c, elem)

        method_name = f"render_{elem.__class__.__name__.lower()}"
        method = getattr(self, method_name, None)
        if method is not None:
            return method(c, elem)
        else:
            raise RuntimeError(f"Cannot render AST of type {elem.__class__}")
        # return elem.compile(compiler.replace(root=False))

    def render_column(self, c: Compiler, elem: Column) -> str:
        if c._table_context:
            if len(c._table_context) > 1:
                aliases = [
                    t for t in c._table_context if isinstance(t, TableAlias) and t.source_table is elem.source_table
                ]
                if not aliases:
                    return self.quote(elem.name)
                elif len(aliases) > 1:
                    raise CompileError(f"Too many aliases for column {elem.name}")
                (alias,) = aliases

                return f"{self.quote(alias.name)}.{self.quote(elem.name)}"

        return self.quote(elem.name)

    def render_cte(self, parent_c: Compiler, elem: Cte) -> str:
        c: Compiler = attrs.evolve(parent_c, table_context=[], in_select=False)
        compiled = self.compile(c, elem.source_table)

        name = elem.name or parent_c.new_unique_name()
        name_params = f"{name}({', '.join(elem.params)})" if elem.params else name
        parent_c._subqueries[name_params] = compiled

        return name

    def render_commit(self, c: Compiler, elem: Commit) -> str:
        return "COMMIT" if not c.database.is_autocommit else SKIP

    def render_param(self, c: Compiler, elem: Param) -> str:
        params = cv_params.get()
        return self._compile(c, params[elem.name])

    def render_normalizeasstring(self, c: Compiler, elem: NormalizeAsString) -> str:
        expr = self.compile(c, elem.expr)
        return self.normalize_value_by_type(expr, elem.expr_type or elem.expr.type)

    def render_applyfuncandnormalizeasstring(self, c: Compiler, elem: ApplyFuncAndNormalizeAsString) -> str:
        expr = elem.expr
        expr_type = expr.type

        if isinstance(expr_type, Native_UUID):
            # Normalize first, apply template after (for uuids)
            # Needed because min/max(uuid) fails in postgresql
            expr = NormalizeAsString(expr, expr_type)
            if elem.apply_func is not None:
                expr = elem.apply_func(expr)  # Apply template using Python's string formatting

        else:
            # Apply template before normalizing (for ints)
            if elem.apply_func is not None:
                expr = elem.apply_func(expr)  # Apply template using Python's string formatting
            expr = NormalizeAsString(expr, expr_type)

        return self.compile(c, expr)

    def render_checksum(self, c: Compiler, elem: Checksum) -> str:
        if len(elem.exprs) > 1:
            exprs = [Code(f"coalesce({self.compile(c, expr)}, '<null>')") for expr in elem.exprs]
            # exprs = [self.compile(c, e) for e in exprs]
            expr = Concat(exprs, "|")
        else:
            # No need to coalesce - safe to assume that key cannot be null
            (expr,) = elem.exprs
        expr = self.compile(c, expr)
        md5 = self.md5_as_int(expr)
        return f"sum({md5})"

    def render_concat(self, c: Compiler, elem: Concat) -> str:
        # We coalesce because on some DBs (e.g. MySQL) concat('a', NULL) is NULL
        items = [
            f"coalesce({self.compile(c, Code(self.to_string(self.compile(c, expr))))}, '<null>')" for expr in elem.exprs
        ]
        assert items
        if len(items) == 1:
            return items[0]

        if elem.sep:
            items = list(join_iter(f"'{elem.sep}'", items))
        return self.concat(items)

    def render_alias(self, c: Compiler, elem: Alias) -> str:
        return f"{self.compile(c, elem.expr)} AS {self.quote(elem.name)}"

    def render_count(self, c: Compiler, elem: Count) -> str:
        expr = self.compile(c, elem.expr) if elem.expr else "*"
        if elem.distinct:
            return f"count(distinct {expr})"
        return f"count({expr})"

    def render_code(self, c: Compiler, elem: Code) -> str:
        if not elem.args:
            return elem.code

        args = {k: self.compile(c, v) for k, v in elem.args.items()}
        return elem.code.format(**args)

    def render_func(self, c: Compiler, elem: Func) -> str:
        args = ", ".join(self.compile(c, e) for e in elem.args)
        return f"{elem.name}({args})"

    def render_whenthen(self, c: Compiler, elem: WhenThen) -> str:
        return f"WHEN {self.compile(c, elem.when)} THEN {self.compile(c, elem.then)}"

    def render_casewhen(self, c: Compiler, elem: CaseWhen) -> str:
        assert elem.cases
        when_thens = " ".join(self.compile(c, case) for case in elem.cases)
        else_expr = (" ELSE " + self.compile(c, elem.else_expr)) if elem.else_expr is not None else ""
        return f"CASE {when_thens}{else_expr} END"

    def render_isdistinctfrom(self, c: Compiler, elem: IsDistinctFrom) -> str:
        a = self.to_comparable(self.compile(c, elem.a), elem.a.type)
        b = self.to_comparable(self.compile(c, elem.b), elem.b.type)
        return self.is_distinct_from(a, b)

    def render_unaryop(self, c: Compiler, elem: UnaryOp) -> str:
        return f"({elem.op}{self.compile(c, elem.expr)})"

    def render_binop(self, c: Compiler, elem: BinOp) -> str:
        expr = f" {elem.op} ".join(self.compile(c, a) for a in elem.args)
        return f"({expr})"

    def render_tablepath(self, c: Compiler, elem: TablePath) -> str:
        path = elem.path  # c.database._normalize_table_path(self.name)
        return ".".join(map(self.quote, path))

    def render_tablealias(self, c: Compiler, elem: TableAlias) -> str:
        return f"{self.compile(c, elem.source_table)} {self.quote(elem.name)}"

    def render_tableop(self, parent_c: Compiler, elem: TableOp) -> str:
        c: Compiler = attrs.evolve(parent_c, in_select=False)
        table_expr = f"{self.compile(c, elem.table1)} {elem.op} {self.compile(c, elem.table2)}"
        if parent_c.in_select:
            table_expr = f"({table_expr}) {c.new_unique_name()}"
        elif parent_c.in_join:
            table_expr = f"({table_expr})"
        return table_expr

    def render__resolvecolumn(self, c: Compiler, elem: _ResolveColumn) -> str:
        return self.compile(c, elem._get_resolved())

    def render_select(self, parent_c: Compiler, elem: Select) -> str:
        c: Compiler = attrs.evolve(parent_c, in_select=True)  # .add_table_context(self.table)
        compile_fn = functools.partial(self.compile, c)

        columns = ", ".join(map(compile_fn, elem.columns)) if elem.columns else "*"
        distinct = "DISTINCT " if elem.distinct else ""
        optimizer_hints = self.optimizer_hints(elem.optimizer_hints) if elem.optimizer_hints else ""
        select = f"SELECT {optimizer_hints}{distinct}{columns}"

        if elem.table:
            select += " FROM " + self.compile(c, elem.table)
        elif self.PLACEHOLDER_TABLE:
            select += f" FROM {self.PLACEHOLDER_TABLE}"

        if elem.where_exprs:
            select += " WHERE " + " AND ".join(map(compile_fn, elem.where_exprs))

        if elem.group_by_exprs:
            select += " GROUP BY " + ", ".join(map(compile_fn, elem.group_by_exprs))

        if elem.having_exprs:
            assert elem.group_by_exprs
            select += " HAVING " + " AND ".join(map(compile_fn, elem.having_exprs))

        if elem.order_by_exprs:
            select += " ORDER BY " + ", ".join(map(compile_fn, elem.order_by_exprs))

        if elem.limit_expr is not None:
            has_order_by = bool(elem.order_by_exprs)
            select += " " + self.offset_limit(0, elem.limit_expr, has_order_by=has_order_by)

        if parent_c.in_select:
            select = f"({select}) {c.new_unique_name()}"
        elif parent_c.in_join:
            select = f"({select})"
        return select

    def render_join(self, parent_c: Compiler, elem: Join) -> str:
        tables = [
            t if isinstance(t, TableAlias) else TableAlias(t, name=parent_c.new_unique_name())
            for t in elem.source_tables
        ]
        c = parent_c.add_table_context(*tables, in_join=True, in_select=False)
        op = " JOIN " if elem.op is None else f" {elem.op} JOIN "
        joined = op.join(self.compile(c, t) for t in tables)

        if elem.on_exprs:
            on = " AND ".join(self.compile(c, e) for e in elem.on_exprs)
            res = f"{joined} ON {on}"
        else:
            res = joined

        compile_fn = functools.partial(self.compile, c)
        columns = "*" if elem.columns is None else ", ".join(map(compile_fn, elem.columns))
        select = f"SELECT {columns} FROM {res}"

        if parent_c.in_select:
            select = f"({select}) {c.new_unique_name()}"
        elif parent_c.in_join:
            select = f"({select})"
        return select

    def render_groupby(self, c: Compiler, elem: GroupBy) -> str:
        compile_fn = functools.partial(self.compile, c)

        if elem.values is None:
            raise CompileError(".group_by() must be followed by a call to .agg()")

        keys = [str(i + 1) for i in range(len(elem.keys))]
        columns = (elem.keys or []) + (elem.values or [])
        if isinstance(elem.table, Select) and elem.table.columns is None and elem.table.group_by_exprs is None:
            return self.compile(
                c,
                attrs.evolve(
                    elem.table,
                    columns=columns,
                    group_by_exprs=[Code(k) for k in keys],
                    having_exprs=elem.having_exprs,
                ),
            )

        keys_str = ", ".join(keys)
        columns_str = ", ".join(self.compile(c, x) for x in columns)
        having_str = (
            " HAVING " + " AND ".join(map(compile_fn, elem.having_exprs)) if elem.having_exprs is not None else ""
        )
        select = f"SELECT {columns_str} FROM {self.compile(attrs.evolve(c, in_select=True), elem.table)} GROUP BY {keys_str}{having_str}"

        if c.in_select:
            select = f"({select}) {c.new_unique_name()}"
        elif c.in_join:
            select = f"({select})"
        return select

    def render_in(self, c: Compiler, elem: In) -> str:
        compile_fn = functools.partial(self.compile, c)
        elems = ", ".join(map(compile_fn, elem.list))
        return f"({self.compile(c, elem.expr)} IN ({elems}))"

    def render_cast(self, c: Compiler, elem: Cast) -> str:
        return f"cast({self.compile(c, elem.expr)} as {self.compile(c, elem.target_type)})"

    def render_random(self, c: Compiler, elem: Random) -> str:
        return self.random()

    def render_explain(self, c: Compiler, elem: Explain) -> str:
        return self.explain_as_text(self.compile(c, elem.select))

    def render_currenttimestamp(self, c: Compiler, elem: CurrentTimestamp) -> str:
        return self.current_timestamp()

    def render_timetravel(self, c: Compiler, elem: TimeTravel) -> str:
        assert isinstance(c, AbstractMixin_TimeTravel)
        return self.compile(
            c,
            # TODO: why is it c.? why not self? time-trvelling is the dialect's thing, isnt't it?
            c.time_travel(
                elem.table, before=elem.before, timestamp=elem.timestamp, offset=elem.offset, statement=elem.statement
            ),
        )

    def render_createtable(self, c: Compiler, elem: CreateTable) -> str:
        ne = "IF NOT EXISTS " if elem.if_not_exists else ""
        if elem.source_table:
            return f"CREATE TABLE {ne}{self.compile(c, elem.path)} AS {self.compile(c, elem.source_table)}"

        schema = ", ".join(f"{self.quote(k)} {self.type_repr(v)}" for k, v in elem.path.schema.items())
        pks = (
            ", PRIMARY KEY (%s)" % ", ".join(elem.primary_keys)
            if elem.primary_keys and self.SUPPORTS_PRIMARY_KEY
            else ""
        )
        return f"CREATE TABLE {ne}{self.compile(c, elem.path)}({schema}{pks})"

    def render_droptable(self, c: Compiler, elem: DropTable) -> str:
        ie = "IF EXISTS " if elem.if_exists else ""
        return f"DROP TABLE {ie}{self.compile(c, elem.path)}"

    def render_truncatetable(self, c: Compiler, elem: TruncateTable) -> str:
        return f"TRUNCATE TABLE {self.compile(c, elem.path)}"

    def render_inserttotable(self, c: Compiler, elem: InsertToTable) -> str:
        if isinstance(elem.expr, ConstantTable):
            expr = self.constant_values(elem.expr.rows)
        else:
            expr = self.compile(c, elem.expr)

        columns = "(%s)" % ", ".join(map(self.quote, elem.columns)) if elem.columns is not None else ""

        return f"INSERT INTO {self.compile(c, elem.path)}{columns} {expr}"

    def offset_limit(
        self, offset: Optional[int] = None, limit: Optional[int] = None, has_order_by: Optional[bool] = None
    ) -> str:
        "Provide SQL fragment for limit and offset inside a select"
        if offset:
            raise NotImplementedError("No support for OFFSET in query")

        return f"LIMIT {limit}"

    def concat(self, items: List[str]) -> str:
        "Provide SQL for concatenating a bunch of columns into a string"
        assert len(items) > 1
        joined_exprs = ", ".join(items)
        return f"concat({joined_exprs})"

    def to_comparable(self, value: str, coltype: ColType) -> str:
        """Ensure that the expression is comparable in ``IS DISTINCT FROM``."""
        return value

    def is_distinct_from(self, a: str, b: str) -> str:
        "Provide SQL for a comparison where NULL = NULL is true"
        return f"{a} is distinct from {b}"

    def timestamp_value(self, t: DbTime) -> str:
        "Provide SQL for the given timestamp value"
        return f"'{t.isoformat()}'"

    def random(self) -> str:
        "Provide SQL for generating a random number betweein 0..1"
        return "random()"

    def current_timestamp(self) -> str:
        "Provide SQL for returning the current timestamp, aka now"
        return "current_timestamp()"

    def current_database(self) -> str:
        "Provide SQL for returning the current default database."
        return "current_database()"

    def current_schema(self) -> str:
        "Provide SQL for returning the current default schema."
        return "current_schema()"

    def explain_as_text(self, query: str) -> str:
        "Provide SQL for explaining a query, returned as table(varchar)"
        return f"EXPLAIN {query}"

    def _constant_value(self, v):
        if v is None:
            return "NULL"
        elif isinstance(v, str):
            return f"'{v}'"
        elif isinstance(v, datetime):
            return self.timestamp_value(v)
        elif isinstance(v, UUID):
            return f"'{v}'"
        elif isinstance(v, decimal.Decimal):
            return str(v)
        elif isinstance(v, bytearray):
            return f"'{v.decode()}'"
        elif isinstance(v, Code):
            return v.code
        return repr(v)

    def constant_values(self, rows) -> str:
        values = ", ".join("(%s)" % ", ".join(self._constant_value(v) for v in row) for row in rows)
        return f"VALUES {values}"

    def type_repr(self, t) -> str:
        if isinstance(t, str):
            return t
        elif isinstance(t, TimestampTZ):
            return f"TIMESTAMP({min(t.precision, DEFAULT_DATETIME_PRECISION)})"
        return {
            int: "INT",
            str: "VARCHAR",
            bool: "BOOLEAN",
            float: "FLOAT",
            datetime: "TIMESTAMP",
        }[t]

    def _parse_type_repr(self, type_repr: str) -> Optional[Type[ColType]]:
        return self.TYPE_CLASSES.get(type_repr)

    def parse_type(
        self,
        table_path: DbPath,
        col_name: str,
        type_repr: str,
        datetime_precision: int = None,
        numeric_precision: int = None,
        numeric_scale: int = None,
    ) -> ColType:
        "Parse type info as returned by the database"

        cls = self._parse_type_repr(type_repr)
        if cls is None:
            return UnknownColType(type_repr)

        if issubclass(cls, TemporalType):
            return cls(
                precision=datetime_precision if datetime_precision is not None else DEFAULT_DATETIME_PRECISION,
                rounds=self.ROUNDS_ON_PREC_LOSS,
            )

        elif issubclass(cls, Integer):
            return cls()

        elif issubclass(cls, Boolean):
            return cls()

        elif issubclass(cls, Decimal):
            if numeric_scale is None:
                numeric_scale = 0  # Needed for Oracle.
            return cls(precision=numeric_scale)

        elif issubclass(cls, Float):
            # assert numeric_scale is None
            return cls(
                precision=self._convert_db_precision_to_digits(
                    numeric_precision if numeric_precision is not None else DEFAULT_NUMERIC_PRECISION
                )
            )

        elif issubclass(cls, (JSON, Array, Struct, Text, Native_UUID)):
            return cls()

        raise TypeError(f"Parsing {type_repr} returned an unknown type '{cls}'.")

    def _convert_db_precision_to_digits(self, p: int) -> int:
        """Convert from binary precision, used by floats, to decimal precision."""
        # See: https://en.wikipedia.org/wiki/Single-precision_floating-point_format
        return math.floor(math.log(2**p, 10))

    @property
    @abstractmethod
    def name(self) -> str:
        "Name of the dialect"

    @property
    @abstractmethod
    def ROUNDS_ON_PREC_LOSS(self) -> bool:
        "True if db rounds real values when losing precision, False if it truncates."

    @abstractmethod
    def quote(self, s: str):
        "Quote SQL name"

    @abstractmethod
    def to_string(self, s: str) -> str:
        # TODO rewrite using cast_to(x, str)
        "Provide SQL for casting a column to string"

    @abstractmethod
    def set_timezone_to_utc(self) -> str:
        "Provide SQL for setting the session timezone to UTC"


T = TypeVar("T", bound=BaseDialect)


@attrs.define(frozen=True)
class QueryResult:
    rows: list
    columns: Optional[list] = None

    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, i):
        return self.rows[i]


@attrs.define(frozen=False, kw_only=True)
class Database(abc.ABC):
    """Base abstract class for databases.

    Used for providing connection code and implementation specific SQL utilities.

    Instanciated using :meth:`~data_diff.connect`
    """

    SUPPORTS_ALPHANUMS: ClassVar[bool] = True
    SUPPORTS_UNIQUE_CONSTAINT: ClassVar[bool] = False
    CONNECT_URI_KWPARAMS: ClassVar[List[str]] = []

    default_schema: Optional[str] = None
    _interactive: bool = False
    is_closed: bool = False

    @property
    def name(self):
        return type(self).__name__

    def compile(self, sql_ast):
        return self.dialect.compile(Compiler(self), sql_ast)

    def query(self, sql_ast: Union[Expr, Generator], res_type: type = None):
        """Query the given SQL code/AST, and attempt to convert the result to type 'res_type'

        If given a generator, it will execute all the yielded sql queries with the same thread and cursor.
        The results of the queries a returned by the `yield` stmt (using the .send() mechanism).
        It's a cleaner approach than exposing cursors, but may not be enough in all cases.
        """

        compiler = Compiler(self)
        if isinstance(sql_ast, Generator):
            sql_code = ThreadLocalInterpreter(compiler, sql_ast)
        elif isinstance(sql_ast, list):
            for i in sql_ast[:-1]:
                self.query(i)
            return self.query(sql_ast[-1], res_type)
        else:
            if isinstance(sql_ast, str):
                sql_code = sql_ast
            else:
                if res_type is None:
                    res_type = sql_ast.type
                sql_code = self.compile(sql_ast)
                if sql_code is SKIP:
                    return SKIP

            logger.debug("Running SQL (%s): %s", self.name, sql_code)

        if self._interactive and isinstance(sql_ast, Select):
            explained_sql = self.compile(Explain(sql_ast))
            explain = self._query(explained_sql)
            for row in explain:
                # Most returned a 1-tuple. Presto returns a string
                if isinstance(row, tuple):
                    (row,) = row
                logger.debug("EXPLAIN: %s", row)
            answer = input("Continue? [y/n] ")
            if answer.lower() not in ["y", "yes"]:
                sys.exit(1)

        res = self._query(sql_code)
        if res_type is list:
            return list(res)
        elif res_type is int:
            if not res:
                raise ValueError("Query returned 0 rows, expected 1")
            row = _one(res)
            if not row:
                raise ValueError("Row is empty, expected 1 column")
            res = _one(row)
            if res is None:  # May happen due to sum() of 0 items
                return None
            return int(res)
        elif res_type is datetime:
            res = _one(_one(res))
            if isinstance(res, str):
                res = datetime.fromisoformat(res[:23])  # TODO use a better parsing method
            return res
        elif res_type is tuple:
            assert len(res) == 1, (sql_code, res)
            return res[0]
        elif getattr(res_type, "__origin__", None) is list and len(res_type.__args__) == 1:
            if res_type.__args__ in ((int,), (str,)):
                return [_one(row) for row in res]
            elif res_type.__args__ in [(Tuple,), (tuple,)]:
                return [tuple(row) for row in res]
            elif res_type.__args__ == (dict,):
                return [dict(safezip(res.columns, row)) for row in res]
            else:
                raise ValueError(res_type)
        return res

    def enable_interactive(self):
        self._interactive = True

    def select_table_schema(self, path: DbPath) -> str:
        """Provide SQL for selecting the table schema as (name, type, date_prec, num_prec)"""
        schema, name = self._normalize_table_path(path)

        return (
            "SELECT column_name, data_type, datetime_precision, numeric_precision, numeric_scale "
            "FROM information_schema.columns "
            f"WHERE table_name = '{name}' AND table_schema = '{schema}'"
        )

    def query_table_schema(self, path: DbPath) -> Dict[str, tuple]:
        """Query the table for its schema for table in 'path', and return {column: tuple}
        where the tuple is (table_name, col_name, type_repr, datetime_precision?, numeric_precision?, numeric_scale?)

        Note: This method exists instead of select_table_schema(), just because not all databases support
              accessing the schema using a SQL query.
        """
        rows = self.query(self.select_table_schema(path), list)
        if not rows:
            raise RuntimeError(f"{self.name}: Table '{'.'.join(path)}' does not exist, or has no columns")

        d = {r[0]: r for r in rows}
        assert len(d) == len(rows)
        return d

    def select_table_unique_columns(self, path: DbPath) -> str:
        "Provide SQL for selecting the names of unique columns in the table"
        schema, name = self._normalize_table_path(path)

        return (
            "SELECT column_name "
            "FROM information_schema.key_column_usage "
            f"WHERE table_name = '{name}' AND table_schema = '{schema}'"
        )

    def query_table_unique_columns(self, path: DbPath) -> List[str]:
        """Query the table for its unique columns for table in 'path', and return {column}"""
        if not self.SUPPORTS_UNIQUE_CONSTAINT:
            raise NotImplementedError("This database doesn't support 'unique' constraints")
        res = self.query(self.select_table_unique_columns(path), List[str])
        return list(res)

    def _process_table_schema(
        self, path: DbPath, raw_schema: Dict[str, tuple], filter_columns: Sequence[str] = None, where: str = None
    ):
        """Process the result of query_table_schema().

        Done in a separate step, to minimize the amount of processed columns.
        Needed because processing each column may:
        * throw errors and warnings
        * query the database to sample values

        """
        if filter_columns is None:
            filtered_schema = raw_schema
        else:
            accept = {i.lower() for i in filter_columns}
            filtered_schema = {name: row for name, row in raw_schema.items() if name.lower() in accept}

        col_dict = {row[0]: self.dialect.parse_type(path, *row) for _name, row in filtered_schema.items()}

        self._refine_coltypes(path, col_dict, where)

        # Return a dict of form {name: type} after normalization
        return col_dict

    def _refine_coltypes(self, table_path: DbPath, col_dict: Dict[str, ColType], where: str = None, sample_size=64):
        """Refine the types in the column dict, by querying the database for a sample of their values

        'where' restricts the rows to be sampled.
        """

        text_columns = [k for k, v in col_dict.items() if isinstance(v, Text)]
        if not text_columns:
            return

        if isinstance(self.dialect, AbstractMixin_NormalizeValue):
            fields = [Code(self.dialect.normalize_uuid(self.dialect.quote(c), String_UUID())) for c in text_columns]
        else:
            fields = this[text_columns]

        samples_by_row = self.query(
            table(*table_path).select(*fields).where(Code(where) if where else SKIP).limit(sample_size), list
        )
        if not samples_by_row:
            raise ValueError(f"Table {table_path} is empty.")

        samples_by_col = list(zip(*samples_by_row))

        for col_name, samples in safezip(text_columns, samples_by_col):
            uuid_samples = [s for s in samples if s and is_uuid(s)]

            if uuid_samples:
                if len(uuid_samples) != len(samples):
                    logger.warning(
                        f"Mixed UUID/Non-UUID values detected in column {'.'.join(table_path)}.{col_name}, disabling UUID support."
                    )
                else:
                    assert col_name in col_dict
                    col_dict[col_name] = String_UUID()
                    continue

            if self.SUPPORTS_ALPHANUMS:  # Anything but MySQL (so far)
                alphanum_samples = [s for s in samples if String_Alphanum.test_value(s)]
                if alphanum_samples:
                    if len(alphanum_samples) != len(samples):
                        logger.debug(
                            f"Mixed Alphanum/Non-Alphanum values detected in column {'.'.join(table_path)}.{col_name}. It cannot be used as a key."
                        )
                    else:
                        assert col_name in col_dict
                        col_dict[col_name] = String_VaryingAlphanum()

    # @lru_cache()
    # def get_table_schema(self, path: DbPath) -> Dict[str, ColType]:
    #     return self.query_table_schema(path)

    def _normalize_table_path(self, path: DbPath) -> DbPath:
        if len(path) == 1:
            return self.default_schema, path[0]
        elif len(path) == 2:
            return path

        raise ValueError(f"{self.name}: Bad table path for {self}: '{'.'.join(path)}'. Expected form: schema.table")

    def _query_cursor(self, c, sql_code: str) -> QueryResult:
        assert isinstance(sql_code, str), sql_code
        try:
            c.execute(sql_code)
            if sql_code.lower().startswith(("select", "explain", "show")):
                columns = [col[0] for col in c.description]

                fetched = c.fetchall()
                result = QueryResult(fetched, columns)
                return result
        except Exception as _e:
            # logger.exception(e)
            # logger.error(f'Caused by SQL: {sql_code}')
            raise

    def _query_conn(self, conn, sql_code: Union[str, ThreadLocalInterpreter]) -> QueryResult:
        c = conn.cursor()
        callback = partial(self._query_cursor, c)
        return apply_query(callback, sql_code)

    def close(self):
        "Close connection(s) to the database instance. Querying will stop functioning."
        self.is_closed = True
        return super().close()

    def list_tables(self, tables_like, schema=None):
        return self.query(self.dialect.list_tables(schema or self.default_schema, tables_like))

    @property
    @abstractmethod
    def dialect(self) -> BaseDialect:
        "The dialect of the database. Used internally by Database, and also available publicly."

    @property
    @abstractmethod
    def CONNECT_URI_HELP(self) -> str:
        "Example URI to show the user in help and error messages"

    @property
    @abstractmethod
    def CONNECT_URI_PARAMS(self) -> List[str]:
        "List of parameters given in the path of the URI"

    @abstractmethod
    def _query(self, sql_code: str) -> list:
        "Send query to database and return result"

    @property
    @abstractmethod
    def is_autocommit(self) -> bool:
        "Return whether the database autocommits changes. When false, COMMIT statements are skipped."


@attrs.define(frozen=False)
class ThreadedDatabase(Database):
    """Access the database through singleton threads.

    Used for database connectors that do not support sharing their connection between different threads.
    """

    thread_count: int = 1

    _init_error: Optional[Exception] = None
    _queue: Optional[ThreadPoolExecutor] = None
    thread_local: threading.local = attrs.field(factory=threading.local)

    def __attrs_post_init__(self):
        self._queue = ThreadPoolExecutor(self.thread_count, initializer=self.set_conn)
        logger.info(f"[{self.name}] Starting a threadpool, size={self.thread_count}.")

    def set_conn(self):
        assert not hasattr(self.thread_local, "conn")
        try:
            self.thread_local.conn = self.create_connection()
        except Exception as e:
            self._init_error = e

    def _query(self, sql_code: Union[str, ThreadLocalInterpreter]) -> QueryResult:
        r = self._queue.submit(self._query_in_worker, sql_code)
        return r.result()

    def _query_in_worker(self, sql_code: Union[str, ThreadLocalInterpreter]):
        "This method runs in a worker thread"
        if self._init_error:
            raise self._init_error
        return self._query_conn(self.thread_local.conn, sql_code)

    @abstractmethod
    def create_connection(self):
        "Return a connection instance, that supports the .cursor() method."

    def close(self):
        super().close()
        self._queue.shutdown()

    @property
    def is_autocommit(self) -> bool:
        return False


CHECKSUM_HEXDIGITS = 12  # Must be 12 or lower, otherwise SUM() overflows
MD5_HEXDIGITS = 32

_CHECKSUM_BITSIZE = CHECKSUM_HEXDIGITS << 2
CHECKSUM_MASK = (2**_CHECKSUM_BITSIZE) - 1

DEFAULT_DATETIME_PRECISION = 6
DEFAULT_NUMERIC_PRECISION = 24

TIMESTAMP_PRECISION_POS = 20  # len("2022-06-03 12:24:35.") == 20
