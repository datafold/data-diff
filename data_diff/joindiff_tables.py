"""Provides classes for performing a table diff using JOIN

"""

from contextlib import suppress
from decimal import Decimal
from functools import partial
import logging
from typing import Dict, List, Optional

from runtype import dataclass

from data_diff.databases.database_types import DbPath, Schema
from data_diff.databases.base import QueryError


from .utils import safezip
from .databases.base import Database
from .databases import MySQL, BigQuery, Presto, Oracle, PostgreSQL, Snowflake
from .table_segment import TableSegment
from .diff_tables import TableDiffer, DiffResult
from .thread_utils import ThreadedYielder

from .queries import table, sum_, min_, max_, avg, SKIP, commit
from .queries.api import and_, if_, or_, outerjoin, leftjoin, rightjoin, this, ITable
from .queries.ast_classes import Concat, Count, Expr, Random, TablePath
from .queries.compiler import Compiler
from .queries.extras import NormalizeAsString

logger = logging.getLogger("joindiff_tables")

WRITE_LIMIT = 1000


def merge_dicts(dicts):
    i = iter(dicts)
    res = next(i)
    for d in i:
        res.update(d)
    return res


@dataclass(frozen=False)
class Stats:
    exclusive_count: int
    exclusive_sample: List[tuple]
    diff_ratio_by_column: Dict[str, float]
    diff_ratio_total: float
    metrics: Dict[str, float]


def sample(table):
    return table.order_by(Random()).limit(10)


def create_temp_table(c: Compiler, table: TablePath, expr: Expr):
    db = c.database
    if isinstance(db, BigQuery):
        return f"create table {c.compile(table)} OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) as {c.compile(expr)}"
    elif isinstance(db, Presto):
        return f"create table {c.compile(table)} as {c.compile(expr)}"
    elif isinstance(db, Oracle):
        return f"create global temporary table {c.compile(table)} as {c.compile(expr)}"
    else:
        return f"create temporary table {c.compile(table)} as {c.compile(expr)}"


def drop_table_oracle(name: DbPath):
    t = table(name)
    # Experience shows double drop is necessary
    with suppress(QueryError):
        yield t.drop()
        yield t.drop()
    yield commit


def drop_table(name: DbPath):
    t = table(name)
    yield t.drop(if_exists=True)
    yield commit


def append_to_table_oracle(path: DbPath, expr: Expr):
    """See append_to_table"""
    assert expr.schema, expr
    t = table(path, schema=expr.schema)
    with suppress(QueryError):
        yield t.create()  # uses expr.schema
        yield commit
    yield t.insert_expr(expr)
    yield commit


def append_to_table(path: DbPath, expr: Expr):
    """Append to table
    """
    assert expr.schema, expr
    t = table(path, schema=expr.schema)
    yield t.create(if_not_exists=True)  # uses expr.schema
    yield commit
    yield t.insert_expr(expr)
    yield commit


def bool_to_int(x):
    return if_(x, 1, 0)


def _outerjoin(db: Database, a: ITable, b: ITable, keys1: List[str], keys2: List[str], select_fields: dict) -> ITable:
    on = [a[k1] == b[k2] for k1, k2 in safezip(keys1, keys2)]

    if isinstance(db, Oracle):
        is_exclusive_a = and_(bool_to_int(b[k] == None) for k in keys2)
        is_exclusive_b = and_(bool_to_int(a[k] == None) for k in keys1)
    else:
        is_exclusive_a = and_(b[k] == None for k in keys2)
        is_exclusive_b = and_(a[k] == None for k in keys1)

    if isinstance(db, MySQL):
        # No outer join
        l = leftjoin(a, b).on(*on).select(is_exclusive_a=is_exclusive_a, is_exclusive_b=False, **select_fields)
        r = rightjoin(a, b).on(*on).select(is_exclusive_a=False, is_exclusive_b=is_exclusive_b, **select_fields)
        return l.union(r)

    return outerjoin(a, b).on(*on).select(is_exclusive_a=is_exclusive_a, is_exclusive_b=is_exclusive_b, **select_fields)


def _slice_tuple(t, *sizes):
    i = 0
    for size in sizes:
        yield t[i : i + size]
        i += size
    assert i == len(t)


def json_friendly_value(v):
    if isinstance(v, Decimal):
        return float(v)
    return v


@dataclass
class JoinDiffer(TableDiffer):
    """Finds the diff between two SQL tables in the same database, using JOINs.

    The algorithm uses an OUTER JOIN (or equivalent) with extra checks and statistics.
    The two tables must reside in the same database, and their primary keys must be unique and not null.

    All parameters are optional.

    Parameters:
        threaded (bool): Enable/disable threaded diffing. Needed to take advantage of database threads.
        max_threadpool_size (int): Maximum size of each threadpool. ``None`` means auto. Only relevant when `threaded` is ``True``.
                                   There may be many pools, so number of actual threads can be a lot higher.
        validate_unique_key (bool): Enable/disable validating that the key columns are unique.
                                    Single query, and can't be threaded, so it's very slow on non-cloud dbs.
                                    Future versions will detect UNIQUE constraints in the schema.
        sample_exclusive_rows (bool): Enable/disable sampling of exclusive rows. Creates a temporary table.
        materialize_to_table (DbPath, optional): Path of new table to write diff results to. Disabled if not provided.
        write_limit (int): Maximum number of rows to write when materializing, per thread.
    """

    validate_unique_key: bool = True
    sample_exclusive_rows: bool = True
    materialize_to_table: DbPath = None
    write_limit: int = WRITE_LIMIT
    stats: dict = {}

    def _diff_tables(self, table1: TableSegment, table2: TableSegment) -> DiffResult:
        db = table1.database

        if table1.database is not table2.database:
            raise ValueError("Join-diff only works when both tables are in the same database")

        table1, table2 = self._threaded_call("with_schema", [table1, table2])

        bg_funcs = [partial(self._test_duplicate_keys, table1, table2)] if self.validate_unique_key else []
        if self.materialize_to_table:
            if isinstance(db, Oracle):
                db.query(drop_table_oracle(self.materialize_to_table))
            else:
                db.query(drop_table(self.materialize_to_table))

        with self._run_in_background(*bg_funcs):

            if isinstance(db, (Snowflake, BigQuery)):
                # Don't segment the table; let the database handling parallelization
                yield from self._diff_segments(None, table1, table2, None)
            else:
                yield from self._bisect_and_diff_tables(table1, table2)
            logger.info("Diffing complete")

    def _diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        max_rows: int,
        level=0,
        segment_index=None,
        segment_count=None,
    ):
        assert table1.database is table2.database

        if segment_index or table1.min_key or max_rows:
            logger.info(
                ". " * level + f"Diffing segment {segment_index}/{segment_count}, "
                f"key-range: {table1.min_key}..{table2.max_key}, "
                f"size <= {max_rows}"
            )

        db = table1.database
        diff_rows, a_cols, b_cols, is_diff_cols = self._create_outer_join(table1, table2)

        with self._run_in_background(
            partial(self._collect_stats, 1, table1),
            partial(self._collect_stats, 2, table2),
            partial(self._test_null_keys, table1, table2),
            partial(self._sample_and_count_exclusive, db, diff_rows, a_cols, b_cols),
            partial(self._count_diff_per_column, db, diff_rows, list(a_cols), is_diff_cols),
            partial(self._materialize_diff, db, diff_rows, segment_index=segment_index)
            if self.materialize_to_table
            else None,
        ):

            logger.debug("Querying for different rows")
            for is_xa, is_xb, *x in db.query(diff_rows, list):
                if is_xa and is_xb:
                    # Can't both be exclusive, meaning a pk is NULL
                    # This can happen if the explicit null test didn't finish running yet
                    raise ValueError(f"NULL values in one or more primary keys")
                is_diff, a_row, b_row = _slice_tuple(x, len(is_diff_cols), len(a_cols), len(b_cols))
                if not is_xb:
                    yield "-", tuple(a_row)
                if not is_xa:
                    yield "+", tuple(b_row)

    def _test_duplicate_keys(self, table1, table2):
        logger.debug("Testing for duplicate keys")

        # Test duplicate keys
        for ts in [table1, table2]:
            t = ts._make_select()
            key_columns = ts.key_columns

            q = t.select(total=Count(), total_distinct=Count(Concat(this[key_columns]), distinct=True))
            total, total_distinct = ts.database.query(q, tuple)
            if total != total_distinct:
                raise ValueError("Duplicate primary keys")

    def _test_null_keys(self, table1, table2):
        logger.debug("Testing for null keys")

        # Test null keys
        for ts in [table1, table2]:
            t = ts._make_select()
            key_columns = ts.key_columns

            q = t.select(*this[key_columns]).where(or_(this[k] == None for k in key_columns))
            nulls = ts.database.query(q, list)
            if nulls:
                raise ValueError(f"NULL values in one or more primary keys")

    def _collect_stats(self, i, table):
        logger.info(f"Collecting stats for table #{i}")
        db = table.database

        # Metrics
        col_exprs = merge_dicts(
            {
                f"sum_{c}": sum_(this[c]),
                f"avg_{c}": avg(this[c]),
                f"min_{c}": min_(this[c]),
                f"max_{c}": max_(this[c]),
            }
            for c in table._relevant_columns
            if c == "id"  # TODO just if the right type
        )
        col_exprs["count"] = Count()

        res = db.query(table._make_select().select(**col_exprs), tuple)
        res = dict(zip([f"table{i}_{n}" for n in col_exprs], map(json_friendly_value, res)))
        for k, v in res.items():
            self.stats[k] = self.stats.get(k, 0) + (v or 0)
        # self.stats.update(res)

        logger.debug(f"Done collecting stats for table #{i}")

        # stats.diff_ratio_by_column = diff_stats
        # stats.diff_ratio_total = diff_stats['total_diff']

    def _create_outer_join(self, table1, table2):
        db = table1.database
        if db is not table2.database:
            raise ValueError("Joindiff only applies to tables within the same database")

        keys1 = table1.key_columns
        keys2 = table2.key_columns
        if len(keys1) != len(keys2):
            raise ValueError("The provided key columns are of a different count")

        cols1 = table1._relevant_columns
        cols2 = table2._relevant_columns
        if len(cols1) != len(cols2):
            raise ValueError("The provided columns are of a different count")

        a = table1._make_select()
        b = table2._make_select()

        is_diff_cols = {f"is_diff_{c1}": bool_to_int(a[c1].is_distinct_from(b[c2])) for c1, c2 in safezip(cols1, cols2)}

        a_cols = {f"table1_{c}": NormalizeAsString(a[c]) for c in cols1}
        b_cols = {f"table2_{c}": NormalizeAsString(b[c]) for c in cols2}

        diff_rows = _outerjoin(db, a, b, keys1, keys2, {**is_diff_cols, **a_cols, **b_cols}).where(
            or_(this[c] == 1 for c in is_diff_cols)
        )
        return diff_rows, a_cols, b_cols, is_diff_cols

    def _count_diff_per_column(self, db, diff_rows, cols, is_diff_cols):
        logger.info("Counting differences per column")
        is_diff_cols_counts = db.query(diff_rows.select(sum_(this[c]) for c in is_diff_cols), tuple)
        diff_counts = {}
        for name, count in safezip(cols, is_diff_cols_counts):
            diff_counts[name] = diff_counts.get(name, 0) + (count or 0)
        self.stats["diff_counts"] = diff_counts

    def _sample_and_count_exclusive(self, db, diff_rows, a_cols, b_cols):
        if isinstance(db, Oracle):
            exclusive_rows_query = diff_rows.where((this.is_exclusive_a == 1) | (this.is_exclusive_b == 1))
        else:
            exclusive_rows_query = diff_rows.where(this.is_exclusive_a | this.is_exclusive_b)

        if not self.sample_exclusive_rows:
            logger.info("Counting exclusive rows")
            self.stats["exclusive_count"] = db.query(exclusive_rows_query.count(), int)
            return

        logger.info("Counting and sampling exclusive rows")

        def exclusive_rows(expr):
            c = Compiler(db)
            name = c.new_unique_table_name("temp_table")
            exclusive_rows = table(name, schema=expr.source_table.schema)
            yield create_temp_table(c, exclusive_rows, expr.limit(self.write_limit))

            count = yield exclusive_rows.count()
            self.stats["exclusive_count"] = self.stats.get("exclusive_count", 0) + count[0][0]
            sample_rows = yield sample(exclusive_rows.select(*this[list(a_cols)], *this[list(b_cols)]))
            self.stats["exclusive_sample"] = self.stats.get("exclusive_sample", []) + sample_rows

            # Only drops if create table succeeded (meaning, the table didn't already exist)
            yield exclusive_rows.drop()

        # Run as a sequence of thread-local queries (compiled into a ThreadLocalInterpreter)
        db.query(exclusive_rows(exclusive_rows_query), None)

    def _materialize_diff(self, db, diff_rows, segment_index=None):
        assert self.materialize_to_table

        f = append_to_table_oracle if isinstance(db, Oracle) else append_to_table
        db.query(f(self.materialize_to_table, diff_rows.limit(self.write_limit)))
        logger.info(f"Materialized diff to table '{'.'.join(self.materialize_to_table)}'.")
