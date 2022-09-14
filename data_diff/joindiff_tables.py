"""Provides classes for performing a table diff using JOIN

"""

from decimal import Decimal
from functools import partial
import logging
from contextlib import contextmanager
from typing import Dict, List

from runtype import dataclass

from .utils import safezip
from .databases.base import Database
from .table_segment import TableSegment
from .diff_tables import ThreadBase, DiffResult

from .queries import table, sum_, min_, max_, avg
from .queries.api import and_, if_, or_, outerjoin, this
from .queries.ast_classes import Concat, Count, Expr, Random
from .queries.compiler import Compiler
from .queries.extras import NormalizeAsString


logger = logging.getLogger("joindiff_tables")


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
    # TODO
    return table.order_by(Random()).limit(10)


@contextmanager
def temp_table(db: Database, expr: Expr):
    c = Compiler(db)
    name = c.new_unique_name("tmp_table")
    db.query(f"create temporary table {c.quote(name)} as {c.compile(expr)}", None)
    try:
        yield table(name, schema=expr.source_table.schema)
    finally:
        db.query(f"drop table {c.quote(name)}", None)


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
class JoinDifferBase(ThreadBase):
    """Finds the diff between two SQL tables using JOINs"""

    stats: dict = {}

    def diff_tables(self, table1: TableSegment, table2: TableSegment) -> DiffResult:
        table1, table2 = self._threaded_call("with_schema", [table1, table2])

        if table1.database is not table2.database:
            raise ValueError("Join-diff only works when both tables are in the same database")

        with self._run_in_background(
                    partial(self._test_null_or_duplicate_keys, table1, table2),
                    partial(self._collect_stats, 1, table1),
                    partial(self._collect_stats, 2, table2)
                ):
            yield from self._outer_join(table1, table2)

        logger.info("Diffing complete")

    def _test_null_or_duplicate_keys(self, table1, table2):
        logger.info("Testing for null or duplicate keys")

        # Test null or duplicate keys
        for ts in [table1, table2]:
            t = table(*ts.table_path, schema=ts._schema)
            key_columns = [ts.key_column]  # XXX

            q = t.select(total=Count(), total_distinct=Count(Concat(key_columns), distinct=True))
            total, total_distinct = ts.database.query(q, tuple)
            if total != total_distinct:
                raise ValueError("Duplicate primary keys")

            q = t.select(*key_columns).where(or_(this[k] == None for k in key_columns))
            nulls = ts.database.query(q, list)
            if nulls:
                raise ValueError(f"NULL values in one or more primary keys")

        logger.debug("Done testing for null or duplicate keys")

    def _collect_stats(self, i, table):
        logger.info(f"Collecting stats for table #{i}")
        db = table.database

        # Metrics
        col_exprs = merge_dicts(
            {
                f"sum_{c}": sum_(c),
                f"avg_{c}": avg(c),
                f"min_{c}": min_(c),
                f"max_{c}": max_(c),
            }
            for c in table._relevant_columns
            if c == "id"  # TODO just if the right type
        )
        col_exprs["count"] = Count()

        res = db.query(table._make_select().select(**col_exprs), tuple)
        res = dict(zip([f"table{i}_{n}" for n in col_exprs], map(json_friendly_value, res)))
        self.stats.update(res)

        logger.debug(f"Done collecting stats for table #{i}")

        # stats.diff_ratio_by_column = diff_stats
        # stats.diff_ratio_total = diff_stats['total_diff']


def bool_to_int(x):
    return if_(x, 1, 0)


class JoinDiffer(JoinDifferBase):
    def _outer_join(self, table1, table2):
        db = table1.database
        if db is not table2.database:
            raise ValueError("Joindiff only applies to tables within the same database")

        keys1 = [table1.key_column]  # XXX
        keys2 = [table2.key_column]  # XXX
        if len(keys1) != len(keys2):
            raise ValueError("The provided key columns are of a different count")

        cols1 = table1._relevant_columns
        cols2 = table2._relevant_columns
        if len(cols1) != len(cols2):
            raise ValueError("The provided columns are of a different count")

        a = table1._make_select()
        b = table2._make_select()

        is_diff_cols = {
            f"is_diff_{c1}": bool_to_int(a[c1].is_distinct_from(b[c2])) for c1, c2 in safezip(cols1, cols2)
        }

        a_cols = {f"table1_{c}": NormalizeAsString(a[c]) for c in cols1}
        b_cols = {f"table2_{c}": NormalizeAsString(b[c]) for c in cols2}

        diff_rows = (
            outerjoin(a, b)
            .on(a[k1] == b[k2] for k1, k2 in safezip(keys1, keys2))
            .select(
                is_exclusive_a=and_(b[k] == None for k in keys2),
                is_exclusive_b=and_(a[k] == None for k in keys1),
                **is_diff_cols,
                **a_cols,
                **b_cols,
            )
            .where(or_(this[c] == 1 for c in is_diff_cols))
        )

        with self._run_in_background(
                    partial(self._sample_and_count_exclusive, db, diff_rows, a_cols, b_cols),
                    partial(self._count_diff_per_column, db, diff_rows, cols1, is_diff_cols)
                ):

            logger.info("Querying for different rows")
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

    def _count_diff_per_column(self, db, diff_rows, cols, is_diff_cols):
        logger.info("Counting differences per column")
        is_diff_cols_counts = db.query(diff_rows.select(sum_(this[c]) for c in is_diff_cols), tuple)
        diff_counts = {}
        for name, count in safezip(cols, is_diff_cols_counts):
            diff_counts[name] = count
        self.stats['diff_counts'] = diff_counts

    def _sample_and_count_exclusive(self, db, diff_rows, a_cols, b_cols):
        logger.info("Counting and sampling exclusive rows")
        exclusive_rows_query = diff_rows.where(this.is_exclusive_a | this.is_exclusive_b)
        with temp_table(db, exclusive_rows_query) as exclusive_rows:
            self.stats["exclusive_count"] = db.query(exclusive_rows.count(), int)
            sample_rows = db.query(sample(exclusive_rows.select(*this[list(a_cols)], *this[list(b_cols)])), list)
            self.stats["exclusive_sample"] = sample_rows
