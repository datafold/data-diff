"""Provides classes for performing a table diff using JOIN

"""
from decimal import Decimal
from functools import partial
import logging
from typing import List, Optional
from itertools import chain

import attrs

from data_diff.databases import Database, MsSQL, MySQL, BigQuery, Presto, Oracle, Snowflake
from data_diff.abcs.database_types import NumericType, DbPath
from data_diff.databases.base import Compiler
from data_diff.queries.api import (
    table,
    sum_,
    and_,
    if_,
    or_,
    outerjoin,
    leftjoin,
    rightjoin,
    this,
    when,
)
from data_diff.queries.ast_classes import Concat, Count, Expr, Random, TablePath, Code, ITable
from data_diff.queries.extras import NormalizeAsString
from data_diff.info_tree import InfoTree
from data_diff.query_utils import append_to_table, drop_table
from data_diff.utils import safezip
from data_diff.table_segment import TableSegment
from data_diff.diff_tables import TableDiffer, DiffResult
from data_diff.thread_utils import ThreadedYielder


logger = logging.getLogger("joindiff_tables")

TABLE_WRITE_LIMIT = 1000


def merge_dicts(dicts):
    i = iter(dicts)
    try:
        res = next(i)
    except StopIteration:
        return {}

    for d in i:
        res.update(d)
    return res


def sample(table_expr):
    return table_expr.order_by(Random()).limit(10)


def create_temp_table(c: Compiler, path: TablePath, expr: Expr) -> str:
    db = c.database
    c: Compiler = attrs.evolve(c, root=False)  # we're compiling fragments, not full queries
    if isinstance(db, BigQuery):
        return f"create table {c.dialect.compile(c, path)} OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) as {c.dialect.compile(c, expr)}"
    elif isinstance(db, Presto):
        return f"create table {c.dialect.compile(c, path)} as {c.dialect.compile(c, expr)}"
    elif isinstance(db, Oracle):
        return f"create global temporary table {c.dialect.compile(c, path)} as {c.dialect.compile(c, expr)}"
    else:
        return f"create temporary table {c.dialect.compile(c, path)} as {c.dialect.compile(c, expr)}"


def bool_to_int(x):
    return if_(x, 1, 0)


def _outerjoin(db: Database, a: ITable, b: ITable, keys1: List[str], keys2: List[str], select_fields: dict) -> ITable:
    on = [a[k1] == b[k2] for k1, k2 in safezip(keys1, keys2)]

    is_exclusive_a = and_(b[k] == None for k in keys2)
    is_exclusive_b = and_(a[k] == None for k in keys1)

    if isinstance(db, MsSQL):
        # There is no "IS NULL" or "ISNULL()" as expressions, only as conditions.
        is_exclusive_a = when(is_exclusive_a).then(1).else_(0)
        is_exclusive_b = when(is_exclusive_b).then(1).else_(0)

    if isinstance(db, Oracle):
        is_exclusive_a = bool_to_int(is_exclusive_a)
        is_exclusive_b = bool_to_int(is_exclusive_b)

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


@attrs.define(frozen=True)
class JoinDiffer(TableDiffer):
    """Finds the diff between two SQL tables in the same database, using JOINs.

    The algorithm uses an OUTER JOIN (or equivalent) with extra checks and statistics.
    The two tables must reside in the same database, and their primary keys must be unique and not null.

    All parameters are optional.

    Parameters:
        threaded (bool): Enable/disable threaded diffing. Needed to take advantage of database threads.
        max_threadpool_size (int): Maximum size of each threadpool. ``None`` means auto.
                                   Only relevant when `threaded` is ``True``.
                                   There may be many pools, so number of actual threads can be a lot higher.
        validate_unique_key (bool): Enable/disable validating that the key columns are unique. (default: True)
                                    If there are no UNIQUE constraints in the schema, it is done in a single query,
                                    and can't be threaded, so it's very slow on non-cloud dbs.
        sample_exclusive_rows (bool): Enable/disable sampling of exclusive rows. (default: False)
                                      Creates a temporary table.
        materialize_to_table (DbPath, optional): Path of new table to write diff results to. Disabled if not provided.
        materialize_all_rows (bool): Materialize every row, not just those that are different. (default: False)
        table_write_limit (int): Maximum number of rows to write when materializing, per thread.
        skip_null_keys (bool): Skips diffing any rows with null PKs (displays a warning if any are null) (default: False)
    """

    validate_unique_key: bool = True
    sample_exclusive_rows: bool = False
    materialize_to_table: Optional[DbPath] = None
    materialize_all_rows: bool = False
    table_write_limit: int = TABLE_WRITE_LIMIT
    skip_null_keys: bool = False

    stats: dict = attrs.field(factory=dict)

    def _diff_tables_root(self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree) -> DiffResult:
        db = table1.database

        if table1.database is not table2.database:
            raise ValueError("Join-diff only works when both tables are in the same database")

        table1, table2 = self._threaded_call("with_schema", [table1, table2])

        bg_funcs = [partial(self._test_duplicate_keys, table1, table2)] if self.validate_unique_key else []
        if self.materialize_to_table:
            drop_table(db, self.materialize_to_table)

        with self._run_in_background(*bg_funcs):
            if isinstance(db, (Snowflake, BigQuery)):
                # Don't segment the table; let the database handling parallelization
                yield from self._diff_segments(None, table1, table2, info_tree, None)
            else:
                yield from self._bisect_and_diff_tables(table1, table2, info_tree)
            logger.info("Diffing complete")
            if self.materialize_to_table:
                logger.info("Materialized diff to table '%s'.", ".".join(self.materialize_to_table))

    def _diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
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
        diff_rows, a_cols, b_cols, is_diff_cols, all_rows = self._create_outer_join(table1, table2)

        with self._run_in_background(
            partial(self._collect_stats, 1, table1, info_tree),
            partial(self._collect_stats, 2, table2, info_tree),
            partial(self._test_null_keys, table1, table2),
            partial(self._sample_and_count_exclusive, db, diff_rows, a_cols, b_cols),
            partial(self._count_diff_per_column, db, diff_rows, list(a_cols), is_diff_cols),
            partial(
                self._materialize_diff,
                db,
                all_rows if self.materialize_all_rows else diff_rows,
                segment_index=segment_index,
            )
            if self.materialize_to_table
            else None,
        ):
            assert len(a_cols) == len(b_cols)
            logger.debug("Querying for different rows")
            diff = db.query(diff_rows, list)
            info_tree.info.set_diff(diff, schema=tuple(diff_rows.schema.items()))
            for is_xa, is_xb, *x in diff:
                if is_xa and is_xb:
                    # Can't both be exclusive, meaning a pk is NULL
                    # This can happen if the explicit null test didn't finish running yet
                    if self.skip_null_keys:
                        # warning is thrown in explicit null test
                        continue
                    else:
                        raise ValueError("NULL values in one or more primary keys")
                # _is_diff, a_row, b_row = _slice_tuple(x, len(is_diff_cols), len(a_cols), len(b_cols))
                _is_diff, ab_row = _slice_tuple(x, len(is_diff_cols), len(a_cols) + len(b_cols))
                a_row, b_row = ab_row[::2], ab_row[1::2]
                assert len(a_row) == len(b_row)
                if not is_xb:
                    yield "-", tuple(a_row)
                if not is_xa:
                    yield "+", tuple(b_row)

    def _test_duplicate_keys(self, table1: TableSegment, table2: TableSegment):
        logger.debug("Testing for duplicate keys")

        # Test duplicate keys
        for ts in [table1, table2]:
            unique = (
                ts.database.query_table_unique_columns(ts.table_path) if ts.database.SUPPORTS_UNIQUE_CONSTAINT else []
            )

            t = ts.make_select()
            key_columns = ts.key_columns

            unvalidated = list(set(key_columns) - set(unique))
            if unvalidated:
                logger.info(f"Validating that the are no duplicate keys in columns: {unvalidated}")
                # Validate that there are no duplicate keys
                self.stats["validated_unique_keys"] = self.stats.get("validated_unique_keys", []) + [unvalidated]
                q = t.select(total=Count(), total_distinct=Count(Concat(this[unvalidated]), distinct=True))
                total, total_distinct = ts.database.query(q, tuple)
                if total != total_distinct:
                    raise ValueError("Duplicate primary keys")

    def _test_null_keys(self, table1, table2):
        logger.debug("Testing for null keys")

        # Test null keys
        for ts in [table1, table2]:
            t = ts.make_select()
            key_columns = ts.key_columns

            q = t.select(*this[key_columns]).where(or_(this[k] == None for k in key_columns))
            nulls = ts.database.query(q, list)
            if nulls:
                if self.skip_null_keys:
                    logger.warning(
                        f"NULL values in one or more primary keys of {ts.table_path}. Skipping rows with NULL keys."
                    )
                else:
                    raise ValueError(f"NULL values in one or more primary keys of {ts.table_path}")

    def _collect_stats(self, i, table_seg: TableSegment, info_tree: InfoTree):
        logger.debug(f"Collecting stats for table #{i}")
        db = table_seg.database

        # Metrics
        col_exprs = merge_dicts(
            {
                # f"min_{c}": min_(this[c]),
                # f"max_{c}": max_(this[c]),
            }
            if c in table_seg.key_columns
            else {
                f"sum_{c}": sum_(this[c]),
                # f"avg_{c}": avg(this[c]),
                # f"min_{c}": min_(this[c]),
                # f"max_{c}": max_(this[c]),
            }
            for c in table_seg.relevant_columns
            if isinstance(table_seg._schema[c], NumericType)
        )
        col_exprs["count"] = Count()

        res = db.query(table_seg.make_select().select(**col_exprs), tuple)

        for col_name, value in safezip(col_exprs, res):
            if value is not None:
                value = json_friendly_value(value)
                stat_name = f"table{i}_{col_name}"

                if col_name == "count":
                    info_tree.info.rowcounts[i] = value

                if stat_name in self.stats:
                    self.stats[stat_name] += value
                else:
                    self.stats[stat_name] = value

        logger.debug("Done collecting stats for table #%s", i)

    def _create_outer_join(self, table1, table2):
        db = table1.database
        if db is not table2.database:
            raise ValueError("Joindiff only applies to tables within the same database")

        keys1 = table1.key_columns
        keys2 = table2.key_columns
        if len(keys1) != len(keys2):
            raise ValueError("The provided key columns are of a different count")

        cols1 = table1.relevant_columns
        cols2 = table2.relevant_columns
        if len(cols1) != len(cols2):
            raise ValueError("The provided columns are of a different count")

        a = table1.make_select()
        b = table2.make_select()

        is_diff_cols = {f"is_diff_{c1}": bool_to_int(a[c1].is_distinct_from(b[c2])) for c1, c2 in safezip(cols1, cols2)}

        a_cols = {f"{c}_a": NormalizeAsString(a[c]) for c in cols1}
        b_cols = {f"{c}_b": NormalizeAsString(b[c]) for c in cols2}
        # Order columns as col1_a, col1_b, col2_a, col2_b, etc.
        cols = {k: v for k, v in chain(*zip(a_cols.items(), b_cols.items()))}

        all_rows = _outerjoin(db, a, b, keys1, keys2, {**is_diff_cols, **cols})
        diff_rows = all_rows.where(or_(this[c] == 1 for c in is_diff_cols))
        return diff_rows, a_cols, b_cols, is_diff_cols, all_rows

    def _count_diff_per_column(self, db, diff_rows, cols, is_diff_cols):
        logger.debug("Counting differences per column")
        is_diff_cols_counts = db.query(diff_rows.select(sum_(this[c]) for c in is_diff_cols), tuple)
        diff_counts = {}
        for name, count in safezip(cols, is_diff_cols_counts):
            diff_counts[name] = diff_counts.get(name, 0) + (count or 0)
        self.stats["diff_counts"] = diff_counts

    def _sample_and_count_exclusive(self, db, diff_rows, a_cols, b_cols):
        if isinstance(db, (Oracle, MsSQL)):
            exclusive_rows_query = diff_rows.where((this.is_exclusive_a == 1) | (this.is_exclusive_b == 1))
        else:
            exclusive_rows_query = diff_rows.where(this.is_exclusive_a | this.is_exclusive_b)

        if not self.sample_exclusive_rows:
            logger.debug("Counting exclusive rows")
            self.stats["exclusive_count"] = db.query(exclusive_rows_query.count(), int)
            return

        logger.info("Counting and sampling exclusive rows")

        def exclusive_rows(expr):
            c = Compiler(db)
            name = c.new_unique_table_name("temp_table")
            exclusive_rows = table(name, schema=expr.source_table.schema)
            yield Code(create_temp_table(c, exclusive_rows, expr.limit(self.table_write_limit)))

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

        append_to_table(db, self.materialize_to_table, diff_rows.limit(self.table_write_limit))
