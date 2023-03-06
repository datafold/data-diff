import math
import time
from typing import List, Tuple
import logging
from itertools import product

from runtype import dataclass

from .utils import safezip, Vector
from sqeleton.utils import ArithString, split_space
from sqeleton.databases import Database, DbPath, DbKey, DbTime
from sqeleton.schema import Schema, create_schema
from sqeleton.queries import Count, Checksum, SKIP, table, this, Expr, min_, max_, Code, Compiler
from sqeleton.queries.extras import ApplyFuncAndNormalizeAsString, NormalizeAsString

logger = logging.getLogger("table_segment")

RECOMMENDED_CHECKSUM_DURATION = 20


def split_key_space(min_key: DbKey, max_key: DbKey, count: int) -> List[DbKey]:
    assert min_key < max_key

    if max_key - min_key <= count:
        count = 1

    if isinstance(min_key, ArithString):
        assert type(min_key) is type(max_key)
        checkpoints = min_key.range(max_key, count)
    else:
        checkpoints = split_space(min_key, max_key, count)

    assert all(min_key < x < max_key for x in checkpoints)
    return [min_key] + checkpoints + [max_key]


def int_product(nums: List[int]) -> int:
    p = 1
    for n in nums:
        p *= n
    return p


def split_compound_key_space(mn: Vector, mx: Vector, count: int) -> List[List[DbKey]]:
    """Returns a list of split-points for each key dimension, essentially returning an N-dimensional grid of split points."""
    return [split_key_space(mn_k, mx_k, count) for mn_k, mx_k in safezip(mn, mx)]


def create_mesh_from_points(*values_per_dim: list) -> List[Tuple[Vector, Vector]]:
    """Given a list of values along each axis of N dimensional space,
    return an array of boxes whose start-points & end-points align with the given values,
    and together consitute a mesh filling that space entirely (within the bounds of the given values).

    Assumes given values are already ordered ascending.

    len(boxes) == ∏i( len(i)-1 )

    Example:
        ::
            >>> d1 = 'a', 'b', 'c'
            >>> d2 = 1, 2, 3
            >>> d3 = 'X', 'Y'
            >>> create_mesh_from_points(d1, d2, d3)
            [
                [('a', 1, 'X'), ('b', 2, 'Y')],
                [('a', 2, 'X'), ('b', 3, 'Y')],
                [('b', 1, 'X'), ('c', 2, 'Y')],
                [('b', 2, 'X'), ('c', 3, 'Y')]
            ]
    """
    assert all(len(v) >= 2 for v in values_per_dim), values_per_dim

    # Create tuples of (v1, v2) for each pair of adjacent values
    ranges = [list(zip(values[:-1], values[1:])) for values in values_per_dim]

    assert all(a <= b for r in ranges for a, b in r)

    # Create a product of all the ranges
    res = [tuple(Vector(a) for a in safezip(*r)) for r in product(*ranges)]

    expected_len = int_product(len(v) - 1 for v in values_per_dim)
    assert len(res) == expected_len, (len(res), expected_len)
    return res


@dataclass
class TableSegment:
    """Signifies a segment of rows (and selected columns) within a table

    Parameters:
        database (Database): Database instance. See :meth:`connect`
        table_path (:data:`DbPath`): Path to table in form of a tuple. e.g. `('my_dataset', 'table_name')`
        key_columns (Tuple[str]): Name of the key column, which uniquely identifies each row (usually id)
        update_column (str, optional): Name of updated column, which signals that rows changed.
                                       Usually updated_at or last_update. Used by `min_update` and `max_update`.
        extra_columns (Tuple[str, ...], optional): Extra columns to compare
        min_key (:data:`Vector`, optional): Lowest key value, used to restrict the segment
        max_key (:data:`Vector`, optional): Highest key value, used to restrict the segment
        min_update (:data:`DbTime`, optional): Lowest update_column value, used to restrict the segment
        max_update (:data:`DbTime`, optional): Highest update_column value, used to restrict the segment
        where (str, optional): An additional 'where' expression to restrict the search space.
        optimizer_hints (str, optional): Optimizer hints for SELECT queries

        case_sensitive (bool): If false, the case of column names will adjust according to the schema. Default is true.
        hash_query_type (str)

    """

    # Location of table
    database: Database
    table_path: DbPath

    # Columns
    key_columns: Tuple[str, ...]
    update_column: str = None
    extra_columns: Tuple[str, ...] = ()

    # Restrict the segment
    min_key: Vector = None
    max_key: Vector = None
    min_update: DbTime = None
    max_update: DbTime = None
    where: str = None
    optimizer_hints: str = None

    # group_by column
    group_by_column: str = None 
    hash_query_type: str = 'multi'

    # use to force cast certain columns to non-standard types 
    column_type_overrides: List[Tuple] = None

    case_sensitive: bool = True
    _schema: Schema = None

    def __post_init__(self):
        if not self.update_column and (self.min_update or self.max_update):
            raise ValueError("Error: the min_update/max_update feature requires 'update_column' to be set.")

        if self.min_key is not None and self.max_key is not None and self.min_key >= self.max_key:
            raise ValueError(f"Error: min_key expected to be smaller than max_key! ({self.min_key} >= {self.max_key})")

        if self.min_update is not None and self.max_update is not None and self.min_update >= self.max_update:
            raise ValueError(
                f"Error: min_update expected to be smaller than max_update! ({self.min_update} >= {self.max_update})"
            )

    def _where(self):
        return f"({self.where})" if self.where else None

    def _with_raw_schema(self, raw_schema: dict) -> "TableSegment":
        if self.column_type_overrides is not None:
            for col_info in self.column_type_overrides:
                col_name = col_info[0]
                if col_name not in raw_schema:
                    raise ValueError(f'Column {col_name} not found in schema for DB {self.database}')
                raw_schema[col_name] = col_info

        schema = self.database._process_table_schema(self.table_path, raw_schema, self.relevant_columns, self._where())

        return self.new(_schema=create_schema(self.database, self.table_path, schema, self.case_sensitive))

    def with_schema(self) -> "TableSegment":
        "Queries the table schema from the database, and returns a new instance of TableSegment, with a schema."
        if self._schema:
            return self

        return self._with_raw_schema(self.database.query_table_schema(self.table_path))

    def get_schema(self):
        return self.database.query_table_schema(self.table_path)

    def _make_key_range(self):
        if self.min_key is not None:
            for mn, k in safezip(self.min_key, self.key_columns):
                yield mn <= this[k]
        if self.max_key is not None:
            for k, mx in safezip(self.key_columns, self.max_key):
                yield this[k] < mx

    def _make_update_range(self):
        if self.min_update is not None:
            yield self.min_update <= this[self.update_column]
        if self.max_update is not None:
            yield this[self.update_column] < self.max_update

    @property
    def source_table(self):
        return table(*self.table_path, schema=self._schema)

    def make_select(self):
        return self.source_table.where(
            *self._make_key_range(), *self._make_update_range(), Code(self._where()) if self.where else SKIP
        )

    def get_values(self) -> list:
        "Download all the relevant values of the segment from the database"
        select = self.make_select().select(*self._relevant_columns_repr)
        return self.database.query(select, List[Tuple])

    def choose_checkpoints(self, count: int) -> List[DbKey]:
        "Suggests a bunch of evenly-spaced checkpoints to split by (not including start, end)"

        assert self.is_bounded

        # Take Nth root of count, to approximate the appropriate box size
        count = int(count ** (1 / len(self.key_columns))) or 1

        return split_compound_key_space(self.min_key, self.max_key, count)

    def segment_by_checkpoints(self, checkpoints: List[List[DbKey]]) -> List["TableSegment"]:
        "Split the current TableSegment to a bunch of smaller ones, separated by the given checkpoints"

        return [self.new_key_bounds(min_key=s, max_key=e) for s, e in create_mesh_from_points(*checkpoints)]

    def new(self, **kwargs) -> "TableSegment":
        """Creates a copy of the instance using 'replace()'"""
        return self.replace(**kwargs)

    def new_key_bounds(self, min_key: Vector, max_key: Vector) -> "TableSegment":
        if self.min_key is not None:
            assert self.min_key <= min_key, (self.min_key, min_key)
            assert self.min_key < max_key

        if self.max_key is not None:
            assert min_key < self.max_key
            assert max_key <= self.max_key

        return self.replace(min_key=min_key, max_key=max_key)

    @property
    def relevant_columns(self) -> List[str]:
        extras = list(self.extra_columns)

        if self.update_column and self.update_column not in extras:
            extras = [self.update_column] + extras

        return list(self.key_columns) + extras

    @property
    def _relevant_columns_repr(self) -> List[Expr]:
        return [NormalizeAsString(this[c]) for c in self.relevant_columns]

    def count(self) -> int:
        """Count how many rows are in the segment, in one pass."""
        return self.database.query(self.make_select().select(Count()), int)

    def count_and_checksum(self) -> Tuple[int, int]:
        """Count and checksum the rows in the segment, in one pass."""
        logging.info('count_and_checksum')
        time.sleep(1)

        start = time.monotonic()
        q = self.make_select().select(
            Count(), Checksum(self._relevant_columns_repr), optimizer_hints=self.optimizer_hints
        )
        count, checksum = self.database.query(q, tuple)
        duration = time.monotonic() - start
        if duration > RECOMMENDED_CHECKSUM_DURATION:
            logger.warning(
                "Checksum is taking longer than expected (%.2f). "
                "We recommend increasing --bisection-factor or decreasing --threads.",
                duration,
            )

        if count:
            assert checksum, (count, checksum)
        # return count or 0, int(checksum) if count else None
        return f'{self.min_key} - {self.max_key}', count or 0, int(checksum) if count else None

    def count_and_checksum_by_group(self, checkpoints: List, bisection_factor: int, optimizer_hints=False) -> Tuple[Tuple[int, int]]:
        """Count and checksum each group"""

        # TODO: REMOVE
        logging.info('count_and_checksum_by_group')
        time.sleep(4)
        """
        If group_by col is PK:
            if PK is num: use GROUP BY FLOOR(div_factor)
            if PK is alphanum: figure out way to group_by.

                look at 'checkpoints' (which should be passed in, maybe instead of 'groups' count)
                somehow determine how many chars to GROUP BY with 'substring(n)'
                    - hmmmmmm... We prob don't have much control over how many groups we end up with
                    - eg. If we group by "substring(0, 1)" there could be up to N groups (N == number of allowed chars) 
                    - eg. If we group by "substring(0, 2)" there could be up to N*N groups

                Another option is to use the CASE, WHEN version of the group by query, with all the ranges explicitly listed in the query. 

            # TODO: if self.group_by_column != self.key_columns[0]:
        """
        # logging.info('--------')
        # logging.info('count_and_checksum_by_group')
        # logging.info(f'max_rows: {max_rows}')
        # logging.info(f'groups: {len(checkpoints)}')
        # logging.info(f'max_key: {self.max_key}')
        # logging.info(f'min_key: {self.min_key}')
        # logging.info('--------')

        # unpack min keys to value since this algo ignores composite keys until the end
        min_key = self.min_key[0]
        max_key = self.max_key[0]

        # NOTE: using key range instead of 'max_rows' (actual row count) for consistency with multi-query approach
        #       when using a single PK column of a composite PK.
        key_range = max_key - min_key

        # follows same logic as split_space in sqeleton
        div_factor = math.floor((key_range+1) / (bisection_factor))

        group_by_col = self.key_columns[0]
        group_by_expr = f'FLOOR(({group_by_col} - {min_key})/{div_factor})'

        maybe_optimizer_hints = \
            {'optimizer_hints': self.optimizer_hints} if optimizer_hints else {}

        q = (self.make_select()
            .select(
                Code(group_by_expr),
                Count(), 
                Checksum(self._relevant_columns_repr),
                **maybe_optimizer_hints)
            .order_by(Code(group_by_expr))
            .group_by(self.source_table[group_by_col])
            .agg(self.source_table[group_by_col])
            .table
            .replace(group_by_exprs=[Code(group_by_expr)]))

        start = time.monotonic()
        rows = self.database.query(q, list)
        duration = time.monotonic() - start

        # insert missing groups
        for i, exp_grp in enumerate(checkpoints):
            # empty_result =  [0, None] # TODO: Cleanup. [exp_grp, 0, None]
            empty_result =  [exp_grp, 0, None] # TODO: Cleanup. [exp_grp, 0, None]
            if i >= len(rows):
                # no more returned groups, insert next expected one
                rows.insert(i, empty_result)
                continue

            grp_val = rows[i][0] * div_factor + min_key
            assert grp_val in checkpoints, (grp_val, checkpoints)

            if grp_val > exp_grp:
                # group was skipped, insert it
                rows.insert(i, empty_result)
                continue

            # group exists, just map its value
            row_ls = list(rows[i])

            ## OPTION 1 (debug) - includes group label
            row_ls[0] = grp_val # TODO: Cleanup.
            rows[i] = row_ls
            
            # OPTION 2: remove first item (group label)
            # rows[i] = row_ls[1:]

        return rows

    def query_key_range(self) -> Tuple[tuple, tuple]:
        """Query database for minimum and maximum key. This is used for setting the initial bounds."""
        # Normalizes the result (needed for UUIDs) after the min/max computation
        select = self.make_select().select(
            (ApplyFuncAndNormalizeAsString(this[k], f) for k in self.key_columns for f in (min_, max_)),
            optimizer_hints=self.optimizer_hints
        )
        result = tuple(self.database.query(select, tuple))

        if any(i is None for i in result):
            raise ValueError("Table appears to be empty")

        # Min/max keys are interleaved
        min_key, max_key = result[::2], result[1::2]
        assert len(min_key) == len(max_key)

        return min_key, max_key

    @property
    def is_bounded(self):
        return self.min_key is not None and self.max_key is not None

    def approximate_size(self):
        if not self.is_bounded:
            raise RuntimeError("Cannot approximate the size of an unbounded segment. Must have min_key and max_key.")
        diff = self.max_key - self.min_key
        assert all(d > 0 for d in diff)
        return int_product(diff)
