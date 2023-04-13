from dataclasses import field
import math
import time
from typing import Any, List, Optional, Tuple, Union
import logging
from itertools import product

from runtype import dataclass

from .utils import safezip, Vector, split_space
from sqeleton.utils import ArithString
from sqeleton.databases import Database, DbPath, DbKey, DbTime
from sqeleton.schema import Schema, create_schema
from sqeleton.queries import Count, Checksum, SKIP, table, this, Expr, min_, max_, Code, Compiler
from sqeleton.queries.extras import ApplyFuncAndNormalizeAsString, NormalizeAsString
from sqeleton.abcs import database_types as DB_TYPES


LOG_FORMAT = "[%(db)s] %(message)s"
DATE_FORMAT = "%H:%M:%S"
FORMATTER = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

base_logger = logging.getLogger("table_segment")
base_logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(FORMATTER)
base_logger.addHandler(handler)
base_logger.propagate = False


RECOMMENDED_CHECKSUM_DURATION = 20

COL_TYPE_OVERRIDE_MAP = {
    'String_VaryingAlphanum': DB_TYPES.String_VaryingAlphanum,
    'String_FixedAlphanum': DB_TYPES.String_FixedAlphanum,
    'Integer': DB_TYPES.Integer,
    'Decimal': DB_TYPES.Decimal,
}


def get_database_type(type_info: Union[str, tuple]) -> Any:
    type_str = type_info
    if type(type_info) == tuple:
        type_str = type_info[0]

    cls = COL_TYPE_OVERRIDE_MAP[type_str]
    
    if type_str == 'Decimal':
        return cls(type_info[1])

    return cls()


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

        true_key_columns (list[str]): Hack for GroupingHashDiffer to be able to properly sort diff results 
                                of tables with composite keys. Temporary until we properly support group_by_col.
                                If ``None``, key_column is used (which is equivalent to [0]).
                                ex. [0, 2, 3]  --> 3-col composite PK.
                                0 - first one should always be 0 (corresponds to the key_column)
                                2 - The 2nd column in the 'extras' list (TableSegment.relevant_columns)
                                        Might want to skip 1 since it will be the update_column if it exists
                                3 - 3rd column in the extras list 

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
    true_key_indices: list = None
    group_min: Any = None
    group_max: Any = None
    group_grains: list = ['month', 'day', 'minute', 'second']
    true_key_columns: list = None

    # use to force cast certain columns to non-standard types 
    column_type_overrides: dict[str, Tuple] = field(default_factory=dict)

    # use to cast/convert certain columns to non-standard types 
    col_conversions: dict[str, dict] = field(default_factory=dict)

    case_sensitive: bool = True
    _schema: Schema = None

    logger: logging.LoggerAdapter = None

    def __post_init__(self):
        if not self.update_column and (self.min_update or self.max_update):
            raise ValueError("Error: the min_update/max_update feature requires 'update_column' to be set.")

        if self.min_key is not None and self.max_key is not None and self.min_key >= self.max_key:
            raise ValueError(f"Error: min_key expected to be smaller than max_key! ({self.min_key} >= {self.max_key})")

        if self.min_update is not None and self.max_update is not None and self.min_update >= self.max_update:
            raise ValueError(
                f"Error: min_update expected to be smaller than max_update! ({self.min_update} >= {self.max_update})"
            )

        super().__setattr__('logger', logging.LoggerAdapter(base_logger, {'db':self.database.name}))

    def _where(self):
        return f"({self.where})" if self.where else None

    def _with_raw_schema(self, raw_schema: dict) -> "TableSegment":
        schema = self.database._process_table_schema(self.table_path, raw_schema, self.relevant_columns, self._where())
        self.logger.info(f'schema: {schema}')

        if self.column_type_overrides is not None:
            for col, col_info in self.column_type_overrides.items():
                if all(c not in raw_schema for c in [col.lower(), col.upper()]):
                    raise ValueError(f'Column {col} not found in schema for DB {self.database}')
                schema[col] = get_database_type(col_info)

        return self.new(_schema=create_schema(self.database, self.table_path, schema, self.case_sensitive))

    def with_schema(self) -> "TableSegment":
        "Queries the table schema from the database, and returns a new instance of TableSegment, with a schema."
        if self._schema:
            return self

        return self._with_raw_schema(self.database.query_table_schema(self.table_path))

    def get_schema(self):
        return self.database.query_table_schema(self.table_path)
    
    def set_query_timeout(self, timeout: int) -> None:
        self.database.set_query_timeout(timeout)

    def _make_key_range(self):
        col_usage = 'where_key_range'
        if self.min_key is not None:
            for mn, k in safezip(self.min_key, self.key_columns):
                converted_col, exclude_from = self.col_conversion(k)
                if converted_col and col_usage not in exclude_from:
                    yield Code(f"{converted_col} >= '{mn}'")
                else:
                    yield mn <= this[k]
        if self.max_key is not None:
            for k, mx in safezip(self.key_columns, self.max_key):
                converted_col, exclude_from = self.col_conversion(k)
                if converted_col and col_usage not in exclude_from:
                    yield Code(f"{converted_col} < '{mx}'")
                else:
                    yield this[k] < mx

    def _make_update_range(self, include_min: bool = True, include_max: bool = True):
        # TODO: add support for column conversion of update_column range
        if include_min and self.min_update is not None:
            yield self.min_update <= this[self.update_column]
        if include_max and self.max_update is not None:
            yield this[self.update_column] < self.max_update

    def _make_groupby_range(self, include_min: bool = True, include_max: bool = True):
        # TODO: add support for column conversion of group range
        if include_min and self.group_min is not None:
            yield self.group_min <= this[self.group_by_column]
        if include_max and self.group_max is not None:
            yield this[self.group_by_column] < self.group_max

    @property
    def source_table(self):
        return table(*self.table_path, schema=self._schema)

    def make_select(self, use_min_update = True, use_max_update = True,
                    use_group_min = True, use_group_max = True):
        return self.source_table.where(
            *self._make_key_range(), 
            *self._make_update_range(include_min=use_min_update, include_max=use_max_update), 
            *self._make_groupby_range(include_min=use_group_min, include_max=use_group_max), 
            Code(self._where()) if self.where else SKIP
        )

    def get_values(self) -> list:
        "Download all the relevant values of the segment from the database"
        select = self.make_select(use_max_update=False).select(
            *self._relevant_columns_repr('select_values'))
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
            assert max_key <= self.max_key, (max_key, self.max_key)

        return self.replace(min_key=min_key, max_key=max_key)

    @property
    def relevant_columns(self) -> List[str]:
        extras = list(self.extra_columns)
        key_cols = self.true_key_columns or self.key_columns

        if self.update_column and self.update_column not in extras \
            and self.update_column not in list(self.key_columns):
            extras = [self.update_column] + extras

        if self.group_by_column and self.group_by_column not in extras \
            and self.group_by_column not in list(self.key_columns):
            extras = [self.group_by_column] + extras

        return list(key_cols) + extras
    
    @property
    def update_col_idx(self) -> int:
        if not self.update_column:
            raise ValueError(f'No update_column specified for table {self.table_path}')
        # key columns are first, so next index is the update_column
        return len(self.key_columns)
    
    def col_conversion(self, c: str) -> tuple[Optional[str], Optional[list]]:
        c_type = self._schema[c].__class__.__name__

        # get conversion info for column
        conversion_info = self.col_conversions.get(c.lower(), 
                                                   self.col_conversions.get(c.upper()))

        if conversion_info:
            allowed_types = conversion_info.get('is_type', None)
            if allowed_types and c_type not in allowed_types:
                return None, None

            placeholders = conversion_info['template'].count('{}')
            return conversion_info['template'].format(*([c]*placeholders)), conversion_info.get('exclude_from', [])
        else:
            return None, None
        
    def _relevant_columns_repr(self, usage: str) -> List[Expr]:
        normalized_cols = []
        for c in self.relevant_columns:
            converted_col, exclude_from = self.col_conversion(c)
            if converted_col and usage not in exclude_from:
                normalized_cols.append(Code(converted_col))
            else:
                normalized_cols.append(NormalizeAsString(this[c]))

        return normalized_cols
    
    @property
    def key_indices(self) -> Tuple[str]:
        key_cols = self.true_key_columns or self.key_columns
        return [self.relevant_columns.index(c) for c in key_cols]

    def count(self) -> int:
        """Count how many rows are in the segment, in one pass."""
        return self.database.query(self.make_select().select(Count()), int)

    def count_and_checksum(self) -> Tuple[int, int]:
        """Count and checksum the rows in the segment, in one pass."""

        start = time.monotonic()
        q = self.make_select().select(
            Count(), Checksum(self._relevant_columns_repr('select_checksum')), optimizer_hints=self.optimizer_hints
        )
        count, checksum = self.database.query(q, tuple)
        duration = time.monotonic() - start
        if duration > RECOMMENDED_CHECKSUM_DURATION:
            self.logger.warning(
                "Checksum is taking longer than expected (%.2f). "
                "We recommend increasing --bisection-factor or decreasing --threads.",
                duration,
            )
        else:
            self.logger.info('Checksum took %.2f seconds', duration)

        if count:
            assert checksum, (count, checksum)
        # return count or 0, int(checksum) if count else None
        return self.min_key, count or 0, int(checksum) if count else None

    def count_and_checksum_by_group(self, checkpoints: List, bisection_factor: int, level=int) -> Tuple[Tuple[int, int]]:
        """Count and checksum each group"""

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
            {'optimizer_hints': self.optimizer_hints} if level == 0 else {}

        q = (self.make_select()
            .select(
                Code(group_by_expr),
                Count(), 
                Checksum(self._relevant_columns_repr('select_checksum')),
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
    
    def count_and_checksum_by_ts_group(self, level: int) -> Tuple[Tuple[int, int]]:
        GRAINS = {
            'year': {
                'ora': "TRUNC({group_by_col}, 'Y')"
            },
            'month': {
                'ora': "TRUNC({group_by_col}, 'MM')"
            },
            'day': {
                'ora': "TRUNC({group_by_col}, 'DD')"
            },
            'hour': {
                'ora': "TRUNC({group_by_col}, 'HH')"
            },
            'minute': {
                'ora': "TRUNC({group_by_col}, 'MI')"
            },
            'second': {
                'ora': "cast({group_by_col} as date)"
            }
            # 'half_second': {
            #     'ora': "TRUNC({group_by_col}, )"
            # }
            # 'tenth_second': {
            #     'ora': "TRUNC({group_by_col}, )"
            # }
            # 'millisecond':  {
            #     'ora': "TRUNC({group_by_col}, )"
            # }
        }


        group_by_col = self.group_by_column
        curr_grain = self.group_grains[level]

        # temp hack (should utilize sqeleton)
        if self.database.name in ['Redshift', 'PostgreSQL']:
            group_by_expr = f"DATE_TRUNC('{curr_grain}', {group_by_col})"
        elif self.database.name == 'Oracle':
            group_by_expr = GRAINS[curr_grain]['ora'].format(group_by_col=group_by_col)
        else:
            raise NotImplementedError(f'Unsupported database for checksum by TS group: {self.database.name}')

        maybe_optimizer_hints = \
            {'optimizer_hints': self.optimizer_hints} if level == 0 else {}

        q = (self.make_select(use_max_update=True)
            .select(
                Code(group_by_expr),
                Count(), 
                Checksum(self._relevant_columns_repr('select_checksum')),
                **maybe_optimizer_hints)
            .order_by(Code(group_by_expr))
            .group_by(self.source_table[group_by_col])
            .agg(self.source_table[group_by_col])
            .table
            .replace(group_by_exprs=[Code(group_by_expr)]))        

        start = time.monotonic()
        rows = self.database.query(q, list)
        duration = time.monotonic() - start

        self.logger.info('Checksum_by_ts_group query took %s seconds', duration)

        return rows

    def query_key_range(self) -> Tuple[tuple, tuple]:
        """Query database for minimum and maximum key. This is used for setting the initial bounds."""
        # Normalizes the result (needed for UUIDs) after the min/max computation
        self.logger.info(f'query_key_range: {self.min_key} - {self.max_key}')

        def normalize_range_select():
            for k in self.key_columns:
                converted_col, exclude_from = self.col_conversion(k)
                for f in (min_, max_):
                    if converted_col and 'key_range_select' not in exclude_from:
                        yield ApplyFuncAndNormalizeAsString(Code(converted_col), f)
                    else:
                        yield ApplyFuncAndNormalizeAsString(this[k], f)

        select = self.make_select().select(
            normalize_range_select(),
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
