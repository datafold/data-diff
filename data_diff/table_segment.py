import time
from typing import List, Tuple
import logging

from runtype import dataclass

from .utils import ArithString, split_space, ArithAlphanumeric

from .databases.base import Database
from .databases.database_types import DbPath, DbKey, DbTime, Native_UUID, Schema, create_schema
from .sql import Select, Checksum, Compare, Count, TableName, Time, Value

logger = logging.getLogger("table_segment")

RECOMMENDED_CHECKSUM_DURATION = 10


@dataclass
class TableSegment:
    """Signifies a segment of rows (and selected columns) within a table

    Parameters:
        database (Database): Database instance. See :meth:`connect`
        table_path (:data:`DbPath`): Path to table in form of a tuple. e.g. `('my_dataset', 'table_name')`
        key_column (str): Name of the key column, which uniquely identifies each row (usually id)
        update_column (str, optional): Name of updated column, which signals that rows changed (usually updated_at or last_update)
        extra_columns (Tuple[str, ...], optional): Extra columns to compare
        min_key (:data:`DbKey`, optional): Lowest key_column value, used to restrict the segment
        max_key (:data:`DbKey`, optional): Highest key_column value, used to restrict the segment
        min_update (:data:`DbTime`, optional): Lowest update_column value, used to restrict the segment
        max_update (:data:`DbTime`, optional): Highest update_column value, used to restrict the segment
        where (str, optional): An additional 'where' expression to restrict the search space.

        case_sensitive (bool): If false, the case of column names will adjust according to the schema. Default is true.

    """

    # Location of table
    database: Database
    table_path: DbPath

    # Columns
    key_column: str
    update_column: str = None
    extra_columns: Tuple[str, ...] = ()

    # Restrict the segment
    min_key: DbKey = None
    max_key: DbKey = None
    min_update: DbTime = None
    max_update: DbTime = None

    where: str = None
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

    @property
    def _update_column(self):
        return self._quote_column(self.update_column)

    def _quote_column(self, c: str) -> str:
        if self._schema:
            c = self._schema.get_key(c)  # Get the actual name. Might be case-insensitive.
        return self.database.quote(c)

    def _normalize_column(self, name: str, template: str = None) -> str:
        if not self._schema:
            raise RuntimeError(
                "Cannot compile query when the schema is unknown. Please use TableSegment.with_schema()."
            )

        col_type = self._schema[name]
        col = self._quote_column(name)

        if isinstance(col_type, Native_UUID):
            # Normalize first, apply template after (for uuids)
            # Needed because min/max(uuid) fails in postgresql
            col = self.database.normalize_value_by_type(col, col_type)
            if template is not None:
                col = template % col  # Apply template using Python's string formatting
            return col

        # Apply template before normalizing (for ints)
        if template is not None:
            col = template % col  # Apply template using Python's string formatting

        return self.database.normalize_value_by_type(col, col_type)

    def _with_raw_schema(self, raw_schema: dict) -> "TableSegment":
        schema = self.database._process_table_schema(self.table_path, raw_schema, self._relevant_columns, self.where)
        return self.new(_schema=create_schema(self.database, self.table_path, schema, self.case_sensitive))

    def with_schema(self) -> "TableSegment":
        "Queries the table schema from the database, and returns a new instance of TableSegment, with a schema."
        if self._schema:
            return self

        return self._with_raw_schema(self.database.query_table_schema(self.table_path))

    def _make_key_range(self):
        if self.min_key is not None:
            yield Compare("<=", Value(self.min_key), self._quote_column(self.key_column))
        if self.max_key is not None:
            yield Compare("<", self._quote_column(self.key_column), Value(self.max_key))

    def _make_update_range(self):
        if self.min_update is not None:
            yield Compare("<=", Time(self.min_update), self._update_column)
        if self.max_update is not None:
            yield Compare("<", self._update_column, Time(self.max_update))

    def _make_select(self, *, table=None, columns=None, where=None, group_by=None, order_by=None):
        if columns is None:
            columns = [self._normalize_column(self.key_column)]
        where = [
            *self._make_key_range(),
            *self._make_update_range(),
            *([] if where is None else [where]),
            *([] if self.where is None else [self.where]),
        ]
        order_by = None if order_by is None else [order_by]
        return Select(
            table=table or TableName(self.table_path),
            where=where,
            columns=columns,
            group_by=group_by,
            order_by=order_by,
        )

    def get_values(self) -> list:
        "Download all the relevant values of the segment from the database"
        select = self._make_select(columns=self._relevant_columns_repr)
        return self.database.query(select, List[Tuple])

    def choose_checkpoints(self, count: int) -> List[DbKey]:
        "Suggests a bunch of evenly-spaced checkpoints to split by (not including start, end)"
        assert self.is_bounded
        if isinstance(self.min_key, ArithString):
            assert type(self.min_key) is type(self.max_key)
            checkpoints = self.min_key.range(self.max_key, count)
            assert all(self.min_key <= x <= self.max_key for x in checkpoints)
            return checkpoints

        return split_space(self.min_key, self.max_key, count)

    def segment_by_checkpoints(self, checkpoints: List[DbKey]) -> List["TableSegment"]:
        "Split the current TableSegment to a bunch of smaller ones, separated by the given checkpoints"

        if self.min_key and self.max_key:
            assert all(self.min_key <= c < self.max_key for c in checkpoints)
        checkpoints.sort()

        # Calculate sub-segments
        positions = [self.min_key] + checkpoints + [self.max_key]
        ranges = list(zip(positions[:-1], positions[1:]))

        # Create table segments
        tables = [self.new(min_key=s, max_key=e) for s, e in ranges]

        return tables

    def new(self, **kwargs) -> "TableSegment":
        """Using new() creates a copy of the instance using 'replace()'"""
        return self.replace(**kwargs)

    @property
    def _relevant_columns(self) -> List[str]:
        extras = list(self.extra_columns)

        if self.update_column and self.update_column not in extras:
            extras = [self.update_column] + extras

        return [self.key_column] + extras

    @property
    def _relevant_columns_repr(self) -> List[str]:
        return [self._normalize_column(c) for c in self._relevant_columns]

    def count(self) -> Tuple[int, int]:
        """Count how many rows are in the segment, in one pass."""
        return self.database.query(self._make_select(columns=[Count()]), int)

    def count_and_checksum(self) -> Tuple[int, int]:
        """Count and checksum the rows in the segment, in one pass."""
        start = time.monotonic()
        count, checksum = self.database.query(
            self._make_select(columns=[Count(), Checksum(self._relevant_columns_repr)]), tuple
        )
        duration = time.monotonic() - start
        if duration > RECOMMENDED_CHECKSUM_DURATION:
            logger.warning(
                f"Checksum is taking longer than expected ({duration:.2f}s). "
                "We recommend increasing --bisection-factor or decreasing --threads."
            )

        if count:
            assert checksum, (count, checksum)
        return count or 0, int(checksum) if count else None

    def query_key_range(self) -> Tuple[int, int]:
        """Query database for minimum and maximum key. This is used for setting the initial bounds."""
        # Normalizes the result (needed for UUIDs) after the min/max computation
        select = self._make_select(
            columns=[
                self._normalize_column(self.key_column, "min(%s)"),
                self._normalize_column(self.key_column, "max(%s)"),
            ]
        )
        min_key, max_key = self.database.query(select, tuple)

        if min_key is None or max_key is None:
            raise ValueError("Table appears to be empty")

        return min_key, max_key

    @property
    def is_bounded(self):
        return self.min_key is not None and self.max_key is not None

    def approximate_size(self):
        if not self.is_bounded:
            raise RuntimeError("Cannot approximate the size of an unbounded segment. Must have min_key and max_key.")
        return self.max_key - self.min_key
