import time
from typing import List, Tuple
import logging

from runtype import dataclass

from sqeleton.utils import ArithString, split_space
from sqeleton.databases import Database, DbPath, DbKey, DbTime
from sqeleton.schema import Schema, create_schema
from sqeleton.queries import Count, Checksum, SKIP, table, this, Expr, min_, max_, Code
from sqeleton.queries.extras import ApplyFuncAndNormalizeAsString, NormalizeAsString

logger = logging.getLogger("table_segment")

RECOMMENDED_CHECKSUM_DURATION = 20


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
        min_key (:data:`DbKey`, optional): Lowest key value, used to restrict the segment
        max_key (:data:`DbKey`, optional): Highest key value, used to restrict the segment
        min_update (:data:`DbTime`, optional): Lowest update_column value, used to restrict the segment
        max_update (:data:`DbTime`, optional): Highest update_column value, used to restrict the segment
        where (str, optional): An additional 'where' expression to restrict the search space.

        case_sensitive (bool): If false, the case of column names will adjust according to the schema. Default is true.

    """

    # Location of table
    database: Database
    table_path: DbPath

    # Columns
    key_columns: Tuple[str, ...]
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

    def _where(self):
        return f"({self.where})" if self.where else None

    def _with_raw_schema(self, raw_schema: dict) -> "TableSegment":
        schema = self.database._process_table_schema(self.table_path, raw_schema, self.relevant_columns, self._where())
        return self.new(_schema=create_schema(self.database, self.table_path, schema, self.case_sensitive))

    def with_schema(self) -> "TableSegment":
        "Queries the table schema from the database, and returns a new instance of TableSegment, with a schema."
        if self._schema:
            return self

        return self._with_raw_schema(self.database.query_table_schema(self.table_path))

    def _make_key_range(self):
        if self.min_key is not None:
            assert len(self.key_columns) == 1
            (k,) = self.key_columns
            yield self.min_key <= this[k]
        if self.max_key is not None:
            assert len(self.key_columns) == 1
            (k,) = self.key_columns
            yield this[k] < self.max_key

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

        if self.max_key - self.min_key <= count:
            count = 1

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
        start = time.monotonic()
        q = self.make_select().select(Count(), Checksum(self._relevant_columns_repr))
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
        return count or 0, int(checksum) if count else None

    def query_key_range(self) -> Tuple[int, int]:
        """Query database for minimum and maximum key. This is used for setting the initial bounds."""
        # Normalizes the result (needed for UUIDs) after the min/max computation
        (k,) = self.key_columns
        select = self.make_select().select(
            ApplyFuncAndNormalizeAsString(this[k], min_),
            ApplyFuncAndNormalizeAsString(this[k], max_),
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
