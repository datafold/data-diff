"""Provides classes for performing a table diff
"""

from abc import ABC, abstractmethod
import time
from operator import attrgetter, methodcaller
from collections import defaultdict
from typing import List, Tuple, Iterator, Optional
import logging
from concurrent.futures import ThreadPoolExecutor

from runtype import dataclass

from .sql import Select, Checksum, Compare, DbPath, DbKey, DbTime, Count, TableName, Time, Min, Max
from .database import Database, PrecisionType, ColType

logger = logging.getLogger("diff_tables")

RECOMMENDED_CHECKSUM_DURATION = 10

DEFAULT_BISECTION_THRESHOLD = 1024 * 16
DEFAULT_BISECTION_FACTOR = 32


def safezip(*args):
    "zip but makes sure all sequences are the same length"
    assert len(set(map(len, args))) == 1
    return zip(*args)


def split_space(start, end, count):
    size = end - start
    return list(range(start, end, (size + 1) // (count + 1)))[1 : count + 1]


def parse_table_name(t):
    return tuple(t.split("."))


class Schema(ABC):
    @abstractmethod
    def get_key(self, key: str) -> str:
        ...

    @abstractmethod
    def __getitem__(self, key: str) -> str:
        ...

    @abstractmethod
    def __setitem__(self, key: str, value):
        ...

    @abstractmethod
    def __contains__(self, key: str) -> bool:
        ...


class Schema_CaseSensitive(dict, Schema):
    def get_key(self, key):
        return key


class Schema_CaseInsensitive(Schema):
    def __init__(self, initial):
        self._dict = {k.lower(): (k, v) for k, v in dict(initial).items()}

    def get_key(self, key: str) -> str:
        return self._dict[key.lower()][0]

    def __getitem__(self, key: str) -> str:
        return self._dict[key.lower()][1]

    def __setitem__(self, key: str, value):
        k = key.lower()
        if k in self._dict:
            key = self._dict[k][0]
        self._dict[k] = key, value

    def __contains__(self, key):
        return key.lower() in self._dict


@dataclass(frozen=False)
class TableSegment:
    """Signifies a segment of rows (and selected columns) within a table

    Parameters:
        database (Database): Database instance. See :meth:`connect_to_uri`
        table_path (:data:`DbPath`): Path to table in form of a tuple. e.g. `('my_dataset', 'table_name')`
        key_column (str): Name of the key column, which uniquely identifies each row (usually id)
        update_column (str, optional): Name of updated column, which signals that rows changed (usually updated_at or last_update)
        extra_columns (Tuple[str, ...], optional): Extra columns to compare
        min_key (:data:`DbKey`, optional): Lowest key_column value, used to restrict the segment
        max_key (:data:`DbKey`, optional): Highest key_column value, used to restrict the segment
        min_update (:data:`DbTime`, optional): Lowest update_column value, used to restrict the segment
        max_update (:data:`DbTime`, optional): Highest update_column value, used to restrict the segment

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

    case_sensitive: bool = True
    _schema: Schema = None

    def __post_init__(self):
        if not self.update_column and (self.min_update or self.max_update):
            raise ValueError("Error: min_update/max_update feature requires to specify 'update_column'")

        if self.min_key is not None and self.max_key is not None and self.min_key >= self.max_key:
            raise ValueError("Error: min_key expected to be smaller than max_key!")

        if self.min_update is not None and self.max_update is not None and self.min_update >= self.max_update:
            raise ValueError("Error: min_update expected to be smaller than max_update!")

    @property
    def _key_column(self):
        return self._quote_column(self.key_column)

    @property
    def _update_column(self):
        return self._quote_column(self.update_column)

    def _quote_column(self, c):
        if self._schema:
            c = self._schema.get_key(c)
        return self.database.quote(c)

    def with_schema(self) -> "TableSegment":
        "Queries the table schema from the database, and returns a new instance of TableSegmentWithSchema."
        if self._schema:
            return self
        schema = self.database.query_table_schema(self.table_path)
        if self.case_sensitive:
            schema = Schema_CaseSensitive(schema)
        else:
            if len({k.lower() for k in schema}) < len(schema):
                logger.warn(
                    f'Ambiguous schema for {self.database}:{".".join(self.table_path)} | Columns = {", ".join(list(schema))}'
                )
                logger.warn("We recommend to disable case-insensitivity (remove --any-case).")
            schema = Schema_CaseInsensitive(schema)
        return self.new(_schema=schema)

    def _make_key_range(self):
        if self.min_key is not None:
            yield Compare("<=", str(self.min_key), self._key_column)
        if self.max_key is not None:
            yield Compare("<", self._key_column, str(self.max_key))

    def _make_update_range(self):
        if self.min_update is not None:
            yield Compare("<=", Time(self.min_update), self._update_column)
        if self.max_update is not None:
            yield Compare("<", self._update_column, Time(self.max_update))

    def _make_select(self, *, table=None, columns=None, where=None, group_by=None, order_by=None):
        if columns is None:
            columns = [self._key_column]
        where = list(self._make_key_range()) + list(self._make_update_range()) + ([] if where is None else [where])
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
        if not self._schema:
            raise RuntimeError(
                "Cannot compile query when the schema is unknown. Please use TableSegment.with_schema()."
            )
        return [
            self.database.normalize_value_by_type(self._quote_column(c), self._schema[c])
            for c in self._relevant_columns
        ]

    def count(self) -> Tuple[int, int]:
        """Count how many rows are in the segment, in one pass."""
        return self.database.query(self._make_select(columns=[Count()]), int)

    def count_and_checksum(self) -> Tuple[int, int]:
        """Count and checksum the rows in the segment, in one pass."""
        start = time.time()
        count, checksum = self.database.query(
            self._make_select(columns=[Count(), Checksum(self._relevant_columns_repr)]), tuple
        )
        duration = time.time() - start
        if duration > RECOMMENDED_CHECKSUM_DURATION:
            logger.warn(
                f"Checksum is taking longer than expected ({duration:.2f}s). "
                "We recommend increasing --bisection-factor or decreasing --threads."
            )

        return count or 0, checksum if checksum is None else int(checksum)

    def query_key_range(self) -> Tuple[int, int]:
        """Query database for minimum and maximum key. This is used for setting the initial bounds."""
        select = self._make_select(columns=[Min(self._key_column), Max(self._key_column)])
        min_key, max_key = self.database.query(select, tuple)

        if min_key is None or max_key is None:
            raise ValueError("Table appears to be empty")

        return min_key, max_key

    @property
    def is_bounded(self):
        return self.min_key is not None and self.max_key is not None


def diff_sets(a: set, b: set) -> Iterator:
    s1 = set(a)
    s2 = set(b)
    d = defaultdict(list)

    # The first item is always the key (see TableDiffer._relevant_columns)
    for i in s1 - s2:
        d[i[0]].append(("-", i))
    for i in s2 - s1:
        d[i[0]].append(("+", i))

    for k, v in sorted(d.items(), key=lambda i: i[0]):
        yield from v


DiffResult = Iterator[Tuple[str, tuple]]  # Iterator[Tuple[Literal["+", "-"], tuple]]


@dataclass
class TableDiffer:
    """Finds the diff between two SQL tables

    The algorithm uses hashing to quickly check if the tables are different, and then applies a
    bisection search recursively to find the differences efficiently.

    Works best for comparing tables that are mostly the name, with minor discrepencies.

    Parameters:
        bisection_factor (int): Into how many segments to bisect per iteration.
        bisection_threshold (int): When should we stop bisecting and compare locally (in row count).
        threaded (bool): Enable/disable threaded diffing. Needed to take advantage of database threads.
        max_threadpool_size (int): Maximum size of each threadpool. ``None`` means auto. Only relevant when `threaded` is ``True``.
                                   There may be many pools, so number of actual threads can be a lot higher.
    """

    bisection_factor: int = DEFAULT_BISECTION_FACTOR
    bisection_threshold: int = DEFAULT_BISECTION_THRESHOLD
    threaded: bool = True
    max_threadpool_size: Optional[int] = 1

    # Enable/disable debug prints
    debug: bool = False

    stats: dict = {}

    def diff_tables(self, table1: TableSegment, table2: TableSegment) -> DiffResult:
        """Diff the given tables.

        Parameters:
            table1 (TableSegment): The "before" table to compare. Or: source table
            table2 (TableSegment): The "after" table to compare. Or: target table

        Returns:
            An iterator that yield pair-tuples, representing the diff. Items can be either
            ('+', columns) for items in table1 but not in table2
            ('-', columns) for items in table2 but not in table1
            Where `columns` is a tuple of values for the involved columns, i.e. (id, ...extra)
        """
        if self.bisection_factor >= self.bisection_threshold:
            raise ValueError("Incorrect param values (bisection factor must be lower than threshold)")
        if self.bisection_factor < 2:
            raise ValueError("Must have at least two segments per iteration (i.e. bisection_factor >= 2)")

        table1, table2 = self._threaded_call("with_schema", [table1, table2])
        self._validate_and_adjust_columns(table1, table2)

        key_ranges = self._threaded_call("query_key_range", [table1, table2])
        mins, maxs = zip(*key_ranges)

        # We add 1 because our ranges are exclusive of the end (like in Python)
        min_key = min(map(int, mins))
        max_key = max(map(int, maxs)) + 1

        table1 = table1.new(min_key=min_key, max_key=max_key)
        table2 = table2.new(min_key=min_key, max_key=max_key)

        logger.info(
            f"Diffing tables | segments: {self.bisection_factor}, bisection threshold: {self.bisection_threshold}. "
            f"key-range: {table1.min_key}..{table2.max_key}, "
            f"size: {table2.max_key-table1.min_key}"
        )

        return self._bisect_and_diff_tables(table1, table2)

    def _validate_and_adjust_columns(self, table1, table2):
        for c in table1._relevant_columns:
            if c not in table1._schema:
                raise ValueError(f"Column '{c}' not found in schema for table {table1}")
            if c not in table2._schema:
                raise ValueError(f"Column '{c}' not found in schema for table {table2}")

            # Update schemas to minimal mutual precision
            col1 = table1._schema[c]
            col2 = table2._schema[c]
            if isinstance(col1, PrecisionType):
                if not isinstance(col2, PrecisionType):
                    raise TypeError(f"Incompatible types for column {c}:  {col1} <-> {col2}")

                lowest = min(col1, col2, key=attrgetter("precision"))

                if col1.precision != col2.precision:
                    logger.warn(f"Using reduced precision {lowest} for column '{c}'. Types={col1}, {col2}")

                table1._schema[c] = col1.replace(precision=lowest.precision, rounds=lowest.rounds)
                table2._schema[c] = col2.replace(precision=lowest.precision, rounds=lowest.rounds)

    def _bisect_and_diff_tables(self, table1, table2, level=0, max_rows=None):
        assert table1.is_bounded and table2.is_bounded

        if max_rows is None:
            # We can be sure that row_count <= max_rows
            max_rows = table1.max_key - table1.min_key

        # If count is below the threshold, just download and compare the columns locally
        # This saves time, as bisection speed is limited by ping and query performance.
        if max_rows < self.bisection_threshold:
            rows1, rows2 = self._threaded_call("get_values", [table1, table2])
            diff = list(diff_sets(rows1, rows2))
            logger.info(". " * level + f"Diff found {len(diff)} different rows.")
            self.stats["rows_downloaded"] = self.stats.get("rows_downloaded", 0) + max(len(rows1), len(rows2))
            yield from diff
            return

        # Choose evenly spaced checkpoints (according to min_key and max_key)
        checkpoints = table1.choose_checkpoints(self.bisection_factor - 1)

        # Create new instances of TableSegment between each checkpoint
        segmented1 = table1.segment_by_checkpoints(checkpoints)
        segmented2 = table2.segment_by_checkpoints(checkpoints)

        # Recursively compare each pair of corresponding segments between table1 and table2
        diff_iters = [
            self._diff_tables(t1, t2, level + 1, i + 1, len(segmented1))
            for i, (t1, t2) in enumerate(safezip(segmented1, segmented2))
        ]

        for res in self._thread_map(list, diff_iters):
            yield from res

    def _diff_tables(self, table1, table2, level=0, segment_index=None, segment_count=None):
        logger.info(
            ". " * level + f"Diffing segment {segment_index}/{segment_count}, "
            f"key-range: {table1.min_key}..{table2.max_key}, "
            f"size: {table2.max_key-table1.min_key}"
        )

        (count1, checksum1), (count2, checksum2) = self._threaded_call("count_and_checksum", [table1, table2])

        if count1 == 0 and count2 == 0:
            logger.warn(
                "Uneven distribution of keys detected. (big gaps in the key column). "
                "For better performance, we recommend to increase the bisection-threshold."
            )
            assert checksum1 is None and checksum2 is None
            return

        if level == 1:
            self.stats["table1_count"] = self.stats.get("table1_count", 0) + count1

        if checksum1 != checksum2:
            yield from self._bisect_and_diff_tables(table1, table2, level=level, max_rows=max(count1, count2))

    def _thread_map(self, func, iter):
        if not self.threaded:
            return map(func, iter)

        task_pool = ThreadPoolExecutor(max_workers=self.max_threadpool_size)
        return task_pool.map(func, iter)

    def _threaded_call(self, func, iter):
        return list(self._thread_map(methodcaller(func), iter))
