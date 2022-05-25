"""Provides classes for performing a table diff
"""

from collections import defaultdict
import logging
from typing import List, Tuple
import datetime

from runtype import dataclass

from .sql import Select, Checksum, Compare, DbPath, DbKey, DbTime, Count, Enum, TableName, In, Value, Time
from .database import Database

logger = logging.getLogger("diff_tables")


def safezip(*args):
    "zip but makes sure all sequences are the same length"
    assert len(set(map(len, args))) == 1
    return zip(*args)


@dataclass(frozen=False)
class TableSegment:
    database: Database
    table_path: DbPath
    key_column: str
    update_column: str = None
    extra_columns: Tuple[str, ...] = ()
    start_key: DbKey = None
    end_key: DbKey = None
    min_time: DbTime = None
    max_time: DbTime = None

    _count: int = None
    _checksum: int = None

    def __post_init__(self):
        if not self.update_column and (self.min_time or self.max_time):
            raise ValueError("Error: min_time/max_time feature requires to specify 'update_column'")

    def _make_key_range(self):
        if self.start_key is not None:
            yield Compare("<=", str(self.start_key), self.key_column)
        if self.end_key is not None:
            yield Compare("<", self.key_column, str(self.end_key))

    def _make_update_range(self):
        if self.min_time is not None:
            yield Compare("<=", Time(self.min_time), self.update_column)
        if self.max_time is not None:
            yield Compare("<", self.update_column, Time(self.max_time))

    def _make_select(self, *, table=None, columns=None, where=None,
                     group_by=None, order_by=None, where_or=None):
        if columns is None:
            columns = [self.key_column]
        where = list(self._make_key_range()) + list(self._make_update_range()) + ([] if where is None else [where])
        order_by = None if order_by is None else [order_by]
        return Select(
            table=table or TableName(self.table_path),
            where=where,
            columns=columns,
            group_by=group_by,
            where_or=where_or,
            order_by=order_by,
        )

    def get_values(self) -> list:
        "Download all the relevant values of the segment from the database"
        select = self._make_select(columns=self._relevant_columns)
        return self.database.query(select, List[Tuple])

    def choose_checkpoints(self, bisection_factor: int) -> List[DbKey]:
        """
        Choose `bisection_factor - 1` (because of start and end) checkpoints in
        the keyspace.

        For example, a table of 1000 with `bisection_factor` of 4 would yield
        the following checkpoints:
            [250, 500, 750]

        Which would yield the following segments:
            [1..249, 250..499, 500..749, 750..1000]
        """

        assert self.end_key is not None
        assert self.start_key is not None
        assert bisection_factor >= 2
        # 1..11 for bisection_factor 2 would mean gap=round(10/2)=5
        # which means checkpoints returns only 1 value:
        #   [1 + 5 - 1] => [5]
        # Then `segment_by_checkpoints` will produce:
        #   [1..5, 5..11]
        gap = round((self.end_key - self.start_key) / (bisection_factor))
        assert gap >= 1

        proposed_checkpoints = [self.start_key + gap - 1]
        # -2 because we add start + end in `segment_by_checkpoints`!
        for i in range(bisection_factor - 2):
            proposed_checkpoints.append(proposed_checkpoints[i] + gap - 1)

        return proposed_checkpoints

    def segment_by_checkpoints(self, checkpoints: List[DbKey]) -> List["TableSegment"]:
        "Split the current TableSegment to a bunch of smaller ones, separate by the given checkpoints"
        # Make sure start_key and end_key are set, for the beginning and end
        # they may not be.
        assert self.start_key is not None
        assert self.end_key is not None
        assert all(self.start_key <= c < self.end_key for c in checkpoints)

        # Calculate sub-segments, turns checkpoints such as [250, 500, 750] into
        # [1..249, 250..499, 500..749, 750..1000].
        positions = [self.start_key] + checkpoints + [self.end_key]
        ranges = list(zip(positions[:-1], positions[1:]))

        # Create table segments
        tables = [self.new(start_key=s, end_key=e) for s, e in ranges]

        return tables

    def new(self, _count=None, _checksum=None, **kwargs) -> "TableSegment":
        """Using new() creates a copy of the instance using 'replace()', and makes sure the cache is reset"""
        return self.replace(_count=None, _checksum=None, **kwargs)

    def __repr__(self):
        return f"{type(self.database).__name__}/{', '.join(self.table_path)}"

    def query_start_key_and_end_key(self) -> Tuple[int, int]:
        """Query database for minimum and maximum key. This is used for setting
        the boundaries of the initial, full table table segment."""
        select = self._make_select(columns=[f"min({self.key_column})", f"max({self.key_column})"])
        res = self.database.query(select, Tuple)[0]

        start_key = res[0] or 1
        # TableSegments are always exclusive the last key:
        #   (1..250) => # WHERE i >= 1 AND i < 250
        # Thus, for the very last segment (which is the one where these
        # aren't automatically set!) -- we have to add 1.
        end_key = res[1] + 1 if res[1] else 1

        return (start_key, end_key)

    def compute_checksum_and_count(self):
        """
        Query the database for the checksum and count for this segment. Note
        that it will _not_ include the `end_key` in this segment, as that's the
        beginning of the next segment.
        """
        if self.start_key is None or self.end_key is None:
            raise ValueError("""
                `start_key` and/or `end_key` are not set. Likely this is because
                you didn't call `set_initial_start_key_and_end_key` to get the
                min(key) and max(key) from the database for the initial, whole
                table segment.
            """)
        if self._count is not None or self._checksum is not None:
            return  # already computed

        # Get the count in the same index pass. Much cheaper than doing it
        # separately.
        select = self._make_select(columns=[Count(), Checksum(self._relevant_columns)])
        result = self.database.query(select, Tuple)[0]
        self._count = result[0] if result[1] else 0
        self._checksum = int(result[1]) if result[1] else 0


    @property
    def count(self) -> int:
        if self._count is None:
            raise ValueError("""
                You must call compute_checksum_and_count() before
                accessing the count to ensure only one index scan
                is performed.
             """)

        return self._count

    @property
    def _relevant_columns(self) -> List[str]:
        # The user may duplicate columns across -k, -t, -c so we de-dup here.
        relevant = list(set(
            [self.key_column]
            + ([self.update_column] if self.update_column is not None else [])
            + list(self.extra_columns)
        ))
        relevant.sort()
        return relevant

    @property
    def checksum(self) -> int:
        if self._checksum is None:
            # Get the count in the same index pass. Much cheaper than doing it
            # separately.
            select = self._make_select(columns=[Count(), Checksum(self._relevant_columns)])
            result = self.database.query(select, Tuple)
            self._checksum = int(result[0][1])
            self._count = result[0][0]

        return self._checksum


def diff_sets(a: set, b: set) -> iter:
    s1 = set(a)
    s2 = set(b)
    d = defaultdict(list)

    # The first item is always the key (see TableDiffer._relevant_columns)
    for i in s1 - s2:
        d[i[0]].append(("+", i))
    for i in s2 - s1:
        d[i[0]].append(("-", i))

    for k, v in sorted(d.items(), key=lambda i: i[0]):
        yield from v


DiffResult = iter  # Iterator[Tuple[Literal["+", "-"], tuple]]

@dataclass
class TableDiffer:
    """Finds the diff between two SQL tables

    The algorithm uses hashing to quickly check if the tables are different, and then applies a
    bisection search recursively to find the differences efficiently.

    Works best for comparing tables that are mostly the name, with minor discrepencies.
    """

    bisection_factor: int = 32  # Into how many segments to bisect per iteration
    bisection_threshold: int = 10000  # When should we stop bisecting and compare locally (in row count)
    debug: bool = False

    def diff_tables(self, table1: TableSegment, table2: TableSegment) -> DiffResult:
        """Diff the given tables.

        Returned value is an iterator that yield pair-tuples, representing the diff. Items can be either
            ('+', columns) for items in table1 but not in table2
            ('-', columns) for items in table2 but not in table1
            Where `columns` is a tuple of values for the involved columns, i.e. (id, ...extra)
        """
        if self.bisection_factor >= self.bisection_threshold:
            raise ValueError("Incorrect param values")
        if self.bisection_factor < 2:
            raise ValueError("Must have at least two segments per iteration")

        self.set_initial_start_key_and_end_key(table1, table2)

        logger.info(
            f"Diffing tables {repr(table1)} and {repr(table2)} | keys: {table1.start_key}...{table2.end_key} | segments: {self.bisection_factor}, bisection threshold: {self.bisection_threshold}."
        )

        return self._diff_tables(table1, table2, self.bisection_factor)

    def set_initial_start_key_and_end_key(self, table1: TableSegment, table2: TableSegment):
        """For the initial, full table segment we need to set the boundaries of
        the minimum and maximum key."""

        table1_start_key, table1_end_key = table1.query_start_key_and_end_key()
        table2_start_key, table2_end_key = table2.query_start_key_and_end_key()

        table1.start_key = min(table1_start_key, table2_start_key)
        table2.start_key = table1.start_key
        # The +1 in the end key is to make sure that last row is encapsulated in
        # the final range. Because every range query assumes < end_key, not <=.
        table1.end_key = max(table1_end_key, table2_end_key)
        table2.end_key = table1.end_key

        assert table1.start_key <= table1.end_key

    def _diff_tables(self, table1, table2, bisection_factor, level=0):
        if level > 50:
            raise Exception("Recursing too deep; likely bug for infinite recursion")

        # This is the upper bound, but it might be smaller if there are gaps.
        # E.g. between id 1..10, id 5 might have been hard deleted.
        keyspace_size = table1.end_key - table1.start_key

        # We only check beyond level > 0, because otherwise we might scan the
        # entire index with COUNT(*). For large tables with billions of rows, we
        # need to split the COUNT(*) by the `bisection_factor`.
        if level > 0 or keyspace_size < self.bisection_threshold:
            # In case the first segment is below the threshold
            # This is we get the count
            table1.compute_checksum_and_count()
            table2.compute_checksum_and_count()
            count1 = table1.count # These have been precomputed with the checksum
            count2 = table2.count

            # If count is below the threshold, just download and compare the columns locally
            # This saves time, as bisection speed is limited by ping and query performance.
            if count1 < self.bisection_threshold and count2 < self.bisection_threshold:
                rows1 = table1.get_values()
                rows2 = table2.get_values()
                diff = list(diff_sets(rows1, rows2))
                logger.info(". " * level + f"Diff found {len(diff)} different rows.")
                yield from diff
                return

        # Find checkpoints between the two tables, e.g. [250, 500, 750] for a
        # table with 1000 ids and a bisection factor of 4.
        checkpoints = table1.choose_checkpoints(bisection_factor)
        assert checkpoints

        # Create new instances of TableSegment between each checkpoint
        # [1..249, 250..499, 500..749, 750..1000]
        segmented1 = table1.segment_by_checkpoints(checkpoints)
        segmented2 = table2.segment_by_checkpoints(checkpoints)

        # Compare each pair of corresponding segments between table1 and table2
        for i, (t1, t2) in enumerate(safezip(segmented1, segmented2)):
            n_keys = t1.end_key - t1.start_key
            logger.info(". " * level + f"Diffing segment {i+1}/{len(segmented1)} keys={t1.start_key}..{t1.end_key-1} n_keys={n_keys}")
            t1.compute_checksum_and_count()
            t2.compute_checksum_and_count()

            if t1.checksum != t2.checksum:
                yield from self._diff_tables(t1, t2, bisection_factor, level + 1)
