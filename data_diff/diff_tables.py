"""Provides classes for performing a table diff
"""

<<<<<<< HEAD
from collections import defaultdict
from typing import List, Tuple
import logging
=======
from typing import List, Tuple, Iterator, Literal
import logging
import datetime
>>>>>>> 8914eaf (no full index scans)

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

<<<<<<< HEAD
=======
        # This will only happen on the first TableSegment
        if self.start_key is None or self.end_key is None:
            select = self._make_select(columns=[f"min({self.key_column})", f"max({self.key_column})"])
            res = self.database.query(select, Tuple)[0] or (0, 0)

            if self.start_key is None:
                self.start_key = res[0]
            if self.end_key is None:
                self.end_key = res[1]

>>>>>>> 8914eaf (no full index scans)
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

<<<<<<< HEAD
    def _make_select(self, *, table=None, columns=None, where=None, group_by=None, order_by=None):
=======
    def _make_select(self, *, table=None, columns=None, where=None,
                     group_by=None, order_by=None, where_or=None):
>>>>>>> 8914eaf (no full index scans)
        if columns is None:
            columns = [self.key_column]
        where = list(self._make_key_range()) + list(self._make_update_range()) + ([] if where is None else [where])
        order_by = None if order_by is None else [order_by]
        return Select(
            table=table or TableName(self.table_path),
            where=where,
            columns=columns,
            group_by=group_by,
<<<<<<< HEAD
=======
            where_or=where_or,
>>>>>>> 8914eaf (no full index scans)
            order_by=order_by,
        )

    def get_values(self) -> list:
        "Download all the relevant values of the segment from the database"
        select = self._make_select(columns=self._relevant_columns)
        return self.database.query(select, List[Tuple])

<<<<<<< HEAD
    def choose_checkpoints(self, count: int) -> List[DbKey]:
        "Suggests a bunch of evenly-spaced checkpoints to split by"
        ratio = int(self.count / count)
        assert ratio > 1
        skip = f"mod(idx, {ratio}) = 0"
        select = self._make_select(table=Enum(self.table_path, order_by=self.key_column), where=skip)
        return self.database.query(select, List[int])
=======
    def choose_checkpoints(self, bisection_factor: int) -> List[DbKey]:
        "Suggests a bunch of evenly-spaced checkpoints to split by"
        gap = round((self.end_key - self.start_key + 1) / bisection_factor)
        assert gap >= 1

        checkpoints = [self.start_key + gap]
        for i in range(bisection_factor - 1):
            checkpoints.append(checkpoints[i] + gap)

        # The _make_select will ensure it's still within the valid key space!
        lookaround = 1000

        columns = []
        where_or = []
        for i in range(bisection_factor - 1):
            columns.append(f"MAX(CASE WHEN id >= {checkpoints[i]-lookaround} AND id < {checkpoints[i]} THEN id ELSE -1 END)")
            where_or.append(f"(id >= {checkpoints[i]-lookaround} AND id < {checkpoints[i]})")

        select = self._make_select(columns=columns, where_or=where_or)
        real_checkpoints = self.database.query(select, List[Tuple])
        return list(real_checkpoints[0])
>>>>>>> 8914eaf (no full index scans)

    def segment_by_checkpoints(self, checkpoints: List[DbKey]) -> List["TableSegment"]:
        "Split the current TableSegment to a bunch of smaller ones, separate by the given checkpoints"

        if self.start_key and self.end_key:
            assert all(self.start_key <= c < self.end_key for c in checkpoints)
        checkpoints.sort()

        # Calculate sub-segments
        positions = [self.start_key] + checkpoints + [self.end_key]
        ranges = list(zip(positions[:-1], positions[1:]))

        # Create table segments
        tables = [self.new(start_key=s, end_key=e) for s, e in ranges]

        return tables

<<<<<<< HEAD
        ## Calculate checksums in one go, to prevent repetitive individual calls
        # selects = [t._make_select(columns=[Checksum(self._relevant_columns)]) for t in tables]
        # res = self.database.query(Select(columns=selects), list)
        # checksums ,= res
        # assert len(checksums) == len(checkpoints) + 1
        # return [t.new(_checksum=checksum) for t, checksum in safezip(tables, checksums)]

=======
>>>>>>> 8914eaf (no full index scans)
    def new(self, _count=None, _checksum=None, **kwargs) -> "TableSegment":
        """Using new() creates a copy of the instance using 'replace()', and makes sure the cache is reset"""
        return self.replace(_count=None, _checksum=None, **kwargs)

<<<<<<< HEAD
    @property
    def count(self) -> int:
        if self._count is None:
            self._count = self.database.query(self._make_select(columns=[Count()]), int)
=======
    def __repr__(self):
        return f"{type(self.database).__name__}/{', '.join(self.table_path)}"

    @property
    def count(self) -> int:
        if self._count is None:
            raise ValueError("You should always get the count after the checksum to avoid another index scan")
>>>>>>> 8914eaf (no full index scans)
        return self._count

    @property
    def _relevant_columns(self) -> List[str]:
<<<<<<< HEAD
        return (
            [self.key_column]
            + ([self.update_column] if self.update_column is not None else [])
            + list(self.extra_columns)
        )
=======
        return list(set(
            [self.key_column]
            + ([self.update_column] if self.update_column is not None else [])
            + list(self.extra_columns)
        ))
>>>>>>> 8914eaf (no full index scans)

    @property
    def checksum(self) -> int:
        if self._checksum is None:
<<<<<<< HEAD
            self._checksum = (
                self.database.query(self._make_select(columns=[Checksum(self._relevant_columns)]), int) or 0
            )
=======
            # Get the count in the same index pass. Much cheaper than doing it
            # separately.
            select = self._make_select(columns=[Count(), Checksum(self._relevant_columns)])
            result = self.database.query(select, Tuple)
            self._checksum = int(result[0][1])
            self._count = result[0][0]

>>>>>>> 8914eaf (no full index scans)
        return self._checksum


def diff_sets(a: set, b: set) -> iter:
    s1 = set(a)
    s2 = set(b)
<<<<<<< HEAD
    d = defaultdict(list)

    # The first item is always the key (see TableDiffer._relevant_columns)
    for i in s1 - s2:
        d[i[0]].append(("+", i))
    for i in s2 - s1:
        d[i[0]].append(("-", i))

    for k, v in sorted(d.items(), key=lambda i: i[0]):
        yield from v


DiffResult = iter  # Iterator[Tuple[Literal["+", "-"], tuple]]
=======
    for i in s1 - s2:
        yield "+", i
    for i in s2 - s1:
        yield "-", i


DiffResult = Iterator[Tuple[Literal["+", "-"], tuple]]
>>>>>>> 8914eaf (no full index scans)


@dataclass
class TableDiffer:
    """Finds the diff between two SQL tables

    The algorithm uses hashing to quickly check if the tables are different, and then applies a
    bisection search recursively to find the differences efficiently.

    Works best for comparing tables that are mostly the name, with minor discrepencies.
    """

    bisection_factor: int = 32  # Into how many segments to bisect per iteration
<<<<<<< HEAD
    bisection_threshold: int = 1024**2  # When should we stop bisecting and compare locally (in row count)
=======
    bisection_threshold: int = 10000  # When should we stop bisecting and compare locally (in row count)
>>>>>>> 8914eaf (no full index scans)
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

        logger.info(
<<<<<<< HEAD
            f"Diffing tables of size {table1.count} and {table2.count} | segments: {self.bisection_factor}, bisection threshold: {self.bisection_threshold}."
        )

        if table1.checksum == table2.checksum:
            return []  # No differences

        return self._diff_tables(table1, table2)

    def _diff_tables(self, table1, table2, level=0):
        count1 = table1.count
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

        # Find mutual checkpoints between the two tables
        checkpoints = table1.choose_checkpoints(self.bisection_factor - 1)
        assert checkpoints
        mutual_checkpoints = table2.find_checkpoints([Value(c) for c in checkpoints])
        mutual_checkpoints = list(set(mutual_checkpoints))  # Duplicate values are a problem!
        logger.debug(". " * level + f"Found {len(mutual_checkpoints)} mutual checkpoints (out of {len(checkpoints)}).")
        if not mutual_checkpoints:
            raise Exception("Tables are too different.")

        # Create new instances of TableSegment between each checkpoint
        segmented1 = table1.segment_by_checkpoints(mutual_checkpoints)
        segmented2 = table2.segment_by_checkpoints(mutual_checkpoints)
        if self.debug:
            logger.debug("Performing sanity tests for chosen segments (assert sum of fragments == whole)")
            assert count1 == sum(s.count for s in segmented1)
            assert count2 == sum(s.count for s in segmented2)

        # Compare each pair of corresponding segments between table1 and table2
        for i, (t1, t2) in enumerate(safezip(segmented1, segmented2)):
            logger.info(". " * level + f"Diffing segment {i+1}/{len(segmented1)} of size {t1.count} and {t2.count}")
            if t1.checksum != t2.checksum:
                # Apply recursively
                yield from self._diff_tables(t1, t2, level + 1)
=======
            f"Diffing tables {repr(table1)} and {repr(table2)} | segments: {self.bisection_factor}, bisection threshold: {self.bisection_threshold}."
        )

        return self._diff_tables(table1, table2)

    def _diff_tables(self, table1, table2, level=0, bisection_factor=None):
        if bisection_factor is None:
            bisection_factor = self.bisection_factor
        if level > 50:
            raise Exception("Recursing too far; likely infinite loop")

        # TODO: As an optimization, get an approximate count here from the
        # database's information tables (if available), and if it's roughly
        # below the threshold, then allow getting the values on the first pass.
        
        # We only check beyond level > 0, because otherwise we might scan the
        # entire index in one query. For large tables with billions of rows, we
        # need to split by the `bisection_factor`.
        if level > 0:
            count1 = table1.count
            count2 = table2.count
            # TODO: MAX KEY - MIN_KEY + 1 too?

            # If count is below the threshold, just download and compare the columns locally
            # This saves time, as bisection speed is limited by ping and query performance.
            if count1 < self.bisection_threshold and count2 < self.bisection_threshold:
                rows1 = table1.get_values()
                rows2 = table2.get_values()
                diff = list(diff_sets(rows1, rows2))
                logger.info(". " * level + f"Diff found {len(diff)} different rows.")
                yield from diff
                return

        # Find checkpoints between the two tables
        checkpoints = table1.choose_checkpoints(bisection_factor)
        assert checkpoints

        # Create new instances of TableSegment between each checkpoint
        segmented1 = table1.segment_by_checkpoints(checkpoints)
        segmented2 = table2.segment_by_checkpoints(checkpoints)
        # print(segmented1)

        # Compare each pair of corresponding segments between table1 and table2
        for i, (t1, t2) in enumerate(safezip(segmented1, segmented2)):
            logger.info(". " * level + f"Diffing segment {i+1}/{len(segmented1)} keys={t1.start_key}..{t1.end_key}")
            if t1.checksum != t2.checksum:
                yield from self._diff_tables(t1, t2, level + 1, max(int(bisection_factor / 2), 2))
>>>>>>> 8914eaf (no full index scans)
