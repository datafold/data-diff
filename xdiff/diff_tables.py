"""Provides classes for performing a table diff
"""

from typing import List, Tuple, Iterator, Literal
import logging

from runtype import dataclass

from .sql import Select, Checksum, Compare, DbPath, DbKey, Count, Enum, TableName, In, Value
from .database import Database

logger = logging.getLogger('diff_tables')

def safezip(*args):
    "zip but makes sure all sequences are the same length"
    assert len(set(map(len, args))) == 1
    return zip(*args)


@dataclass(frozen=False)
class TableSegment:
    database: Database
    table_path: DbPath
    key_column: str
    extra_columns: Tuple[str, ...]
    start: DbKey = None
    end: DbKey = None

    _count: int = None
    _checksum: int = None

    def _make_range_pred(self):
        if self.start is not None:
            yield Compare('<=', str(self.start), self.key_column)
        if self.end is not None:
            yield Compare('<', self.key_column, str(self.end))

    def _make_select(self, *, table=None, columns=None, where=None, group_by=None, order_by=None):
        if columns is None:
            columns = [self.key_column]
        where = list(self._make_range_pred()) + ([] if where is None else [where])
        order_by = None if order_by is None else [order_by]
        return Select(table=table or TableName(self.table_path), where=where, columns=columns, group_by=group_by, order_by=order_by)

    def get_values(self) -> list:
        "Download all the relevant values of the segment from the database"
        select = self._make_select(columns=self._relevant_columns)
        return self.database.query(select, List[Tuple])

    def choose_checkpoints(self, count: int) -> List[DbKey]:
        "Suggests a bunch of evenly-spaced checkpoints to split by"
        ratio = int(self.count / count)
        assert ratio > 1
        skip = f'mod(idx, {ratio}) = 0'
        select = self._make_select(table=Enum(self.table_path, order_by=self.key_column), where=skip)
        return self.database.query(select, List[int])

    def find_checkpoints(self, checkpoints: List[DbKey]) -> List[DbKey]:
        "Takes a list of potential checkpoints and returns those that exist"
        where = In(self.key_column, checkpoints)
        return self.database.query(self._make_select(where=where), List[int])

    def segment_by_checkpoints(self, checkpoints: List[DbKey]) -> List['TableSegment']:
        "Split the current TableSegment to a bunch of smaller ones, separate by the given checkpoints"

        if self.start and self.end:
            assert all(self.start <= c < self.end for c in checkpoints)
        checkpoints.sort()

        # Calculate sub-segments
        positions = [self.start] + checkpoints + [self.end]
        ranges = list(zip( positions[:-1], positions[1:] ))
        
        # Create table segments
        tables = [self.new(start=s, end=e) for s, e in ranges]

        return tables

        ## Calculate checksums in one go, to prevent repetitive individual calls
        # selects = [t._make_select(columns=[Checksum(self._relevant_columns)]) for t in tables]
        # res = self.database.query(Select(columns=selects), list)
        # checksums ,= res
        # assert len(checksums) == len(checkpoints) + 1
        # return [t.new(_checksum=checksum) for t, checksum in safezip(tables, checksums)]

    def new(self, _count=None, _checksum=None, **kwargs) -> 'TableSegment':
        """Using new() creates a copy of the instance using 'replace()', and makes sure the cache is reset"""
        return self.replace(_count=None, _checksum=None, **kwargs)

    @property
    def count(self) -> int:
        if self._count is None:
            self._count = self.database.query(self._make_select(columns=[Count()]), int)
        return self._count

    @property
    def _relevant_columns(self) -> List[str]:
        return [self.key_column] + list(self.extra_columns)

    @property
    def checksum(self) -> int:
        if self._checksum is None:
            self._checksum = self.database.query(self._make_select(columns=[Checksum(self._relevant_columns)]), int) or 0
        return self._checksum


def diff_sets(a: set, b: set) -> iter:
    s1 = set(a)
    s2 = set(b)
    for i in s1-s2:
        yield '+', i
    for i in s2-s1:
        yield '-', i

DiffResult = Iterator[Tuple[Literal['+', '-'], tuple]]

@dataclass
class TableDiffer:
    """Finds the diff between two SQL tables

    The algorithm uses hashing to quickly check if the tables are different, and then applies a
    bisection search recursively to find the differences efficiently.

    Works best for comparing tables that are mostly the name, with minor discrepencies.
    """

    bisection_factor: int = 32             # Into how many segments to bisect per iteration
    bisection_threshold: int = 1024**2   # When should we stop bisecting and compare locally (in row count)
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

        logger.info(f'Diffing tables of size {table1.count} and {table2.count} | segments: {self.bisection_factor}, bisection threshold: {self.bisection_threshold}.')

        if table1.checksum == table2.checksum:
            return []   # No differences

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
            logger.info('. '*level + f'Diff found {len(diff)} different rows.')
            yield from diff
            return

        # Find mutual checkpoints between the two tables
        checkpoints = table1.choose_checkpoints(self.bisection_factor-1)
        assert checkpoints
        mutual_checkpoints = table2.find_checkpoints([Value(c) for c in checkpoints])
        mutual_checkpoints = list(set(mutual_checkpoints))        # Duplicate values are a problem!
        logger.debug('. '*level + f'Found {len(mutual_checkpoints)} mutual checkpoints (out of {len(checkpoints)}).')
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
            logger.info('. '*level + f'Diffing segment {i+1}/{len(segmented1)} of size {t1.count} and {t2.count}')
            if t1.checksum != t2.checksum:
                # Apply recursively
                yield from self._diff_tables(t1, t2, level+1)
