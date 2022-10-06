"""Provides classes for performing a table diff
"""

import time
import os
from numbers import Number
from operator import attrgetter, methodcaller
from collections import defaultdict
from typing import Tuple, Iterator, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from runtype import dataclass

from .utils import safezip, run_as_daemon
from .thread_utils import ThreadedYielder
from .databases.database_types import IKey, NumericType, PrecisionType, StringType, ColType_UUID
from .table_segment import TableSegment
from .tracking import create_end_event_json, create_start_event_json, send_event_json, is_tracking_enabled

logger = logging.getLogger("diff_tables")

BENCHMARK = os.environ.get("BENCHMARK", False)
DEFAULT_BISECTION_THRESHOLD = 1024 * 16
DEFAULT_BISECTION_FACTOR = 32


def diff_sets(a: set, b: set) -> Iterator:
    s1 = set(a)
    s2 = set(b)
    d = defaultdict(list)

    # The first item is always the key (see TableDiffer._relevant_columns)
    for i in s1 - s2:
        d[i[0]].append(("-", i))
    for i in s2 - s1:
        d[i[0]].append(("+", i))

    for _k, v in sorted(d.items(), key=lambda i: i[0]):
        yield from v


DiffResult = Iterator[Tuple[str, tuple]]  # Iterator[Tuple[Literal["+", "-"], tuple]]


@dataclass
class TableDiffer:
    """Finds the diff between two SQL tables

    The algorithm uses hashing to quickly check if the tables are different, and then applies a
    bisection search recursively to find the differences efficiently.

    Works best for comparing tables that are mostly the same, with minor discrepencies.

    Parameters:
        bisection_factor (int): Into how many segments to bisect per iteration.
        bisection_threshold (Number): When should we stop bisecting and compare locally (in row count).
        threaded (bool): Enable/disable threaded diffing. Needed to take advantage of database threads.
        max_threadpool_size (int): Maximum size of each threadpool. ``None`` means auto. Only relevant when `threaded` is ``True``.
                                   There may be many pools, so number of actual threads can be a lot higher.
    """

    bisection_factor: int = DEFAULT_BISECTION_FACTOR
    bisection_threshold: Number = DEFAULT_BISECTION_THRESHOLD  # Accepts inf for tests
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
            ('-', columns) for items in table1 but not in table2
            ('+', columns) for items in table2 but not in table1
            Where `columns` is a tuple of values for the involved columns, i.e. (id, ...extra)
        """
        # Validate options
        if self.bisection_factor >= self.bisection_threshold:
            raise ValueError("Incorrect param values (bisection factor must be lower than threshold)")
        if self.bisection_factor < 2:
            raise ValueError("Must have at least two segments per iteration (i.e. bisection_factor >= 2)")

        if is_tracking_enabled():
            options = dict(self)
            event_json = create_start_event_json(options)
            run_as_daemon(send_event_json, event_json)

        self.stats["diff_count"] = 0
        start = time.monotonic()
        error = None
        try:

            # Query and validate schema
            table1, table2 = self._threaded_call("with_schema", [table1, table2])
            self._validate_and_adjust_columns(table1, table2)

            key_type = table1._schema[table1.key_column]
            key_type2 = table2._schema[table2.key_column]
            if not isinstance(key_type, IKey):
                raise NotImplementedError(f"Cannot use column of type {key_type} as a key")
            if not isinstance(key_type2, IKey):
                raise NotImplementedError(f"Cannot use column of type {key_type2} as a key")
            assert key_type.python_type is key_type2.python_type

            # Query min/max values
            key_ranges = self._threaded_call_as_completed("query_key_range", [table1, table2])

            # Start with the first completed value, so we don't waste time waiting
            min_key1, max_key1 = self._parse_key_range_result(key_type, next(key_ranges))

            table1, table2 = [t.new(min_key=min_key1, max_key=max_key1) for t in (table1, table2)]

            logger.info(
                f"Diffing tables | segments: {self.bisection_factor}, bisection threshold: {self.bisection_threshold}. "
                f"key-range: {table1.min_key}..{table2.max_key}, "
                f"size: table1 <= {table1.approximate_size()}, table2 <= {table2.approximate_size()}"
            )

            ti = ThreadedYielder(self.max_threadpool_size)
            # Bisect (split) the table into segments, and diff them recursively.
            ti.submit(self._bisect_and_diff_tables, ti, table1, table2)

            # Now we check for the second min-max, to diff the portions we "missed".
            min_key2, max_key2 = self._parse_key_range_result(key_type, next(key_ranges))

            if min_key2 < min_key1:
                pre_tables = [t.new(min_key=min_key2, max_key=min_key1) for t in (table1, table2)]
                ti.submit(self._bisect_and_diff_tables, ti, *pre_tables)

            if max_key2 > max_key1:
                post_tables = [t.new(min_key=max_key1, max_key=max_key2) for t in (table1, table2)]
                ti.submit(self._bisect_and_diff_tables, ti, *post_tables)

            yield from ti

        except BaseException as e:  # Catch KeyboardInterrupt too
            error = e
        finally:
            if is_tracking_enabled():
                runtime = time.monotonic() - start
                table1_count = self.stats.get("table1_count")
                table2_count = self.stats.get("table2_count")
                diff_count = self.stats.get("diff_count")
                err_message = str(error)[:20]  # Truncate possibly sensitive information.
                event_json = create_end_event_json(
                    error is None,
                    runtime,
                    table1.database.name,
                    table2.database.name,
                    table1_count,
                    table2_count,
                    diff_count,
                    err_message,
                )
                send_event_json(event_json)

            if error:
                raise error

    def _parse_key_range_result(self, key_type, key_range):
        mn, mx = key_range
        cls = key_type.make_value
        # We add 1 because our ranges are exclusive of the end (like in Python)
        try:
            return cls(mn), cls(mx) + 1
        except (TypeError, ValueError) as e:
            raise type(e)(f"Cannot apply {key_type} to {mn}, {mx}.") from e

    def _validate_and_adjust_columns(self, table1, table2):
        for c1, c2 in safezip(table1._relevant_columns, table2._relevant_columns):
            if c1 not in table1._schema:
                raise ValueError(f"Column '{c1}' not found in schema for table {table1}")
            if c2 not in table2._schema:
                raise ValueError(f"Column '{c2}' not found in schema for table {table2}")

            # Update schemas to minimal mutual precision
            col1 = table1._schema[c1]
            col2 = table2._schema[c2]
            if isinstance(col1, PrecisionType):
                if not isinstance(col2, PrecisionType):
                    raise TypeError(f"Incompatible types for column '{c1}':  {col1} <-> {col2}")

                lowest = min(col1, col2, key=attrgetter("precision"))

                if col1.precision != col2.precision:
                    logger.warning(f"Using reduced precision {lowest} for column '{c1}'. Types={col1}, {col2}")

                table1._schema[c1] = col1.replace(precision=lowest.precision, rounds=lowest.rounds)
                table2._schema[c2] = col2.replace(precision=lowest.precision, rounds=lowest.rounds)

            elif isinstance(col1, NumericType):
                if not isinstance(col2, NumericType):
                    raise TypeError(f"Incompatible types for column '{c1}':  {col1} <-> {col2}")

                lowest = min(col1, col2, key=attrgetter("precision"))

                if col1.precision != col2.precision:
                    logger.warning(f"Using reduced precision {lowest} for column '{c1}'. Types={col1}, {col2}")

                table1._schema[c1] = col1.replace(precision=lowest.precision)
                table2._schema[c2] = col2.replace(precision=lowest.precision)

            elif isinstance(col1, ColType_UUID):
                if not isinstance(col2, ColType_UUID):
                    raise TypeError(f"Incompatible types for column '{c1}':  {col1} <-> {col2}")

            elif isinstance(col1, StringType):
                if not isinstance(col2, StringType):
                    raise TypeError(f"Incompatible types for column '{c1}':  {col1} <-> {col2}")

        for t in [table1, table2]:
            for c in t._relevant_columns:
                ctype = t._schema[c]
                if not ctype.supported:
                    logger.warning(
                        f"[{t.database.name}] Column '{c}' of type '{ctype}' has no compatibility handling. "
                        "If encoding/formatting differs between databases, it may result in false positives."
                    )

    def _bisect_and_diff_tables(
        self, ti: ThreadedYielder, table1: TableSegment, table2: TableSegment, level=0, max_rows=None
    ):
        assert table1.is_bounded and table2.is_bounded

        max_space_size = max(table1.approximate_size(), table2.approximate_size())
        if max_rows is None:
            # We can be sure that row_count <= max_rows iff the table key is unique
            max_rows = max_space_size

        # If count is below the threshold, just download and compare the columns locally
        # This saves time, as bisection speed is limited by ping and query performance.
        if max_rows < self.bisection_threshold or max_space_size < self.bisection_factor * 2:
            rows1, rows2 = self._threaded_call("get_values", [table1, table2])
            diff = list(diff_sets(rows1, rows2))

            # Initial bisection_threshold larger than count. Normally we always
            # checksum and count segments, even if we get the values. At the
            # first level, however, that won't be true.
            if level == 0:
                self.stats["table1_count"] = len(rows1)
                self.stats["table2_count"] = len(rows2)

            self.stats["diff_count"] += len(diff)

            logger.info(". " * level + f"Diff found {len(diff)} different rows.")
            self.stats["rows_downloaded"] = self.stats.get("rows_downloaded", 0) + max(len(rows1), len(rows2))
            return diff

        # Choose evenly spaced checkpoints (according to min_key and max_key)
        biggest_table = max(table1, table2, key=methodcaller('approximate_size'))
        checkpoints = biggest_table.choose_checkpoints(self.bisection_factor - 1)

        # Create new instances of TableSegment between each checkpoint
        segmented1 = table1.segment_by_checkpoints(checkpoints)
        segmented2 = table2.segment_by_checkpoints(checkpoints)

        # Recursively compare each pair of corresponding segments between table1 and table2
        for i, (t1, t2) in enumerate(safezip(segmented1, segmented2)):
            ti.submit(self._diff_tables, ti, t1, t2, max_rows, level + 1, i + 1, len(segmented1), priority=level)

    def _diff_tables(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        max_rows: int,
        level=0,
        segment_index=None,
        segment_count=None,
    ):
        logger.info(
            ". " * level + f"Diffing segment {segment_index}/{segment_count}, "
            f"key-range: {table1.min_key}..{table2.max_key}, "
            f"size <= {max_rows}"
        )

        # When benchmarking, we want the ability to skip checksumming. This
        # allows us to download all rows for comparison in performance. By
        # default, data-diff will checksum the section first (when it's below
        # the threshold) and _then_ download it.
        if BENCHMARK:
            if max_rows < self.bisection_threshold:
                return self._bisect_and_diff_tables(ti, table1, table2, level=level, max_rows=max_rows)

        (count1, checksum1), (count2, checksum2) = self._threaded_call("count_and_checksum", [table1, table2])

        if count1 == 0 and count2 == 0:
            # logger.warning(
            #     f"Uneven distribution of keys detected in segment {table1.min_key}..{table2.max_key}. (big gaps in the key column). "
            #     "For better performance, we recommend to increase the bisection-threshold."
            # )
            assert checksum1 is None and checksum2 is None
            return

        if level == 1:
            self.stats["table1_count"] = self.stats.get("table1_count", 0) + count1
            self.stats["table2_count"] = self.stats.get("table2_count", 0) + count2

        if checksum1 != checksum2:
            return self._bisect_and_diff_tables(ti, table1, table2, level=level, max_rows=max(count1, count2))

    def _thread_map(self, func, iterable):
        if not self.threaded:
            return map(func, iterable)

        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            return task_pool.map(func, iterable)

    def _threaded_call(self, func, iterable):
        "Calls a method for each object in iterable."
        return list(self._thread_map(methodcaller(func), iterable))

    def _thread_as_completed(self, func, iterable):
        if not self.threaded:
            yield from map(func, iterable)
            return

        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            futures = [task_pool.submit(func, item) for item in iterable]
            for future in as_completed(futures):
                yield future.result()

    def _threaded_call_as_completed(self, func, iterable):
        "Calls a method for each object in iterable. Returned in order of completion."
        return self._thread_as_completed(methodcaller(func), iterable)
