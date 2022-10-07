"""Provides classes for performing a table diff
"""

import re
import time
from abc import ABC, abstractmethod
from enum import Enum
from contextlib import contextmanager
from operator import methodcaller
from typing import Tuple, Iterator, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from runtype import dataclass

from .utils import run_as_daemon, safezip, getLogger
from .thread_utils import ThreadedYielder
from .table_segment import TableSegment
from .tracking import create_end_event_json, create_start_event_json, send_event_json, is_tracking_enabled
from .databases.database_types import IKey

logger = getLogger(__name__)


class Algorithm(Enum):
    AUTO = "auto"
    JOINDIFF = "joindiff"
    HASHDIFF = "hashdiff"


DiffResult = Iterator[Tuple[str, tuple]]  # Iterator[Tuple[Literal["+", "-"], tuple]]


def truncate_error(error: str):
    first_line = error.split("\n", 1)[0]
    return re.sub("'(.*?)'", "'***'", first_line)


@dataclass
class ThreadBase:
    "Provides utility methods for optional threading"

    threaded: bool = True
    max_threadpool_size: Optional[int] = 1

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

    @contextmanager
    def _run_in_background(self, *funcs):
        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            futures = [task_pool.submit(f) for f in funcs if f is not None]
            yield futures
            for f in futures:
                f.result()


class TableDiffer(ThreadBase, ABC):
    bisection_factor = 32

    def diff_tables(self, table1: TableSegment, table2: TableSegment) -> DiffResult:
        """Diff the given tables.

        Parameters:
            table1 (TableSegment): The "before" table to compare. Or: source table
            table2 (TableSegment): The "after" table to compare. Or: target table

        Returns:
            An iterator that yield pair-tuples, representing the diff. Items can be either -
            ('-', row) for items in table1 but not in table2.
            ('+', row) for items in table2 but not in table1.
            Where `row` is a tuple of values, corresponding to the diffed columns.
        """

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

            yield from self._diff_tables(table1, table2)

        except BaseException as e:  # Catch KeyboardInterrupt too
            error = e
        finally:
            if is_tracking_enabled():
                runtime = time.monotonic() - start
                table1_count = self.stats.get("table1_count")
                table2_count = self.stats.get("table2_count")
                diff_count = self.stats.get("diff_count")
                err_message = truncate_error(repr(error))
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

    def _validate_and_adjust_columns(self, table1: TableSegment, table2: TableSegment) -> DiffResult:
        pass

    def _diff_tables(self, table1: TableSegment, table2: TableSegment) -> DiffResult:
        return self._bisect_and_diff_tables(table1, table2)

    @abstractmethod
    def _diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        max_rows: int,
        level=0,
        segment_index=None,
        segment_count=None,
    ):
        ...

    def _bisect_and_diff_tables(self, table1, table2):
        if len(table1.key_columns) > 1:
            raise NotImplementedError("Composite key not supported yet!")
        if len(table2.key_columns) > 1:
            raise NotImplementedError("Composite key not supported yet!")
        (key1,) = table1.key_columns
        (key2,) = table2.key_columns

        key_type = table1._schema[key1]
        key_type2 = table2._schema[key2]
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
            # f"Diffing tables | segments: {self.bisection_factor}, bisection threshold: {self.bisection_threshold}. "
            f"Diffing segments at key-range: {table1.min_key}..{table2.max_key}. "
            f"size: table1 <= {table1.approximate_size()}, table2 <= {table2.approximate_size()}"
        )

        ti = ThreadedYielder(self.max_threadpool_size)
        # Bisect (split) the table into segments, and diff them recursively.
        ti.submit(self._bisect_and_diff_segments, ti, table1, table2)

        # Now we check for the second min-max, to diff the portions we "missed".
        min_key2, max_key2 = self._parse_key_range_result(key_type, next(key_ranges))

        if min_key2 < min_key1:
            pre_tables = [t.new(min_key=min_key2, max_key=min_key1) for t in (table1, table2)]
            ti.submit(self._bisect_and_diff_segments, ti, *pre_tables)

        if max_key2 > max_key1:
            post_tables = [t.new(min_key=max_key1, max_key=max_key2) for t in (table1, table2)]
            ti.submit(self._bisect_and_diff_segments, ti, *post_tables)

        return ti

    def _parse_key_range_result(self, key_type, key_range):
        mn, mx = key_range
        cls = key_type.make_value
        # We add 1 because our ranges are exclusive of the end (like in Python)
        try:
            return cls(mn), cls(mx) + 1
        except (TypeError, ValueError) as e:
            raise type(e)(f"Cannot apply {key_type} to '{mn}', '{mx}'.") from e

    def _bisect_and_diff_segments(
        self, ti: ThreadedYielder, table1: TableSegment, table2: TableSegment, level=0, max_rows=None
    ):
        assert table1.is_bounded and table2.is_bounded

        # Choose evenly spaced checkpoints (according to min_key and max_key)
        biggest_table = max(table1, table2, key=methodcaller("approximate_size"))
        checkpoints = biggest_table.choose_checkpoints(self.bisection_factor - 1)

        # Create new instances of TableSegment between each checkpoint
        segmented1 = table1.segment_by_checkpoints(checkpoints)
        segmented2 = table2.segment_by_checkpoints(checkpoints)

        # Recursively compare each pair of corresponding segments between table1 and table2
        for i, (t1, t2) in enumerate(safezip(segmented1, segmented2)):
            ti.submit(self._diff_segments, ti, t1, t2, max_rows, level + 1, i + 1, len(segmented1), priority=level)
