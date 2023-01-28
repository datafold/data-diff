"""Provides classes for performing a table diff
"""

import re
import time
import pandas as pd
from tabulate import tabulate
from abc import ABC, abstractmethod
from enum import Enum
from contextlib import contextmanager
from operator import methodcaller
from typing import Dict, Tuple, Iterator, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from runtype import dataclass

from data_diff.info_tree import InfoTree, SegmentInfo

from .utils import run_as_daemon, safezip, getLogger
from .thread_utils import ThreadedYielder
from .table_segment import TableSegment
from .tracking import create_end_event_json, create_start_event_json, send_event_json, is_tracking_enabled
from sqeleton.abcs import IKey

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


@dataclass
class DiffStats:
    diff_by_sign: Dict[str, int]
    table1_count: int
    table2_count: int
    unchanged: int
    diff_percent: float


@dataclass
class DiffResultWrapper:
    diff: iter  # DiffResult
    info_tree: InfoTree
    stats: dict
    result_list: list = []

    def __iter__(self):
        yield from self.result_list
        for i in self.diff:
            self.result_list.append(i)
            yield i

        for sign, values in self.result_list:
            store_extra_columns = self.info_tree.info.tables[0].extra_columns
            if store_extra_columns != None:
                break
                
        diff_output = self.result_list.copy()
        additional_columns_to_diff = list(store_extra_columns)
        primary_key = "org_id"

        df_output = pd.DataFrame(diff_output, columns =['a', 'b'])
        column_values = pd.DataFrame(df_output['b'].to_list(), columns = [primary_key] + additional_columns_to_diff)
        all_columns_values_tall = pd.concat([df_output, column_values], axis=1).\
        drop('b', axis=1)
        all_columns_values_tall.loc[all_columns_values_tall['a'] == '-', 'a'] = 'Table A'
        all_columns_values_tall.loc[all_columns_values_tall['a'] == '+', 'a'] = 'Table B'
        all_columns_values_pivot = all_columns_values_tall.\
            pivot(index=primary_key, columns='a',values=[primary_key] + additional_columns_to_diff)

        all_columns = [primary_key] + additional_columns_to_diff
        each_column_twice = [item for item in all_columns for _ in range(2)]
        tuples = [(x, 'Table A' if idx % 2 == 0 else 'Table B') for idx, x in enumerate(each_column_twice)]
        index = pd.MultiIndex.from_tuples(tuples, names=[None, "a"])
        df_with_index = pd.DataFrame(columns=index)
        all_columns_values_pivot_multiindex = pd.concat([df_with_index, all_columns_values_pivot])

        all_columns_values_pivot_multiindex.columns = [': '.join(i) for i in all_columns_values_pivot_multiindex.columns]
        matching_primary_key_rows = all_columns_values_pivot_multiindex.loc[all_columns_values_pivot_multiindex[primary_key+": Table A"] == \
        all_columns_values_pivot_multiindex[primary_key+": Table B"]]

        # Display missing primary keys
        pks_missing_from_table_a = all_columns_values_pivot_multiindex[[primary_key+": Table A"]].isna().sum()[0]   
        pks_missing_from_table_b = all_columns_values_pivot_multiindex[[primary_key+": Table B"]].isna().sum()[0]
        primary_keys_df = pd.DataFrame([[pks_missing_from_table_a, pks_missing_from_table_b]], columns=['Primary Keys Missing from Table A', 'Primary Keys Missing from Table B'])
        
        #blankIndex=[''] * len(primary_keys_df)
        #primary_keys_df.index=blankIndex
        print(tabulate(primary_keys_df, headers='keys', tablefmt='psql', showindex=False))

        # Display columns with conflicts
        columns_with_conflicts = []
        conflicts_df = pd.DataFrame(columns=['Column', 'Conflicting Rows'])
        
        for i in additional_columns_to_diff:
            conflicts = (matching_primary_key_rows[i+": Table A"] != matching_primary_key_rows[i+": Table B"]) & \
                (matching_primary_key_rows[i+": Table A"].notnull() | matching_primary_key_rows[i+": Table B"].notnull())
            sum_conflicts = sum(conflicts)
            conflicts_df.loc[len(conflicts_df.index)] = [i, sum_conflicts]
            if sum(conflicts) > 0:
                columns_with_conflicts += [i+": Table A", i+": Table B"]

        # blankIndex=[''] * len(conflicts_df)
        # conflicts_df.index=blankIndex
        print(tabulate(conflicts_df, headers='keys', tablefmt='psql', showindex=False))

    def _get_stats(self) -> DiffStats:
        list(self)  # Consume the iterator into result_list, if we haven't already

        diff_by_key = {}

        for sign, values in self.result_list:
           # print(self.info_tree.info.tables[0].extra_columns)
            k = values[: len(self.info_tree.info.tables[0].key_columns)]
            if k in diff_by_key:
                assert sign != diff_by_key[k]
                diff_by_key[k] = "!"
            else:
                diff_by_key[k] = sign

        diff_by_sign = {k: 0 for k in "+-!"}
        for sign in diff_by_key.values():
            diff_by_sign[sign] += 1

        table1_count = self.info_tree.info.rowcounts[1]
        table2_count = self.info_tree.info.rowcounts[2]
        unchanged = table1_count - diff_by_sign["-"] - diff_by_sign["!"]
        diff_percent = 1 - unchanged / max(table1_count, table2_count)

        return DiffStats(diff_by_sign, table1_count, table2_count, unchanged, diff_percent)

    def get_stats_string(self):

        diff_stats = self._get_stats()
        string_output = "\n\t\t|\tTable A\t\t|\tTable B\n"
        string_output += "------------------------------------------------------------\n"
        string_output += f"Rows\t\t|\t{diff_stats.table1_count}\t\t|\t{diff_stats.table2_count}\n"
        string_output += f"Exclusive PKs\t|\t{diff_stats.diff_by_sign['-']}\t\t|\t{diff_stats.diff_by_sign['+']}\n"
        string_output += "------------------------------------------------------------\n\n"
        string_output += f"Rows with value conflicts: {diff_stats.diff_by_sign['!']}\n"
        string_output += f"Matching Rows: {diff_stats.unchanged}\n"
        string_output += f"Difference Score: {100*diff_stats.diff_percent:.2f}%\n"

        if self.stats:
            for k, v in sorted(self.stats.items()):
                if type(v) is dict: # Only the value of diff_counts is dict, with one key per column.
                    for column, diff_count in v.items():
                        string_output += f"{column}\t{diff_count}\n" # Report the number of conflicts.

        return ""

    def get_stats_dict(self):

        diff_stats = self._get_stats()
        json_output = {
            "rows_A": diff_stats.table1_count,
            "rows_B": diff_stats.table2_count,
            "exclusive_A": diff_stats.diff_by_sign["-"],
            "exclusive_B": diff_stats.diff_by_sign["+"],
            "updated": diff_stats.diff_by_sign["!"],
            "unchanged": diff_stats.unchanged,
            "total": sum(diff_stats.diff_by_sign.values()),
            "stats": self.stats,
        }

        return json_output


class TableDiffer(ThreadBase, ABC):
    bisection_factor = 32
    stats: dict = {}

    def diff_tables(self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree = None) -> DiffResultWrapper:
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
        if info_tree is None:
            info_tree = InfoTree(SegmentInfo([table1, table2]))
        return DiffResultWrapper(self._diff_tables_wrapper(table1, table2, info_tree), info_tree, self.stats)

    def _diff_tables_wrapper(self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree) -> DiffResult:
        if is_tracking_enabled():
            options = dict(self)
            options["differ_name"] = type(self).__name__
            event_json = create_start_event_json(options)
            run_as_daemon(send_event_json, event_json)

        start = time.monotonic()
        error = None
        try:

            # Query and validate schema
            table1, table2 = self._threaded_call("with_schema", [table1, table2])
            self._validate_and_adjust_columns(table1, table2)

            yield from self._diff_tables_root(table1, table2, info_tree)

        except BaseException as e:  # Catch KeyboardInterrupt too
            error = e
        finally:
            info_tree.aggregate_info()

            if is_tracking_enabled():
                runtime = time.monotonic() - start
                rowcounts = info_tree.info.rowcounts
                table1_count = rowcounts[1] if rowcounts else None
                table2_count = rowcounts[2] if rowcounts else None
                diff_count = info_tree.info.diff_count
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

    def _diff_tables_root(self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree) -> DiffResult:
        return self._bisect_and_diff_tables(table1, table2, info_tree)

    @abstractmethod
    def _diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        max_rows: int,
        level=0,
        segment_index=None,
        segment_count=None,
    ):
        ...

    def _bisect_and_diff_tables(self, table1, table2, info_tree):
        if len(table1.key_columns) > 1:
            raise NotImplementedError("Composite key not supported yet!")
        if len(table2.key_columns) > 1:
            raise NotImplementedError("Composite key not supported yet!")
        if len(table1.key_columns) != len(table2.key_columns):
            raise ValueError("Tables should have an equivalent number of key columns!")
        (key1,) = table1.key_columns
        (key2,) = table2.key_columns

        key_type = table1._schema[key1]
        key_type2 = table2._schema[key2]
        if not isinstance(key_type, IKey):
            raise NotImplementedError(f"Cannot use column of type {key_type} as a key")
        if not isinstance(key_type2, IKey):
            raise NotImplementedError(f"Cannot use column of type {key_type2} as a key")
        if key_type.python_type is not key_type2.python_type:
            raise TypeError(f"Incompatible key types: {key_type} and {key_type2}")

        # Query min/max values
        key_ranges = self._threaded_call_as_completed("query_key_range", [table1, table2])

        # Start with the first completed value, so we don't waste time waiting
        min_key1, max_key1 = self._parse_key_range_result(key_type, next(key_ranges))

        table1, table2 = [t.new(min_key=min_key1, max_key=max_key1) for t in (table1, table2)]

        logger.info(
            f"Diffing segments at key-range: {table1.min_key}..{table2.max_key}. "
            f"size: table1 <= {table1.approximate_size()}, table2 <= {table2.approximate_size()}"
        )

        ti = ThreadedYielder(self.max_threadpool_size)
        # Bisect (split) the table into segments, and diff them recursively.
        ti.submit(self._bisect_and_diff_segments, ti, table1, table2, info_tree)

        # Now we check for the second min-max, to diff the portions we "missed".
        min_key2, max_key2 = self._parse_key_range_result(key_type, next(key_ranges))

        if min_key2 < min_key1:
            pre_tables = [t.new(min_key=min_key2, max_key=min_key1) for t in (table1, table2)]
            ti.submit(self._bisect_and_diff_segments, ti, *pre_tables, info_tree)

        if max_key2 > max_key1:
            post_tables = [t.new(min_key=max_key1, max_key=max_key2) for t in (table1, table2)]
            ti.submit(self._bisect_and_diff_segments, ti, *post_tables, info_tree)

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
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        level=0,
        max_rows=None,
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
            info_node = info_tree.add_node(t1, t2, max_rows=max_rows)
            ti.submit(
                self._diff_segments, ti, t1, t2, info_node, max_rows, level + 1, i + 1, len(segmented1), priority=level
            )
