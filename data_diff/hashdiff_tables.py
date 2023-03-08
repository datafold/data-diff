from functools import partial
import os
from numbers import Number
import logging
from collections import defaultdict
import sys
from typing import Iterator, List
from operator import attrgetter, methodcaller

from runtype import dataclass

from sqeleton.abcs import ColType_UUID, NumericType, PrecisionType, StringType, Boolean, IKey

from .info_tree import InfoTree
from .utils import safezip
from .thread_utils import ThreadedYielder
from .table_segment import TableSegment

from .diff_tables import TableDiffer

BENCHMARK = os.environ.get("BENCHMARK", False)

DEFAULT_BISECTION_THRESHOLD = 1024 * 16
DEFAULT_BISECTION_FACTOR = 32

logger = logging.getLogger("hashdiff_tables")


def diff_sets(a: set, b: set) -> Iterator:
    sa = set(a)
    sb = set(b)

    # The first item is always the key (see TableDiffer.relevant_columns)
    # TODO update when we add compound keys to hashdiff
    d = defaultdict(list)
    for row in a:
        if row not in sb:
            d[row[0]].append(("-", row))
    for row in b:
        if row not in sa:
            d[row[0]].append(("+", row))

    for _k, v in sorted(d.items(), key=lambda i: i[0]):
        yield from v


@dataclass
class HashDiffer(TableDiffer):
    """Finds the diff between two SQL tables

    The algorithm uses hashing to quickly check if the tables are different, and then applies a
    bisection search recursively to find the differences efficiently.

    Works best for comparing tables that are mostly the same, with minor discrepancies.

    Parameters:
        bisection_factor (int): Into how many segments to bisect per iteration.
        bisection_threshold (Number): When should we stop bisecting and compare locally (in row count).
        threaded (bool): Enable/disable threaded diffing. Needed to take advantage of database threads.
        max_threadpool_size (int): Maximum size of each threadpool. ``None`` means auto.
                                   Only relevant when `threaded` is ``True``.
                                   There may be many pools, so number of actual threads can be a lot higher.
    """

    bisection_factor: int = DEFAULT_BISECTION_FACTOR
    bisection_threshold: Number = DEFAULT_BISECTION_THRESHOLD  # Accepts inf for tests

    stats: dict = {}

    def __post_init__(self):
        # Validate options
        if self.bisection_factor >= self.bisection_threshold:
            raise ValueError("Incorrect param values (bisection factor must be lower than threshold)")
        if self.bisection_factor < 2:
            raise ValueError("Must have at least two segments per iteration (i.e. bisection_factor >= 2)")

    def _validate_and_adjust_columns(self, table1, table2):
        for c1, c2 in safezip(table1.relevant_columns, table2.relevant_columns):
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

            elif isinstance(col1, (NumericType, Boolean)):
                if not isinstance(col2, (NumericType, Boolean)):
                    raise TypeError(f"Incompatible types for column '{c1}':  {col1} <-> {col2}")

                lowest = min(col1, col2, key=attrgetter("precision"))

                if col1.precision != col2.precision:
                    logger.warning(f"Using reduced precision {lowest} for column '{c1}'. Types={col1}, {col2}")

                if lowest.precision != col1.precision:
                    table1._schema[c1] = col1.replace(precision=lowest.precision)
                if lowest.precision != col2.precision:
                    table2._schema[c2] = col2.replace(precision=lowest.precision)

            elif isinstance(col1, ColType_UUID):
                if not isinstance(col2, ColType_UUID):
                    raise TypeError(f"Incompatible types for column '{c1}':  {col1} <-> {col2}")

            elif isinstance(col1, StringType):
                if not isinstance(col2, StringType):
                    raise TypeError(f"Incompatible types for column '{c1}':  {col1} <-> {col2}")

        for t in [table1, table2]:
            for c in t.relevant_columns:
                ctype = t._schema[c]
                if not ctype.supported:
                    logger.warning(
                        f"[{t.database.name}] Column '{c}' of type '{ctype}' has no compatibility handling. "
                        "If encoding/formatting differs between databases, it may result in false positives."
                    )

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
                return self._bisect_and_diff_segments(ti, table1, table2, info_tree, level=level, max_rows=max_rows)

        (count1, checksum1), (count2, checksum2) = self._threaded_call("count_and_checksum", [table1, table2])

        assert not info_tree.info.rowcounts
        info_tree.info.rowcounts = {1: count1, 2: count2}

        if count1 == 0 and count2 == 0:
            logger.debug(
                "Uneven distribution of keys detected in segment %s..%s (big gaps in the key column). "
                "For better performance, we recommend to increase the bisection-threshold.",
                table1.min_key,
                table1.max_key,
            )
            assert checksum1 is None and checksum2 is None
            info_tree.info.is_diff = False
            return

        if checksum1 == checksum2:
            info_tree.info.is_diff = False
            return

        info_tree.info.is_diff = True
        return self._bisect_and_diff_segments(ti, table1, table2, info_tree, level=level, max_rows=max(count1, count2))

    def _bisect_and_diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        level=0,
        max_rows=None,
    ):
        logging.info('HashDiffer._bisect_and_diff_segments')

        assert table1.is_bounded and table2.is_bounded

        max_space_size = max(table1.approximate_size(), table2.approximate_size())
        if max_rows is None:
            # We can be sure that row_count <= max_rows iff the table key is unique
            max_rows = max_space_size
            info_tree.info.max_rows = max_rows

        # If count is below the threshold, just download and compare the columns locally
        # This saves time, as bisection speed is limited by ping and query performance.
        if max_rows < self.bisection_threshold or max_space_size < self.bisection_factor * 2:
            rows1, rows2 = self._threaded_call("get_values", [table1, table2])
            diff = list(diff_sets(rows1, rows2))

            info_tree.info.set_diff(diff)
            info_tree.info.rowcounts = {1: len(rows1), 2: len(rows2)}

            logger.info(". " * level + f"Diff found {len(diff)} different rows.")
            self.stats["rows_downloaded"] = self.stats.get("rows_downloaded", 0) + max(len(rows1), len(rows2))
            return diff

        return super()._bisect_and_diff_segments(ti, table1, table2, info_tree, level, max_rows)


@dataclass
class SinglePassHashDiffer(HashDiffer):

    stats: dict = {}

    def _diff_segments(
        self,
        ti: ThreadedYielder,
        segment1: TableSegment,
        segment2: TableSegment,
        result1: List,
        result2: List,
        info_tree: InfoTree,
        max_rows: int,
        level=0,
        segment_index=None,
        segment_count=None,
    ):
        _, count1, checksum1 = result1
        _, count2, checksum2 = result2

        info_tree.info.rowcounts = {1: count1, 2: count2}

        if count1 == 0 and count2 == 0:
            logger.debug(
                "Uneven distribution of keys detected in segment %s..%s (big gaps in the key column). "
                "For better performance, we recommend to increase the bisection-threshold.",
                segment1.min_key,
                segment1.max_key,
            )
            assert checksum1 is None and checksum2 is None
            info_tree.info.is_diff = False
            return

        if checksum1 == checksum2:
            info_tree.info.is_diff = False
            return

        info_tree.info.is_diff = True
        return self._bisect_and_diff_segments(ti, segment1, segment2, info_tree, level=level, max_rows=max(count1, count2))

    def _bisect_and_diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        level=0,
        max_rows=None,
    ):
        logging.info('SinglePassHashDiffer._bisect_and_diff_segments')

        assert table1.is_bounded and table2.is_bounded

        max_space_size = max(table1.approximate_size(), table2.approximate_size())
        # logging.info(f'max_space_size: {max_space_size}')
        if max_rows is None:
            # We can be sure that row_count <= max_rows iff the table key is unique
            max_rows = max_space_size
            info_tree.info.max_rows = max_rows

        # If count is below the threshold, just download and compare the columns locally
        # This saves time, as bisection speed is limited by ping and query performance.
        # NOTE: Instead of using max_space_size for 2nd comparison, using max_rows since its the ACTUAL count of rows
        #       as retreived from the last query. This especially important when dealing with a composite PK table 
        #       because the approximate_size is likely a gross underestimation.
        if max_rows < self.bisection_threshold or max_rows < self.bisection_factor * 2:
            logging.info('get_values')
            rows1, rows2 = self._threaded_call("get_values", [table1, table2])
            diff = list(diff_sets(rows1, rows2))

            info_tree.info.set_diff(diff)
            info_tree.info.rowcounts = {1: len(rows1), 2: len(rows2)}

            logger.info(". " * level + f"Diff found {len(diff)} different rows.")
            self.stats["rows_downloaded"] = self.stats.get("rows_downloaded", 0) + max(len(rows1), len(rows2))
            return diff

        # Choose evenly spaced checkpoints (according to min_key and max_key)
        biggest_table = max(table1, table2, key=methodcaller("approximate_size"))
        checkpoints = biggest_table.choose_checkpoints(self.bisection_factor - 1)
        # logging.info(f'checkpoints: {checkpoints}')

        if table1.hash_query_type == 'multi' and table2.hash_query_type == 'multi':
            raise ValueError('At least 1 table must use the groupby hash query type')

        segmented1 = table1.segment_by_checkpoints(checkpoints)
        segmented2 = table2.segment_by_checkpoints(checkpoints)

        bg_funcs = {'t1': [], 't2': []}
        if table1.hash_query_type == 'multi':
            bg_funcs['t1'] = [seg.count_and_checksum for seg in segmented1]
        else:
            bg_funcs['t1'] = [partial(table1.count_and_checksum_by_group, checkpoints[0], self.bisection_factor, optimizer_hints=(level == 0))]

        if table2.hash_query_type == 'multi':
            bg_funcs['t2'] = [seg.count_and_checksum for seg in segmented2]
        else:
            bg_funcs['t2'] = [partial(table2.count_and_checksum_by_group, checkpoints[0], self.bisection_factor, optimizer_hints=(level == 0))]

        logging.info(f'Running in background: {len(bg_funcs["t1"]) + len(bg_funcs["t2"])}')

        def print_res(label, res):
            out = [str(r) for r in res]
            logging.info('{}\n{}'.format(label, "\n".join(out)))
        
        # wait for all queries to complete
        all_results = ti._submit_and_block(*bg_funcs['t1'], *bg_funcs['t2'], priority=level)
        table1_res = all_results[:len(bg_funcs['t1'])]
        table2_res = all_results[len(bg_funcs['t1']):]

        if len(bg_funcs['t1']) == 1:
            table1_res = table1_res[0]
        
        if len(bg_funcs['t2']) == 1:
            table2_res = table2_res[0]

        print_res('table1_res', table1_res)
        print_res('table2_res', table2_res)

        # compare results for each segment in parallel
        for idx, (res1, res2, seg1, seg2) in enumerate(zip(table1_res, table2_res, segmented1, segmented2)):
            info_node = info_tree.add_node(seg1, seg2, max_rows=max_rows)
            ti.submit(self._diff_segments, ti, seg1, seg2, res1, res2, info_node, max_rows, level + 1, idx+1, len(segmented1)) #), priority=level)

    def _resolve_key_range(self, key_range_res, usr_key_range):
        key_range_res = list(key_range_res)
        if usr_key_range[0] is not None:
            key_range_res[0] = usr_key_range[0]
        if usr_key_range[1] is not None:
            key_range_res[1] = usr_key_range[1]
        return tuple(key_range_res)

    def _bisect_and_diff_tables(self, table1, table2, info_tree):
        logging.info('SinglePassHashDiffer._bisect_and_diff_tables')
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
        usr_key_range = (table1.min_key, table1.max_key)
        if all(k is not None for k in [table1.min_key, table1.max_key, table2.min_key, table2.max_key]):
            key_ranges = (kr for kr in [(table1.min_key, table1.max_key), (table2.min_key, table2.max_key)])
        else:
            # Query min/max values
            key_ranges = self._threaded_call_as_completed("query_key_range", [table1, table2])

        # Wait for both
        min_key1, max_key1 = self._parse_key_range_result((key_type,), self._resolve_key_range(next(key_ranges), usr_key_range))
        min_key2, max_key2 = self._parse_key_range_result((key_type,), self._resolve_key_range(next(key_ranges), usr_key_range))

        min_key = min(min_key1, min_key2)
        max_key = max(max_key1, max_key2)

        table1, table2 = [t.new(min_key=min_key, max_key=max_key) for t in (table1, table2)]

        logger.info(
            f"Diffing segments at key-range: {table1.min_key}..{table2.max_key}. "
            f"size: table1 <= {table1.approximate_size()}, table2 <= {table2.approximate_size()}"
        )

        ti = ThreadedYielder(self.max_threadpool_size)
        # Bisect (split) the table into segments, and diff them recursively.
        ti.submit(self._bisect_and_diff_segments, ti, table1, table2, info_tree)
        # self._bisect_and_diff_segments(ti, table1, table2, info_tree)

        # TODO: I don't think we need to do this part since we already got min/max keys for both tables up front
        # # Now we check for the second min-max, to diff the portions we "missed".
        # min_key2, max_key2 = self._parse_key_range_result(key_type, next(key_ranges))

        # if min_key2 < min_key1:
        #     pre_tables = [t.new(min_key=min_key2, max_key=min_key1) for t in (table1, table2)]
        #     ti.submit(self._bisect_and_diff_segments, ti, *pre_tables, info_tree)

        # if max_key2 > max_key1:
        #     post_tables = [t.new(min_key=max_key1, max_key=max_key2) for t in (table1, table2)]
        #     ti.submit(self._bisect_and_diff_segments, ti, *post_tables, info_tree)

        return ti