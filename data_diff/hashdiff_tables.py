from functools import partial
import os
from numbers import Number
import logging
from collections import defaultdict
from typing import Any, Callable, Iterator, List, Tuple
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


def diff_sets(a: set, b: set, key_indices: list = None) -> Iterator:
    sa = set(a)
    sb = set(b)

    if key_indices == None:
        key_indices = [0]

    # NOTE: updated to support sorting on multiple PK columns (compound keys)
    d = defaultdict(list)
    for row in a:
        key = tuple(row[idx] for idx in key_indices)
        if row not in sb:
            d[key].append(("-", row))
    for row in b:
        key = tuple(row[idx] for idx in key_indices)
        if row not in sa:
            d[key].append(("+", row))
    
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

        super().__post_init__()
        
    def _validate_and_adjust_columns(self, table1, table2):
        for c1, c2 in safezip(table1.relevant_columns, table2.relevant_columns):
            if c1 not in table1._schema:
                raise ValueError(f"Column '{c1}' not found in schema for table {table1}")
            if c2 not in table2._schema:
                raise ValueError(f"Column '{c2}' not found in schema for table {table2}")

            # Update schemas to minimal mutual precision
            col1 = table1._schema[c1]
            col2 = table2._schema[c2]

            # if user passed specialized conversions for either column, skip validation
            t1_overrides = {**table1.col_conversions, **table1.column_type_overrides}
            t2_overrides = {**table2.col_conversions, **table2.column_type_overrides}
            if any(c in t1_overrides for c in [c1.lower(), c1.upper()]):
                continue
            if any(c in t2_overrides for c in [c2.lower(), c2.upper()]):
                continue

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

    def _remove_diffs_outside_update_range(self, 
                                           diff: list, 
                                           tbl1: TableSegment, 
                                           tbl2: TableSegment) -> list:
        def keep_outside_update_range(r):
            _, row = r
            update_col_idx = tbl1.update_col_idx

            if not row[update_col_idx]:
                # update column is null, exclude from outside range list
                return False

            if tbl1.max_update and row[update_col_idx] >= tbl1.max_update.strftime('%Y-%m-%d %H:%M:%S'):
                return True
            if tbl1.min_update and row[update_col_idx] < tbl1.min_update.strftime('%Y-%m-%d %H:%M:%S'):
                return True
            return False

        def extract_key(r):
            _, row = r
            return tuple(row[idx] for idx in tbl1.key_indices)
        
        # only filter if both tables have the same update range
        if tbl1.update_column is not None and tbl2.update_column is not None and \
            tbl1.max_update == tbl2.max_update and tbl1.min_update == tbl2.min_update:

            # get rows outside update range
            pks_outside_urange = [extract_key(d) for d in diff if keep_outside_update_range(d)]

            # remove any row from diff whose PK is in outside_urange
            modified = [d for d in diff if extract_key(d) not in pks_outside_urange]
            
            if len(modified) < len(diff):
                logging.info(f'Discarded {len(diff) - len(modified)} rows outside update range ({tbl1.min_update} - {tbl1.max_update})')
            return modified

        return diff

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
        seg_path=''
    ):
        segment_path = f'{seg_path}' if seg_path else ''
        logger.info(
            ". " * level + f"Diffing segment {segment_path}/{segment_count}, "
            f"key-range: {table1.min_key}..{table2.max_key}, "
            f"size <= {max_rows}"
        )

        # When benchmarking, we want the ability to skip checksumming. This
        # allows us to download all rows for comparison in performance. By
        # default, data-diff will checksum the section first (when it's below
        # the threshold) and _then_ download it.
        if BENCHMARK:
            if max_rows < self.bisection_threshold:
                return self._bisect_and_diff_segments(ti, table1, table2, info_tree, level=level, max_rows=max_rows, seg_path=segment_path)

        (_, count1, checksum1), (_, count2, checksum2) = self._threaded_call("count_and_checksum", [table1, table2])

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

        logging.info(f'Mismatch checksum {table1.min_key} - {table2.max_key}\n'
                     f'T1: cs={checksum1}, count={count1}\n'
                     f'T2: cs={checksum2}, count={count2}')

        info_tree.info.is_diff = True
        return self._bisect_and_diff_segments(ti, table1, table2, info_tree, level=level, max_rows=max(count1, count2), seg_path=segment_path)

    def _bisect_and_diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        level=0,
        max_rows=None,
        seg_path=''
    ):
        assert table1.is_bounded and table2.is_bounded

        max_space_size = max(table1.approximate_size(), table2.approximate_size())
        if max_rows is None:
            # We can be sure that row_count <= max_rows iff the table key is unique
            max_rows = max_space_size
            info_tree.info.max_rows = max_rows

        # If count is below the threshold, just download and compare the columns locally
        # This saves time, as bisection speed is limited by ping and query performance.
        if max_rows < self.bisection_threshold or max_space_size < self.bisection_factor * 2:
            logging.info(f'Downloading rows from T1 {table1.min_key} - {table1.max_key}')
            logging.info(f'Downloading rows from T2 {table2.min_key} - {table2.max_key}')
            self.set_query_timeouts([table1, table2])

            rows1, rows2 = self._threaded_call("get_values", [table1, table2])
            diff = list(diff_sets(rows1, rows2, table1.key_indices))

            diff = self._remove_diffs_outside_update_range(diff, table1, table2)

            info_tree.info.set_diff(diff)
            info_tree.info.rowcounts = {1: len(rows1), 2: len(rows2)}

            logger.info(". " * level + f"Diff found {len(diff)} different rows.")
            self.stats["rows_downloaded"] = self.stats.get("rows_downloaded", 0) + max(len(rows1), len(rows2))
            return diff

        return super()._bisect_and_diff_segments(ti, table1, table2, info_tree, level, max_rows, seg_path=seg_path)


@dataclass
class GroupingHashDiffer(HashDiffer):

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
        seg_path=''
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

        logging.info(f'Mismatch checksum {segment1.min_key} - {segment1.max_key}\n'
                     f'T1: cs={checksum1}, count={count1}\n'
                     f'T2: cs={checksum2}, count={count2}')

        info_tree.info.is_diff = True
        return self._bisect_and_diff_segments(ti, segment1, segment2, info_tree, level=level, max_rows=max(count1, count2), seg_path=seg_path)

    def query_wrapper(self, query_fn: Callable, state: dict, *args: Any, **kwargs: Any) -> Tuple[Tuple[int, int]]:
        seg_path = state['seg_path']
        segment_path = f'{seg_path}' if seg_path else ''

        logger.info(
            ". " * state['level'] + f"Hashing segment {segment_path}/{state['total']}, "
            f"key-range: {state['segment'].min_key}..{state['segment'].max_key}, "
            f"size <= {state['max_rows']}"
        )

        return query_fn(*args, **kwargs)

    def _bisect_and_diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        level=0,
        max_rows=None,
        seg_path=''
    ):

        assert table1.is_bounded and table2.is_bounded

        max_space_size = max(table1.approximate_size(), table2.approximate_size())
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
            self.set_query_timeouts([table1, table2])
            rows1, rows2 = self._threaded_call("get_values", [table1, table2])
            logging.info(f'rows1: {len(rows1)}')
            logging.info(f'rows2: {len(rows2)}')
            diff = list(diff_sets(rows1, rows2, table1.key_indices))

            diff = self._remove_diffs_outside_update_range(diff, table1, table2)

            info_tree.info.set_diff(diff)
            info_tree.info.rowcounts = {1: len(rows1), 2: len(rows2)}

            logger.info(". " * level + f"Diff found {len(diff)} different rows.")
            self.stats["rows_downloaded"] = self.stats.get("rows_downloaded", 0) + max(len(rows1), len(rows2))
            return diff

        # Choose evenly spaced checkpoints (according to min_key and max_key)
        biggest_table = max(table1, table2, key=methodcaller("approximate_size"))
        checkpoints = biggest_table.choose_checkpoints(self.bisection_factor - 1)
        logger.info(f'checkpoints: {checkpoints}')

        if table1.hash_query_type == 'multi' and table2.hash_query_type == 'multi':
            raise ValueError('At least 1 table must use the groupby hash query type')

        segmented1 = table1.segment_by_checkpoints(checkpoints)
        segmented2 = table2.segment_by_checkpoints(checkpoints)

        def _state(seg: TableSegment, idx: int, total: int) -> dict:
            return {
                'level': level, 'segment': seg, 
                'idx': idx, 'total': total, 
                'max_rows': max_rows, 'seg_path': seg_path
            }

        def print_res(label, res):
            out = [str(r) for r in res]
            logger.info('{}\n{}'.format(label, "\n".join(out)))

        query_fns = []
        if table1.hash_query_type == 'multi':
            for idx, seg in enumerate(segmented1):
                query_fns.append(partial(self.query_wrapper,
                                      seg.count_and_checksum, 
                                      _state(seg, idx, len(segmented1))))
        else:
            logger.info(
                ". " * level + "Hashing table-1 segments by group "
                f"key-range: {table1.min_key}..{table1.max_key}, "
                f"size <= {max_rows}"
            )
            query_fns.append(partial(table1.count_and_checksum_by_group, checkpoints[0], self.bisection_factor, optimizer_hints=(level == 0)))

        if table2.hash_query_type == 'multi':
            for idx, seg in enumerate(segmented2):
                query_fns.append(partial(self.query_wrapper,
                                seg.count_and_checksum, 
                                _state(seg, idx, len(segmented2))))
        else:
            logger.info(
                ". " * level + "Hashing table-2 segments by group "
                f"key-range: {table2.min_key}..{table2.max_key}, "
                f"size <= {max_rows}"
            )
            query_fns.insert(0, partial(table2.count_and_checksum_by_group, checkpoints[0], self.bisection_factor, optimizer_hints=(level == 0)))


        self.set_query_timeouts([table1, table2])

        # wait for all queries to complete.
        all_results = ti._submit_and_block(*query_fns, priority=level)

        if table2.hash_query_type != 'multi':
            table2_res = all_results[0]
            table1_res = all_results[1:] if table1.hash_query_type == 'multi' else all_results[1]
        else:
            table1_res = all_results[0]
            table2_res = all_results[1:] if table2.hash_query_type == 'multi' else all_results[1]

        print_res('table1_res', table1_res)
        print_res('table2_res', table2_res)

        # compare results for each segment in parallel
        for idx, (res1, res2, seg1, seg2) in enumerate(zip(table1_res, table2_res, segmented1, segmented2)):
            info_node = info_tree.add_node(seg1, seg2, max_rows=max_rows)

            if level == 0:
                segment_path = f'{idx+1}'
            else:
                segment_path = f'{seg_path}.{idx+1}'

            ti.submit(self._diff_segments, ti, seg1, seg2, res1, res2, info_node, max_rows, level + 1, idx+1, len(segmented1), seg_path=segment_path)

    def _resolve_key_range(self, key_range_res, usr_key_range):
        key_range_res = list(key_range_res)
        for idx, uk in enumerate(usr_key_range):
            if uk is not None:
                key_range_res[idx] = usr_key_range[idx]
        return tuple(key_range_res)

    def _bisect_and_diff_tables(self, table1, table2, info_tree):
        if len(table1.key_columns) > 1:
            raise NotImplementedError("Composite key not supported yet!")
        if len(table2.key_columns) > 1:
            raise NotImplementedError("Composite key not supported yet!")
        if len(table1.key_columns) != len(table2.key_columns):
            raise ValueError("Tables should have an equivalent number of key columns!")
        (key1,) = table1.key_columns
        (key2,) = table2.key_columns

        key_types1 = [table1._schema[i] for i in table1.key_columns]
        key_types2 = [table2._schema[i] for i in table2.key_columns]

        for kt in key_types1 + key_types2:
            if not isinstance(kt, IKey):
                raise NotImplementedError(f"Cannot use a column of type {kt} as a key")

        for kt1, kt2 in safezip(key_types1, key_types2):
            if kt1.python_type is not kt2.python_type:
                raise TypeError(f"Incompatible key types: {kt1} and {kt2}")

        self.set_query_timeouts([table1, table2])

        # Query min/max values
        if all(k is not None for k in [table1.min_key, table1.max_key, table2.min_key, table2.max_key]):
            key_ranges = (kr for kr in [(table1.min_key, table1.max_key), (table2.min_key, table2.max_key)])
        elif all(k is not None for k in [table1.min_key, table1.max_key]):
            t2_ranges = self._threaded_call_as_completed("query_key_range", [table2])
            key_ranges = (kr for kr in [((table1.min_key), table1.max_key), next(t2_ranges)])
        elif all(k is not None for k in [table2.min_key, table2.max_key]):
            t1_ranges = self._threaded_call_as_completed("query_key_range", [table1])
            key_ranges = (kr for kr in [next(t1_ranges), (table2.min_key, table2.max_key)])
        else:
            # Query min/max values
            key_ranges = self._threaded_call_as_completed("query_key_range", [table1, table2])

        # Wait for both
        min_key1, max_key1 = self._parse_key_range_result(key_types1, next(key_ranges))
        min_key2, max_key2 = self._parse_key_range_result(key_types2, next(key_ranges))

        min_key = min(min_key1, min_key2)
        max_key = max(max_key1, max_key2)

        table1, table2 = [t.new(min_key=min_key, max_key=max_key) for t in (table1, table2)]

        logger.info(
            f"Diffing segments at key-range: {table1.min_key}..{table2.max_key}. "
            f"size: table1 <= {table1.approximate_size()}, table2 <= {table2.approximate_size()}"
        )

        # Bisect (split) the table into segments, and diff them recursively.
        self.ti.submit(self._bisect_and_diff_segments, self.ti, table1, table2, info_tree)

        return self.ti