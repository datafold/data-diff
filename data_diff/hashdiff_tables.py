import os
from numbers import Number
import logging
from collections import defaultdict
from typing import Iterator
from operator import attrgetter

from runtype import dataclass

from data_diff.sqeleton.abcs import ColType_UUID, NumericType, PrecisionType, StringType, Boolean, JSON

from .info_tree import InfoTree
from .utils import safezip, diffs_are_equiv_jsons
from .thread_utils import ThreadedYielder
from .table_segment import TableSegment

from .diff_tables import TableDiffer

BENCHMARK = os.environ.get("BENCHMARK", False)

DEFAULT_BISECTION_THRESHOLD = 1024 * 16
DEFAULT_BISECTION_FACTOR = 32

logger = logging.getLogger("hashdiff_tables")


def diff_sets(a: list, b: list, json_cols: dict = None) -> Iterator:
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

    warned_diff_cols = set()
    for _k, v in sorted(d.items(), key=lambda i: i[0]):
        if json_cols:
            parsed_match, overriden_diff_cols = diffs_are_equiv_jsons(v, json_cols)
            if parsed_match:
                to_warn = overriden_diff_cols - warned_diff_cols
                for w in to_warn:
                    logger.warning(
                        f"Equivalent JSON objects with different string representations detected "
                        f"in column '{w}'. These cases are NOT reported as differences."
                    )
                    warned_diff_cols.add(w)
                continue
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
            json_cols = {
                i: colname
                for i, colname in enumerate(table1.extra_columns)
                if isinstance(table1._schema[colname], JSON)
            }
            diff = list(diff_sets(rows1, rows2, json_cols))

            info_tree.info.set_diff(diff)
            info_tree.info.rowcounts = {1: len(rows1), 2: len(rows2)}

            logger.info(". " * level + f"Diff found {len(diff)} different rows.")
            self.stats["rows_downloaded"] = self.stats.get("rows_downloaded", 0) + max(len(rows1), len(rows2))
            return diff

        return super()._bisect_and_diff_segments(ti, table1, table2, info_tree, level, max_rows)
