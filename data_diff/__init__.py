from typing import Tuple, Iterator, Optional, Union

from .tracking import disable_tracking
from .databases.connect import connect
from .databases.database_types import DbKey, DbTime, DbPath
from .diff_tables import TableSegment, TableDiffer, DEFAULT_BISECTION_THRESHOLD, DEFAULT_BISECTION_FACTOR


def connect_to_table(
    db_info: Union[str, dict],
    table_name: Union[DbPath, str],
    key_column: str = "id",
    thread_count: Optional[int] = 1,
    **kwargs,
):
    """Connects to the given database, and creates a TableSegment instance

    Parameters:
        db_info: Either a URI string, or a dict of connection options.
        table_name: Name of the table as a string, or a tuple that signifies the path.
        key_column: Name of the key column
        thread_count: Number of threads for this connection (only if using a threadpooled implementation)
    """

    db = connect(db_info, thread_count=thread_count)

    if isinstance(table_name, str):
        table_name = db.parse_table_name(table_name)

    return TableSegment(db, table_name, key_column, **kwargs)


def diff_tables(
    table1: TableSegment,
    table2: TableSegment,
    *,
    # Name of the key column, which uniquely identifies each row (usually id)
    key_column: str = None,
    # Name of updated column, which signals that rows changed (usually updated_at or last_update)
    update_column: str = None,
    # Extra columns to compare
    extra_columns: Tuple[str, ...] = None,
    # Start/end key_column values, used to restrict the segment
    min_key: DbKey = None,
    max_key: DbKey = None,
    # Start/end update_column values, used to restrict the segment
    min_update: DbTime = None,
    max_update: DbTime = None,
    # Into how many segments to bisect per iteration
    bisection_factor: int = DEFAULT_BISECTION_FACTOR,
    # When should we stop bisecting and compare locally (in row count)
    bisection_threshold: int = DEFAULT_BISECTION_THRESHOLD,
    # Enable/disable threaded diffing. Needed to take advantage of database threads.
    threaded: bool = True,
    # Maximum size of each threadpool. None = auto. Only relevant when threaded is True.
    # There may be many pools, so number of actual threads can be a lot higher.
    max_threadpool_size: Optional[int] = 1,
    # Enable/disable debug prints
    debug: bool = False,
) -> Iterator:
    """Efficiently finds the diff between table1 and table2.

    Example:
        >>> table1 = connect_to_table('postgresql:///', 'Rating', 'id')
        >>> list(diff_tables(table1, table1))
        []

    """
    tables = [table1, table2]
    override_attrs = {
        k: v
        for k, v in dict(
            key_column=key_column,
            update_column=update_column,
            extra_columns=extra_columns,
            min_key=min_key,
            max_key=max_key,
            min_update=min_update,
            max_update=max_update,
        ).items()
        if v is not None
    }

    segments = [t.new(**override_attrs) for t in tables] if override_attrs else tables

    differ = TableDiffer(
        bisection_factor=bisection_factor,
        bisection_threshold=bisection_threshold,
        debug=debug,
        threaded=threaded,
        max_threadpool_size=max_threadpool_size,
    )
    return differ.diff_tables(*segments)
