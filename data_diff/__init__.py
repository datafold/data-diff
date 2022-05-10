from typing import Tuple

from .database import connect_to_uri
from .diff_tables import TableSegment, TableDiffer

def create_source(db_uri: str, table_name: str, key_column: str, extra_columns: Tuple[str, ...] = ()):
    db = connect_to_uri(db_uri)
    return TableSegment(db, (table_name,), key_column, tuple(extra_columns))