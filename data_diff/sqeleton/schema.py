import logging

from .utils import CaseAwareMapping, CaseInsensitiveDict, CaseSensitiveDict
from .abcs import AbstractDatabase, DbPath

logger = logging.getLogger("schema")

Schema = CaseAwareMapping


def create_schema(db: AbstractDatabase, table_path: DbPath, schema: dict, case_sensitive: bool) -> CaseAwareMapping:
    logger.debug(f"[{db.name}] Schema = {schema}")

    if case_sensitive:
        return CaseSensitiveDict(schema)

    if len({k.lower() for k in schema}) < len(schema):
        logger.warning(f'Ambiguous schema for {db}:{".".join(table_path)} | Columns = {", ".join(list(schema))}')
        logger.warning("We recommend to disable case-insensitivity (set --case-sensitive).")
    return CaseInsensitiveDict(schema)
