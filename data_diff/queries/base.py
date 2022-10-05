from typing import Generator

from data_diff.databases.database_types import DbPath, DbKey, Schema


class _SKIP:
    def __repr__(self):
        return 'SKIP'

SKIP = _SKIP()


class CompileError(Exception):
    pass


def args_as_tuple(exprs):
    if len(exprs) == 1:
        (e,) = exprs
        if isinstance(e, Generator):
            return tuple(e)
    return exprs
