from typing import Generator

from ..abcs import DbPath, DbKey
from ..schema import Schema


class _SKIP:
    def __repr__(self):
        return "SKIP"


SKIP = _SKIP()


class SqeletonError(Exception):
    pass


def args_as_tuple(exprs):
    if len(exprs) == 1:
        (e,) = exprs
        if isinstance(e, Generator):
            return tuple(e)
    return exprs
