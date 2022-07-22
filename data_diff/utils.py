import math

from typing import Sequence, Optional, Tuple, Union, Dict, Any
from uuid import UUID


def safezip(*args):
    "zip but makes sure all sequences are the same length"
    assert len(set(map(len, args))) == 1
    return zip(*args)


def split_space(start, end, count):
    size = end - start
    return list(range(start, end, (size + 1) // (count + 1)))[1 : count + 1]


class ArithUUID(UUID):
    "A UUID that supports basic arithmetic (add, sub)"

    def __add__(self, other: Union[UUID, int]):
        if isinstance(other, int):
            return type(self)(int=self.int + other)
        return NotImplemented

    def __sub__(self, other: Union[UUID, int]):
        if isinstance(other, int):
            return type(self)(int=self.int - other)
        elif isinstance(other, UUID):
            return self.int - other.int
        return NotImplemented

    def __int__(self):
        return self.int


def is_uuid(u):
    try:
        UUID(u)
    except ValueError:
        return False
    return True


def number_to_human(n):
    millnames = ["", "k", "m", "b"]
    n = float(n)
    millidx = max(
        0,
        min(len(millnames) - 1, int(math.floor(0 if n == 0 else math.log10(abs(n)) / 3))),
    )

    return "{:.0f}{}".format(n / 10 ** (3 * millidx), millnames[millidx])


def join_iter(joiner: Any, iterable: iter) -> iter:
    it = iter(iterable)
    yield next(it)
    for i in it:
        yield joiner
        yield i
