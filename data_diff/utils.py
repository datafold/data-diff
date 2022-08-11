import re
import math
from typing import Iterable, Tuple, Union, Any, Sequence
from typing import TypeVar, Generic
from abc import ABC, abstractmethod
from urllib.parse import urlparse
from uuid import UUID
import operator
import string

alphanums = string.digits + string.ascii_lowercase


def safezip(*args):
    "zip but makes sure all sequences are the same length"
    assert len(set(map(len, args))) == 1
    return zip(*args)


def split_space(start, end, count):
    size = end - start
    assert count <= size, (count, size)
    return list(range(start, end, (size + 1) // (count + 1)))[1 : count + 1]


class ArithString:
    @classmethod
    def new(cls, *args, **kw):
        return cls(*args, **kw)


class ArithUUID(UUID, ArithString):
    "A UUID that supports basic arithmetic (add, sub)"

    def __int__(self):
        return self.int

    def __add__(self, other: Union[UUID, int]):
        if isinstance(other, int):
            return self.new(int=self.int + other)
        return NotImplemented

    def __sub__(self, other: Union[UUID, int]):
        if isinstance(other, int):
            return self.new(int=self.int - other)
        elif isinstance(other, UUID):
            return self.int - other.int
        return NotImplemented


def numberToBase(num, base):
    digits = []
    while num > 0:
        num, remainder = divmod(num, base)
        digits.append(remainder)
    return "".join(alphanums[i] for i in digits[::-1])


class ArithAlphanumeric(ArithString):
    def __init__(self, str: str = None, int: int = None, max_len=None):
        if str is None:
            str = numberToBase(int, len(alphanums))
        else:
            assert int is None

        if max_len and len(str) > max_len:
            raise ValueError(f"Length of alphanum value '{str}' is longer than the expected {max_len}")

        self._str = str
        self._max_len = max_len

    @property
    def int(self):
        return int(self._str, len(alphanums))

    def __str__(self):
        s = self._str
        if self._max_len:
            s = s.rjust(self._max_len, "0")
        return s

    def __len__(self):
        return len(self._str)

    def __int__(self):
        return self.int

    def __repr__(self):
        return f'alphanum"{self._str}"'

    def __add__(self, other: "Union[ArithAlphanumeric, int]"):
        if isinstance(other, int):
            res = self.new(int=self.int + other)
            if len(str(res)) != len(self):
                raise ValueError("Overflow error when adding to alphanumeric")
            return res
        return NotImplemented

    def __sub__(self, other: "Union[ArithAlphanumeric, int]"):
        if isinstance(other, int):
            return type(self)(int=self.int - other)
        elif isinstance(other, ArithAlphanumeric):
            return self.int - other.int
        return NotImplemented

    def __ge__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.int >= other.int

    def __lt__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.int < other.int

    def new(self, *args, **kw):
        return type(self)(*args, **kw, max_len=self._max_len)


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


def _join_if_any(sym, args):
    args = list(args)
    if not args:
        return ""
    return sym.join(str(a) for a in args if a)


def remove_password_from_url(url: str, replace_with: str = "***") -> str:
    parsed = urlparse(url)
    account = parsed.username or ""
    if parsed.password:
        account += ":" + replace_with
    host = _join_if_any(":", filter(None, [parsed.hostname, parsed.port]))
    netloc = _join_if_any("@", filter(None, [account, host]))
    replaced = parsed._replace(netloc=netloc)
    return replaced.geturl()


def join_iter(joiner: Any, iterable: Iterable) -> Iterable:
    it = iter(iterable)
    yield next(it)
    for i in it:
        yield joiner
        yield i


V = TypeVar("V")


class CaseAwareMapping(ABC, Generic[V]):
    @abstractmethod
    def get_key(self, key: str) -> str:
        ...

    @abstractmethod
    def __getitem__(self, key: str) -> V:
        ...

    @abstractmethod
    def __setitem__(self, key: str, value: V):
        ...

    @abstractmethod
    def __contains__(self, key: str) -> bool:
        ...


class CaseInsensitiveDict(CaseAwareMapping):
    def __init__(self, initial):
        self._dict = {k.lower(): (k, v) for k, v in dict(initial).items()}

    def get_key(self, key: str) -> str:
        return self._dict[key.lower()][0]

    def __getitem__(self, key: str) -> V:
        return self._dict[key.lower()][1]

    def __setitem__(self, key: str, value):
        k = key.lower()
        if k in self._dict:
            key = self._dict[k][0]
        self._dict[k] = key, value

    def __contains__(self, key):
        return key.lower() in self._dict

    def keys(self) -> Iterable[str]:
        return self._dict.keys()

    def items(self) -> Iterable[Tuple[str, V]]:
        return ((k, v[1]) for k, v in self._dict.items())


class CaseSensitiveDict(dict, CaseAwareMapping):
    def get_key(self, key):
        return key

    def as_insensitive(self):
        return CaseInsensitiveDict(self)


def match_like(pattern: str, strs: Sequence[str]) -> Iterable[str]:
    reo = re.compile(pattern.replace("%", ".*").replace("?", ".") + "$")
    for s in strs:
        if reo.match(s):
            yield s


def accumulate(iterable, func=operator.add, *, initial=None):
    'Return running totals'
    # Taken from https://docs.python.org/3/library/itertools.html#itertools.accumulate, to backport 'initial' to 3.7
    it = iter(iterable)
    total = initial
    if initial is None:
        try:
            total = next(it)
        except StopIteration:
            return
    yield total
    for element in it:
        total = func(total, element)
        yield total
