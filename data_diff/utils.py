import logging
import re
import math
from typing import Iterable, Iterator, MutableMapping, Union, Any, Sequence, Dict
from typing import TypeVar
from abc import abstractmethod
from urllib.parse import urlparse
from uuid import UUID
import operator
import string
import threading
from datetime import datetime

alphanums = " -" + string.digits + string.ascii_uppercase + "_" + string.ascii_lowercase


def safezip(*args):
    "zip but makes sure all sequences are the same length"
    lens = list(map(len, args))
    if len(set(lens)) != 1:
        raise ValueError(f"Mismatching lengths in arguments to safezip: {lens}")
    return zip(*args)


def split_space(start, end, count):
    size = end - start
    assert count <= size, (count, size)
    return list(range(start, end, (size + 1) // (count + 1)))[1 : count + 1]


class ArithString:
    @classmethod
    def new(cls, *args, **kw):
        return cls(*args, **kw)

    def range(self, other: "ArithString", count: int):
        assert isinstance(other, ArithString)
        checkpoints = split_space(self.int, other.int, count)
        return [self.new(int=i) for i in checkpoints]


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


def numberToAlphanum(num: int, base: str = alphanums) -> str:
    digits = []
    while num > 0:
        num, remainder = divmod(num, len(base))
        digits.append(remainder)
    return "".join(base[i] for i in digits[::-1])


def alphanumToNumber(alphanum: str, base: str) -> int:
    num = 0
    for c in alphanum:
        num = num * len(base) + base.index(c)
    return num


def justify_alphanums(s1: str, s2: str):
    max_len = max(len(s1), len(s2))
    s1 = s1.ljust(max_len)
    s2 = s2.ljust(max_len)
    return s1, s2


def alphanums_to_numbers(s1: str, s2: str):
    s1, s2 = justify_alphanums(s1, s2)
    n1 = alphanumToNumber(s1, alphanums)
    n2 = alphanumToNumber(s2, alphanums)
    return n1, n2


class ArithAlphanumeric(ArithString):
    def __init__(self, s: str, max_len=None):
        if s is None:
            raise ValueError("Alphanum string cannot be None")
        if max_len and len(s) > max_len:
            raise ValueError(f"Length of alphanum value '{str}' is longer than the expected {max_len}")

        for ch in s:
            if ch not in alphanums:
                raise ValueError(f"Unexpected character {ch} in alphanum string")

        self._str = s
        self._max_len = max_len

    # @property
    # def int(self):
    #     return alphanumToNumber(self._str, alphanums)

    def __str__(self):
        s = self._str
        if self._max_len:
            s = s.rjust(self._max_len, alphanums[0])
        return s

    def __len__(self):
        return len(self._str)

    def __repr__(self):
        return f'alphanum"{self._str}"'

    def __add__(self, other: "Union[ArithAlphanumeric, int]") -> "ArithAlphanumeric":
        if isinstance(other, int):
            if other != 1:
                raise NotImplementedError("not implemented for arbitrary numbers")
            lastchar = self._str[-1] if self._str else alphanums[0]
            s = self._str[:-1] + alphanums[alphanums.index(lastchar) + other]
            return self.new(s)
        return NotImplemented

    def range(self, other: "ArithAlphanumeric", count: int):
        assert isinstance(other, ArithAlphanumeric)
        n1, n2 = alphanums_to_numbers(self._str, other._str)
        split = split_space(n1, n2, count)
        return [self.new(numberToAlphanum(s)) for s in split]

    def __sub__(self, other: "Union[ArithAlphanumeric, int]") -> float:
        if isinstance(other, ArithAlphanumeric):
            n1, n2 = alphanums_to_numbers(self._str, other._str)
            return n1 - n2

        return NotImplemented

    def __ge__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._str >= other._str

    def __lt__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._str < other._str

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
    try:
        yield next(it)
    except StopIteration:
        return
    for i in it:
        yield joiner
        yield i


V = TypeVar("V")


class CaseAwareMapping(MutableMapping[str, V]):
    @abstractmethod
    def get_key(self, key: str) -> str:
        ...


class CaseInsensitiveDict(CaseAwareMapping):
    def __init__(self, initial):
        self._dict = {k.lower(): (k, v) for k, v in dict(initial).items()}

    def __getitem__(self, key: str) -> V:
        return self._dict[key.lower()][1]

    def __iter__(self) -> Iterator[V]:
        return iter(self._dict)

    def __len__(self) -> int:
        return len(self._dict)

    def __setitem__(self, key: str, value):
        k = key.lower()
        if k in self._dict:
            key = self._dict[k][0]
        self._dict[k] = key, value

    def __delitem__(self, key: str):
        del self._dict[key.lower()]

    def get_key(self, key: str) -> str:
        return self._dict[key.lower()][0]

    def __repr__(self) -> str:
        return repr(dict(self.items()))


class CaseSensitiveDict(dict, CaseAwareMapping):
    def get_key(self, key):
        self[key]  # Throw KeyError is key doesn't exist
        return key

    def as_insensitive(self):
        return CaseInsensitiveDict(self)


def match_like(pattern: str, strs: Sequence[str]) -> Iterable[str]:
    reo = re.compile(pattern.replace("%", ".*").replace("?", ".") + "$")
    for s in strs:
        if reo.match(s):
            yield s


def accumulate(iterable, func=operator.add, *, initial=None):
    "Return running totals"
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


def match_regexps(regexps: Dict[str, Any], s: str) -> Sequence[tuple]:
    for regexp, v in regexps.items():
        m = re.match(regexp + "$", s)
        if m:
            yield m, v


def run_as_daemon(threadfunc, *args):
    th = threading.Thread(target=threadfunc, args=args)
    th.daemon = True
    th.start()
    return th


def getLogger(name):
    return logging.getLogger(name.rsplit(".", 1)[-1])


def eval_name_template(name):
    def get_timestamp(_match):
        return datetime.now().isoformat("_", "seconds").replace(":", "_")

    return re.sub("%t", get_timestamp, name)
