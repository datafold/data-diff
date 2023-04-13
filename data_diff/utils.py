import logging
import re
from typing import Dict, Iterable, Sequence
from urllib.parse import urlparse
import operator
import threading
from datetime import datetime
from tabulate import tabulate


def safezip(*args):
    "zip but makes sure all sequences are the same length"
    lens = list(map(len, args))
    if len(set(lens)) != 1:
        raise ValueError(f"Mismatching lengths in arguments to safezip: {lens}")
    return zip(*args)


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


def truncate_error(error: str):
    first_line = error.split("\n", 1)[0]
    return re.sub("'(.*?)'", "'***'", first_line)


def get_from_dict_with_raise(dictionary: Dict, key: str, error_message: str):
    result = dictionary.get(key)
    if result is None:
        raise ValueError(error_message)
    return result


class Vector(tuple):

    """Immutable implementation of a regular vector over any arithmetic value

    Implements a product order - https://en.wikipedia.org/wiki/Product_order

    Partial implementation: Only the needed functionality is implemented
    """

    def __lt__(self, other: "Vector"):
        if isinstance(other, Vector):
            return all(a < b for a, b in safezip(self, other))
        return NotImplemented

    def __le__(self, other: "Vector"):
        if isinstance(other, Vector):
            return all(a <= b for a, b in safezip(self, other))
        return NotImplemented

    def __gt__(self, other: "Vector"):
        if isinstance(other, Vector):
            return all(a > b for a, b in safezip(self, other))
        return NotImplemented

    def __ge__(self, other: "Vector"):
        if isinstance(other, Vector):
            return all(a >= b for a, b in safezip(self, other))
        return NotImplemented

    def __eq__(self, other: "Vector"):
        if isinstance(other, Vector):
            return all(a == b for a, b in safezip(self, other))
        return NotImplemented

    def __sub__(self, other: "Vector"):
        if isinstance(other, Vector):
            return Vector((a - b) for a, b in safezip(self, other))
        raise NotImplementedError()

    def __repr__(self) -> str:
        return "(%s)" % ", ".join(str(k) for k in self)


def dbt_diff_string_template(
    rows_added: str, rows_removed: str, rows_updated: str, rows_unchanged: str, extra_info_dict: Dict, extra_info_str
) -> str:
    string_output = f"\n{tabulate([[rows_added, rows_removed]], headers=['Rows Added', 'Rows Removed'])}"

    string_output += f"\n\nUpdated Rows: {rows_updated}\n"
    string_output += f"Unchanged Rows: {rows_unchanged}\n\n"

    string_output += extra_info_str

    for k, v in extra_info_dict.items():
        string_output += f"\n{k}: {v}"

    return string_output
