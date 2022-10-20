import random
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Sequence, List

from runtype import dataclass

from data_diff.utils import ArithString
from data_diff.databases.database_types import AbstractDialect, DbPath


@dataclass
class Compiler:
    database: AbstractDialect
    in_select: bool = False  # Compilation runtime flag
    in_join: bool = False  # Compilation runtime flag

    _table_context: List = []  # List[ITable]
    _subqueries: Dict[str, Any] = {}  # XXX not thread-safe
    root: bool = True

    _counter: List = [0]

    def compile(self, elem) -> str:
        res = self._compile(elem)
        if self.root and self._subqueries:
            subq = ", ".join(f"\n  {k} AS ({v})" for k, v in self._subqueries.items())
            self._subqueries.clear()
            return f"WITH {subq}\n{res}"
        return res

    def _compile(self, elem) -> str:
        if elem is None:
            return "NULL"
        elif isinstance(elem, Compilable):
            return elem.compile(self.replace(root=False))
        elif isinstance(elem, str):
            return elem
        elif isinstance(elem, int):
            return str(elem)
        elif isinstance(elem, datetime):
            return self.database.timestamp_value(elem)
        elif isinstance(elem, bytes):
            return f"b'{elem.decode()}'"
        elif isinstance(elem, ArithString):
            return f"'{elem}'"
        assert False, elem

    def new_unique_name(self, prefix="tmp"):
        self._counter[0] += 1
        return f"{prefix}{self._counter[0]}"

    def new_unique_table_name(self, prefix="tmp") -> DbPath:
        self._counter[0] += 1
        return self.database.parse_table_name(f"{prefix}{self._counter[0]}_{'%x'%random.randrange(2**32)}")

    def add_table_context(self, *tables: Sequence, **kw):
        return self.replace(_table_context=self._table_context + list(tables), **kw)

    def quote(self, s: str):
        return self.database.quote(s)


class Compilable(ABC):
    @abstractmethod
    def compile(self, c: Compiler) -> str:
        ...
