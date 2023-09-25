import random
from dataclasses import field
from typing import Any, Dict, Sequence, List

from runtype import dataclass
from typing_extensions import Self

from data_diff.abcs.database_types import AbstractDatabase, AbstractDialect, DbPath
from data_diff.abcs.compiler import AbstractCompiler


class CompileError(Exception):
    pass


@dataclass
class Compiler(AbstractCompiler):
    """
    Compiler bears the context for a single compilation.

    There can be multiple compilation per app run.
    There can be multiple compilers in one compilation (with varying contexts).
    """

    # Database is needed to normalize tables. Dialect is needed for recursive compilations.
    # In theory, it is many-to-many relations: e.g. a generic ODBC driver with multiple dialects.
    # In practice, we currently bind the dialects to the specific database classes.
    database: AbstractDatabase

    in_select: bool = False  # Compilation runtime flag
    in_join: bool = False  # Compilation runtime flag

    _table_context: List = field(default_factory=list)  # List[ITable]
    _subqueries: Dict[str, Any] = field(default_factory=dict)  # XXX not thread-safe
    root: bool = True

    _counter: List = field(default_factory=lambda: [0])

    @property
    def dialect(self) -> AbstractDialect:
        return self.database.dialect

    # TODO: DEPRECATED: Remove once the dialect is used directly in all places.
    def compile(self, elem, params=None) -> str:
        return self.dialect.compile(self, elem, params)

    def new_unique_name(self, prefix="tmp"):
        self._counter[0] += 1
        return f"{prefix}{self._counter[0]}"

    def new_unique_table_name(self, prefix="tmp") -> DbPath:
        self._counter[0] += 1
        table_name = f"{prefix}{self._counter[0]}_{'%x'%random.randrange(2**32)}"
        return self.database.dialect.parse_table_name(table_name)

    def add_table_context(self, *tables: Sequence, **kw) -> Self:
        return self.replace(_table_context=self._table_context + list(tables), **kw)
