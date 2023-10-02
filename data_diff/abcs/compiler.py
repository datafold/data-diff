from abc import ABC

import attrs


@attrs.define(frozen=False)
class AbstractCompiler(ABC):
    pass


@attrs.define(frozen=False, eq=False)
class Compilable(ABC):
    pass
